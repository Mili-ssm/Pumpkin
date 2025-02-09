use std::io::{Cursor, Read, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::chunk::anvil::AnvilChunkFile;
use crate::chunks_io::{ChunkSerializer, LoadedData};
use bytes::{Buf, BufMut};
use log::error;
use pumpkin_config::ADVANCED_CONFIG;
use pumpkin_util::math::vector2::Vector2;

use super::anvil::{AnvilChunkFormat, CHUNK_COUNT, SUBREGION_BITS};
use super::{ChunkData, ChunkReadingError, ChunkSerializingError};

/// The signature of the linear file format
/// used as a header and footer described in https://gist.github.com/Aaron2550/5701519671253d4c6190bde6706f9f98
const SIGNATURE: [u8; 8] = u64::to_be_bytes(0xc3ff13183cca9d9a);

#[derive(Default, Clone, Copy)]
struct LinearChunkHeader {
    size: u32,
    timestamp: u32,
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub enum LinearVersion {
    #[default]
    /// Represents an invalid or uninitialized version.
    None = 0x00,
    /// Version 1 of the Linear Region File Format. (Default)
    ///
    /// Described in: https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools/blob/linearv2/LINEAR.md
    V1 = 0x01,
    /// Version 2 of the Linear Region File Format (currently unsupported).
    ///
    /// Described in: https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools/blob/linearv2/LINEARv2.md
    V2 = 0x02,
}
struct LinearFileHeader {
    /// ( 0.. 1 Bytes) The version of the Linear Region File format.
    version: LinearVersion,
    /// ( 1.. 9 Bytes) The timestamp of the newest chunk in the region file.
    newest_timestamp: u64,
    /// ( 9..10 Bytes) The zstd compression level used for chunk data.
    compression_level: u8,
    /// (10..12 Bytes) The number of non-zero-size chunks in the region file.
    chunks_count: u16,
    /// (12..16 Bytes) The total size in bytes of the compressed chunk headers and chunk data.
    chunks_bytes: u32,
    /// (16..24 Bytes) A hash of the region file (unused).
    region_hash: u64,
}
pub struct LinearFile {
    chunks_headers: Box<[LinearChunkHeader; CHUNK_COUNT]>,
    chunks_data: Vec<u8>,
}

impl LinearChunkHeader {
    const CHUNK_HEADER_SIZE: usize = 8;
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut bytes = bytes;
        LinearChunkHeader {
            size: bytes.get_u32(),
            timestamp: bytes.get_u32(),
        }
    }

    fn to_bytes(self) -> [u8; 8] {
        let mut bytes = Vec::with_capacity(LinearChunkHeader::CHUNK_HEADER_SIZE);

        bytes.put_u32(self.size);
        bytes.put_u32(self.timestamp);

        // This should be a clear code error if the size of the header is not the expected
        // so we can unwrap the conversion safely or panic the entire program if not
        bytes
            .try_into()
            .unwrap_or_else(|_| panic!("ChunkHeader Struct/Size Mismatch"))
    }
}

impl From<u8> for LinearVersion {
    fn from(value: u8) -> Self {
        match value {
            0x01 => LinearVersion::V1,
            0x02 => LinearVersion::V2,
            _ => LinearVersion::None,
        }
    }
}

impl LinearFileHeader {
    const FILE_HEADER_SIZE: usize = 24;

    fn check_version(&self) -> Result<(), ChunkReadingError> {
        match self.version {
            LinearVersion::None => {
                error!("Invalid version in the file header");
                Err(ChunkReadingError::InvalidHeader)
            }
            LinearVersion::V2 => {
                error!("LinearFormat Version 2 for Chunks is not supported yet");
                Err(ChunkReadingError::InvalidHeader)
            }
            _ => Ok(()),
        }
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut buf = bytes;

        LinearFileHeader {
            version: buf.get_u8().into(),
            newest_timestamp: buf.get_u64(),
            compression_level: buf.get_u8(),
            chunks_count: buf.get_u16(),
            chunks_bytes: buf.get_u32(),
            region_hash: buf.get_u64(),
        }
    }

    fn to_bytes(&self) -> [u8; Self::FILE_HEADER_SIZE] {
        let mut bytes: Vec<u8> = Vec::with_capacity(LinearFileHeader::FILE_HEADER_SIZE);

        bytes.put_u8(self.version as u8);
        bytes.put_u64(self.newest_timestamp);
        bytes.put_u8(self.compression_level);
        bytes.put_u16(self.chunks_count);
        bytes.put_u32(self.chunks_bytes);
        bytes.put_u64(self.region_hash);

        // This should be a clear code error if the size of the header is not the expected
        // so we can unwrap the conversion safely or panic the entire program if not
        bytes
            .try_into()
            .unwrap_or_else(|_| panic!("Header Struct/Size Mismatch"))
    }
}

impl LinearFile {
    const fn get_chunk_index(at: Vector2<i32>) -> usize {
        // we need only the 5 last bits of the x and z coordinates
        let decode_x = at.x - ((at.x >> SUBREGION_BITS) << SUBREGION_BITS);
        let decode_z = at.z - ((at.z >> SUBREGION_BITS) << SUBREGION_BITS);

        // we calculate the index of the chunk in the region file
        ((decode_z << SUBREGION_BITS) + decode_x) as usize
    }
    fn check_signature(bytes: &[u8]) -> Result<(), ChunkReadingError> {
        if bytes[0..8] != SIGNATURE {
            error!("Signature at the start of the file is invalid");
            return Err(ChunkReadingError::InvalidHeader);
        }

        if bytes[bytes.len() - 8..] != SIGNATURE {
            error!("Signature at the end of the file is invalid");
            return Err(ChunkReadingError::InvalidHeader);
        }

        Ok(())
    }
}

impl Default for LinearFile {
    fn default() -> Self {
        LinearFile {
            chunks_headers: Box::new([LinearChunkHeader::default(); CHUNK_COUNT]),
            chunks_data: vec![],
        }
    }
}

impl ChunkSerializer for LinearFile {
    type Data = ChunkData;

    fn get_chunk_key(chunk: Vector2<i32>) -> String {
        let (region_x, region_z) = AnvilChunkFile::get_region_coords(chunk);
        format!("./r.{}.{}.linear", region_x, region_z)
    }

    fn to_bytes(&self) -> Vec<u8> {
        // Parse the headers to a buffer
        let headers_buffer: Vec<u8> = self
            .chunks_headers
            .iter()
            .flat_map(|header| header.to_bytes())
            .collect();

        // Compress the data buffer
        let compressed_buffer = zstd::encode_all(
            [headers_buffer.as_slice(), self.chunks_data.as_slice()]
                .concat()
                .as_slice(),
            ADVANCED_CONFIG.chunk.compression.level as i32,
        )
        .unwrap();

        let file_header = LinearFileHeader {
            chunks_bytes: compressed_buffer.len() as u32,
            compression_level: ADVANCED_CONFIG.chunk.compression.level as u8,
            chunks_count: self
                .chunks_headers
                .iter()
                .filter(|&header| header.size != 0)
                .count() as u16,
            newest_timestamp: self
                .chunks_headers
                .iter()
                .map(|header| header.timestamp)
                .max()
                .unwrap_or(0) as u64,
            version: LinearVersion::V1,
            region_hash: 0,
        }
        .to_bytes();

        [
            SIGNATURE.as_slice(),
            file_header.as_slice(),
            compressed_buffer.as_slice(),
            SIGNATURE.as_slice(),
        ]
        .concat()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, ChunkReadingError> {
        Self::check_signature(bytes)?;

        let mut file = Cursor::new(bytes);

        // Skip the signature and read the header
        let mut header_bytes = [0; LinearFileHeader::FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(8))
            .map_err(|err| ChunkReadingError::IoError(err.kind()))?;
        file.read_exact(&mut header_bytes)
            .map_err(|err| ChunkReadingError::IoError(err.kind()))?;

        // Parse the header
        let file_header = LinearFileHeader::from_bytes(&header_bytes);
        file_header.check_version()?;

        // Read the compressed data
        let mut compressed_data = vec![0; file_header.chunks_bytes as usize];
        file.read_exact(compressed_data.as_mut_slice())
            .map_err(|err| ChunkReadingError::IoError(err.kind()))?;

        if compressed_data.len() != file_header.chunks_bytes as usize {
            error!(
                "Invalid compressed data size {} != {}",
                compressed_data.len(),
                file_header.chunks_bytes
            );
            return Err(ChunkReadingError::InvalidHeader);
        }

        // Uncompress the data (header + chunks)
        let buffer = zstd::decode_all(compressed_data.as_slice())
            .map_err(|err| ChunkReadingError::IoError(err.kind()))?;

        let (headers_buffer, chunks_buffer) =
            buffer.split_at(LinearChunkHeader::CHUNK_HEADER_SIZE * CHUNK_COUNT);

        // Parse the chunk headers
        let chunk_headers: [LinearChunkHeader; CHUNK_COUNT] = headers_buffer
            .chunks_exact(8)
            .map(LinearChunkHeader::from_bytes)
            .collect::<Vec<LinearChunkHeader>>()
            .try_into()
            .map_err(|_| ChunkReadingError::InvalidHeader)?;

        // Check if the total bytes of the chunks match the header
        let total_bytes = chunk_headers.iter().map(|header| header.size).sum::<u32>() as usize;
        if chunks_buffer.len() != total_bytes {
            error!(
                "Invalid total bytes of the chunks {} != {}",
                total_bytes,
                chunks_buffer.len(),
            );
            return Err(ChunkReadingError::InvalidHeader);
        }

        Ok(LinearFile {
            chunks_headers: Box::new(chunk_headers),
            chunks_data: chunks_buffer.to_vec(),
        })
    }

    fn add_chunk_data(&mut self, chunk_data: &Self::Data) -> Result<(), ChunkSerializingError> {
        let chunk_index: usize = LinearFile::get_chunk_index(chunk_data.position);
        let chunk_raw = AnvilChunkFormat {} //We use Anvil format to serialize the chunk
            .to_bytes(chunk_data)?;

        let new_chunk_size = chunk_raw.len();
        let old_chunk_size = self.chunks_headers[chunk_index].size as usize;

        self.chunks_headers[chunk_index] = LinearChunkHeader {
            size: new_chunk_size as u32,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        };

        // We calculate the start point of the chunk in the data buffer
        let mut offset: usize = 0;
        for i in 0..chunk_index {
            offset += self.chunks_headers[i].size as usize;
        }

        let old_total_size = self.chunks_data.len();
        let new_total_size = (old_total_size + new_chunk_size) - old_chunk_size;

        // We update the data buffer (avoiding reallocations)
        if new_chunk_size > old_chunk_size {
            self.chunks_data.resize(new_total_size, 0);
        }

        self.chunks_data.copy_within(
            offset + old_chunk_size..old_total_size,
            offset + new_chunk_size,
        );

        self.chunks_data[offset..offset + new_chunk_size].copy_from_slice(&chunk_raw);

        if new_chunk_size < old_chunk_size {
            self.chunks_data.truncate(new_total_size);
        }

        Ok(())
    }

    fn get_chunk_data(
        &self,
        chunk: Vector2<i32>,
    ) -> Result<LoadedData<Self::Data>, ChunkReadingError> {
        // We check if the chunk exists
        let chunk_index: usize = LinearFile::get_chunk_index(chunk);

        let chunk_size = self.chunks_headers[chunk_index].size as usize;
        if chunk_size == 0 {
            return Ok(LoadedData::MissingData(chunk));
        }

        // We iterate over the headers to sum the size of the chunks until the desired one
        let mut offset: usize = 0;
        for i in 0..chunk_index {
            offset += self.chunks_headers[i].size as usize;
        }

        Ok(LoadedData::LoadedData(
            ChunkData::from_bytes(&self.chunks_data[offset..offset + chunk_size], chunk)
                .map_err(ChunkReadingError::ParsingError)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_util::math::vector2::Vector2;
    use std::fs;
    use std::path::PathBuf;

    use crate::chunk::linear::LinearFile;
    use crate::chunks_io::{ChunkFileManager, ChunkIO, LoadedData};
    use crate::generation::{get_world_gen, Seed};
    use crate::level::LevelFolder;

    #[test]
    fn not_existing() {
        let region_path = PathBuf::from("not_existing");
        let chunk_saver = ChunkFileManager::<LinearFile>::default();
        let result = chunk_saver.load_chunks(
            &LevelFolder {
                root_folder: PathBuf::from(""),
                region_folder: region_path,
            },
            &[Vector2::new(0, 0)],
        );
        assert!(
            matches!(result, Ok(chunks) if chunks.len() == 1 && matches!(chunks[0], LoadedData::MissingData(_)))
        );
    }

    #[test]
    fn test_writing() {
        let generator = get_world_gen(Seed(0));
        let level_folder = LevelFolder {
            root_folder: PathBuf::from("./tmp_Linear"),
            region_folder: PathBuf::from("./tmp_Linear/region"),
        };
        if fs::exists(&level_folder.root_folder).unwrap() {
            fs::remove_dir_all(&level_folder.root_folder).expect("Could not delete directory");
        }

        fs::create_dir_all(&level_folder.region_folder).expect("Could not create directory");
        let chunk_saver = ChunkFileManager::<LinearFile>::default();

        // Generate chunks
        let mut chunks = vec![];
        for x in -5..5 {
            for y in -5..5 {
                let position = Vector2::new(x, y);
                chunks.push((position, generator.generate_chunk(position)));
            }
        }

        for i in 0..5 {
            println!("Iteration {}", i + 1);
            chunk_saver
                .save_chunks(
                    &level_folder,
                    &chunks
                        .iter()
                        .map(|(at, chunk)| (*at, chunk))
                        .collect::<Vec<_>>(),
                )
                .expect("Failed to write chunk");

            let read_chunks = chunk_saver
                .load_chunks(
                    &level_folder,
                    &chunks.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
                )
                .expect("Could not read chunk")
                .into_iter()
                .filter_map(|chunk| match chunk {
                    LoadedData::LoadedData(chunk) => Some(chunk),
                    LoadedData::MissingData(_) => None,
                })
                .collect::<Vec<_>>();

            for (at, chunk) in &chunks {
                let read_chunk = read_chunks
                    .iter()
                    .find(|chunk| chunk.position == *at)
                    .expect("Missing chunk");
                assert_eq!(chunk.subchunks, read_chunk.subchunks, "Chunks don't match");
            }
        }

        fs::remove_dir_all(&level_folder.root_folder).expect("Could not delete directory");

        println!("Checked chunks successfully");
    }
}

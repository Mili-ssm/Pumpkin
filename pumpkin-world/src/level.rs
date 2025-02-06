use std::{
    ops::Deref,
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use dashmap::{DashMap, Entry};
use log::info;
use num_traits::Zero;
use pumpkin_config::{chunk::ChunkFormat, ADVANCED_CONFIG};
use pumpkin_util::math::vector2::Vector2;
use rayon::iter::{
    IntoParallelIterator, IntoParallelRefIterator, ParallelBridge, ParallelIterator,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, Mutex, RwLock},
};

use crate::{
    chunk::{
        anvil::AnvilChunkFormat, linear::LinearChunkFormat, ChunkData, ChunkParsingError,
        ChunkReader, ChunkReadingError, ChunkWriter,
    },
    generation::{get_world_gen, Seed, WorldGenerator},
    lock::{anvil::AnvilLevelLocker, LevelLocker},
    world_info::{anvil::AnvilLevelInfo, LevelData, WorldInfoReader, WorldInfoWriter},
};

/// The `Level` module provides functionality for working with chunks within or outside a Minecraft world.
///
/// Key features include:
///
/// - **Chunk Loading:** Efficiently loads chunks from disk.
/// - **Chunk Caching:** Stores accessed chunks in memory for faster access.
/// - **Chunk Generation:** Generates new chunks on-demand using a specified `WorldGenerator`.
///
/// For more details on world generation, refer to the `WorldGenerator` module.
pub struct Level {
    pub seed: Seed,
    pub level_info: LevelData,
    world_info_writer: Arc<dyn WorldInfoWriter>,
    level_folder: LevelFolder,
    loaded_chunks: Arc<DashMap<Vector2<i32>, Arc<RwLock<ChunkData>>>>,
    chunk_watchers: Arc<DashMap<Vector2<i32>, usize>>,
    chunk_reader: Arc<dyn ChunkReader>,
    chunk_writer: Arc<dyn ChunkWriter>,
    world_gen: Arc<dyn WorldGenerator>,
    // Gets unlocked when dropped
    // TODO: Make this a trait
    _locker: Arc<AnvilLevelLocker>,
}

#[derive(Clone)]
pub struct LevelFolder {
    pub root_folder: PathBuf,
    pub region_folder: PathBuf,
}

impl Level {
    pub fn from_root_folder(root_folder: PathBuf) -> Self {
        // If we are using an already existing world we want to read the seed from the level.dat, If not we want to check if there is a seed in the config, if not lets create a random one
        let region_folder = root_folder.join("region");
        if !region_folder.exists() {
            std::fs::create_dir_all(&region_folder).expect("Failed to create Region folder");
        }
        let level_folder = LevelFolder {
            root_folder,
            region_folder,
        };

        // if we fail to lock, lets crash ???. maybe not the best solution when we have a large server with many worlds and one is locked.
        // So TODO
        let locker = AnvilLevelLocker::look(&level_folder).expect("Failed to lock level");

        // TODO: Load info correctly based on world format type
        let level_info = AnvilLevelInfo
            .read_world_info(&level_folder)
            .unwrap_or_default(); // TODO: Improve error handling
        let seed = Seed(level_info.world_gen_settings.seed as u64);
        let world_gen = get_world_gen(seed).into();

        let chunk_format: (Arc<dyn ChunkReader>, Arc<dyn ChunkWriter>) =
            match ADVANCED_CONFIG.chunk.format {
                ChunkFormat::Anvil => (Arc::new(AnvilChunkFormat), Arc::new(AnvilChunkFormat)),
                ChunkFormat::Linear => (Arc::new(LinearChunkFormat), Arc::new(LinearChunkFormat)),
            };

        Self {
            seed,
            world_gen,
            world_info_writer: Arc::new(AnvilLevelInfo),
            level_folder,
            chunk_reader: chunk_format.0,
            chunk_writer: chunk_format.1,
            loaded_chunks: Arc::new(DashMap::new()),
            chunk_watchers: Arc::new(DashMap::new()),
            level_info,
            _locker: Arc::new(locker),
        }
    }

    pub async fn save(&self) {
        log::info!("Saving level...");
        // chunks are automatically saved when all players get removed
        // lets first save all chunks

        // then lets save the world info
        self.world_info_writer
            .write_world_info(self.level_info.clone(), &self.level_folder)
            .expect("Failed to save world info");
    }

    pub fn get_block() {}

    pub fn loaded_chunk_count(&self) -> usize {
        self.loaded_chunks.len()
    }

    pub fn list_cached(&self) {
        for entry in self.loaded_chunks.iter() {
            log::debug!("In map: {:?}", entry.key());
        }
    }

    /// Marks chunks as "watched" by a unique player. When no players are watching a chunk,
    /// it is removed from memory. Should only be called on chunks the player was not watching
    /// before
    pub fn mark_chunks_as_newly_watched(&self, chunks: &[Vector2<i32>]) {
        chunks.iter().for_each(|chunk| {
            self.mark_chunk_as_newly_watched(*chunk);
        });
    }

    pub fn mark_chunk_as_newly_watched(&self, chunk: Vector2<i32>) {
        match self.chunk_watchers.entry(chunk) {
            Entry::Occupied(mut occupied) => {
                let value = occupied.get_mut();
                if let Some(new_value) = value.checked_add(1) {
                    *value = new_value;
                    //log::debug!("Watch value for {:?}: {}", chunk, value);
                } else {
                    log::error!("Watching overflow on chunk {:?}", chunk);
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(1);
            }
        }
    }

    /// Marks chunks no longer "watched" by a unique player. When no players are watching a chunk,
    /// it is removed from memory. Should only be called on chunks the player was watching before
    pub fn mark_chunks_as_not_watched(&self, chunks: &[Vector2<i32>]) -> Vec<Vector2<i32>> {
        chunks
            .iter()
            .filter(|chunk| self.mark_chunk_as_not_watched(**chunk))
            .copied()
            .collect()
    }

    /// Returns whether the chunk should be removed from memory
    pub fn mark_chunk_as_not_watched(&self, chunk: Vector2<i32>) -> bool {
        match self.chunk_watchers.entry(chunk) {
            Entry::Occupied(mut occupied) => {
                let value = occupied.get_mut();
                *value = value.saturating_sub(1);

                if *value == 0 {
                    occupied.remove_entry();
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => {
                // This can be:
                // - Player disconnecting before all packets have been sent
                // - Player moving so fast that the chunk leaves the render distance before it
                // is loaded into memory
                true
            }
        }
    }

    pub async fn clean_chunks(&self, chunks: &[Vector2<i32>]) {
        info!("flushing chunks");

        let chunks_to_clean = chunks
            .par_iter()
            .filter(|chunk| self.chunk_watchers.get(chunk).is_none())
            .filter_map(|chunk| self.loaded_chunks.remove(chunk))
            .collect::<Vec<_>>();
        let writer = self.chunk_writer.clone();
        let folder = self.level_folder.clone();
        tokio::spawn(async move {
            let chunks_to_clean = chunks_to_clean;

            Self::write_chunks(writer, folder, &chunks_to_clean).await;
        });
    }

    pub async fn clean_chunk(&self, chunk: &Vector2<i32>) {
        self.clean_chunks(&[*chunk]).await;
    }

    pub fn is_chunk_watched(&self, chunk: &Vector2<i32>) -> bool {
        self.chunk_watchers.get(chunk).is_some()
    }

    pub fn clean_memory(&self, chunks_to_check: &[Vector2<i32>]) {
        chunks_to_check.iter().for_each(|chunk| {
            if let Some(entry) = self.chunk_watchers.get(chunk) {
                if entry.value().is_zero() {
                    self.chunk_watchers.remove(chunk);
                }
            }

            if self.chunk_watchers.get(chunk).is_none() {
                self.loaded_chunks.remove(chunk);
            }
        });
        self.loaded_chunks.shrink_to_fit();
        self.chunk_watchers.shrink_to_fit();
    }

    pub async fn write_chunks(
        writer: Arc<dyn ChunkWriter>,
        level_folder: LevelFolder,
        chunks_to_write: &[(Vector2<i32>, Arc<RwLock<ChunkData>>)],
    ) {
        if chunks_to_write.is_empty() {
            return;
        }

        let chunks = chunks_to_write
            .iter()
            .map(|(pos, chunk)| (*pos, chunk.clone()))
            .collect::<Vec<_>>();
        tokio::spawn(async move {
            let chunks = chunks
                .par_iter()
                .map(|(pos, chunk)| (*pos, chunk.read()))
                .collect::<Vec<_>>();

            let mut chunk_guards = Vec::with_capacity(chunks.len());

            for (pos, guard) in chunks {
                chunk_guards.push((pos, guard.await));
            }

            let chunks = chunk_guards
                .par_iter()
                .map(|(pos, guard)| (*pos, guard.deref()))
                .collect::<Vec<_>>();

            info!("Writing chunks to disk {:}", chunks.len());

            if let Err(error) = writer.write_chunks(&level_folder, &chunks) {
                log::error!("Failed writing Chunk to disk {}", error.to_string());
            }
        });
    }

    fn load_chunks_from_save(
        chunk_reader: Arc<dyn ChunkReader>,
        save_file: &LevelFolder,
        chunks_pos: &[Vector2<i32>],
    ) -> Result<Vec<(Vector2<i32>, Option<Arc<RwLock<ChunkData>>>)>, ChunkReadingError> {
        if chunks_pos.is_empty() {
            return Ok(vec![]);
        }
        info!("Loading chunks from disk {:}", chunks_pos.len());
        Ok(chunk_reader
            .read_chunks(save_file, chunks_pos)?
            .into_par_iter()
            .map(|(pos, chunk)| match chunk {
                Some(chunk) => (pos, Some(Arc::new(RwLock::new(chunk)))),
                None => (pos, None),
            })
            .collect())
    }

    /// Reads/Generates many chunks in a world
    /// Note: The order of the output chunks will almost never be in the same order as the order of input chunks
    pub fn fetch_chunks(
        &self,
        chunks: &[Vector2<i32>],
        channel: mpsc::Sender<(Arc<RwLock<ChunkData>>, bool)>,
        rt: &Handle,
    ) {
        if chunks.is_empty() {
            return;
        }

        let chunks_to_load = chunks
            .par_iter()
            .filter_map(|pos| {
                if let Some(loaded_chunk) = self.loaded_chunks.get(pos) {
                    let chunk = loaded_chunk.value().clone();
                    let channel = channel.clone();
                    rt.spawn(async move {
                        let _ = channel.send((chunk, false)).await.inspect_err(|err| {
                            log::error!("unable to send chunk to channel: {}", err)
                        });
                    });
                    None
                } else {
                    Some(*pos)
                }
            })
            .collect::<Vec<_>>();

        if chunks_to_load.is_empty() {
            return;
        }

        Self::load_chunks_from_save(
            self.chunk_reader.clone(),
            &self.level_folder,
            &chunks_to_load,
        )
        .unwrap()
        .into_par_iter()
        .for_each(|(pos, chunk)| {
            let mut first_load = false;
            let chunk = self
                .loaded_chunks
                .entry(pos)
                .or_insert_with(|| match chunk {
                    Some(chunk) => chunk,
                    None => {
                        first_load = true;
                        Arc::new(RwLock::new(self.world_gen.generate_chunk(pos)))
                    }
                })
                .clone();

            let channel = channel.clone();
            rt.spawn(async move {
                let _ = channel
                    .send((chunk, first_load))
                    .await
                    .inspect_err(|err| log::error!("unable to send chunk to channel: {}", err));
            });
        });
    }
}

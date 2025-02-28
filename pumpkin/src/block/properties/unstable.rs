use async_trait::async_trait;
use pumpkin_macros::block_property;
use pumpkin_world::block::registry::Block;
use pumpkin_world::item::ItemStack;

use super::{BlockProperty, BlockPropertyMetadata};

#[block_property("unstable")]
pub struct Unstable(bool);

#[async_trait]
impl BlockProperty for Unstable {
    async fn on_interact(&self, value: String, _block: &Block, _item: &ItemStack) -> String {
        if value == Self::True().value() {
            Self::False().value()
        } else {
            Self::True().value()
        }
    }
}

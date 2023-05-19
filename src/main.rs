// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use bigasstable::BigAssTable;

use gtfs::{GtfsCollection, GtfsZipStore, Pushable, TableFacory};

use anyhow::Result;

mod gtfs;

mod csv;

mod bigasstable;

struct BigAssTableFactory {}

impl<I> Pushable<I> for BigAssTable<I> {
    fn push(&mut self, item: I) {
        BigAssTable::push(self, item);
    }

    fn length(&self) -> usize {
        BigAssTable::length(self)
    }
}

impl TableFacory for BigAssTableFactory {
    fn new<I: 'static>() -> Box<dyn gtfs::Pushable<I>> {
        Box::new(BigAssTable::<I>::new())
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let mut gtfs_store =
        GtfsZipStore::from_file("/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip");
    // let mut gtfs_store = GtfsZipStore::from_file("/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip");

    let _gtfs_collection = GtfsCollection::from_store::<_, BigAssTableFactory>(&mut gtfs_store);

    Ok(())
}

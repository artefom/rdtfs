// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use bigasstable::BigAssTable;

use gtfs::{GtfsStore, GtfsZipStore, Pushable, TableFacory};

use anyhow::{Context, Result};

use crate::{binarystore::PartitionedStoreWriter, gtfs::Stop};

mod gtfs;

mod csv;

mod bigasstable;
mod binarystore;

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

    let mut partitioned_stop_times = PartitionedStoreWriter::<Stop>::new(100)?;

    // Write into partitioned writer
    gtfs_store
        .scan(|item| {
            match partitioned_stop_times.write(&item, |x| &x.stop_code) {
                Ok(_) => (),
                Err(err) => println!("Error during write: {:?}", err),
            };
        })
        .context("Could not scan")?;

    let mut reader = partitioned_stop_times.into_reader()?;

    println!("Partition 1");
    for item in reader.read_partition(0)?.unwrap() {
        println!("{:?}", item)
    }

    // let _gtfs_collection = GtfsCollection::from_store::<_, BigAssTableFactory>(&mut gtfs_store);
    Ok(())
}

use std::{fs::File, io::Read};

use zip::ZipArchive;

pub trait FileCollection {
    fn open_by_predicate<'a, F>(&'a mut self, predicate: F) -> Option<(Box<dyn Read + 'a>, u64)>
    where
        F: Fn(String) -> bool;
}

impl FileCollection for ZipArchive<File> {
    fn open_by_predicate<'a, F>(&'a mut self, predicate: F) -> Option<(Box<dyn Read + 'a>, u64)>
    where
        F: Fn(String) -> bool,
    {
        let mut matched_ids: Vec<usize> = Vec::new();

        for file_idx in 0..self.len() {
            let zipped_file = self.by_index(file_idx).unwrap();
            if predicate(zipped_file.name().to_string()) {
                matched_ids.push(file_idx)
            }
        }

        let Some(found) = matched_ids.first() else {
            return None
        };

        let found = self.by_index(*found).unwrap();

        let total_size = found.size();

        Some((Box::new(found), total_size))
    }
}

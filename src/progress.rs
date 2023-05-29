use std::io::{BufRead, Read};

use indicatif::{ProgressBar, ProgressStyle};

/// Reads data and reports progress
pub struct ProgressReader<F> {
    file: F,
    bar: ProgressBar,
}

impl<F> ProgressReader<F> {
    pub fn new(file: F, total_size: u64) -> Self {
        let progress = ProgressBar::new(total_size);

        progress.set_style(
            ProgressStyle::with_template(
                "{bar:40.cyan/blue} {bytes:>7}/{total_bytes:7} {binary_bytes_per_sec} [ETA: {eta}] {msg}",
            )
            .unwrap()
            .progress_chars("##-"),
        );

        ProgressReader {
            file,
            bar: progress,
        }
    }
}

impl<F: Read> Read for ProgressReader<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl<F: BufRead> BufRead for ProgressReader<F> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.file.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        match TryInto::<u64>::try_into(amt) {
            Ok(value) => self.bar.inc(value),
            Err(_) => self.bar.inc(u64::MAX),
        };

        self.file.consume(amt);
    }
}

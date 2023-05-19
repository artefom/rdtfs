use std::marker::PhantomData;

pub struct BigAssTable<T> {
    count: usize,
    _phantom: PhantomData<T>,
}

impl<T> BigAssTable<T> {
    pub fn new() -> Self {
        BigAssTable {
            count: 0,
            _phantom: PhantomData,
        }
    }

    pub fn push(&mut self, _data: T) {
        self.count += 1
    }

    pub fn length(&self) -> usize {
        self.count
    }
}

use std::{marker::PhantomData, slice::Iter};

pub struct Table<Item> {
    data: Vec<Item>,
}

/// Stores items in a table
impl<Item> Table<Item> {
    pub fn new() -> Self {
        Table { data: Vec::new() }
    }

    /// Add item to store
    pub fn append(&mut self, item: Item) {
        self.data.push(item)
    }
}

/// Iterator implementation
pub struct TableIterator<'a, Item> {
    iter: Iter<'a, Item>,

    _phantom: PhantomData<Item>,
}

impl<'a, Item> Iterator for TableIterator<'a, Item> {
    type Item = &'a Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, Item> IntoIterator for &'a Table<Item> {
    type Item = &'a Item;
    type IntoIter = TableIterator<'a, Item>;

    fn into_iter(self) -> Self::IntoIter {
        let iter = self.data.iter();
        TableIterator {
            iter: iter,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn test_iteration() {
        let mut store: Table<i32> = Table::new();

        store.append(1);
        store.append(2);
        store.append(3);

        for item in &store {
            println!("{:?}", item)
        }
    }
}

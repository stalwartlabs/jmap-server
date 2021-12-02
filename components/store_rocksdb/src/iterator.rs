use roaring::RoaringBitmap;
use store::DocumentId;

pub struct RocksDBIterator {
    rb: RoaringBitmap,
}

impl RocksDBIterator {
    pub fn new(rb: RoaringBitmap) -> RocksDBIterator {
        RocksDBIterator { rb }
    }
}

impl IntoIterator for RocksDBIterator {
    type Item = DocumentId;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            iter: self.rb.into_iter(),
        }
    }
}

pub struct IntoIter {
    iter: roaring::bitmap::IntoIter,
}

impl Iterator for IntoIter {
    type Item = DocumentId;

    fn next(&mut self) -> Option<DocumentId> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

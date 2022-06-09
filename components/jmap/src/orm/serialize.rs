use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{AccountId, DocumentId, JMAPStore, Store};

use super::{Object, TinyORM};

impl<T> StoreSerialize for TinyORM<T>
where
    T: Object,
{
    fn serialize(&self) -> Option<Vec<u8>> {
        store::bincode::serialize(self).ok()
    }
}

impl<T> StoreDeserialize for TinyORM<T>
where
    T: Object,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        store::bincode::deserialize(bytes).ok()
    }
}

pub trait JMAPOrm {
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static;
}

impl<T> JMAPOrm for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static,
    {
        self.get_document_value::<TinyORM<U>>(
            account,
            U::collection(),
            document,
            TinyORM::<U>::FIELD_ID,
        )
    }
}

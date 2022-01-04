use jmap_store::{local_store::JMAPLocalStore, JMAPError, JMAP_MAIL};
use mail_parser::RfcHeaders;
use store::{AccountId, DocumentId, Store, StoreError};

use crate::{JMAPMailStoreGet, MessageField};

impl<'x, T> JMAPMailStoreGet<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn get_headers_rfc(
        &'x self,
        account: AccountId,
        document: DocumentId,
    ) -> jmap_store::Result<RfcHeaders> {
        bincode::deserialize(
            &self
                .store
                .get_document_value::<Vec<u8>>(
                    account,
                    JMAP_MAIL,
                    document,
                    MessageField::Internal.into(),
                    crate::MESSAGE_HEADERS,
                )?
                .ok_or_else(|| {
                    JMAPError::InternalError(StoreError::InternalError(format!(
                        "Headers for doc_id {} not found",
                        document
                    )))
                })?,
        )
        .map_err(|e| JMAPError::InternalError(StoreError::InternalError(e.to_string())))
        // TODO all errors have to include more info about context
    }
}

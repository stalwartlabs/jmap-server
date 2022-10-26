use jmap_mail::mail::MessageField;
use store::{
    blob::BlobId,
    config::{env_settings::EnvSettings, jmap::JMAPConfig},
    core::collection::Collection,
    AccountId, DocumentId, JMAPStore, Store,
};
use store_rocksdb::RocksDB;

pub fn main() {
    // Read configuration parameters
    let settings = EnvSettings::new();

    let store: JMAPStore<RocksDB> = JMAPStore::new(
        RocksDB::open(&settings).expect("failed to open database"),
        JMAPConfig::from(&settings),
        &settings,
    );

    dump_message(
        &store,
        settings
            .parse("account-id")
            .expect("A valid 'account' parameter."),
        settings
            .parse("document-id")
            .expect("A valid message 'document-id' parameter."),
    );
}

pub fn dump_message<T>(store: &JMAPStore<T>, account_id: AccountId, document_id: DocumentId)
where
    T: for<'x> Store<'x> + 'static,
{
    let blob_id = store
        .get_document_value::<BlobId>(
            account_id,
            Collection::Mail,
            document_id,
            MessageField::Metadata.into(),
        )
        .expect("Failed to fetch blobId")
        .expect("Blob not found");

    // Fetch message metadata
    let message_data_bytes = store
        .blob_get(&blob_id)
        .expect("Failed to fetch blob")
        .expect("Blob not found");

    // Deserialize message data
    let message_data =
        <jmap_mail::mail::MessageData as store::serialize::StoreDeserialize>::deserialize(
            &message_data_bytes,
        )
        .expect("Failed to deserialize");

    println!(
        "---- MESSAGE DATA ----\n{}",
        serde_json::to_string_pretty(&message_data).unwrap(),
    );

    let fields = jmap::orm::serialize::JMAPOrm::get_orm::<jmap_mail::mail::schema::Email>(
        store,
        account_id,
        document_id,
    )
    .expect("Failed to get ORM")
    .expect("ORM not found");

    println!(
        "---- ORM ----\n{}",
        serde_json::to_string_pretty(&fields).unwrap()
    );

    let raw_message = store
        .blob_get(&message_data.raw_message)
        .expect("Failed to fetch raw message")
        .expect("Raw message not found");

    println!(
        "---- RAW MESSAGE ----\n{}",
        String::from_utf8_lossy(&raw_message)
    );
}

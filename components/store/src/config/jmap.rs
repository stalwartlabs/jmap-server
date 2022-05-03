use crate::nlp::Language;

use super::env_settings::EnvSettings;

pub struct JMAPConfig {
    pub is_in_cluster: bool,

    pub blob_temp_ttl: u64,
    pub default_language: Language,

    pub max_size_upload: usize,
    pub max_concurrent_upload: usize,
    pub max_size_request: usize,
    pub max_concurrent_requests: usize,
    pub max_calls_in_request: usize,
    pub max_objects_in_get: usize,
    pub max_objects_in_set: usize,

    pub query_max_results: usize,
    pub changes_max_results: usize,
    pub mailbox_name_max_len: usize,
    pub mailbox_max_total: usize,
    pub mailbox_max_depth: usize,
    pub mail_attachments_max_size: usize,
    pub mail_import_max_items: usize,
    pub mail_parse_max_items: usize,
}

impl From<&EnvSettings> for JMAPConfig {
    fn from(settings: &EnvSettings) -> Self {
        JMAPConfig {
            max_size_upload: 50000000,
            max_concurrent_upload: 8,
            max_size_request: 10000000,
            max_concurrent_requests: 8,
            max_calls_in_request: 32,
            max_objects_in_get: 500,
            max_objects_in_set: 500,
            blob_temp_ttl: 3600, //TODO configure all params
            changes_max_results: 1000,
            query_max_results: 1000,
            mailbox_name_max_len: 255, //TODO implement
            mailbox_max_total: 1000,
            mailbox_max_depth: 10,
            mail_attachments_max_size: 50000000, //TODO implement
            mail_import_max_items: 2,
            mail_parse_max_items: 5,
            default_language: Language::English,
            is_in_cluster: settings.get("cluster").is_some(),
        }
    }
}

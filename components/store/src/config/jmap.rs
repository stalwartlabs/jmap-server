use crate::nlp::Language;

use super::env_settings::EnvSettings;

pub struct JMAPConfig {
    pub blob_temp_ttl: u64,
    pub default_language: Language,

    pub max_size_upload: usize,
    pub max_concurrent_uploads: usize,
    pub max_size_request: usize,
    pub max_concurrent_requests: usize,
    pub max_calls_in_request: usize,
    pub max_objects_in_get: usize,
    pub max_objects_in_set: usize,

    pub rate_limit_authenticated: (u64, u64),
    pub rate_limit_anonymous: (u64, u64),
    pub use_forwarded_header: bool,

    pub query_max_results: usize,
    pub changes_max_results: usize,
    pub mailbox_name_max_len: usize,
    pub mailbox_max_total: usize,
    pub mailbox_max_depth: usize,
    pub mail_attachments_max_size: usize,
    pub mail_import_max_items: usize,
    pub mail_parse_max_items: usize,

    pub subscription_max_total: usize,
    pub ws_heartbeat_interval: u64,
    pub ws_client_timeout: u64,
    pub ws_throttle: u64,
    pub event_source_throttle: u64,

    pub raft_commit_timeout: u64,
}

impl From<&EnvSettings> for JMAPConfig {
    fn from(settings: &EnvSettings) -> Self {
        JMAPConfig {
            max_size_upload: settings.parse("max-size-upload").unwrap_or(50000000),
            max_concurrent_uploads: settings.parse("max-concurrent-uploads").unwrap_or(4),
            max_concurrent_requests: settings.parse("max-concurrent-requests").unwrap_or(4),
            max_size_request: settings.parse("max-size-request").unwrap_or(10000000),
            max_calls_in_request: settings.parse("max-calls-in-request").unwrap_or(16),
            max_objects_in_get: settings.parse("max-objects-in-get").unwrap_or(500),
            max_objects_in_set: settings.parse("max-objects-in-set").unwrap_or(500),
            blob_temp_ttl: settings.parse("blob-temp-ttl").unwrap_or(3600),
            changes_max_results: settings.parse("changes-max-results").unwrap_or(5000),
            query_max_results: settings.parse("query-max-results").unwrap_or(5000),
            mailbox_name_max_len: settings.parse("mailbox-name-max-len").unwrap_or(255),
            mailbox_max_total: settings.parse("mailbox-max-total").unwrap_or(1000),
            mailbox_max_depth: settings.parse("mailbox-max-depth").unwrap_or(10),
            mail_attachments_max_size: settings
                .parse("mail-attachments-max-size")
                .unwrap_or(50000000),
            mail_import_max_items: settings.parse("mail-import-max-items").unwrap_or(5),
            mail_parse_max_items: settings.parse("mail-parse-max-items").unwrap_or(5),
            subscription_max_total: settings.parse("subscription-max-total").unwrap_or(100),
            ws_client_timeout: settings.parse("ws-client-timeout").unwrap_or(10 * 1000),
            ws_heartbeat_interval: settings.parse("ws-heartbeat-interval").unwrap_or(5 * 1000),
            ws_throttle: settings.parse("ws-throttle").unwrap_or(1000),
            event_source_throttle: settings.parse("event-source-throttle").unwrap_or(1000),
            raft_commit_timeout: settings.parse("raft-commit-timeout").unwrap_or(1000),
            default_language: Language::from_iso_639(
                &settings
                    .get("default-language")
                    .unwrap_or_else(|| "en".to_string()),
            )
            .unwrap_or(Language::English),
            rate_limit_authenticated: settings
                .get("rate-limit-authenticated")
                .unwrap_or_else(|| "1000/60".to_string())
                .split_once('/')
                .and_then(|(a, b)| {
                    a.parse::<u64>()
                        .ok()
                        .map(|a| (a, b.parse::<u64>().unwrap_or(60)))
                })
                .unwrap_or((1000, 60)),
            rate_limit_anonymous: settings
                .get("rate-limit-anonymous")
                .unwrap_or_else(|| "100/60".to_string())
                .split_once('/')
                .and_then(|(a, b)| {
                    a.parse::<u64>()
                        .ok()
                        .map(|a| (a, b.parse::<u64>().unwrap_or(60)))
                })
                .unwrap_or((100, 60)),
            use_forwarded_header: settings.parse("use-forwarded-header").unwrap_or(false),
        }
    }
}

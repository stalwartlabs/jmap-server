
jmap-server
===
- Searching
  - Search by "Header exists" (might be already working?)
- Mailbox
  - Do not count messages in Trash for Mailbox/unreadEmails.
- Blobs
  - S3 connector.
  - Do not replicate blobs when using S3.
  - Escape filenames in src/api/blob.
- Cluster
  - Advance local commit index on Raft leave request.
  - Encrypt packets.
  - Something better than cluster key?
- General
  - Fix logging from subcrates.
  - Set readOnly on shared accounts for jmap session.
  - Control the amount of data stored from all set requests (ORM values, headers, etc.)
  - OAuth authentication with Raft support.
  - Graceful shutdowns
- Testing
  - Cluster read replicas
  - Webmail client using Enron db.
  - References pointing to updatedProperties in mailbox.
  - upToId in queryChanges.
  - IdAssigner.
  - Fuzz testing.
  - Enron database.
- Review all dependencies what kind of code they have.
- Remove /home/vagrant/code/ references from Cargo.toml (from all projects!).
- Remove print!() and println!() from everywhere.

imap-server
===
- Retest, make sure parsing is OK.
- Check TODOs
- Compile for multiple targets.
- Docker image.

jmap-client
===
- Use `Cow<str>`.
- Write documentation and samples.

Final:
===
- Setup jmap.cloud with Enron.
- Rtrdmrk.
- Write documentation and website.
- Post to JMAP mailing list, Reddit, Github, etc.
  
Medium term
===
- Jepsen tests.
- Quota support.
- Sieve filters.
- JMAP Contacts/Calendars.
- Write email/set parsed message directly to store, avoid parsing it again.
- Index PDF, Word and Excel.


Settings
----
- Store
  - db-path: <String>
  - id-cache-size: <Bytes> (32 * 1024 * 1024)
- Blob Store
  - blob-s3: bucket, region, access-key, secret-key
  - blob-nested-levels: Int (2)
  - blob-min-size: 16384
  - blob-temp-ttl: 3600
- Server
  - hostname: 127.0.0.1:8080
  - bind-addr: 127.0.0.1
  - advertise-addr: 127.0.0.1
  - cert-path: /etc/ssl/certs/jmap.pem
  - key-path: /etc/ssl/private/jmap.key
  - http-port: 8080
  - worker-pool-size: Number of CPUs
  - strict-cors: false
- Cluster
  - cluster: Cluster key
  - rpc-port: 7911
  - shard-id: 0
  - seed-nodes: 127.0.0.1:7912;127.0.0.1:7913;127.0.0.1:7914
- E-mail Submissions
  - smtp-relay: user:pass@hostname:port
  - smtp-relay-timeout: 60000
- JMAP
  - max-size-upload: 50000000
  - max-concurrent-uploads: 4
  - max-concurrent-requests: 4
  - max-size-request: 10000000
  - max-calls-in-request: 16
  - max-objects-in-get: 500
  - max-objects-in-set: 500
  - changes-max-results: 5000
  - query-max-results: 5000
  - mailbox-name-max-len: 255
  - mailbox-max-total: 1000
  - mailbox-max-depth: 10
  - mail-attachments-max-size: 50000000
  - mail-import-max-items: 5
  - mail-parse-max-items: 5
  - default-language: en
  - rate-limit-authenticated: 1000/60
  - rate-limit-anonymous: 100/60
  - use-forwarded-header: false
  - subscription-max-total: 100
- Websockets
  - ws-client-timeout: 10 seconds
  - ws-heartbeat-interval: 5 seconds
  - ws-throttle: 1000
- EventSource
  - event-source-throttle: 1000
- Housekeeper
  - schedule-purge-accounts: 0 3 *
  - schedule-purge-blobs: 30 3 *
  - schedule-compact-log: 45 3 *
  - schedule-compact-db: 0 4 *
  - max-changelog-entries: 10000
- Recovery
  - set-admin-password:

- Push Subscriptions
  - push-attempt-interval: 60 * 1000
  - push-attempts-max: 3
  - push-retry-interval: 1000
  - push-timeout: 10 * 1000
  - push-attempt-interval: 60 * 1000
  - push-throttle: 1000

- Cluster
  - key: <String>
  - peer-ping-interval: 500
  - raft-batch-max: 10 * 1024 * 1024
  - raft-commit-timeout: 1000
  - raft-election-timeout: 1000
  - rpc-frame-max: 50 * 1024 * 1024
  - rpc-inactivity-timeout: 5 * 60 * 1000
  - rpc-timeout: 1000
  - rpc-retries-max: 5
  - rpc-backoff-max: 3 * 60 * 1000 (1 minute)


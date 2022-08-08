
jmap-server
===
- Searching
  - Search by "Header exists" (might be already working?)
  - Autodetect language in searches.
- Test searchSnippets from HTML.
- Mailbox
  - Do not count messages in Trash for Mailbox/unreadEmails.
- Principals
  - Argon password encryption.
  - Use DKIM on emailSubmissions.
- Blobs
  - Configure hash_level in LocalBlobStore.
  - Configure max blob size.
  - S3 connector.
  - Do not replicate blobs when using S3.
  - Escape filenames in src/api/blob.
- Housekeeping tasks
  - Blob deletion.
  - Log compaction.
  - Account deletion.
- Read configuration from EnvSettings.
- Fix logging from subcrates.
- Set readOnly on shared accounts for jmap session.
- Control the amount of data stored from all set requests (ORM values, headers, etc.)
- OAuth authentication with Raft support.
- Base32 encoding of ids and blobIds.
- Binaries
  - Postfix ingest
  - Admin binary.
  - Maildir/Mbox import binary.
- Email Delivery
  - Be notified of shutdowns and lost leaderships (all modules).
  - On startup load all pending deliveries.
- Cluster
  - Advance local commit index on Raft leave request.
  - Encrypt packets.
  - Something better than cluster key?
  - Configure MAX_BATCH_SIZE, MAX_FRAME_LENGTH, etc..
  - Test read replicas.
- Testing
  - Test webmail client using Enron db.
  - Run cleanup tasks after calling principal destroy.
  - Set harcoded settings in EnvSettings.
  - References pointing to updatedProperties in mailbox.
  - upToId in queryChanges.
  - IdAssigner.
  - Fuzz testing.
  - Enron database.
- Review all dependencies what kind of code they have.
- Remove /home/vagrant/code/ references from Cargo.toml.
- Remove print!() and println!() from everywhere.

imap-server
===
- Retest, make sure parsing is OK.
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

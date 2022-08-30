jmap + imap + cli
===
- Remove /home/vagrant/code/ references from Cargo.toml (from all projects!).
- Compile for multiple targets.
- Docker image.
- Cargo.toml authors, etc.
- Licenses


jmap-client
===
- Use `Cow<str>`.
- Write documentation and samples.

Distro
====
- https://rust-cli.github.io/book/tutorial/packaging.html
- https://skerritt.blog/packaging-your-rust-package/
- https://www.infinyon.com/blog/2021/04/github-actions-best-practices/
- https://github.com/japaric/trust
- https://github.com/cross-rs/cross
- https://github.com/jordansissel/fpm
- https://gist.github.com/FedericoPonzi/873aea22b652572f5995f23b86543fdb
- https://github.com/ClementTsang/bottom/blob/master/.github/workflows/ci.yml
- https://github.com/nicolas-van/rust-cross-compile-example/blob/main/.github/workflows/rust.yml
- https://github.com/Coding-Badly/rusty-tools/blob/main/.github/workflows/build.yml

- https://github.com/japaric/rust-cross#advanced-topics

- https://www.reddit.com/r/hetzner/comments/hp5zf6/email_services_that_are_hosted_by_hetzner_endup/
- https://kobzol.github.io/rust/ci/2021/05/07/building-rust-binaries-in-ci-that-work-with-older-glibc.html

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
- Use unique nonce for each packet.
- musl targets


Testing
cargo test --all
cargo test jmap_stress_tests -- --ignored
cargo +nightly fuzz run jmap_server

Settings
----
- config: <Path>
- Store
  - db-path: <String>
  - id-cache-size: <Bytes> (32 * 1024 * 1024)
- Blob Store
  - blob-s3: bucket, region, access-key, secret-key
  - blob-nested-levels: Int (2)
  - blob-min-size: 16384
  - blob-temp-ttl: 3600
- Server
  - jmap-url: http://localhost
  - jmap-bind-addr: 127.0.0.1
  - jmap-port: 8080
  - jmap-cert-path: /etc/ssl/certs/jmap.pem
  - jmap-key-path: /etc/ssl/private/jmap.key
  - worker-pool-size: Number of CPUs
  - strict-cors: false
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
  - rate-limit-auth: 10/60
  - use-forwarded-header: false
  - subscription-max-total: 100
  - mail-max-size: 104857600

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

- LMTP
  - lmtp-bind-addr: 127.0.0.1
  - lmtp-port: 11200
  - lmtp-cert-path
  - lmtp-key-path
  - lmtp-tls-only: false
  - lmtp-trusted-ips: ip1;ip2

- Cluster
  - seed-nodes: 127.0.0.1:7912;127.0.0.1:7913;127.0.0.1:7914
  - rpc-bind-addr: 127.0.0.1 (or jmap-bind-addr)
  - rpc-advertise-addr: 127.0.0.1
  - rpc-port: 7911
  - rpc-key: <String>
  - peer-ping-interval: 500
  - raft-batch-max: 10 * 1024 * 1024
  - raft-commit-timeout: 1000
  - raft-election-timeout: 1000
  - rpc-inactivity-timeout: 5 * 60 * 1000
  - rpc-timeout: 1000
  - rpc-retries-max: 5
  - rpc-backoff-max: 3 * 60 * 1000 (1 minute)
  - rpc-cert-path
  - rpc-cert-key
  - rpc-tls-domain: false

- OAuth
  - oauth-key: <String>
  - oauth-user-code-expiry: 1800
  - oauth-auth-code-expiry: 600
  - oauth-token-expiry: 3600
  - oauth-refresh-token-expiry: 30 * 86400
  - oauth-refresh-token-renew: 4 * 86400
  - oauth-max-attempts: 3

-set-admin-password!

Postfix
--------

address_verify_negative_expire_time = 30s
address_verify_negative_refresh_time = 30s
address_verify_positive_expire_time = 12h
address_verify_positive_refresh_time = 6h

virtual_mailbox_domains = example.com
virtual_transport=lmtp:[127.0.0.1]:11200
smtpd_recipient_restrictions = reject_unverified_recipient


Dkim
----
openssl genrsa -out dkim_private.pem 2048
openssl rsa -in dkim_private.pem -pubout -outform der 2>/dev/null | openssl base64 -A

name: [selector]._domainkey.[domain]
value: v=DKIM1; k=rsa; p=<BASE64>


Install Deb
------

sudo apt install /home/vagrant/empty-project/bin/target/debian/stalwart-jmap_0.1.0_amd64.deb 
sudo apt-get remove stalwart-jmap

        /*println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::from_slice::<serde_json::Value>(&request).unwrap()
            )
            .unwrap()
        );*/

                    //println!("{}", serde_json::to_string_pretty(&result).unwrap());



Troubleshooting Import
-----
[1/4] Parsing mailbox...
Failed to read Maildir folder: Too many open files (os error 24)

ulimit -n 65535


Debugging
-----
thread apply all bt





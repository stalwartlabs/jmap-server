# Stalwart JMAP Server

[![Test](https://github.com/stalwartlabs/jmap-server/actions/workflows/test.yml/badge.svg)](https://github.com/stalwartlabs/jmap-server/actions/workflows/test.yml)
[![Build](https://github.com/stalwartlabs/jmap-server/actions/workflows/build.yml/badge.svg)](https://github.com/stalwartlabs/jmap-server/actions/workflows/build.yml)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![](https://img.shields.io/discord/923615863037390889?label=Chat)](https://discord.gg/jtgtCNj66U)
[![](https://img.shields.io/twitter/follow/stalwartlabs?style=flat)](https://twitter.com/stalwartlabs)


**Stalwart JMAP** is an open-source JSON Meta Application Protocol server designed to be secure, fast, robust and scalable.
JMAP is a modern protocol for synchronising data such as mail, calendars, or contacts that makes much more efficient use of network resources.

Key features:

- **JMAP** full compliance:
  - JMAP Core ([RFC 8620](https://datatracker.ietf.org/doc/html/rfc8620))
  - JMAP Mail ([RFC 8621](https://datatracker.ietf.org/doc/html/rfc8621))
  - JMAP over WebSocket ([RFC 8887](https://datatracker.ietf.org/doc/html/rfc8887))
  - JMAP for Sieve Scripts ([DRAFT-SIEVE-12](https://www.ietf.org/archive/id/draft-ietf-jmap-sieve-12.html)).
- **IMAP4** full compliance:
  - IMAP4rev2 ([RFC 9051](https://datatracker.ietf.org/doc/html/rfc9051))
  - IMAP4rev1 ([RFC 3501](https://datatracker.ietf.org/doc/html/rfc3501)) 
  - Numerous [extensions](https://stalw.art/imap/development/rfc/#imap4-extensions) supported.
- **Flexible and robust** message storage:
  - Sieve Script message filtering with support for [all extensions](https://stalw.art/jmap/configure/sieve/#conformed-rfcs).
  - Full-text search support available in 17 languages.
  - Local Mail Transfer Protocol ([LMTP](https://datatracker.ietf.org/doc/html/rfc2033)) message ingestion.
  - [RocksDB](http://rocksdb.org/) backend.
- **Secure**:
  - OAuth 2.0 [authorization code](https://www.rfc-editor.org/rfc/rfc8628) and [device authorization](https://www.rfc-editor.org/rfc/rfc8628) flows.
  - Domain Keys Identified Mail ([DKIM](https://www.rfc-editor.org/rfc/rfc6376)) message signing.
  - Access Control Lists (ACLs).
  - Rate limiting.
  - Memory safe (thanks to Rust).
- **Scalable and fault-tolerant**:
  - Node autodiscovery and failure detection over gossip protocol.
  - Replication and cluster consensus over the [Raft](https://raft.github.io/) protocol.
  - Read-only replicas.
  - No third-party replication or cluster coordination software required.

## Get Started

Install Stalwart JMAP on your server by following the instructions for your platform:

- [Linux / MacOS](https://stalw.art/jmap/get-started/linux/)
- [Windows](https://stalw.art/jmap/get-started/windows/)
- [Docker](https://stalw.art/jmap/get-started/docker/)

You may also [compile Stalwart JMAP from the source](https://stalw.art/jmap/development/compile/).

## Support

If you are having problems running Stalwart JMAP, you found a bug or just have a question,
do not hesitate to reach us on [Github Discussions](https://github.com/stalwartlabs/jmap-server/discussions),
[Reddit](https://www.reddit.com/r/stalwartlabs) or [Discord](https://discord.gg/jtgtCNj66U).
Additionally you may become a sponsor to obtain priority support from Stalwart Labs Ltd.

## Documentation

Table of Contents

- Get Started
  - [Linux / MacOS](https://stalw.art/jmap/get-started/linux/)
  - [Windows](https://stalw.art/jmap/get-started/windows/)
  - [Docker](https://stalw.art/jmap/get-started/docker/)
- Configuration
  - [Overview](https://stalw.art/jmap/configure/overview/)
  - [Web Server](https://stalw.art/jmap/configure/webserver/)
  - [Database](https://stalw.art/jmap/configure/database/)
  - [LMTP Delivery](https://stalw.art/jmap/configure/lmtp/)
  - [SMTP Relay](https://stalw.art/jmap/configure/smtp/)
  - [OAuth Settings](https://stalw.art/jmap/configure/oauth/)
  - [JMAP Settings](https://stalw.art/jmap/configure/jmap/)
  - [Push Notifications](https://stalw.art/jmap/configure/push/)
  - [WebSockets](https://stalw.art/jmap/configure/websocket/)
  - [Rate Limiter](https://stalw.art/jmap/configure/rate-limit/)
- Management
  - [Overview](https://stalw.art/jmap/manage/overview/)
  - [Accounts](https://stalw.art/jmap/manage/accounts/)
  - [Domains](https://stalw.art/jmap/manage/domains/)
  - [Groups](https://stalw.art/jmap/manage/groups/)
  - [Mailing Lists](https://stalw.art/jmap/manage/lists/)
  - [Access Control Lists](https://stalw.art/jmap/manage/acl/)
- Migration
  - [Overview](https://stalw.art/jmap/migrate/overview/)
  - [Import Accounts](https://stalw.art/jmap/migrate/accounts/)
  - [Import Mailboxes](https://stalw.art/jmap/migrate/mailboxes/)
- Clustering
  - [Quick Start](https://stalw.art/jmap/cluster/quick-start/)
  - [Gossip Protocol](https://stalw.art/jmap/cluster/gossip/)
  - [Consensus Protocol](https://stalw.art/jmap/cluster/raft/)
  - [RPC](https://stalw.art/jmap/cluster/rpc/)
- Development
  - [Compiling](https://stalw.art/jmap/development/compile/)
  - [Tests](https://stalw.art/jmap/development/test/)
  - [RFCs conformed](https://stalw.art/jmap/development/rfc/)

## Roadmap

The following major features and enhancements are planned for Stalwart JMAP:

- Quota support
- Filtering support (Sieve filters as well as other mechanisms)
- JMAP Contacts, Calendars and Tasks support (currently IETF drafts)
- Performance enhancements
- Jepsen testing

## Testing

### Base tests

The base tests perform basic testing on different functions across the Stalwart JMAP
code base. To run the base test suite execute:

```bash
cargo test --all
```

### Database tests

The database test suite performs a range of tests on the embedded key-value store such as:

- Concurrent insertions.
- Database queries.
- Concurrent blob insertions and blob expiration.
- Log compactions.

To run the database test suite execute:

```bash
cargo test store_tests -- --ignored
```

### Core tests

The core test suite performs authorization, authentication and JMAP protocol compliance tests such as:

- Access Control Lists (ACL) enforcement.
- Authorization and Rate Limiting.
- Event Source (JMAP Core).
- OAuth authentication.
- Push Subscriptions (JMAP Core).
- WebSockets (JMAP over WebSocket).

To run the core test suite execute:

```bash
cargo test jmap_core_tests -- --ignored
```

### Mail tests

The mail test suite performs e-mail and JMAP Mail protocol compliance tests such as:

- ``Email/*`` functionality and compliance.
- ``Mailbox/*`` functionality and compliance.
- ``Thread/*`` functionality and compliance.
- ``EmailSubmission/*`` functionality and compliance.
- ``SearchSnippet/get`` functionality and compliance.
- ``VacationResponse/*`` functionality and compliance.
- Message thread id creation.
- LMTP message ingestion.

To run the mail test suite execute:

```bash
cargo test jmap_mail_tests -- --ignored
```

### Stress tests

The stress test suite generates concurrent random insert, get, query, update and delete
operations on different JMAP datatypes to ensure data integrity. To run the stress test suite execute:

```bash
cargo test jmap_stress_tests -- --ignored
```

Another way of stress testing Stalwart JMAP is by using the [IMAP stress test tool](https://stalw.art/imap/development/test).


### Cluster tests

The cluster test suite starts a Stalwart JMAP cluster consisting of five nodes and performs the 
following tests:

- Distributed insert, read, update and delete operations.
- Distributed e-mail thread merges.
- Read replicas.
- LMTP ingestion over RPC.
- Raft elections.
- Raft log conflict resolution.

To run the cluster test suite execute:

```bash
cargo test cluster_tests -- --ignored
```

### Cluster fuzz

The cluster fuzz test suite starts a Stalwart JMAP cluster consisting of five nodes and attempts
to corrupt the cluster state and/or its data by randomly performing the following operations:

- Crash current leader.
- Crash follower.
- Start offline follower.
- Start all offline nodes.
- Insert record.
- Update record.
- Delete record.

Please note that this test runs on a loop and does not stop unless a leader fails to be elected
or a data corruption problem is found. If something goes wrong while running the tests, 
all the actions that were executed up to that point will be printed to screen as a JSON structure.
This JSON dump can be later used to reproduce and eventually debug the problem.

To run the cluster fuzz test suite execute:

```bash
cargo test cluster_fuzz -- --ignored
```

### Cluster Jepsen tests

Support for Jepsen testing is [planned](https://github.com/stalwartlabs/jmap-server/issues/8) and in the roadmap. Until then, the clustering module will remain in beta.

### JMAP test suite

Compliance with the JMAP protocol may also be tested using Fastmail's JMAP-TestSuite:

- Clone the JMAP TestSuite repository:
    ```bash
    git clone https://github.com/stalwartlabs/jmap-test-suite.git
    cd jmap-test-suite/
    ```
- Install the recommended Perl dependencies:
    ```bash
    cpanm --installdeps .
    ```
- Run one of the tests, for example:
    ```bash
    JMAP_SERVER_ADAPTER_FILE=eg/stalwart.json perl -I<PATH_TO_PERL_LIB> -I lib t/basic.t
    ```

### Fuzz

To fuzz Stalwart JMAP server with `cargo-fuzz` execute:

```bash
 $ cargo +nightly fuzz run jmap_server
```

## License

Licensed under the terms of the [GNU Affero General Public License](https://www.gnu.org/licenses/agpl-3.0.en.html) as published by
the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
See [LICENSE](LICENSE) for more details.

You can be released from the requirements of the AGPLv3 license by purchasing
a commercial license. Please contact licensing@stalw.art for more details.
  
## Copyright

Copyright (C) 2020-2022, Stalwart Labs Ltd.

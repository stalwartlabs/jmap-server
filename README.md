<h2 align="center">
    <a href="https://stalw.art">
    <img src="https://stalw.art/home/apple-touch-icon.png" height="60">
    </a>
    <br>
    Stalwart JMAP Server
</h1>

<p align="center">
  <i align="center">Secure & Modern JMAP Server</i> 🛡️
</p>

<h4 align="center">
  <a href="https://github.com/stalwartlabs/mail-server/actions/workflows/build.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/stalwartlabs/mail-server/build.yml?style=flat-square" alt="continuous integration">
  </a>
  <a href="https://www.gnu.org/licenses/agpl-3.0">
    <img src="https://img.shields.io/badge/License-AGPL_v3-blue.svg?label=license&style=flat-square" alt="License: AGPL v3">
  </a>
  <a href="https://stalw.art/docs/get-started/">
    <img src="https://img.shields.io/badge/read_the-docs-red?style=flat-square" alt="Documentation">
  </a>
  <br>
  <a href="https://mastodon.social/@stalwartlabs">
    <img src="https://img.shields.io/mastodon/follow/109929667531941122?style=flat-square&logo=mastodon&color=%236364ff" alt="Mastodon">
  </a>
  <a href="https://twitter.com/stalwartlabs">
    <img src="https://img.shields.io/twitter/follow/stalwartlabs?style=flat-square&logo=twitter" alt="Twitter">
  </a>
  <br>
  <a href="https://discord.gg/jtgtCNj66U">
    <img src="https://img.shields.io/discord/923615863037390889?label=discord&style=flat-square" alt="Discord">
  </a>
  <a href="https://matrix.to/#/#stalwart:matrix.org">
    <img src="https://img.shields.io/matrix/stalwartmail%3Amatrix.org?label=matrix&style=flat-square" alt="Matrix">
  </a>
</h4>

**Stalwart JMAP** is an open-source JSON Meta Application Protocol server designed to be secure, fast, robust and scalable.
JMAP is a modern protocol for synchronising data such as mail, calendars, or contacts that makes much more efficient use of network resources.

Key features:

- **JMAP** full compliance:
  - JMAP Core ([RFC 8620](https://datatracker.ietf.org/doc/html/rfc8620))
  - JMAP Mail ([RFC 8621](https://datatracker.ietf.org/doc/html/rfc8621))
  - JMAP over WebSocket ([RFC 8887](https://datatracker.ietf.org/doc/html/rfc8887))
  - JMAP for Sieve Scripts ([DRAFT-SIEVE-13](https://www.ietf.org/archive/id/draft-ietf-jmap-sieve-13.html)).
- **Flexible and scalable**:
  - Pluggable storage backends with **RocksDB**, **FoundationDB**, **PostgreSQL**, **mySQL**, **SQLite**, **S3-Compatible**, **Redis** and **ElasticSearch** support.
  - **Internal**, **LDAP** and **SQL** database authentication.
  - Built-in [SMTP](https://github.com/stalwartlabs/smtp-server) server for local delivery and JMAP Email Submissions.
  - Full-text search available in 17 languages.
  - Disk quotas.
  - Sieve scripting language with support for all [registered extensions](https://www.iana.org/assignments/sieve-extensions/sieve-extensions.xhtml).
  - Email aliases, mailing lists, subaddressing and catch-all addresses support.
  - Integration with **OpenTelemetry** to enable monitoring, tracing, and performance analysis.
- **Secure and robust**:
  - Encryption at rest with **S/MIME** or **OpenPGP**.
  - Built-in Spam and Phishing filter.
  - OAuth 2.0 [authorization code](https://www.rfc-editor.org/rfc/rfc8628) and [device authorization](https://www.rfc-editor.org/rfc/rfc8628) flows.
  - Access Control Lists (ACLs).
  - Rate limiting.
 - Memory safe (thanks to Rust).

## Get Started

Install Stalwart JMAP Server on your server by following the instructions for your platform:

- [Linux / MacOS](https://stalw.art/docs/install/linux)
- [Windows](https://stalw.art/docs/install/windows)
- [Docker](https://stalw.art/docs/install/docker)

All documentation is available at [stalw.art/docs/get-started](https://stalw.art/docs/get-started).

> **Note**
> If you need a more comprehensive solution that includes support for IMAP (Internet Message Access Protocol), you should consider installing the [Stalwart Mail Server](https://github.com/stalwartlabs/mail-server) instead.

## Support

If you are having problems running Stalwart JMAP, you found a bug or just have a question,
do not hesitate to reach us on [Github Discussions](https://github.com/stalwartlabs/jmap-server/discussions),
[Reddit](https://www.reddit.com/r/stalwartlabs) or [Discord](https://discord.gg/jtgtCNj66U).
Additionally you may become a sponsor to obtain priority support from Stalwart Labs Ltd.

## Funding

Part of the development of this project was funded through the [NGI0 Entrust Fund](https://nlnet.nl/entrust), a fund established by [NLnet](https://nlnet.nl/) with financial support from the European Commission's [Next Generation Internet](https://ngi.eu/) programme, under the aegis of DG Communications Networks, Content and Technology under grant agreement No 101069594.

If you find the project useful you can help by [becoming a sponsor](https://github.com/sponsors/stalwartlabs). Thank you!

## License

Licensed under the terms of the [GNU Affero General Public License](https://www.gnu.org/licenses/agpl-3.0.en.html) as published by
the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
See [LICENSE](LICENSE) for more details.

You can be released from the requirements of the AGPLv3 license by purchasing
a commercial license. Please contact licensing@stalw.art for more details.
  
## Copyright

Copyright (C) 2023, Stalwart Labs Ltd.

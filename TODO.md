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

Update docs
- sed -i -r "s/REPLACE_WITH_ENCRYPTION_KEY/`LC_ALL=C tr -dc '[:alpha:]' < /dev/urandom | head -c 64`/g" ${CFG_PATH}/config.yml
- /usr/local/stalwart-jmap
sudo dscl /Local/Default -delete Users/_stalwart-jmap
sudo dscl /Local/Default -delete Groups/_stalwart-jmap
sudo launchctl stop stalwart.jmap
sudo launchctl unload /Library/LaunchDaemons/stalwart.jmap.plist 
sudo rm /Library/LaunchDaemons/stalwart.jmap.plist

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



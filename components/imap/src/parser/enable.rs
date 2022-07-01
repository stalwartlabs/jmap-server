use crate::protocol::{capability::Capability, enable};

use super::receiver::Token;

pub fn parse_enable(tokens: Vec<Token>) -> super::Result<enable::Arguments> {
    let len = tokens.len();
    if len > 0 {
        let mut capabilities = Vec::with_capacity(len);
        for capability in tokens {
            capabilities.push(Capability::parse(&capability.unwrap_bytes())?);
        }
        Ok(enable::Arguments { capabilities })
    } else {
        Err("Missing arguments.".into())
    }
}

impl Capability {
    pub fn parse(value: &[u8]) -> super::Result<Self> {
        if value.eq_ignore_ascii_case(b"IMAP4rev2") {
            Ok(Self::IMAP4rev2)
        } else if value.eq_ignore_ascii_case(b"STARTTLS") {
            Ok(Self::StartTLS)
        } else if value.eq_ignore_ascii_case(b"LOGINDISABLED") {
            Ok(Self::LoginDisabled)
        } else if value.eq_ignore_ascii_case(b"CONDSTORE") {
            Ok(Self::Condstore)
        } else {
            Err(format!(
                "Unsupported capability '{}'.",
                String::from_utf8_lossy(value)
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::receiver::Receiver,
        protocol::{capability::Capability, enable},
    };

    #[test]
    fn parse_enable() {
        let mut receiver = Receiver::new();

        for (command, arguments) in [(
            "t2 ENABLE IMAP4rev2 CONDSTORE\r\n",
            enable::Arguments {
                capabilities: vec![Capability::IMAP4rev2, Capability::Condstore],
            },
        )] {
            receiver.parse(command.as_bytes().to_vec());
            assert_eq!(
                super::parse_enable(receiver.next_request().unwrap().unwrap().tokens).unwrap(),
                arguments
            );
        }
    }
}

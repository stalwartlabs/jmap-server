/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::borrow::Cow;

#[derive(Debug, Clone)]
pub enum Response<'x> {
    Message {
        code: u16,
        esn: u16,
        message: Cow<'static, str>,
    },
    Lhlo {
        local_host: Cow<'x, str>,
        remote_host: Cow<'x, str>,
        extensions: Vec<Extension>,
    },
}

#[derive(Debug, Clone)]
pub enum Extension {
    EightBitMime,
    BinaryMime,
    Size(u32),
    Vrfy,
    Help,
    Pipelining,
    Chunking,
    SmtpUtf8,
    StartTls,
    EnhancedStatusCodes,
}

impl Response<'_> {
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Response::Message { code, esn, message } => format!(
                "{} {}.{}.{} {}\r\n",
                code,
                esn / 100,
                esn % 100 / 10,
                esn % 10,
                message
            )
            .into_bytes(),
            Response::Lhlo {
                local_host,
                remote_host,
                extensions,
            } => {
                let mut buf = Vec::with_capacity(
                    local_host.len() + remote_host.len() + extensions.len() * 20,
                );
                buf.extend_from_slice(b"250-");
                buf.extend_from_slice(local_host.as_bytes());
                buf.extend_from_slice(b" welcomes ");
                buf.extend_from_slice(remote_host.as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (pos, extension) in extensions.iter().enumerate() {
                    if pos < extensions.len() - 1 {
                        buf.extend_from_slice(b"250-");
                    } else {
                        buf.extend_from_slice(b"250 ");
                    };
                    match extension {
                        Extension::EightBitMime => buf.extend_from_slice(b"8BITMIME"),
                        Extension::BinaryMime => buf.extend_from_slice(b"BINARYMIME"),
                        Extension::Size(size) => {
                            buf.extend_from_slice(b"SIZE ");
                            buf.extend_from_slice(size.to_string().as_bytes())
                        }
                        Extension::Vrfy => buf.extend_from_slice(b"VRFY"),
                        Extension::Help => buf.extend_from_slice(b"HELP"),
                        Extension::Pipelining => buf.extend_from_slice(b"PIPELINING"),
                        Extension::Chunking => buf.extend_from_slice(b"CHUNKING"),
                        Extension::SmtpUtf8 => buf.extend_from_slice(b"SMTPUTF8"),
                        Extension::StartTls => buf.extend_from_slice(b"STARTTLS"),
                        Extension::EnhancedStatusCodes => {
                            buf.extend_from_slice(b"ENHANCEDSTATUSCODES")
                        }
                    }
                    buf.extend_from_slice(b"\r\n");
                }

                buf
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Extension, Response};

    #[test]
    fn lmtp_response() {
        for (response, expected_text) in [
            (
                Response::Message {
                    code: 354,
                    esn: 312,
                    message: "go ahead".into(),
                },
                "354 3.1.2 go ahead\r\n",
            ),
            (
                Response::Lhlo {
                    local_host: "foo.com".into(),
                    remote_host: "bar.com".into(),
                    extensions: vec![
                        Extension::EightBitMime,
                        Extension::BinaryMime,
                        Extension::Size(123),
                        Extension::Vrfy,
                        Extension::Help,
                        Extension::Pipelining,
                        Extension::Chunking,
                        Extension::SmtpUtf8,
                        Extension::StartTls,
                    ],
                },
                concat!(
                    "250-foo.com welcomes bar.com\r\n",
                    "250-8BITMIME\r\n",
                    "250-BINARYMIME\r\n",
                    "250-SIZE 123\r\n",
                    "250-VRFY\r\n",
                    "250-HELP\r\n",
                    "250-PIPELINING\r\n",
                    "250-CHUNKING\r\n",
                    "250-SMTPUTF8\r\n",
                    "250 STARTTLS\r\n"
                ),
            ),
        ] {
            assert_eq!(
                String::from_utf8(response.into_bytes()).unwrap(),
                expected_text
            );
        }
    }
}

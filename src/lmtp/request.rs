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

use std::{borrow::Cow, iter::Peekable, vec::IntoIter};

use store::tracing::debug;

use super::response::Response;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Param {
    BodyBinaryMime,
    Body8BitMime,
    Size(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Lhlo {
        domain: String,
    },
    Mail {
        sender: String,
        params: Vec<Param>,
    },
    Rcpt {
        recipient: String,
        params: Vec<Param>,
    },
    Data {
        data: Vec<u8>,
    },
    Bdat {
        data: Vec<u8>,
        is_last: bool,
    },
    Rset,
    Vrfy {
        mailbox: String,
    },
    Expn {
        list: String,
    },
    Help {
        argument: Option<String>,
    },
    Noop,
    Quit,
    StartTls,
}

#[derive(Debug, Clone)]
pub enum Event {
    NeedsMoreBytes,
    Data,
    Message { response: Response<'static> },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum State {
    Start,
    Request { in_addr: bool },
    Bdat { chunk_size: usize, is_last: bool },
    Data { state: StateData },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StateData {
    None,
    Cr,
    CrLf,
    CrLfDot,
    CrLfDotCr,
}

enum Token {
    Lt,
    Gt,
    Eq,
    Colon,
    Text(String),
}

pub struct RequestParser {
    pub buf: Vec<u8>,
    tokens: Vec<Token>,
    pub state: State,
    pub command_size: usize,
    pub max_command_size: usize,
    pub max_message_size: usize,
}

impl RequestParser {
    pub fn new(max_command_size: usize, max_message_size: usize) -> Self {
        RequestParser {
            buf: Vec::with_capacity(10),
            tokens: Vec::with_capacity(5),
            state: State::Start,
            command_size: 0,
            max_command_size,
            max_message_size,
        }
    }

    pub fn error_reset(&mut self, message: impl Into<Cow<'static, str>>) -> Event {
        self.buf = Vec::with_capacity(10);
        self.state = State::Start;
        self.tokens.clear();
        self.command_size = 0;
        Event::parse_error(message)
    }

    fn push_buf(&mut self) -> Result<(), Event> {
        if !self.buf.is_empty() {
            self.tokens.push(Token::Text(
                String::from_utf8(std::mem::take(&mut self.buf))
                    .map_err(|_| Event::parse_error("Invalid UTF-8"))?,
            ));
            self.buf = Vec::with_capacity(50);
        }
        Ok(())
    }

    fn push_token(&mut self, token: Token) -> Result<(), Event> {
        self.command_size += 1;
        if self.command_size > self.max_command_size {
            return Err(Event::parse_error("Request too long"));
        }
        self.tokens.push(token);
        Ok(())
    }

    pub fn parse(&mut self, bytes: &mut std::slice::Iter<'_, u8>) -> Result<Request, Event> {
        #[allow(clippy::while_let_on_iterator)]
        while let Some(&ch) = bytes.next() {
            match self.state {
                State::Start => {
                    if !ch.is_ascii_whitespace() {
                        self.buf.push(ch.to_ascii_lowercase());
                        self.command_size = 1;
                        self.state = State::Request { in_addr: false };
                    } else if ch == b'\n' {
                        return Err(self.error_reset("Expected a command."));
                    }
                }
                State::Request { in_addr } => match ch {
                    b':' if !in_addr => {
                        self.push_buf()?;
                        self.push_token(Token::Colon)?;
                    }
                    b'=' if !in_addr => {
                        self.push_buf()?;
                        self.push_token(Token::Eq)?;
                    }
                    b'<' => {
                        self.push_buf()?;
                        self.push_token(Token::Lt)?;
                        self.state = State::Request { in_addr: true };
                    }
                    b'>' => {
                        self.push_buf()?;
                        self.push_token(Token::Gt)?;
                        self.state = State::Request { in_addr: false };
                    }
                    b'\r' => (),
                    b'\n' => {
                        self.push_buf()?;
                        self.command_size = 0;
                        let mut tokens = std::mem::take(&mut self.tokens).into_iter().peekable();
                        self.tokens = Vec::with_capacity(5);
                        self.state = State::Start;

                        return match tokens
                            .next()
                            .and_then(|t| t.unwrap_text())
                            .unwrap()
                            .as_str()
                        {
                            "lhlo" => Ok(Request::Lhlo {
                                domain: tokens.next().and_then(|t| t.unwrap_text()).ok_or_else(
                                    || {
                                        Event::parse_error(
                                            "LHLO requires a domain name as argument.",
                                        )
                                    },
                                )?,
                            }),
                            "mail" => {
                                if matches!(tokens.next(), Some(Token::Text(from)) if from == "from")
                                    && matches!(tokens.next(), Some(Token::Colon))
                                    && matches!(tokens.next(), Some(Token::Lt))
                                {
                                    let sender = match tokens.peek() {
                                        Some(Token::Text(_)) => {
                                            if let Some(Token::Text(sender)) = tokens.next() {
                                                sender
                                            } else {
                                                unreachable!()
                                            }
                                        }
                                        Some(Token::Gt) => "".to_string(),
                                        _ => {
                                            return Err(Event::parse_error(
                                                "MAIL FROM requires a mailbox as an argument.",
                                            ));
                                        }
                                    };

                                    Ok(Request::Mail {
                                        sender,
                                        params: self.parse_params(&mut tokens)?,
                                    })
                                } else {
                                    Err(Event::parse_error("Invalid MAIL FROM syntax."))
                                }
                            }
                            "rcpt" => {
                                if matches!(tokens.next(), Some(Token::Text(to)) if to == "to")
                                    && matches!(tokens.next(), Some(Token::Colon))
                                    && matches!(tokens.next(), Some(Token::Lt))
                                {
                                    Ok(Request::Rcpt {
                                        recipient: tokens
                                            .next()
                                            .and_then(|t| t.unwrap_text())
                                            .ok_or_else(|| {
                                                Event::parse_error(
                                                    "RCPT TO requires a mailbox as an argument.",
                                                )
                                            })?,
                                        params: self.parse_params(&mut tokens)?,
                                    })
                                } else {
                                    Err(Event::parse_error("Invalid RCPT TO syntax."))
                                }
                            }
                            "data" => {
                                self.state = State::Data {
                                    state: StateData::None,
                                };
                                Err(Event::Data)
                            }
                            "bdat" => {
                                let chunk_size = tokens
                                    .next()
                                    .and_then(|t| t.unwrap_text())
                                    .ok_or_else(|| {
                                        Event::parse_error(
                                            "BDAT requires a chunk size as argument.",
                                        )
                                    })?
                                    .parse::<usize>()
                                    .map_err(|_| {
                                        Event::parse_error("Failed to parse chunk size.")
                                    })?;
                                let is_last = tokens
                                    .next()
                                    .and_then(|t| t.unwrap_text())
                                    .map_or(false, |s| s.as_str() == "last");

                                if chunk_size == 0 {
                                    self.state = State::Start;
                                    Ok(Request::Bdat {
                                        data: Vec::with_capacity(0),
                                        is_last,
                                    })
                                } else if chunk_size < self.max_message_size {
                                    self.buf = Vec::with_capacity(chunk_size);
                                    self.state = State::Bdat {
                                        chunk_size,
                                        is_last,
                                    };
                                    continue;
                                } else {
                                    Err(Event::esn(
                                        500,
                                        534,
                                        format!(
                                            "BDAT chunk size exceeds maximum of {} bytes.",
                                            self.max_message_size
                                        ),
                                    ))
                                }
                            }
                            "rset" => Ok(Request::Rset),
                            "vrfy" => Ok(Request::Vrfy {
                                mailbox: tokens.next().and_then(|t| t.unwrap_text()).ok_or_else(
                                    || Event::parse_error("EXPN requires a valid text argument."),
                                )?,
                            }),
                            "expn" => Ok(Request::Expn {
                                list: tokens.next().and_then(|t| t.unwrap_text()).ok_or_else(
                                    || Event::parse_error("EXPN requires a valid text argument."),
                                )?,
                            }),
                            "help" => Ok(Request::Help {
                                argument: tokens.next().and_then(|t| t.unwrap_text()),
                            }),
                            "noop" => Ok(Request::Noop),
                            "starttls" => Ok(Request::StartTls),
                            "quit" => Ok(Request::Quit),
                            cmd => Err(self
                                .error_reset(format!("Unknown command '{}'.", cmd.to_uppercase()))),
                        };
                    }
                    _ => {
                        if !ch.is_ascii_whitespace() {
                            self.command_size += 1;
                            if self.command_size > self.max_command_size {
                                return Err(Event::parse_error("Request is too long."));
                            }
                            self.buf
                                .push(if in_addr { ch } else { ch.to_ascii_lowercase() });
                        } else {
                            self.push_buf()?;
                        }
                    }
                },
                State::Data { state } => {
                    let state = match ch {
                        b'\r' => match state {
                            StateData::None => StateData::Cr,
                            StateData::CrLfDot => StateData::CrLfDotCr,
                            StateData::Cr => {
                                self.buf.extend_from_slice(b"\r");
                                StateData::Cr
                            }
                            StateData::CrLf => {
                                self.buf.extend_from_slice(b"\r\n");
                                StateData::Cr
                            }
                            StateData::CrLfDotCr => {
                                self.buf.extend_from_slice(b"\r\n.");
                                StateData::Cr
                            }
                        },
                        b'\n' => match state {
                            StateData::None => {
                                self.buf.push(b'\n');
                                StateData::None
                            }
                            StateData::Cr => StateData::CrLf,
                            StateData::CrLfDotCr => {
                                let data = std::mem::take(&mut self.buf);
                                self.buf = Vec::with_capacity(10);
                                self.state = State::Start;
                                return Ok(Request::Data { data });
                            }
                            StateData::CrLf => {
                                self.buf.extend_from_slice(b"\r\n\n");
                                StateData::None
                            }
                            StateData::CrLfDot => {
                                self.buf.extend_from_slice(b"\r\n.\n");
                                StateData::None
                            }
                        },
                        b'.' => match state {
                            StateData::None => {
                                self.buf.push(b'.');
                                StateData::None
                            }
                            StateData::CrLf => StateData::CrLfDot,
                            StateData::Cr => {
                                self.buf.extend_from_slice(b"\r.");
                                StateData::None
                            }
                            StateData::CrLfDot => {
                                // Remove extra dot
                                self.buf.extend_from_slice(b"\r\n.");
                                StateData::None
                            }
                            StateData::CrLfDotCr => {
                                self.buf.extend_from_slice(b"\r\n.\r.");
                                StateData::None
                            }
                        },
                        _ => {
                            match state {
                                StateData::Cr => {
                                    self.buf.extend_from_slice(b"\r");
                                }
                                StateData::CrLf | StateData::CrLfDot => {
                                    self.buf.extend_from_slice(b"\r\n");
                                }
                                StateData::CrLfDotCr => {
                                    self.buf.extend_from_slice(b"\r\n.\r");
                                }
                                StateData::None => (),
                            }
                            self.buf.push(ch);
                            StateData::None
                        }
                    };

                    if self.buf.len() > self.max_message_size {
                        return Err(Event::esn(
                            500,
                            534,
                            format!(
                                "Message exceeds maximum of {} bytes.",
                                self.max_message_size
                            ),
                        ));
                    }

                    self.state = State::Data { state };
                }
                State::Bdat {
                    chunk_size,
                    is_last,
                } => {
                    self.buf.push(ch);
                    if self.buf.len() == chunk_size {
                        self.state = State::Start;
                        let data = std::mem::take(&mut self.buf);
                        self.buf = Vec::with_capacity(10);
                        return Ok(Request::Bdat { data, is_last });
                    }
                }
            }
        }

        Err(Event::NeedsMoreBytes)
    }

    fn parse_params(&self, tokens: &mut Peekable<IntoIter<Token>>) -> Result<Vec<Param>, Event> {
        if !matches!(tokens.next(), Some(Token::Gt)) {
            return Err(Event::parse_error("Missing > after mailbox."));
        }

        let mut params = Vec::new();
        while let Some(param_name) = tokens.next() {
            let param_name = param_name
                .unwrap_text()
                .ok_or_else(|| Event::parse_error("Parameter name must be a text value."))?;
            if !matches!(tokens.next(), Some(Token::Eq)) {
                debug!(
                    "Unsupported LMTP parameter '{}'.",
                    param_name.to_ascii_uppercase()
                );
                continue;
            }
            let param_value = match tokens.next() {
                Some(Token::Text(text)) => text,
                Some(_) => {
                    return Err(Event::parse_error("Parameter value must be a text value."));
                }
                None => return Err(Event::parse_error("Missing parameter value.")),
            };

            match param_name.as_str() {
                "body" => match param_value.as_str() {
                    "binarymime" => {
                        params.push(Param::BodyBinaryMime);
                        continue;
                    }
                    "8bitmime" => {
                        params.push(Param::Body8BitMime);
                        continue;
                    }
                    _ => {}
                },
                "size" => {
                    let size = param_value
                        .parse()
                        .map_err(|_| Event::parse_error("Size parameter must be a number."))?;
                    if size > self.max_message_size as u32 {
                        return Err(Event::esn(
                            552,
                            534,
                            format!(
                                "Message cannot exceed maximum of {} bytes.",
                                self.max_message_size
                            ),
                        ));
                    }

                    params.push(Param::Size(size));
                    continue;
                }
                _ => {
                    /*return Err(Event::esn(
                        500,
                        551,
                        format!(
                            "Unsupported parameter '{}'.",
                            param_name.to_ascii_uppercase()
                        ),
                    ));*/
                }
            }

            debug!(
                "Unsupported LMTP parameter {}={}.",
                param_name.to_ascii_uppercase(),
                param_value.to_ascii_uppercase()
            );

            /*return Err(Event::esn(
                500,
                551,
                format!(
                    "Unsupported {} value '{}'.",
                    param_name.to_ascii_uppercase(),
                    param_value.to_ascii_uppercase()
                ),
            ));*/
        }

        Ok(params)
    }
}

impl Token {
    pub fn unwrap_text(self) -> Option<String> {
        match self {
            Token::Text(text) => Some(text),
            _ => None,
        }
    }
}

impl Event {
    pub fn parse_error(message: impl Into<Cow<'static, str>>) -> Self {
        Event::Message {
            response: Response::Message {
                code: 500,
                esn: 552,
                message: message.into(),
            },
        }
    }

    pub fn esn(code: u16, esn: u16, message: impl Into<Cow<'static, str>>) -> Self {
        Event::Message {
            response: Response::Message {
                code,
                esn,
                message: message.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::lmtp::request::Event;

    use super::{Param, Request, RequestParser};

    #[test]
    fn lmtp_parser() {
        let mut parser = RequestParser::new(1024, 1024);
        for (chunks, expected_commands) in [
            (
                vec!["LHLO ", "  foo.edu\r\n"],
                vec![Request::Lhlo {
                    domain: "foo.edu".to_string(),
                }],
            ),
            (
                vec![
                    "MAIL FROM:<chris@bar.com>\r\n",
                    "MAIL FROM:<> SIZE=1024 BODY=8BITMIME\r\n",
                    "RCPT TO:<jones@foo.edu>\r\n",
                    "VRFY address\r\n",
                    "EXPN mailing-list \r\n",
                ],
                vec![
                    Request::Mail {
                        sender: "chris@bar.com".to_string(),
                        params: Vec::new(),
                    },
                    Request::Mail {
                        sender: "".to_string(),
                        params: vec![Param::Size(1024), Param::Body8BitMime],
                    },
                    Request::Rcpt {
                        recipient: "jones@foo.edu".to_string(),
                        params: Vec::new(),
                    },
                    Request::Vrfy {
                        mailbox: "address".to_string(),
                    },
                    Request::Expn {
                        list: "mailing-list".to_string(),
                    },
                ],
            ),
            (
                vec![
                    "MAIL FROM : < EAK@bar.com  > BODY=BINARYMIME\r\n",
                    "RCPT TO : < JONES@Foo.Edu > BODY = 8BITMIME\r\n",
                    "mail from:<hello@world.com> size=123 body=8bitmime\r\n",
                ],
                vec![
                    Request::Mail {
                        sender: "EAK@bar.com".to_string(),
                        params: vec![Param::BodyBinaryMime],
                    },
                    Request::Rcpt {
                        recipient: "JONES@Foo.Edu".to_string(),
                        params: vec![Param::Body8BitMime],
                    },
                    Request::Mail {
                        sender: "hello@world.com".to_string(),
                        params: vec![Param::Size(123), Param::Body8BitMime],
                    },
                ],
            ),
            (
                vec![
                    "help my-command \r\n",
                    " rset\r\n",
                    "noop\r\n",
                    "quit immediately\r\n",
                ],
                vec![
                    Request::Help {
                        argument: "my-command".to_string().into(),
                    },
                    Request::Rset,
                    Request::Noop,
                    Request::Quit,
                ],
            ),
            (
                vec![
                    "bdat 6\r\nabc",
                    "123",
                    "bdat  0  last \r\n",
                    " BDAT 1 last\r\n",
                    "a",
                ],
                vec![
                    Request::Bdat {
                        data: b"abc123".to_vec(),
                        is_last: false,
                    },
                    Request::Bdat {
                        data: vec![],
                        is_last: true,
                    },
                    Request::Bdat {
                        data: b"a".to_vec(),
                        is_last: true,
                    },
                ],
            ),
            (
                vec!["data\r\n", "hi\r\n", "..\r\n", ".a\r\n", "\r\n.\r\n"],
                vec![Request::Data {
                    data: b"hi\r\n.\r\na\r\n".to_vec(),
                }],
            ),
            (
                vec!["data\r\n", "\r\na\rb\nc\r\n.d\r\n.\re", "\r\n.\r\n"],
                vec![Request::Data {
                    data: b"\r\na\rb\nc\r\nd\r\n.\re".to_vec(),
                }],
            ),
        ] {
            let mut commands = Vec::new();
            for chunk in &chunks {
                let mut bytes = chunk.as_bytes().iter();
                loop {
                    match parser.parse(&mut bytes) {
                        Ok(command) => commands.push(command),
                        Err(Event::NeedsMoreBytes | Event::Data) => break,
                        Err(err) => panic!("{:?} for chunks {:#?}", err, chunks),
                    }
                }
            }
            assert_eq!(commands, expected_commands, "{:#?}", commands);
        }
    }
}

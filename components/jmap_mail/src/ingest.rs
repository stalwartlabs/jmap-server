use mail_parser::Message;

use crate::{parse::build_message_document, MessageStoreError};

pub fn ingest_message(raw_message: &[u8]) -> crate::Result<()> {
    let message = Message::parse(raw_message).ok_or(MessageStoreError::ParseError)?;

    let document = build_message_document(message)?;

    Ok(())
}

/*

Message 1
ID: 001

Message 2
ID: 002
References: 001

Message 3
ID: 003
References: 002

Message 4
ID: 004
References: 003

Message 5
ID: 005
References: 002, 001


1, 2, 3, 4, 5 =>
1 = ThreadId 1
2 = SELECT ids WHERE ref = 1 => id(1) => ThreadId 1
3 = SELECT ids WHERE ref = 2 => id(2) => ThreadId 1
4 = SELECT ids WHERE ref = 3 => id(3) => ThreadId 1
5 = SELECT ids WHERE ref = 2, 1 => id(1,2) => ThreadId 1

5, 4, 3, 2, 1 =>
5 = SELECT ids WHERE ref = 5, 2, 1 => NULL => ThreadId 5
4 = SELECT ids WHERE ref = 4, 3 => NULL => ThreadId 4
3 = SELECT ids WHERE ref = 3, 2 => id(4, 5) => ThreadId 4, 5 => Delete 5 => Merge => ThreadId 4
2 = SELECT ids WHERE ref = 2, 1 => id(3, 4, 5) => ThreadId 4
1 = SELECT ids WHERE ref = 1 => id(2, 3, 4, 5) => ThreadId 4


*/

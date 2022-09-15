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

#![no_main]
use jmap::types::blob::JMAPBlob;
use jmap::types::date::JMAPDate;
use jmap::types::jmap::JMAPId;
use jmap::types::json_pointer::JSONPointer;
use jmap::types::state::JMAPState;
use jmap_server::lmtp::request::RequestParser;
use libfuzzer_sys::fuzz_target;
use store::serialize::{
    base32::{Base32Reader, BASE32_ALPHABET},
    leb128::{Leb128Iterator, Leb128Reader},
};

static DATE_ALPHABET: &[u8] = b"0123456789TZ+-:.";
static POINTER_ALPHABET: &[u8] = b"0123456789abcdefghijklm~*/";
static LMTP_ALPHABET: &[u8] = b" MAILFROM:<>=";

fuzz_target!(|data: &[u8]| {
    // Leb128 decoding
    data.read_leb128::<usize>();
    data.skip_leb128();
    data.iter().next_leb128::<usize>();
    data.iter().skip_leb128();

    // Base32 reader
    let base32_data = into_alphabet(data, BASE32_ALPHABET);
    Base32Reader::new(data).for_each(|_| {});
    Base32Reader::new(&base32_data).for_each(|_| {});

    // JMAP Id
    let str_data = String::from_utf8_lossy(data);
    JMAPId::parse(&str_data);
    JMAPId::parse(std::str::from_utf8(&base32_data).unwrap());

    // JMAP Date
    JMAPDate::parse(&str_data);
    JMAPDate::parse(&String::from_utf8(into_alphabet(data, DATE_ALPHABET)).unwrap());

    // JMAP Blob
    let base32_data = String::from_utf8(base32_data).unwrap();
    JMAPBlob::parse(&str_data);
    JMAPBlob::parse(&base32_data);

    // JMAP State
    JMAPState::parse(&str_data);
    JMAPState::parse(&base32_data);

    // JSON Pointer
    JSONPointer::parse(&str_data);
    JSONPointer::parse(&String::from_utf8(into_alphabet(data, POINTER_ALPHABET)).unwrap());

    // LMTP Parser
    RequestParser::new(1024, 1024).parse(&mut data.iter()).ok();
    RequestParser::new(1024, 1024)
        .parse(&mut into_alphabet(data, LMTP_ALPHABET).iter())
        .ok();
});

fn into_alphabet(data: &[u8], alphabet: &[u8]) -> Vec<u8> {
    data.iter()
        .map(|&byte| alphabet[byte as usize % alphabet.len()])
        .collect()
}

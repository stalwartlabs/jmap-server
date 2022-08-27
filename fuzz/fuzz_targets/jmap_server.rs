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

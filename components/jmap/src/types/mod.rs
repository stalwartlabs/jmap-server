pub mod blob;
pub mod jmap;
pub mod json_pointer;
pub mod state;
pub mod type_state;

pub struct HexWriter {
    pub result: String,
}

impl HexWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        HexWriter {
            result: String::with_capacity(capacity),
        }
    }
}

impl std::io::Write for HexWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use std::fmt::Write;

        for &byte in buf {
            write!(&mut self.result, "{:02x}", byte).unwrap();
        }
        Ok(2 * buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[inline(always)]
pub fn hex_reader(id: &str, start_pos: usize) -> impl Iterator<Item = u8> + '_ {
    (start_pos..id.len())
        .step_by(2)
        .map(move |i| u8::from_str_radix(id.get(i..i + 2).unwrap_or(""), 16).unwrap_or(u8::MAX))
}

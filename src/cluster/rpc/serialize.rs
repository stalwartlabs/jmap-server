use super::Protocol;
use actix_web::web::{self, Buf};
use store::bincode;
use store::serialize::leb128::Leb128;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct RpcEncoder {
    max_frame_length: usize,
}

impl RpcEncoder {
    pub fn new(max_frame_length: usize) -> Self {
        Self { max_frame_length }
    }
}

impl Decoder for RpcEncoder {
    type Item = Protocol;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut web::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < std::mem::size_of::<u32>() {
            // Not enough data to read length marker.
            return Ok(None);
        }
        let (frame_len, bytes_read) = usize::from_leb128_bytes(src).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode frame length.",
            )
        })?;

        if frame_len > self.max_frame_length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", frame_len),
            ));
        } else if src.len() < bytes_read + frame_len {
            src.reserve(bytes_read + frame_len - src.len());
            return Ok(None);
        }

        let result = bincode::deserialize::<Protocol>(&src[bytes_read..bytes_read + frame_len])
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to deserialize RPC request.: {}", e),
                )
            });
        src.advance(bytes_read + frame_len);

        Ok(Some(result?))
    }
}

impl Encoder<Protocol> for RpcEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: Protocol, dst: &mut web::BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize RPC request.: {}", e),
            )
        })?;
        let mut bytes_len = Vec::with_capacity(std::mem::size_of::<u32>() + 1);
        bytes.len().to_leb128_bytes(&mut bytes_len);

        dst.reserve(bytes_len.len() + bytes.len());
        dst.extend_from_slice(&bytes_len);
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

use super::Protocol;
use actix_web::web::{self, Buf};
use store::{
    bincode,
    serialize::leb128::{Leb128Reader, Leb128Vec},
};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct RpcEncoder {}

const MAX_FRAME_LENGTH: usize = 50 * 1024 * 1024;

impl Decoder for RpcEncoder {
    type Item = Protocol;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut web::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < std::mem::size_of::<u32>() {
            // Not enough data to read length marker.
            return Ok(None);
        }
        let (frame_len, bytes_read) = src.as_ref().read_leb128::<usize>().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode frame length.",
            )
        })?;

        if frame_len > MAX_FRAME_LENGTH {
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
        bytes_len.push_leb128(bytes.len());

        dst.reserve(bytes_len.len() + bytes.len());
        dst.extend_from_slice(&bytes_len);
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

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

use super::Protocol;
use actix_web::web::{self, Buf};
use store::{
    bincode,
    serialize::leb128::{Leb128Reader, Leb128Vec},
};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct RpcEncoder {}

const MAX_FRAME_LENGTH: usize = 150 * 1024 * 1024;

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

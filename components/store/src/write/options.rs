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

pub struct IndexOptions {}

impl IndexOptions {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> u64 {
        0
    }
}

pub trait Options {
    const F_STORE: u64 = 0x01 << 32;
    const F_INDEX: u64 = 0x02 << 32;
    const F_CLEAR: u64 = 0x04 << 32;
    const F_NONE: u64 = 0;
    const F_KEYWORD: u64 = 1;
    const F_TOKENIZE: u64 = 2;
    const F_FULL_TEXT: u64 = 3;

    fn store(self) -> Self;
    fn index(self) -> Self;
    fn clear(self) -> Self;
    fn keyword(self) -> Self;
    fn tokenize(self) -> Self;
    fn full_text(self, part_id: u32) -> Self;

    fn is_store(&self) -> bool;
    fn is_index(&self) -> bool;
    fn is_clear(&self) -> bool;
    fn is_full_text(&self) -> bool;
    fn get_text_options(&self) -> u64;
}

impl Options for u64 {
    fn store(mut self) -> Self {
        self |= Self::F_STORE;
        self
    }

    fn index(mut self) -> Self {
        self |= Self::F_INDEX;
        self
    }

    fn keyword(self) -> Self {
        self | Self::F_KEYWORD
    }

    fn tokenize(self) -> Self {
        self | Self::F_TOKENIZE
    }

    fn full_text(self, part_id: u32) -> Self {
        self | (Self::F_FULL_TEXT + part_id as u64)
    }

    fn clear(mut self) -> Self {
        self |= Self::F_CLEAR;
        self
    }

    fn is_store(&self) -> bool {
        self & Self::F_STORE != 0
    }

    fn is_index(&self) -> bool {
        self & Self::F_INDEX != 0
    }

    fn is_clear(&self) -> bool {
        self & Self::F_CLEAR != 0
    }

    fn is_full_text(&self) -> bool {
        *self & 0xFFFFFFFF >= Self::F_FULL_TEXT
    }

    fn get_text_options(&self) -> u64 {
        *self & 0xFFFFFFFF
    }
}

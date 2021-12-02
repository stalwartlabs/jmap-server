use std::convert::TryInto;

use dashmap::mapref::entry::Entry;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use store::{
    field::TokenIterator,
    term_index::{MatchTerm, Term},
    StoreError, TermId,
};

use crate::RocksDBStore;

const LAST_TERM_KEY: &[u8; 1] = &[0];

impl RocksDBStore {
    pub fn get_match_terms(
        &self,
        mut tokens: TokenIterator,
    ) -> crate::Result<Option<Vec<MatchTerm>>> {
        let cf_terms = self
            .db
            .cf_handle("terms")
            .ok_or_else(|| StoreError::InternalError("No terms column family found.".into()))?;
        let mut result = Vec::with_capacity(10);
        let mut token_map = Vec::with_capacity(10);
        let mut query = Vec::with_capacity(10);

        while let Some(token) = tokens.next() {
            query.push((&cf_terms, Vec::from(token.word.as_bytes())));
            if token.is_exact {
                token_map.push((result.len(), true, tokens.stemmed_token.is_none()));
                result.push(MatchTerm {
                    id: 0,
                    id_stemmed: 0,
                })
            } else {
                token_map.push((result.len() - 1, false, true));
            }
        }

        if query.is_empty() {
            return Ok(None);
        }

        for (term_id, (pos, is_exact, is_last)) in self
            .db
            .multi_get_cf(query)
            .into_iter()
            .zip(token_map.into_iter())
        {
            let term = &mut result[pos];
            if let Some(term_id) = term_id.map_err(|e| StoreError::InternalError(e.to_string()))? {
                *(if is_exact {
                    &mut term.id
                } else {
                    &mut term.id_stemmed
                }) = TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                    StoreError::InternalError("Failed to deserialize term id.".into())
                })?);
            }
            if is_last && term.id == 0 && term.id_stemmed == 0 {
                return Ok(None);
            }
        }

        Ok(Some(result))
    }

    pub fn get_terms(&self, tokens: TokenIterator) -> crate::Result<Vec<Term>> {
        let cf_terms = self
            .db
            .cf_handle("terms")
            .ok_or_else(|| StoreError::InternalError("No terms column family found.".into()))?;
        let mut result = Vec::with_capacity(10);
        let mut query = Vec::with_capacity(10);
        let mut token_map = Vec::with_capacity(10);

        for token in tokens {
            query.push((&cf_terms, Vec::from(token.word.as_bytes())));
            if token.is_exact {
                result.push(Term::new(0, 0, &token));
            }
            self.term_id_lock
                .entry(token.word.to_string())
                .or_insert((0, 0))
                .value_mut()
                .1 += 1;
            token_map.push((result.len() - 1, token.word, token.is_exact));
        }

        for (term_id, (pos, word, is_exact)) in self
            .db
            .multi_get_cf(query)
            .into_iter()
            .zip(token_map.into_iter())
        {
            let term = &mut result[pos];
            let mut term_entry =
                if let Entry::Occupied(term_entry) = self.term_id_lock.entry(word.to_string()) {
                    term_entry
                } else {
                    panic!("Term not found in term_id_lock");
                };
            let term_lock = term_entry.get_mut();

            *(if is_exact {
                &mut term.id
            } else {
                &mut term.id_stemmed
            }) = if let Some(term_id) =
                term_id.map_err(|e| StoreError::InternalError(e.to_string()))?
            {
                let term_id = TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                    StoreError::InternalError("Failed to deserialize term id.".into())
                })?);
                if term_lock.0 == 0 {
                    term_lock.0 = term_id;
                }
                term_id
            } else if term_lock.0 == 0 {
                let term_id = {
                    let mut last_term = self.term_id_last.lock().unwrap();
                    *last_term += 1;
                    self.db
                        .put_cf(&cf_terms, LAST_TERM_KEY, (*last_term).to_le_bytes())
                        .map_err(|e| StoreError::InternalError(e.to_string()))?;
                    *last_term
                };
                self.db
                    .put_cf(&cf_terms, word.as_bytes(), term_id.to_le_bytes())
                    .map_err(|e| StoreError::InternalError(e.to_string()))?;

                term_lock.0 = term_id;
                term_id
            } else {
                term_lock.0
            };

            term_lock.1 -= 1;
            if term_lock.1 == 0 {
                term_entry.remove();
            }
        }

        Ok(result)
    }

    pub fn get_last_term_id(&self) -> crate::Result<TermId> {
        get_last_term_id(&self.db)
    }
}

pub fn get_last_term_id(db: &DBWithThreadMode<MultiThreaded>) -> crate::Result<TermId> {
    Ok(db
        .get_cf(
            &db.cf_handle("terms")
                .ok_or_else(|| StoreError::InternalError("No terms column family found.".into()))?,
            LAST_TERM_KEY,
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?
        .map(|v| TermId::from_le_bytes(v.try_into().unwrap()))
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use nlp::Language;
    use store::field::TokenIterator;

    use crate::RocksDBStore;

    const NUM_TOKENS: u64 = 10;
    const NUM_THREADS: usize = 20;

    #[test]
    fn unique_term_ids() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS)
            .build()
            .unwrap()
            .scope(|s| {
                let mut temp_dir = std::env::temp_dir();
                temp_dir.push("stalwart_termid_test");
                if temp_dir.exists() {
                    std::fs::remove_dir_all(&temp_dir).unwrap();
                }
                let db = Arc::new(RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap());

                for _ in 0..NUM_THREADS {
                    let t_db = db.clone();
                    s.spawn(move |_| {
                        let text = (0..NUM_TOKENS)
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(" ");
                        let mut term_ids = HashSet::new();
                        for term in t_db
                            .get_terms(TokenIterator::new(&text, Language::English, false))
                            .unwrap()
                        {
                            assert!((1..=NUM_TOKENS).contains(&term.id));
                            assert!(!term_ids.contains(&term.id));
                            term_ids.insert(term.id);
                        }
                        assert_eq!(term_ids.len(), NUM_TOKENS as usize);
                        assert_eq!(t_db.get_last_term_id().unwrap(), NUM_TOKENS);
                    });
                }

                std::fs::remove_dir_all(&temp_dir).unwrap();
            });
    }
}

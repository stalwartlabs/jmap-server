use std::{convert::TryInto, sync::atomic::Ordering};

use store::{
    field::TokenIterator,
    term_index::{MatchTerm, Term},
    StoreError, TermId,
};

use crate::{get_last_id, RocksDBStore, LAST_TERM_ID_KEY};

#[derive(Debug, Default)]
pub struct TermLock {
    pub term_id: TermId,
    pub lock_count: usize,
}

impl RocksDBStore {
    pub fn get_match_terms(&self, tokens: TokenIterator) -> crate::Result<Option<Vec<MatchTerm>>> {
        let cf_terms = self.get_handle("terms")?;
        let mut result = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            // First try obtaining the term ID from the cache
            let term_id = if let Some(term_id) = self.term_id_cache.get(&token_word) {
                term_id
            } else {
                // Retrieve the ID from the KV store
                let term_id = if let Some(term_id) = self
                    .db
                    .get_cf(&cf_terms, token_word.as_bytes())
                    .map_err(|e| StoreError::InternalError(e.to_string()))?
                {
                    TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                        StoreError::InternalError("Failed to deserialize term id.".into())
                    })?)
                } else {
                    // Term does not exist, return None.
                    return Ok(None);
                };

                // Add term to the cache
                self.term_id_cache.insert(token_word, term_id);
                term_id
            };

            if token.is_exact {
                result.push(MatchTerm {
                    id: term_id,
                    id_stemmed: term_id,
                });
            } else {
                result.last_mut().unwrap().id_stemmed = term_id;
            }
        }

        Ok(if !result.is_empty() {
            Some(result)
        } else {
            None
        })
    }

    pub fn get_terms(&self, tokens: TokenIterator) -> crate::Result<Vec<Term>> {
        let cf_terms = self.get_handle("terms")?;
        let cf_values = self.get_handle("values")?;

        let mut result = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            // First try obtaining the term ID from the cache
            let term_id = if let Some(term_id) = self.term_id_cache.get(&token_word) {
                term_id
            } else {
                // Lock the term
                let _term_lock = self.term_id_lock.lock_hash(&token_word);

                // Retrieve the ID from the KV store
                let term_id = if let Some(term_id) = self
                    .db
                    .get_cf(&cf_terms, token_word.as_bytes())
                    .map_err(|e| StoreError::InternalError(e.to_string()))?
                {
                    TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                        StoreError::InternalError("Failed to deserialize term id.".into())
                    })?)
                } else {
                    // Term does not exist, create it.
                    let term_id = self.term_id_last.fetch_add(1, Ordering::Relaxed);
                    //TODO on unclean exists retrieve last id manually
                    self.db
                        .merge_cf(&cf_values, LAST_TERM_ID_KEY, (1u64).to_le_bytes())
                        .map_err(|e| StoreError::InternalError(e.to_string()))?;
                    self.db
                        .put_cf(&cf_terms, token_word.as_bytes(), term_id.to_le_bytes())
                        .map_err(|e| StoreError::InternalError(e.to_string()))?;
                    term_id
                };

                // Add term to the cache
                self.term_id_cache.insert(token_word, term_id);
                term_id
            };

            if token.is_exact {
                result.push(Term::new(term_id, term_id, token.offset, token.len));
            } else {
                result.last_mut().unwrap().id_stemmed = term_id;
            }
        }

        Ok(result)
    }

    pub fn get_last_term_id(&self) -> crate::Result<TermId> {
        get_last_id(&self.db, LAST_TERM_ID_KEY)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use nlp::Language;
    use store::{
        field::TokenIterator,
        term_index::{MatchTerm, Term},
        Store,
    };

    use crate::{RocksDBStore, RocksDBStoreConfig};

    const NUM_TOKENS: u64 = 10;
    const NUM_THREADS: usize = 20;

    #[test]
    fn stemmed_duplicates() {
        const TEXT: &str = "love loving lovingly loved lovely";

        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_sd_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        let db = RocksDBStore::open(RocksDBStoreConfig::default_config(
            temp_dir.to_str().unwrap(),
        ))
        .unwrap();

        assert_eq!(
            db.get_terms(TokenIterator::new(TEXT, Language::English, true))
                .unwrap(),
            vec![
                Term {
                    id: 0,
                    id_stemmed: 0,
                    offset: 0,
                    len: 4
                },
                Term {
                    id: 1,
                    id_stemmed: 0,
                    offset: 5,
                    len: 6
                },
                Term {
                    id: 2,
                    id_stemmed: 0,
                    offset: 12,
                    len: 8
                },
                Term {
                    id: 3,
                    id_stemmed: 0,
                    offset: 21,
                    len: 5
                },
                Term {
                    id: 4,
                    id_stemmed: 0,
                    offset: 27,
                    len: 6
                }
            ]
        );

        assert_eq!(
            db.get_match_terms(TokenIterator::new(TEXT, Language::English, true))
                .unwrap()
                .unwrap(),
            vec![
                MatchTerm {
                    id: 0,
                    id_stemmed: 0
                },
                MatchTerm {
                    id: 1,
                    id_stemmed: 0
                },
                MatchTerm {
                    id: 2,
                    id_stemmed: 0
                },
                MatchTerm {
                    id: 3,
                    id_stemmed: 0
                },
                MatchTerm {
                    id: 4,
                    id_stemmed: 0
                }
            ]
        );

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn concurrent_duplicates() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS)
            .build()
            .unwrap()
            .scope(|s| {
                let mut temp_dir = std::env::temp_dir();
                temp_dir.push("strdb_cd_test");
                if temp_dir.exists() {
                    std::fs::remove_dir_all(&temp_dir).unwrap();
                }
                let db = Arc::new(
                    RocksDBStore::open(RocksDBStoreConfig::default_config(
                        temp_dir.to_str().unwrap(),
                    ))
                    .unwrap(),
                );

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
                            assert!((0..NUM_TOKENS).contains(&term.id));
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

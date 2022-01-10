use std::{collections::HashMap, convert::TryInto, sync::atomic::Ordering};

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
        let mut query = Vec::with_capacity(10);

        let mut word_dict = HashMap::new();

        for token in tokens {
            let word_pos = if let Some(word_pos) = word_dict.get(&token.word) {
                *word_pos
            } else {
                query.push((&cf_terms, Vec::from(token.word.as_bytes())));

                let word_pos = (word_dict.len() + 1) as u64;
                word_dict.insert(token.word.clone(), word_pos);
                word_pos
            };

            if token.is_exact {
                result.push(MatchTerm {
                    id: word_pos,
                    id_stemmed: 0,
                })
            } else {
                result.last_mut().unwrap().id_stemmed = word_pos;
            }
        }

        if query.is_empty() {
            return Ok(None);
        }

        let mut id_list = vec![0u64; word_dict.len()];

        for (term_id, id) in self
            .db
            .multi_get_cf(query)
            .into_iter()
            .zip(id_list.iter_mut())
        {
            if let Some(term_id) = term_id.map_err(|e| StoreError::InternalError(e.to_string()))? {
                *id = TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                    StoreError::InternalError("Failed to deserialize term id.".into())
                })?);
            }
        }

        for term in &mut result {
            if term.id == 0 {
                return Ok(None);
            }
            term.id = id_list[(term.id - 1) as usize];
            if term.id_stemmed > 0 {
                term.id_stemmed = id_list[(term.id_stemmed - 1) as usize];
            }
        }

        Ok(Some(result))
    }

    pub fn get_terms(&self, tokens: TokenIterator) -> crate::Result<Vec<Term>> {
        let cf_terms = self.get_handle("terms")?;
        let cf_values = self.get_handle("values")?;

        let mut result = Vec::with_capacity(10);
        let mut query = Vec::with_capacity(10);

        let mut word_dict = HashMap::with_capacity(10);
        let mut word_list = Vec::with_capacity(10);

        for mut token in tokens {
            let word_pos = if let Some(word_pos) = word_dict.get(&token.word) {
                *word_pos
            } else {
                self.term_id_lock
                    .entry(token.word.to_string())
                    .or_insert(TermLock::default())
                    .value_mut()
                    .lock_count += 1;
                query.push((&cf_terms, Vec::from(token.word.as_bytes())));

                let word_pos = (word_list.len() + 1) as u64;
                word_dict.insert(token.word.clone(), word_pos);
                word_list.push((0u64, std::mem::take(&mut token.word)));
                word_pos
            };

            if token.is_exact {
                result.push(Term::new(word_pos, 0, &token));
            } else {
                result.last_mut().unwrap().id_stemmed = word_pos;
            }
        }

        for (term_id, (word_id, word)) in self
            .db
            .multi_get_cf(query)
            .into_iter()
            .zip(word_list.iter_mut())
        {
            let mut term_entry = if let dashmap::mapref::entry::Entry::Occupied(term_entry) =
                self.term_id_lock.entry(word.to_string())
            {
                term_entry
            } else {
                // This should never happen.
                panic!("Term not found in term_id_lock");
            };
            let term_lock = term_entry.get_mut();

            *word_id = if let Some(term_id) =
                term_id.map_err(|e| StoreError::InternalError(e.to_string()))?
            {
                let term_id = TermId::from_le_bytes(term_id.try_into().map_err(|_| {
                    StoreError::InternalError("Failed to deserialize term id.".into())
                })?);
                if term_lock.term_id == 0 {
                    term_lock.term_id = term_id;
                }
                term_id
            } else if term_lock.term_id == 0 {
                let term_id = self.term_id_last.fetch_add(1, Ordering::Relaxed);
                //TODO on unclean exists retrieve last id manually
                self.db
                    .merge_cf(&cf_values, LAST_TERM_ID_KEY, (1u64).to_le_bytes())
                    .map_err(|e| StoreError::InternalError(e.to_string()))?;
                self.db
                    .put_cf(&cf_terms, word.as_bytes(), term_id.to_le_bytes())
                    .map_err(|e| StoreError::InternalError(e.to_string()))?;

                term_lock.term_id = term_id;
                term_id
            } else {
                term_lock.term_id
            };

            term_lock.lock_count -= 1;
            if term_lock.lock_count == 0 {
                term_entry.remove();
            }
        }

        for term in &mut result {
            term.id = word_list[(term.id - 1) as usize].0;
            if term.id_stemmed > 0 {
                term.id_stemmed = word_list[(term.id_stemmed - 1) as usize].0;
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
                    id: 1,
                    id_stemmed: 0,
                    offset: 0,
                    len: 4
                },
                Term {
                    id: 2,
                    id_stemmed: 1,
                    offset: 5,
                    len: 6
                },
                Term {
                    id: 3,
                    id_stemmed: 1,
                    offset: 12,
                    len: 8
                },
                Term {
                    id: 4,
                    id_stemmed: 1,
                    offset: 21,
                    len: 5
                },
                Term {
                    id: 5,
                    id_stemmed: 1,
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
                    id: 1,
                    id_stemmed: 0
                },
                MatchTerm {
                    id: 2,
                    id_stemmed: 1
                },
                MatchTerm {
                    id: 3,
                    id_stemmed: 1
                },
                MatchTerm {
                    id: 4,
                    id_stemmed: 1
                },
                MatchTerm {
                    id: 5,
                    id_stemmed: 1
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

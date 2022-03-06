use std::sync::atomic::Ordering;

use crate::{
    field::TokenIterator,
    serialize::LAST_TERM_ID_KEY,
    term_index::{MatchTerm, Term},
    ColumnFamily, JMAPStore, Store, TermId, WriteOperation,
};

#[derive(Debug, Default)]
pub struct TermLock {
    pub term_id: TermId,
    pub lock_count: usize,
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_match_terms(
        &self,
        tokens: TokenIterator<'_>,
    ) -> crate::Result<Option<Vec<MatchTerm>>> {
        let mut result = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            // First try obtaining the term ID from the cache
            let term_id = if let Some(term_id) = self.term_id_cache.get(&token_word) {
                term_id
            } else {
                // Retrieve the ID from the KV store
                if let Some(term_id) = self
                    .get(ColumnFamily::Terms, token_word.as_bytes().to_vec())
                    .await?
                {
                    // Add term to the cache
                    self.term_id_cache.insert(token_word, term_id).await;
                    term_id
                } else {
                    // Term does not exist, return None.
                    return Ok(None);
                }
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

    pub async fn get_terms(&self, tokens: TokenIterator<'_>) -> crate::Result<Vec<Term>> {
        let mut result = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            // First try obtaining the term ID from the cache
            let term_id = if let Some(term_id) = self.term_id_cache.get(&token_word) {
                term_id
            } else {
                // Lock the term
                let _term_lock = self.term_id_lock.lock_hash(&token_word).await;

                // Retrieve the ID from the KV store
                let term_id = if let Some(term_id) = self
                    .get(ColumnFamily::Terms, token_word.as_bytes().to_vec())
                    .await?
                {
                    term_id
                } else {
                    // Term does not exist, create it.
                    let term_id = self.term_id_last.fetch_add(1, Ordering::Relaxed);
                    //TODO on unclean exists retrieve last id manually
                    let mut batch = Vec::with_capacity(2);
                    batch.push(WriteOperation::Merge {
                        cf: ColumnFamily::Values,
                        key: LAST_TERM_ID_KEY.to_vec(),
                        value: (1u64).to_le_bytes().into(),
                    });
                    batch.push(WriteOperation::Set {
                        cf: ColumnFamily::Terms,
                        key: token_word.as_bytes().to_vec(),
                        value: term_id.to_le_bytes().into(),
                    });
                    self.write(batch).await?;
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

    /*pub fn get_last_term_id(&self) -> crate::Result<TermId> {
        get_last_id(&self.db, LAST_TERM_ID_KEY)
    }*/
}

//TODO test

/*
#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use nlp::Language;

    use crate::{
        field::TokenIterator,
        term_index::{MatchTerm, Term},
    };

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
*/

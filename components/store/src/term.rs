use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use tokio::sync::watch;

use crate::{
    field::TokenIterator,
    serialize::{StoreSerialize, LAST_TERM_ID_KEY},
    term_index::{MatchTerm, Term},
    ColumnFamily, JMAPStore, Store, TermId, WriteOperation,
};

#[derive(Debug, Clone)]
pub enum CachedTerm {
    Term(TermId),
    InFlight(watch::Receiver<TermId>),
}

enum CacheRxTx {
    Rx(watch::Receiver<TermId>),
    Tx(watch::Sender<TermId>),
    None,
}

impl CacheRxTx {
    pub fn is_none(&self) -> bool {
        matches!(self, CacheRxTx::None)
    }
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
                    self.term_id_cache.insert(token_word, term_id);
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
                    self.write(vec![
                        WriteOperation::Merge {
                            cf: ColumnFamily::Values,
                            key: LAST_TERM_ID_KEY.to_vec(),
                            value: (1u64).serialize().unwrap(),
                        },
                        WriteOperation::Set {
                            cf: ColumnFamily::Terms,
                            key: token_word.as_bytes().to_vec(),
                            value: term_id.serialize().unwrap(),
                        },
                    ])
                    .await?;
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

    /*
    pub async fn get_match_terms(
        &self,
        tokens: TokenIterator<'_>,
    ) -> crate::Result<Option<Vec<MatchTerm>>> {
        let mut results = Vec::with_capacity(10);
        let mut cache_misses = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            // First try obtaining the term ID from the cache
            let term_id = if let Some(cached_term) = self_id_cache.get(&token_word) {
                match cached_term {
                    CachedTerm::Term(term_id) => term_id,
                    CachedTerm::InFlight(mut rx) => {
                        if let Err(err) = rx.changed().await {
                            debug!("Error waiting for term ID: {}", err);
                        }
                        let term_id = *rx.borrow();
                        if term_id == TermId::MAX {
                            return Err(StoreError::NotFound);
                        }
                        term_id
                    }
                }
            } else {
                cache_misses.push((token_word, token.is_exact, results.len()));

                TermId::MAX
            };

            if token.is_exact {
                results.push(MatchTerm {
                    id: term_id,
                    id_stemmed: term_id,
                });
            } else {
                results.last_mut().unwrap().id_stemmed = term_id;
            }
        }

        if !cache_misses.is_empty() {
            for (term_id, (word, is_exact, pos)) in self
                .multi_get::<TermId>(
                    ColumnFamily::Terms,
                    cache_misses
                        .iter()
                        .map(|(word, _, _)| word.as_bytes().to_vec())
                        .collect(),
                )
                .await?
                .into_iter()
                .zip(cache_misses)
            {
                if let Some(term_id) = term_id {
                    // Add term to the cache
                    self_id_cache.insert(word, CachedTerm::Term(term_id));
                    if is_exact {
                        results[pos].id = term_id;
                        results[pos].id_stemmed = term_id;
                    } else {
                        results[pos - 1].id_stemmed = term_id;
                    }
                } else {
                    return Ok(None);
                }
            }
        }

        Ok(if !results.is_empty() {
            Some(results)
        } else {
            None
        })
    }

    pub async fn get_terms(&self, tokens: TokenIterator<'_>) -> crate::Result<Vec<Term>> {
        let mut results = Vec::with_capacity(10);
        let mut cache_misses = Vec::with_capacity(10);
        let mut await_terms = Vec::with_capacity(10);

        for token in tokens {
            let token_word = token.word.into_owned();
            let mut cache_miss = CacheRxTx::None;

            // First try obtaining the term ID from the cache
            let term_id = match self
                .term_id_cache
                .get_or_insert_with(token_word.clone(), || {
                    let (tx, rx) = watch::channel(TermId::MAX);
                    cache_miss = CacheRxTx::Tx(tx);
                    CachedTerm::InFlight(rx)
                }) {
                CachedTerm::Term(term_id) => term_id,
                CachedTerm::InFlight(rx) if cache_miss.is_none() => {
                    cache_miss = CacheRxTx::Rx(rx);
                    TermId::MAX
                }
                _ => TermId::MAX,
            };

            match cache_miss {
                CacheRxTx::Rx(rx) => {
                    await_terms.push((token.is_exact, results.len(), rx));
                }
                CacheRxTx::Tx(tx) => {
                    cache_misses.push((token_word, token.is_exact, results.len(), tx));
                }
                CacheRxTx::None => (),
            }

            if token.is_exact {
                results.push(Term::new(term_id, term_id, token.offset, token.len));
            } else {
                results.last_mut().unwrap().id_stemmed = term_id;
            }
        }

        if !cache_misses.is_empty() {
            let mut insert_terms = Vec::with_capacity(cache_misses.len());

            for (term_id, (word, is_exact, pos, tx)) in self
                .multi_get::<TermId>(
                    ColumnFamily::Terms,
                    cache_misses
                        .iter()
                        .map(|(word, _, _, _)| word.as_bytes().to_vec())
                        .collect(),
                )
                .await?
                .into_iter()
                .zip(cache_misses)
            {
                if let Some(term_id) = term_id {
                    // Notify other processes
                    tx.send(term_id).ok();

                    // Add term to the cache
                    self_id_cache.insert(word, CachedTerm::Term(term_id));
                    if is_exact {
                        results[pos].id = term_id;
                        results[pos].id_stemmed = term_id;
                    } else {
                        results[pos - 1].id_stemmed = term_id;
                    }
                } else {
                    insert_terms.push((word, is_exact, pos, tx));
                }
            }

            if !insert_terms.is_empty() {
                let mut write_batch = Vec::with_capacity(insert_terms.len() * 2);
                let mut term_ids = Vec::with_capacity(insert_terms.len());

                for (word, _, _, _) in &insert_terms {
                    let term_id = self_id_last.fetch_add(1, Ordering::Relaxed);

                    write_batch.push(WriteOperation::Merge {
                        cf: ColumnFamily::Values,
                        key: LAST_TERM_ID_KEY.to_vec(),
                        value: (1u64).serialize().unwrap(),
                    });
                    write_batch.push(WriteOperation::Set {
                        cf: ColumnFamily::Terms,
                        key: word.as_bytes().to_vec(),
                        value: term_id.serialize().unwrap(),
                    });
                    term_ids.push(term_id);
                }

                if let Err(err) = self.write(write_batch).await {
                    for (word, _, _, tx) in insert_terms {
                        tx.send(TermId::MAX).ok();
                        self_id_cache.invalidate(&word);
                    }

                    return Err(err);
                } else {
                    for ((word, is_exact, pos, tx), term_id) in
                        insert_terms.into_iter().zip(term_ids)
                    {
                        // Add term to the cache
                        self_id_cache.insert(word, CachedTerm::Term(term_id));

                        // Notify other processes
                        tx.send(term_id).ok();

                        if is_exact {
                            results[pos].id = term_id;
                            results[pos].id_stemmed = term_id;
                        } else {
                            results[pos - 1].id_stemmed = term_id;
                        }
                    }
                }
            }
        }

        if !await_terms.is_empty() {
            for (is_exact, pos, mut rx) in await_terms {
                if let Err(err) = rx.changed().await {
                    debug!("Error waiting for term ID: {}", err);
                }
                let term_id = *rx.borrow();
                if term_id == TermId::MAX {
                    return Err(StoreError::NotFound);
                }

                if is_exact {
                    results[pos].id = term_id;
                    results[pos].id_stemmed = term_id;
                } else {
                    results[pos - 1].id_stemmed = term_id;
                }
            }
        }

        Ok(results)
    }*/
}

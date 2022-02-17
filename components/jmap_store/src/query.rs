use store::{
    AccountId, CollectionId, Comparator, DocumentId, DocumentSet, Filter, FilterOperator,
    LogicalOperator, Store,
};

use crate::{JMAPComparator, JMAPError, JMAPFilter, JMAPId, JMAPLogicalOperator};

struct QueryState<T, U>
where
    T: DocumentSet,
{
    op: JMAPLogicalOperator,
    terms: Vec<Filter<T>>,
    it: std::vec::IntoIter<JMAPFilter<U>>,
}

pub fn build_query<'x, T, U, V, W, X>(
    store: &'x T,
    account_id: AccountId,
    collection_id: CollectionId,
    filter: JMAPFilter<U>,
    sort: Vec<JMAPComparator<V>>,
    mut cond_fnc: W,
    mut sort_fnc: X,
) -> crate::Result<T::Iter>
where
    T: Store<'x>,
    W: FnMut(U) -> crate::Result<Filter<T::Set>>,
    X: FnMut(JMAPComparator<V>) -> crate::Result<Comparator<T::Set>>,
{
    let state: Option<QueryState<T::Set, U>> = match filter {
        JMAPFilter::Operator(op) => Some(QueryState {
            op: op.operator,
            terms: Vec::with_capacity(op.conditions.len()),
            it: op.conditions.into_iter(),
        }),
        JMAPFilter::None => None,
        cond => Some(QueryState {
            op: JMAPLogicalOperator::And,
            it: vec![cond].into_iter(),
            terms: Vec::with_capacity(1),
        }),
    };

    store
        .query(
            account_id,
            collection_id,
            if let Some(mut state) = state {
                let mut state_stack = Vec::new();
                let mut filter;

                'outer: loop {
                    while let Some(term) = state.it.next() {
                        match term {
                            JMAPFilter::Condition(cond) => {
                                state.terms.push(cond_fnc(cond)?);
                            }
                            JMAPFilter::Operator(op) => {
                                let new_state = QueryState {
                                    op: op.operator,
                                    terms: Vec::with_capacity(op.conditions.len()),
                                    it: op.conditions.into_iter(),
                                };
                                state_stack.push(state);
                                state = new_state;
                            }
                            JMAPFilter::None => {}
                        }
                    }

                    filter = Filter::Operator(FilterOperator {
                        operator: match state.op {
                            JMAPLogicalOperator::And => LogicalOperator::And,
                            JMAPLogicalOperator::Or => LogicalOperator::Or,
                            JMAPLogicalOperator::Not => LogicalOperator::Not,
                        },
                        conditions: state.terms,
                    });

                    if let Some(prev_state) = state_stack.pop() {
                        state = prev_state;
                        state.terms.push(filter);
                    } else {
                        break 'outer;
                    }
                }

                filter
            } else {
                Filter::None
            },
            if !sort.is_empty() {
                let mut terms: Vec<Comparator<T::Set>> = Vec::with_capacity(sort.len());
                for comp in sort {
                    terms.push(sort_fnc(comp)?);
                }
                Comparator::List(terms)
            } else {
                Comparator::None
            },
        )
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
pub fn paginate_results<U, V>(
    doc_ids: impl Iterator<Item = DocumentId>,
    num_results: usize,
    limit: usize,
    mut position: i32,
    anchor: Option<u64>,
    mut anchor_offset: i32,
    use_filter: bool,
    mut map_one: Option<U>,
    map_many: Option<V>,
) -> crate::Result<(Vec<JMAPId>, usize)>
where
    U: FnMut(DocumentId) -> crate::Result<Option<JMAPId>>,
    V: FnMut(Vec<DocumentId>) -> crate::Result<Vec<JMAPId>>,
{
    let mut start_position: usize = 0;

    let results = if use_filter || anchor.is_some() {
        let has_anchor = anchor.is_some();
        let mut results = Vec::with_capacity(if limit > 0 { limit } else { num_results });
        let mut anchor_found = false;

        for doc_id in doc_ids {
            let result = if let Some(map_one) = &mut map_one {
                if let Some(jmap_id) = map_one(doc_id)? {
                    jmap_id
                } else {
                    continue;
                }
            } else {
                doc_id as JMAPId
            };

            if !has_anchor {
                if position >= 0 {
                    if position > 0 {
                        position -= 1;
                    } else {
                        results.push(result);
                        if limit > 0 && results.len() == limit {
                            break;
                        }
                    }
                } else {
                    results.push(result);
                }
            } else if anchor_offset >= 0 {
                if !anchor_found {
                    if &result != anchor.as_ref().unwrap() {
                        continue;
                    }
                    anchor_found = true;
                }

                if anchor_offset > 0 {
                    anchor_offset -= 1;
                } else {
                    results.push(result);
                    if limit > 0 && results.len() == limit {
                        break;
                    }
                }
            } else {
                anchor_found = &result == anchor.as_ref().unwrap();
                results.push(result);

                if !anchor_found {
                    continue;
                }

                position = anchor_offset;

                break;
            }
        }

        if !has_anchor || anchor_found {
            if position >= 0 {
                start_position = position as usize;
                results
            } else {
                let position = position.abs() as usize;
                let start_offset = if position < results.len() {
                    results.len() - position
                } else {
                    0
                };
                start_position = start_offset;
                let end_offset = if limit > 0 {
                    std::cmp::min(start_offset + limit, results.len())
                } else {
                    results.len()
                };

                results[start_offset..end_offset].to_vec()
            }
        } else {
            return Err(JMAPError::AnchorNotFound);
        }
    } else {
        let doc_ids = if position != 0 && limit > 0 {
            start_position = if position > 0 {
                position as usize
            } else {
                let position = position.abs();
                if num_results > position as usize {
                    num_results - position as usize
                } else {
                    0
                }
            };
            doc_ids
                .skip(start_position)
                .take(limit)
                .collect::<Vec<DocumentId>>()
        } else if limit > 0 {
            doc_ids.take(limit).collect::<Vec<DocumentId>>()
        } else if position != 0 {
            start_position = if position > 0 {
                position as usize
            } else {
                let position = position.abs();
                if num_results > position as usize {
                    num_results - position as usize
                } else {
                    0
                }
            };
            doc_ids.skip(start_position).collect::<Vec<DocumentId>>()
        } else {
            doc_ids.collect::<Vec<DocumentId>>()
        };

        if let Some(mut map_many) = map_many {
            map_many(doc_ids)?
        } else {
            doc_ids.into_iter().map(|doc_id| doc_id as JMAPId).collect()
        }
    };

    Ok((results, start_position))
}

use store::{
    query::JMAPStoreQuery, Comparator, DocumentId, Filter, FilterOperator, Collection, JMAPId,
    LogicalOperator,
};

use crate::{
    changes::JMAPState, JMAPComparator, JMAPError, JMAPFilter, JMAPLogicalOperator,
    JMAPQueryRequest, JMAPQueryResponse,
};

struct QueryState<T> {
    op: JMAPLogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<JMAPFilter<T>>,
}

impl<T, U, V> JMAPQueryRequest<T, U, V> {
    pub fn build_query<W, X, Y>(
        &mut self,
        collection: Collection,
        mut condition_map_fnc: W,
        mut comparator_map_fnc: X,
        filter_map_fnc: Option<Y>,
    ) -> store::Result<JMAPStoreQuery<Y>>
    where
        W: FnMut(T) -> store::Result<Filter>,
        X: FnMut(JMAPComparator<U>) -> store::Result<Comparator>,
        Y: FnMut(DocumentId) -> store::Result<Option<JMAPId>>,
    {
        let state: Option<QueryState<T>> = match std::mem::take(&mut self.filter) {
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

        let filter = if let Some(mut state) = state {
            let mut state_stack = Vec::new();
            let mut filter;

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term {
                        JMAPFilter::Condition(cond) => {
                            state.terms.push(condition_map_fnc(cond)?);
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
        };

        let sort = if !self.sort.is_empty() {
            let mut terms: Vec<Comparator> = Vec::with_capacity(self.sort.len());
            for comp in std::mem::take(&mut self.sort) {
                terms.push(comparator_map_fnc(comp)?);
            }
            Comparator::List(terms)
        } else {
            Comparator::None
        };

        Ok(JMAPStoreQuery {
            account_id: self.account_id,
            collection,
            filter_map_fnc,
            filter,
            sort,
        })
    }

    pub fn into_response<W>(
        mut self,
        jmap_ids: W,
        query_state: JMAPState,
        is_immutable: bool,
    ) -> crate::Result<JMAPQueryResponse>
    where
        W: Iterator<Item = JMAPId>,
    {
        let has_anchor = self.anchor.is_some();
        let total_results = jmap_ids.size_hint().0;
        let mut results = Vec::with_capacity(if self.limit > 0 {
            self.limit
        } else {
            total_results
        });
        let mut anchor_found = false;

        for jmap_id in jmap_ids {
            if !has_anchor {
                if self.position >= 0 {
                    if self.position > 0 {
                        self.position -= 1;
                    } else {
                        results.push(jmap_id);
                        if self.limit > 0 && results.len() == self.limit {
                            break;
                        }
                    }
                } else {
                    results.push(jmap_id);
                }
            } else if self.anchor_offset >= 0 {
                if !anchor_found {
                    if &jmap_id != self.anchor.as_ref().unwrap() {
                        continue;
                    }
                    anchor_found = true;
                }

                if self.anchor_offset > 0 {
                    self.anchor_offset -= 1;
                } else {
                    results.push(jmap_id);
                    if self.limit > 0 && results.len() == self.limit {
                        break;
                    }
                }
            } else {
                anchor_found = &jmap_id == self.anchor.as_ref().unwrap();
                results.push(jmap_id);

                if !anchor_found {
                    continue;
                }

                self.position = self.anchor_offset;

                break;
            }
        }

        let start_position;
        let results = if !has_anchor || anchor_found {
            if self.position >= 0 {
                start_position = self.position as usize;
                results
            } else {
                let position = self.position.abs() as usize;
                let start_offset = if position < results.len() {
                    results.len() - position
                } else {
                    0
                };
                start_position = start_offset;
                let end_offset = if self.limit > 0 {
                    std::cmp::min(start_offset + self.limit, results.len())
                } else {
                    results.len()
                };

                results[start_offset..end_offset].to_vec()
            }
        } else {
            return Err(JMAPError::AnchorNotFound);
        };

        Ok(JMAPQueryResponse {
            account_id: self.account_id,
            include_total: self.calculate_total,
            query_state,
            position: start_position,
            total: total_results,
            limit: self.limit,
            ids: results,
            is_immutable,
        })
    }
}

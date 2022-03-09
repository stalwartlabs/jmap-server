use store::{Comparator, Filter, FilterOperator, JMAPId, LogicalOperator};

use crate::{JMAPComparator, JMAPError, JMAPFilter, JMAPLogicalOperator};

struct QueryState<T> {
    op: JMAPLogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<JMAPFilter<T>>,
}

pub fn build_query<U, V, W, X>(
    filter: JMAPFilter<U>,
    sort: Vec<JMAPComparator<V>>,
    mut cond_fnc: W,
    mut sort_fnc: X,
) -> crate::Result<(Filter, Comparator)>
where
    W: FnMut(U) -> crate::Result<Filter>,
    X: FnMut(JMAPComparator<V>) -> crate::Result<Comparator>,
{
    let state: Option<QueryState<U>> = match filter {
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

    Ok((
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
            let mut terms: Vec<Comparator> = Vec::with_capacity(sort.len());
            for comp in sort {
                terms.push(sort_fnc(comp)?);
            }
            Comparator::List(terms)
        } else {
            Comparator::None
        },
    ))
}

pub fn paginate_results(
    jmap_ids: impl Iterator<Item = JMAPId>,
    num_results: usize,
    limit: usize,
    mut position: i32,
    anchor: Option<u64>,
    mut anchor_offset: i32,
) -> crate::Result<(Vec<JMAPId>, usize)> {
    let has_anchor = anchor.is_some();
    let mut results = Vec::with_capacity(if limit > 0 { limit } else { num_results });
    let mut anchor_found = false;

    for jmap_id in jmap_ids {
        if !has_anchor {
            if position >= 0 {
                if position > 0 {
                    position -= 1;
                } else {
                    results.push(jmap_id);
                    if limit > 0 && results.len() == limit {
                        break;
                    }
                }
            } else {
                results.push(jmap_id);
            }
        } else if anchor_offset >= 0 {
            if !anchor_found {
                if &jmap_id != anchor.as_ref().unwrap() {
                    continue;
                }
                anchor_found = true;
            }

            if anchor_offset > 0 {
                anchor_offset -= 1;
            } else {
                results.push(jmap_id);
                if limit > 0 && results.len() == limit {
                    break;
                }
            }
        } else {
            anchor_found = &jmap_id == anchor.as_ref().unwrap();
            results.push(jmap_id);

            if !anchor_found {
                continue;
            }

            position = anchor_offset;

            break;
        }
    }

    let start_position;
    let results = if !has_anchor || anchor_found {
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
    };

    Ok((results, start_position))
}

use std::sync::Arc;

use store::{
    core::JMAPIdPrefix,
    read::{
        comparator::Comparator,
        filter::{Filter, FilterOperator, LogicalOperator},
    },
    roaring::RoaringBitmap,
    AccountId, DocumentId, JMAPStore, SharedBitmap, Store,
};

use crate::{
    error::method::MethodError,
    request::{
        query::{self, FilterDeserializer, QueryRequest, QueryResponse},
        ACLEnforce,
    },
    types::jmap::JMAPId,
};

use super::{changes::JMAPChanges, Object};

pub struct QueryHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject,
{
    pub store: &'y JMAPStore<T>,
    pub shared_documents: Option<Arc<Option<RoaringBitmap>>>,
    pub account_id: AccountId,
    pub request: QueryRequest<O>,
    pub filter: Filter,
    pub comparator: Comparator,
}

pub trait QueryObject: Object {
    type QueryArguments;
    type Filter: FilterDeserializer;
    type Comparator: for<'de> serde::Deserialize<'de>;
}

pub type ExtraFilterFnc = fn(Vec<JMAPId>) -> crate::Result<Vec<JMAPId>>;

struct QueryState<O: QueryObject> {
    op: LogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<query::Filter<O::Filter>>,
}

impl<'y, O, T> QueryHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject,
{
    pub fn new(
        store: &'y JMAPStore<T>,
        request: QueryRequest<O>,
        shared_documents: Option<
            impl FnOnce(AccountId, &[AccountId]) -> store::Result<Arc<Option<RoaringBitmap>>>,
        >,
    ) -> crate::Result<Self> {
        let account_id = request.account_id.into();
        let acl = request.acl.as_ref().unwrap();
        Ok(QueryHelper {
            store,
            account_id,
            shared_documents: match shared_documents {
                Some(fnc) if acl.is_shared(account_id) => fnc(account_id, &acl.member_of)?.into(),
                _ => {
                    debug_assert!(!acl.is_shared(account_id));
                    None
                }
            },
            request,
            filter: Filter::None,
            comparator: Comparator::None,
        })
    }

    pub fn parse_filter(
        &mut self,
        mut parse_fnc: impl FnMut(O::Filter) -> crate::Result<Filter>,
    ) -> crate::Result<()> {
        if let Some(state) = self.request.filter.take() {
            let mut state = match state {
                query::Filter::FilterOperator(op) => QueryState::<O> {
                    op: op.operator.into(),
                    terms: Vec::with_capacity(op.conditions.len()),
                    it: op.conditions.into_iter(),
                },
                condition => QueryState {
                    op: LogicalOperator::And,
                    it: vec![condition].into_iter(),
                    terms: Vec::with_capacity(1),
                },
            };

            let mut state_stack = Vec::new();
            let mut filter;

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term {
                        query::Filter::FilterOperator(op) => {
                            state_stack.push(state);
                            state = QueryState {
                                op: op.operator.into(),
                                terms: Vec::with_capacity(op.conditions.len()),
                                it: op.conditions.into_iter(),
                            };
                        }
                        query::Filter::FilterCondition(cond) => {
                            state.terms.push(parse_fnc(cond)?);
                        }
                        query::Filter::Empty => (),
                    }
                }

                filter = if !state.terms.is_empty() {
                    Filter::Operator(FilterOperator {
                        operator: state.op,
                        conditions: state.terms,
                    })
                } else {
                    Filter::None
                };

                if let Some(prev_state) = state_stack.pop() {
                    state = prev_state;
                    if !matches!(filter, Filter::None) {
                        state.terms.push(filter);
                    }
                } else {
                    break 'outer;
                }
            }

            self.filter = filter;
        }
        Ok(())
    }

    pub fn parse_comparator(
        &mut self,
        mut parse_fnc: impl FnMut(query::Comparator<O::Comparator>) -> crate::Result<Comparator>,
    ) -> crate::Result<()> {
        if let Some(sort) = self.request.sort.take() {
            let mut terms: Vec<Comparator> = Vec::with_capacity(sort.len());
            for comp in sort {
                terms.push(parse_fnc(comp)?);
            }
            self.comparator = Comparator::List(terms);
        }
        Ok(())
    }

    pub fn query<X, W>(
        self,
        filter_map_fnc: X,
        extra_filters: Option<W>,
    ) -> crate::Result<QueryResponse>
    where
        X: FnMut(DocumentId) -> store::Result<Option<store::JMAPId>>,
        W: FnMut(Vec<JMAPId>) -> crate::Result<Vec<JMAPId>>,
    {
        let collection = O::collection();
        let mut result = QueryResponse {
            account_id: self.request.account_id,
            position: 0,
            query_state: self.store.get_state(self.account_id, collection)?,
            total: None,
            limit: None,
            ids: Vec::with_capacity(0),
            is_immutable: false,
            can_calculate_changes: true,
        };

        // Do not run the query if there are no shared documents to include
        if let Some(shared_documents) = &self.shared_documents {
            if !shared_documents.has_some_access() {
                return Ok(result);
            }
        }

        let results_it = self.store.query_store::<X>(
            self.account_id,
            collection,
            self.filter,
            self.comparator,
        )?;

        let limit = if let Some(limit) = &self.request.limit {
            if *limit > 0 {
                std::cmp::min(*limit, self.store.config.query_max_results)
            } else {
                if self.request.calculate_total.unwrap_or(false) {
                    result.total = Some(results_it.len());
                }
                return Ok(result);
            }
        } else {
            self.store.config.query_max_results
        };

        result.ids = Vec::with_capacity(if limit > 0 && limit < results_it.len() {
            limit
        } else {
            results_it.len()
        });

        let position = self.request.position.unwrap_or(0);
        let anchor = self.request.anchor;
        let anchor_offset = self.request.anchor_offset.unwrap_or(0);

        let total_results = if let Some(mut extra_filters) = extra_filters {
            let results = if let Some(shared_documents) = self.shared_documents {
                // Filter out documents that are not shared
                results_it
                    .set_filter_map(filter_map_fnc)
                    .into_iter()
                    .filter_map(|id| {
                        if shared_documents.has_access(id.get_document_id()) {
                            Some(id.into())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<JMAPId>>()
            } else {
                results_it
                    .set_filter_map(filter_map_fnc)
                    .into_iter()
                    .map(|id| id.into())
                    .collect::<Vec<JMAPId>>()
            };

            let results = extra_filters(results)?;
            let total_results = results.len();

            result.paginate(results.into_iter(), limit, position, anchor, anchor_offset)?;

            total_results
        } else {
            let total_results = results_it.len();
            if let Some(shared_documents) = self.shared_documents {
                // Filter out documents that are not shared
                result.paginate(
                    results_it
                        .set_filter_map(filter_map_fnc)
                        .into_iter()
                        .filter_map(|id| {
                            if shared_documents.has_access(id.get_document_id()) {
                                Some(id.into())
                            } else {
                                None
                            }
                        }),
                    limit,
                    position,
                    anchor,
                    anchor_offset,
                )?;
            } else {
                result.paginate(
                    results_it
                        .set_filter_map(filter_map_fnc)
                        .into_iter()
                        .map(|id| id.into()),
                    limit,
                    position,
                    anchor,
                    anchor_offset,
                )?;
            }

            total_results
        };

        if limit > 0 && limit < total_results {
            result.limit = limit.into();
        }

        if self.request.calculate_total.unwrap_or(false) {
            result.total = Some(total_results);
        }

        Ok(result)
    }
}

impl QueryResponse {
    pub fn paginate<W>(
        &mut self,
        jmap_ids: W,
        limit: usize,
        mut position: i32,
        anchor: Option<JMAPId>,
        mut anchor_offset: i32,
    ) -> crate::Result<()>
    where
        W: Iterator<Item = JMAPId>,
    {
        let has_anchor = anchor.is_some();
        let mut anchor_found = false;
        let requested_position = position;

        for jmap_id in jmap_ids {
            if !has_anchor {
                if position >= 0 {
                    if position > 0 {
                        position -= 1;
                    } else {
                        self.ids.push(jmap_id);
                        if limit > 0 && self.ids.len() == limit {
                            break;
                        }
                    }
                } else {
                    self.ids.push(jmap_id);
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
                    self.ids.push(jmap_id);
                    if limit > 0 && self.ids.len() == limit {
                        break;
                    }
                }
            } else {
                anchor_found = &jmap_id == anchor.as_ref().unwrap();
                self.ids.push(jmap_id);

                if !anchor_found {
                    continue;
                }

                position = anchor_offset;

                break;
            }
        }

        if !has_anchor || anchor_found {
            if !has_anchor && requested_position >= 0 {
                self.position = if position == 0 { requested_position } else { 0 };
            } else if position >= 0 {
                self.position = position;
            } else {
                let position = position.abs() as usize;
                let start_offset = if position < self.ids.len() {
                    self.ids.len() - position
                } else {
                    0
                };
                self.position = start_offset as i32;
                let end_offset = if limit > 0 {
                    std::cmp::min(start_offset + limit, self.ids.len())
                } else {
                    self.ids.len()
                };

                self.ids = self.ids[start_offset..end_offset].to_vec()
            }
        } else {
            return Err(MethodError::AnchorNotFound);
        };

        Ok(())
    }
}

impl From<query::Operator> for LogicalOperator {
    fn from(op: query::Operator) -> Self {
        match op {
            query::Operator::And => LogicalOperator::And,
            query::Operator::Or => LogicalOperator::Or,
            query::Operator::Not => LogicalOperator::Not,
        }
    }
}

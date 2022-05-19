use std::collections::HashMap;

use store::{
    core::collection::Collection,
    read::{
        comparator::Comparator,
        filter::{Filter, FilterOperator, LogicalOperator},
        QueryFilterMap,
    },
    AccountId, JMAPStore, Store,
};

use crate::{
    error::method::MethodError,
    id::{jmap::JMAPId, state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::query::{self, QueryRequest, QueryResponse},
};

use super::{changes::JMAPChanges, get::GetObject, Object};

pub struct QueryHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject<T>,
{
    pub store: &'y JMAPStore<T>,
    pub account_id: AccountId,
    pub request: QueryRequest<O, T>,
    pub data: O::QueryHelper,
}

impl<'y, O, T> QueryFilterMap for QueryHelper<'y, O, T>
where
    T: for<'x> Store<'x> + 'static,
    O: QueryObject<T>,
{
    fn filter_map_id(
        &mut self,
        document_id: store::DocumentId,
    ) -> store::Result<Option<store::JMAPId>> {
        O::filter_map_id(self, document_id)
    }
}

pub trait QueryObject<T>: Object
where
    T: for<'x> Store<'x> + 'static,
{
    type QueryArguments;
    type QueryHelper: Default;
    type Filter: for<'de> serde::Deserialize<'de>;
    type Comparator: for<'de> serde::Deserialize<'de>;

    fn init_query(helper: &mut QueryHelper<Self, T>) -> crate::Result<()>;
    fn parse_filter(
        helper: &mut QueryHelper<Self, T>,
        filter: Self::Filter,
    ) -> crate::Result<Filter>;
    fn parse_comparator(
        helper: &mut QueryHelper<Self, T>,
        comparator: query::Comparator<Self::Comparator>,
    ) -> crate::Result<Comparator>;
    fn has_more_filters(helper: &mut QueryHelper<Self, T>) -> bool;
    fn apply_filters(
        helper: &mut QueryHelper<Self, T>,
        results: Vec<JMAPId>,
    ) -> crate::Result<Vec<JMAPId>>;
    fn filter_map_id(
        helper: &mut QueryHelper<Self, T>,
        document_id: store::DocumentId,
    ) -> store::Result<Option<store::JMAPId>>;
    fn is_immutable(helper: &mut QueryHelper<Self, T>) -> bool;
}

struct QueryState<O, T>
where
    O: QueryObject<T>,
    T: for<'x> Store<'x> + 'static,
{
    op: LogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<query::Filter<O::Filter>>,
}

pub trait JMAPQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query<'y, 'z: 'y, O>(&'z self, request: QueryRequest<O, T>) -> crate::Result<QueryResponse>
    where
        O: QueryObject<T>;
}

impl<T> JMAPQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query<'y, 'z: 'y, O>(
        &'z self,
        mut request: QueryRequest<O, T>,
    ) -> crate::Result<QueryResponse>
    where
        O: QueryObject<T>,
    {
        let collection = O::collection();
        let mut helper = QueryHelper {
            store: self,
            account_id: request.account_id.into(),
            request,
            data: O::QueryHelper::default(),
        };

        O::init_query(&mut helper)?;

        let filter = if let Some(state) = helper.request.filter.take() {
            let mut state = match state {
                query::Filter::FilterOperator(op) => QueryState::<O, T> {
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
                            state.terms.push(O::parse_filter(&mut helper, cond)?);
                        }
                    }
                }

                filter = Filter::Operator(FilterOperator {
                    operator: state.op,
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

        let sort = if let Some(sort) = helper.request.sort.take() {
            let mut terms: Vec<store::read::comparator::Comparator> =
                Vec::with_capacity(sort.len());
            for comp in sort {
                terms.push(O::parse_comparator(&mut helper, comp)?);
            }
            store::read::comparator::Comparator::List(terms)
        } else {
            store::read::comparator::Comparator::None
        };

        let results_it =
            self.query_store::<QueryHelper<'_, O, T>>(helper.account_id, collection, filter, sort)?;

        let mut limit = helper.request.limit.as_ref().copied().unwrap_or(0);
        if limit == 0 || limit > self.config.query_max_results {
            limit = self.config.query_max_results
        };

        let mut result = QueryResponse {
            account_id: helper.request.account_id,
            position: 0.into(),
            query_state: self.get_state(helper.account_id, collection)?,
            total: None,
            limit: None,
            ids: Vec::with_capacity(if limit > 0 && limit < results_it.len() {
                limit
            } else {
                results_it.len()
            }),
            is_immutable: O::is_immutable(&mut helper),
            can_calculate_changes: true,
        };

        let position = helper.request.position.unwrap_or(0);
        let anchor = helper.request.anchor;
        let anchor_offset = helper.request.anchor_offset.unwrap_or(0);

        let total_results = if !O::has_more_filters(&mut helper) {
            let total_results = results_it.len();
            result.paginate(
                results_it
                    .set_filter_map(&mut helper)
                    .into_iter()
                    .map(|id| id.into()),
                limit,
                position,
                anchor,
                anchor_offset,
            )?;

            total_results
        } else {
            let results = results_it
                .set_filter_map(&mut helper)
                .into_iter()
                .map(|id| id.into())
                .collect::<Vec<JMAPId>>();
            let results = O::apply_filters(&mut helper, results)?;
            let total_results = results.len();

            result.paginate(results.into_iter(), limit, position, anchor, anchor_offset)?;

            total_results
        };

        if limit > 0 && limit < total_results {
            result.limit = limit.into();
        }

        if helper.request.calculate_total.unwrap_or(false) {
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
            if position >= 0 {
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

use std::collections::HashMap;

use store::{
    query::QueryFilterMap, AccountId, Collection, Comparator, Filter, FilterOperator, JMAPId,
    JMAPStore, LogicalOperator, Store,
};

use crate::{
    error::method::MethodError,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::json::JSONValue,
    request::query::QueryRequest,
};

use super::changes::JMAPChanges;

pub trait QueryObject<'y, T>: QueryFilterMap + Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(store: &'y JMAPStore<T>, request: &QueryRequest) -> crate::Result<Self>;
    fn parse_filter(&mut self, cond: HashMap<String, JSONValue>) -> crate::Result<Filter>;
    fn parse_comparator(
        &mut self,
        property: String,
        is_ascending: bool,
        collation: Option<String>,
        arguments: HashMap<String, JSONValue>,
    ) -> crate::Result<Comparator>;
    fn has_more_filters(&self) -> bool;
    fn apply_filters(&mut self, results: Vec<JMAPId>) -> crate::Result<Vec<JMAPId>>;
    fn is_immutable(&self) -> bool;
    fn collection() -> Collection;
}

struct QueryState {
    op: LogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<JSONValue>,
}

pub struct QueryResult {
    pub account_id: AccountId,
    pub position: usize,
    pub query_state: JMAPState,
    pub total: Option<usize>,
    pub limit: Option<usize>,
    pub ids: Vec<JSONValue>,
    pub is_immutable: bool,
}

pub trait JMAPQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query<'y, 'z: 'y, V>(&'z self, request: QueryRequest) -> crate::Result<QueryResult>
    where
        V: QueryObject<'y, T>;
}

impl<T> JMAPQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn query<'y, 'z: 'y, V>(&'z self, request: QueryRequest) -> crate::Result<QueryResult>
    where
        V: QueryObject<'y, T>,
    {
        let mut object = V::new(self, &request)?;
        let collection = V::collection();

        let state: Option<QueryState> = match request.filter.parse_operator() {
            Ok(state) => state,
            Err(obj) => Some(QueryState {
                op: LogicalOperator::And,
                it: vec![JSONValue::Object(obj)].into_iter(),
                terms: Vec::with_capacity(1),
            }),
        };

        let filter = if let Some(mut state) = state {
            let mut state_stack = Vec::new();
            let mut filter;

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term.parse_operator() {
                        Ok(Some(new_state)) => {
                            state_stack.push(state);
                            state = new_state;
                        }
                        Err(cond) => {
                            state.terms.push(object.parse_filter(cond)?);
                        }
                        Ok(None) => {}
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

        let sort = if let Some(sort) = request.sort {
            let mut terms: Vec<store::Comparator> = Vec::with_capacity(sort.len());
            for comp in sort {
                terms.push(object.parse_comparator(
                    comp.property,
                    comp.is_ascending,
                    comp.collation,
                    comp.arguments,
                )?);
            }
            store::Comparator::List(terms)
        } else {
            store::Comparator::None
        };

        let results_it = self.query_store::<V>(request.account_id, collection, filter, sort)?;

        let limit = if request.limit == 0 || request.limit > self.config.query_max_results {
            self.config.query_max_results
        } else {
            request.limit
        };

        let mut result = QueryResult {
            account_id: request.account_id,
            position: 0,
            query_state: self.get_state(request.account_id, collection)?,
            total: None,
            limit: None,
            ids: Vec::with_capacity(if limit > 0 && limit < results_it.len() {
                limit
            } else {
                results_it.len()
            }),
            is_immutable: object.is_immutable(),
        };

        let total_results = if !object.has_more_filters() {
            let total_results = results_it.len();
            result.paginate(
                results_it.set_filter_map(&mut object),
                limit,
                request.position,
                request.anchor,
                request.anchor_offset,
            )?;

            total_results
        } else {
            let results = results_it
                .set_filter_map(&mut object)
                .into_iter()
                .collect::<Vec<JMAPId>>();
            let results = object.apply_filters(results)?;
            let total_results = results.len();

            result.paginate(
                results.into_iter(),
                limit,
                request.position,
                request.anchor,
                request.anchor_offset,
            )?;

            total_results
        };

        if limit > 0 && limit < total_results {
            result.limit = limit.into();
        }

        if request.calculate_total {
            result.total = Some(total_results);
        }

        Ok(result)
    }
}

impl From<QueryResult> for JSONValue {
    fn from(query_result: QueryResult) -> Self {
        let mut result = HashMap::new();
        result.insert(
            "accountId".to_string(),
            (query_result.account_id as JMAPId).to_jmap_string().into(),
        );
        result.insert("position".to_string(), query_result.position.into());
        result.insert("queryState".to_string(), query_result.query_state.into());
        if let Some(total) = query_result.total {
            result.insert("total".to_string(), total.into());
        }
        if let Some(limit) = query_result.limit {
            result.insert("limit".to_string(), limit.into());
        }
        result.insert("ids".to_string(), query_result.ids.into());
        result.into()
    }
}

impl QueryResult {
    pub fn paginate<W>(
        &mut self,
        jmap_ids: W,
        limit: usize,
        mut position: i64,
        anchor: Option<JMAPId>,
        mut anchor_offset: i64,
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
                        self.ids.push(jmap_id.to_jmap_string().into());
                        if limit > 0 && self.ids.len() == limit {
                            break;
                        }
                    }
                } else {
                    self.ids.push(jmap_id.to_jmap_string().into());
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
                    self.ids.push(jmap_id.to_jmap_string().into());
                    if limit > 0 && self.ids.len() == limit {
                        break;
                    }
                }
            } else {
                anchor_found = &jmap_id == anchor.as_ref().unwrap();
                self.ids.push(jmap_id.to_jmap_string().into());

                if !anchor_found {
                    continue;
                }

                position = anchor_offset;

                break;
            }
        }

        if !has_anchor || anchor_found {
            if position >= 0 {
                self.position = position as usize;
            } else {
                let position = position.abs() as usize;
                let start_offset = if position < self.ids.len() {
                    self.ids.len() - position
                } else {
                    0
                };
                self.position = start_offset;
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

impl JSONValue {
    fn parse_operator(self) -> Result<Option<QueryState>, HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(mut obj) => {
                if let (Some(JSONValue::String(operator)), Some(JSONValue::Array(conditions))) =
                    (obj.remove("operator"), obj.remove("conditions"))
                {
                    let op = match operator.as_str() {
                        "AND" => LogicalOperator::And,
                        "OR" => LogicalOperator::Or,
                        "NOT" => LogicalOperator::Not,
                        _ => return Err(obj),
                    };

                    Ok(Some(QueryState {
                        op,
                        terms: Vec::with_capacity(conditions.len()),
                        it: conditions.into_iter(),
                    }))
                } else {
                    Err(obj)
                }
            }
            _ => Ok(None),
        }
    }
}

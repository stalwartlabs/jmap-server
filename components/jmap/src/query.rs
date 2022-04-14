use std::collections::HashMap;

use store::{
    query::JMAPStoreQuery, Collection, DocumentId, Filter, FilterOperator, JMAPId, LogicalOperator,
};

use crate::{
    changes::JMAPState, id::JMAPIdSerialize, json::JSONValue, request::QueryRequest, JMAPError,
};

#[derive(Debug)]
pub struct QueryResult {
    pub is_immutable: bool,
    pub result: JSONValue,
}

struct QueryState {
    op: LogicalOperator,
    terms: Vec<Filter>,
    it: std::vec::IntoIter<JSONValue>,
}

#[derive(Debug, Clone)]
pub struct Comparator {
    pub property: String,
    pub is_ascending: bool,
    pub collation: Option<String>,
    pub arguments: HashMap<String, JSONValue>,
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

    pub fn parse_comparator(self) -> crate::Result<Comparator> {
        let mut comparator = self.unwrap_object().ok_or_else(|| {
            JMAPError::InvalidArguments("Comparator is not an object.".to_string())
        })?;

        Ok(Comparator {
            property: comparator
                .remove("property")
                .and_then(|v| v.unwrap_string())
                .ok_or_else(|| {
                    JMAPError::InvalidArguments(
                        "Comparator has no 'property' parameter.".to_string(),
                    )
                })?,
            is_ascending: comparator
                .remove("isAscending")
                .and_then(|v| v.unwrap_bool())
                .unwrap_or(true),
            collation: comparator
                .remove("collation")
                .and_then(|v| v.unwrap_string()),
            arguments: comparator,
        })
    }
}

impl QueryRequest {
    pub fn build_query<W, X, Y>(
        &mut self,
        collection: Collection,
        mut condition_map_fnc: W,
        mut comparator_map_fnc: X,
        filter_map_fnc: Option<Y>,
    ) -> crate::Result<JMAPStoreQuery<Y>>
    where
        W: FnMut(HashMap<String, JSONValue>) -> crate::Result<Filter>,
        X: FnMut(Comparator) -> crate::Result<store::Comparator>,
        Y: FnMut(DocumentId) -> store::Result<Option<JMAPId>>,
    {
        let state: Option<QueryState> = match std::mem::take(&mut self.filter).parse_operator() {
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
                            state.terms.push(condition_map_fnc(cond)?);
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

        let sort = if let Some(sort) = std::mem::take(&mut self.sort) {
            let mut terms: Vec<store::Comparator> = Vec::with_capacity(sort.len());
            for comp in sort {
                terms.push(comparator_map_fnc(comp)?);
            }
            store::Comparator::List(terms)
        } else {
            store::Comparator::None
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
    ) -> crate::Result<JSONValue>
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
                        results.push(jmap_id.to_jmap_string().into());
                        if self.limit > 0 && results.len() == self.limit {
                            break;
                        }
                    }
                } else {
                    results.push(jmap_id.to_jmap_string().into());
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
                    results.push(jmap_id.to_jmap_string().into());
                    if self.limit > 0 && results.len() == self.limit {
                        break;
                    }
                }
            } else {
                anchor_found = &jmap_id == self.anchor.as_ref().unwrap();
                results.push(jmap_id.to_jmap_string().into());

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

        let mut response = HashMap::new();
        response.insert(
            "accountId".to_string(),
            (self.account_id as JMAPId).to_jmap_string().into(),
        );
        response.insert("position".to_string(), start_position.into());
        response.insert("queryState".to_string(), query_state.into());
        if self.calculate_total {
            response.insert("total".to_string(), total_results.into());
        }
        if self.limit > 0 && self.limit < total_results {
            response.insert("limit".to_string(), self.limit.into());
        }
        response.insert("ids".to_string(), results.into());

        Ok(response.into())
    }
}

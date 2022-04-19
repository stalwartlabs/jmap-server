use std::collections::{HashMap, HashSet};

use jmap::error::method::MethodError;
use jmap::jmap_store::changes::JMAPChanges;
use jmap::protocol::json::JSONValue;
use jmap::request::query::QueryRequest;
use store::query::JMAPIdMapFnc;

use store::Store;
use store::{
    Collection, Comparator, FieldComparator, FieldValue, Filter, JMAPId, JMAPStore, StoreError, Tag,
};

use super::{Mailbox, MailboxProperties};

pub trait JMAPMailMailboxQuery {
    fn mailbox_query(&self, request: QueryRequest) -> jmap::Result<JSONValue>;
}

impl<T> JMAPMailMailboxQuery for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_query(&self, mut request: QueryRequest) -> jmap::Result<JSONValue> {
        let cond_fnc = |cond: HashMap<String, JSONValue>| {
            if let Some((cond_name, cond_value)) = cond.into_iter().next() {
                Ok(match cond_name.as_str() {
                    "parentId" => Filter::eq(
                        MailboxProperties::ParentId.into(),
                        FieldValue::LongInteger(
                            cond_value
                                .parse_jmap_id(true)?
                                .map(|id| id + 1)
                                .unwrap_or(0),
                        ),
                    ),
                    "name" => Filter::eq(
                        MailboxProperties::Name.into(),
                        FieldValue::Text(cond_value.parse_string()?.to_lowercase()),
                    ),
                    "role" => Filter::eq(
                        MailboxProperties::Role.into(),
                        FieldValue::Text(cond_value.parse_string()?),
                    ),
                    "hasAnyRole" => {
                        let filter = Filter::eq(
                            MailboxProperties::HasRole.into(),
                            FieldValue::Tag(Tag::Static(0)),
                        );
                        if !cond_value.parse_bool()? {
                            Filter::not(vec![filter])
                        } else {
                            filter
                        }
                    }
                    "isSubscribed" => todo!(), //TODO implement
                    _ => {
                        return Err(MethodError::UnsupportedFilter(format!(
                            "Unsupported filter '{}'.",
                            cond_name
                        )))
                    }
                })
            } else {
                Ok(Filter::None)
            }
        };

        let sort_fnc = |comp: jmap::request::query::Comparator| {
            Ok(Comparator::Field(FieldComparator {
                field: match comp.property.as_ref() {
                    "name" => MailboxProperties::Name,
                    "role" => MailboxProperties::Role,
                    "parentId" => MailboxProperties::ParentId,
                    _ => {
                        return Err(MethodError::UnsupportedSort(format!(
                            "Unsupported sort property '{}'.",
                            comp.property
                        )))
                    }
                }
                .into(),
                ascending: comp.is_ascending,
            }))
        };

        let account_id = request.account_id;
        let mut results = self
            .query::<JMAPIdMapFnc>(request.build_query(
                Collection::Mailbox,
                cond_fnc,
                sort_fnc,
                None,
            )?)?
            .into_iter()
            .collect::<Vec<JMAPId>>();

        let filter_as_tree = request
            .arguments
            .get("filterAsTree")
            .and_then(|v| v.to_bool())
            .unwrap_or(false);

        let sort_as_tree = request
            .arguments
            .get("sortAsTree")
            .and_then(|v| v.to_bool())
            .unwrap_or(false);

        if !results.is_empty() && (filter_as_tree || sort_as_tree) {
            let mut hierarchy = HashMap::new();
            let mut tree = HashMap::new();

            for doc_id in self
                .get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default()
            {
                let mailbox = self
                    .get_document_value::<Mailbox>(
                        request.account_id,
                        Collection::Mailbox,
                        doc_id,
                        MailboxProperties::Id.into(),
                    )?
                    .ok_or_else(|| {
                        StoreError::InternalError("Mailbox data not found".to_string())
                    })?;
                hierarchy.insert((doc_id + 1) as JMAPId, mailbox.parent_id);
                tree.entry(mailbox.parent_id)
                    .or_insert_with(HashSet::new)
                    .insert((doc_id + 1) as JMAPId);
            }

            if filter_as_tree {
                let mut filtered_ids = Vec::with_capacity(results.len());

                for &doc_id in &results {
                    let mut keep = false;
                    let mut jmap_id = (doc_id + 1) as JMAPId;

                    for _ in 0..self.config.mailbox_max_depth {
                        if let Some(&parent_id) = hierarchy.get(&jmap_id) {
                            if parent_id == 0 {
                                keep = true;
                                break;
                            } else if !results.contains(&(parent_id - 1)) {
                                break;
                            } else {
                                jmap_id = parent_id;
                            }
                        } else {
                            break;
                        }
                    }
                    if keep {
                        filtered_ids.push(doc_id);
                    }
                }
                if filtered_ids.len() != results.len() {
                    results = filtered_ids;
                }
            }

            if sort_as_tree && results.len() > 1 {
                let mut stack = Vec::new();
                let mut sorted_list = Vec::with_capacity(results.len());
                let mut jmap_id = 0;

                'outer: for _ in 0..(results.len() * 10 * self.config.mailbox_max_depth) {
                    let (mut children, mut it) = if let Some(children) = tree.remove(&jmap_id) {
                        (children, results.iter())
                    } else if let Some(prev) = stack.pop() {
                        prev
                    } else {
                        break;
                    };

                    while let Some(&doc_id) = it.next() {
                        jmap_id = (doc_id + 1) as JMAPId;
                        if children.remove(&jmap_id) {
                            sorted_list.push(doc_id);
                            if sorted_list.len() == results.len() {
                                break 'outer;
                            } else {
                                stack.push((children, it));
                                continue 'outer;
                            }
                        }
                    }

                    if !children.is_empty() {
                        jmap_id = *children.iter().next().unwrap();
                        children.remove(&jmap_id);
                        stack.push((children, it));
                    }
                }
                results = sorted_list;
            }
        }

        if request.limit == 0 || request.limit > self.config.query_max_results {
            request.limit = self.config.query_max_results;
        }

        request.into_response(
            results.into_iter(),
            self.get_state(account_id, Collection::Mailbox)?,
        )
    }
}

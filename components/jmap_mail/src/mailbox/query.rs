use std::collections::{HashMap, HashSet};

use jmap::error::method::MethodError;
use jmap::jmap_store::query::QueryObject;
use jmap::protocol::json::JSONValue;
use jmap::request::query::QueryRequest;

use store::query::QueryFilterMap;
use store::{AccountId, DocumentId, Store};
use store::{
    Collection, Comparator, FieldComparator, FieldValue, Filter, JMAPId, JMAPStore, StoreError, Tag,
};

use super::{Mailbox, MailboxProperties};

pub struct QueryMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
    filter_as_tree: bool,
    sort_as_tree: bool,
}

impl<'y, T> QueryFilterMap for QueryMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn filter_map_id(&mut self, document_id: DocumentId) -> store::Result<Option<JMAPId>> {
        Ok(Some(document_id as JMAPId))
    }
}

impl<'y, T> QueryObject<'y, T> for QueryMailbox<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn init(store: &'y JMAPStore<T>, request: &QueryRequest) -> jmap::Result<Self> {
        Ok(QueryMailbox {
            store,
            account_id: request.account_id,
            filter_as_tree: request
                .arguments
                .get("filterAsTree")
                .and_then(|v| v.to_bool())
                .unwrap_or(false),
            sort_as_tree: request
                .arguments
                .get("sortAsTree")
                .and_then(|v| v.to_bool())
                .unwrap_or(false),
        })
    }

    fn parse_filter(&mut self, cond: HashMap<String, JSONValue>) -> jmap::Result<Filter> {
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
    }

    fn parse_comparator(
        &mut self,
        property: String,
        is_ascending: bool,
        _collation: Option<String>,
        _arguments: HashMap<String, JSONValue>,
    ) -> jmap::Result<Comparator> {
        Ok(Comparator::Field(FieldComparator {
            field: match property.as_ref() {
                "name" => MailboxProperties::Name,
                "role" => MailboxProperties::Role,
                "parentId" => MailboxProperties::ParentId,
                _ => {
                    return Err(MethodError::UnsupportedSort(format!(
                        "Unsupported sort property '{}'.",
                        property
                    )))
                }
            }
            .into(),
            ascending: is_ascending,
        }))
    }

    fn has_more_filters(&self) -> bool {
        self.filter_as_tree || self.sort_as_tree
    }

    fn apply_filters(&mut self, mut results: Vec<JMAPId>) -> jmap::Result<Vec<JMAPId>> {
        let mut hierarchy = HashMap::new();
        let mut tree = HashMap::new();

        for doc_id in self
            .store
            .get_document_ids(self.account_id, Collection::Mailbox)?
            .unwrap_or_default()
        {
            let mailbox = self
                .store
                .get_document_value::<Mailbox>(
                    self.account_id,
                    Collection::Mailbox,
                    doc_id,
                    MailboxProperties::Id.into(),
                )?
                .ok_or_else(|| StoreError::InternalError("Mailbox data not found".to_string()))?;
            hierarchy.insert((doc_id + 1) as JMAPId, mailbox.parent_id);
            tree.entry(mailbox.parent_id)
                .or_insert_with(HashSet::new)
                .insert((doc_id + 1) as JMAPId);
        }

        if self.filter_as_tree {
            let mut filtered_ids = Vec::with_capacity(results.len());

            for &doc_id in &results {
                let mut keep = false;
                let mut jmap_id = (doc_id + 1) as JMAPId;

                for _ in 0..self.store.config.mailbox_max_depth {
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

        if self.sort_as_tree && results.len() > 1 {
            let mut stack = Vec::new();
            let mut sorted_list = Vec::with_capacity(results.len());
            let mut jmap_id = 0;

            'outer: for _ in 0..(results.len() * 10 * self.store.config.mailbox_max_depth) {
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
        Ok(results)
    }

    fn is_immutable(&self) -> bool {
        false
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }
}

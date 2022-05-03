use jmap::jmap_store::raft::{RaftObject, RaftUpdate};
use store::{write::batch::WriteBatch, AccountId, DocumentId, JMAPStore, Store};

use super::set::SetEmailSubmission;

impl<T> RaftObject<T> for SetEmailSubmission
where
    T: for<'x> Store<'x> + 'static,
{
    fn raft_prepare_update(
        store: &JMAPStore<T>,
        document_id: DocumentId,
        as_insert: bool,
    ) -> store::Result<Option<RaftUpdate>> {
        Ok(None)
    }

    fn raft_apply_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        account_id: AccountId,
        update: RaftUpdate,
    ) -> store::Result<()> {
        Ok(())
    }
}
/*if let Some((raw_message, received_at)) = insert {
    let mut document = Document::new(Collection::Mail, document_id);

    // Parse and build message document
    let (reference_ids, thread_name) =
        document.parse_message(raw_message, vec![], vec![], received_at.into())?;

    for mailbox in mailboxes {
        document.tag(MessageField::Mailbox, mailbox, IndexOptions::new());
    }

    for keyword in keywords {
        document.tag(MessageField::Keyword, keyword, IndexOptions::new());
    }

    for reference_id in reference_ids {
        document.text(
            MessageField::MessageIdRef,
            Text::keyword(reference_id),
            IndexOptions::new(),
        );
    }

    // Add thread id and name
    document.tag(
        MessageField::ThreadId,
        Tag::Id(thread_id),
        IndexOptions::new(),
    );
    document.text(
        MessageField::ThreadName,
        Text::keyword(thread_name),
        IndexOptions::new().sort(),
    );

    batch.insert_document(document);
} else {
    let mut document = Document::new(Collection::Mail, document_id);

    // Process mailbox changes
    if let Some(current_mailboxes) = self.get_document_tags(
        account_id,
        Collection::Mail,
        document_id,
        MessageField::Mailbox.into(),
    )? {
        if current_mailboxes.items != mailboxes {
            for current_mailbox in &current_mailboxes.items {
                if !mailboxes.contains(current_mailbox) {
                    document.tag(
                        MessageField::Mailbox,
                        current_mailbox.clone(),
                        IndexOptions::new().clear(),
                    );
                }
            }

            for mailbox in mailboxes {
                if !current_mailboxes.contains(&mailbox) {
                    document.tag(MessageField::Mailbox, mailbox, IndexOptions::new());
                }
            }
        }
    } else {
        debug!(
            "Raft update failed: No mailbox tags found for message {}.",
            document_id
        );
        return Ok(());
    };

    // Process keyword changes
    let current_keywords = if let Some(current_keywords) = self.get_document_tags(
        account_id,
        Collection::Mail,
        document_id,
        MessageField::Keyword.into(),
    )? {
        current_keywords.items
    } else {
        HashSet::new()
    };
    if current_keywords != keywords {
        for current_keyword in &current_keywords {
            if !keywords.contains(current_keyword) {
                document.tag(
                    MessageField::Keyword,
                    current_keyword.clone(),
                    IndexOptions::new().clear(),
                );
            }
        }

        for keyword in keywords {
            if !current_keywords.contains(&keyword) {
                document.tag(MessageField::Keyword, keyword, IndexOptions::new());
            }
        }
    }

    // Handle thread id changes
    if let Some(current_thread_id) = self.get_document_tag_id(
        account_id,
        Collection::Mail,
        document_id,
        MessageField::ThreadId.into(),
    )? {
        if thread_id != current_thread_id {
            document.tag(
                MessageField::ThreadId,
                Tag::Id(thread_id),
                IndexOptions::new(),
            );
            document.tag(
                MessageField::ThreadId,
                Tag::Id(current_thread_id),
                IndexOptions::new().clear(),
            );
        }
    } else {
        debug!(
            "Raft update failed: No thread id found for message {}.",
            document_id
        );
        return Ok(());
    };

    if !document.is_empty() {
        batch.update_document(document);
    }
}*/

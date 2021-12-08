use store_rocksdb::RocksDBStore;


fn main() {

    let mut temp_dir = std::env::temp_dir();
    temp_dir.push("strdb_query_test");
    /*if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }*/

    let db = RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap();
    //insert_artworks(RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap());
    //sort_artworks(&db);
    
}


/*

Flags:
KEYWORD/MAILBOX/THREADS + [FIELD_ID|String] = Bitmap

Full text:
WORD + FIELD_ID = Bitmap

Sort:
Field + String/Date/Int = 0


*/

/*

user.keyword.<keyword> = bitmap //  has attachment, deleted, etc.
user.mailbox.<mailbox_id> = bitmap

<word>.from.<user> = bitmap
<word>.to.<user> = bitmap
<word>.cc.<user> = bitmap
<word>.bcc.<user> = bitmap
<word?>.header.<user> = bitmap


<word>.subject.<user> = bitmap
<word>.body.<user> = bitmap
<word>.text.<user> = bitmap

user.threads = bitmap
user.threads.<thread_id> = bitmap

All In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> = user.threads.<thread_id>

Some In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> != 0

None In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> = 0



Blob
----
user.<ID>.header = serialized mail parser
user.<ID>.body.<BID> = blob body
user.<ID>.attachments.<AID> = blob attachment


Sorting
--------
user.subject.<field>.<ID> = 0
user.from.<field>.<ID> = 0
user.to.<field>.<ID> = 0
user.cc.<field>.<ID> = 0
user.size.<size>.<ID> = 0
user.received_at.<date>.<ID> = 0
user.sent_at.<date>.<ID> = 0

- Message-ID and no redID: Set thread id to ID + create thread id
- Message-ID and refID:
   * Look for parent ID in "user.message-ID.refID" INTERSECT with "user.subject.NEW_SUBJECT"
   * If there is a match, use the ThreadID from the parent + add message ID to thread ID
   * If there is no match, use the ID from the current message + create thread id


Filtering:
- Keyword
- Folder
- Date
- Size
- Thread
- HasAttachment
- Fulltext
  - From
  - To
  - Cc
  - Bcc
  - Subject
  - Body
  - Text Attachments
  - Header?

Sorting:
- receivedAt
- sentAt
- Size
- From
- To
- Subject
- Keyword









*/

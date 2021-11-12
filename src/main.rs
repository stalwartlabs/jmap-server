use nlp::{lang::detect_language, tokenizers::tokenize};


fn main() {

    println!("Hello, world!");
    let text = "Hello, world!";

    for token in tokenize(text, detect_language(text).0, 40) {
        println!("{:?}", token);
    }

}

/*

user.keyword.<keyword> = bitmap //  has attachment, deleted, etc.
user.folder.<folder_id> = bitmap

<word>.from.<user> = bitmap
<word>.to.<user> = bitmap
<word>.cc.<user> = bitmap
<word>.bcc.<user> = bitmap
<header_name>.header.<user>.<header_value> = bitmap

<word>.<exact?>.subject.<user> = bitmap
<word>.<exact?>.body.<user> = bitmap
<word>.<exact?>.text.<user> = bitmap

user.threads = bitmap
user.threads.<thread_id> = bitmap

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
use std::fs;

use jmap_mail::parse::parse_message;


fn main() {

    for file_name in fs::read_dir("/vagrant/Code/stalwart/test/dovecot").unwrap() {
        let file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(false, |e| e != "eml") {
            continue;
        }
        if !file_name.file_name().unwrap().to_str().unwrap().starts_with("m005") {
            continue;
        }

        let input = fs::read(&file_name).unwrap();

        let builder = parse_message(&input).unwrap();
        for field in builder {
            println!("{:?}", field);
        }

        break;

    }


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

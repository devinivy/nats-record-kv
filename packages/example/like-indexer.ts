import { consumer, type RecordId } from '@nats-firehose/record-kv'
import { DatabaseSync } from 'node:sqlite'

type IndexerOptions = {
  host?: string
}

export async function likeIndexer(opts: IndexerOptions = {}) {
  const model = getModel()
  const messages = consumer({
    ...opts,
    name: 'like-indexer',
    collection: 'app.bsky.feed.like',
  })
  for await (const [id, record, msg] of messages) {
    if (!record) {
      model.removeLike({ recordId: id })
    } else if (typeof record['subject']?.['uri'] === 'string') {
      const subject = record['subject']['uri']
      model.addLike({ subject, recordId: id })
    }
    msg.ack()
  }
}

function getModel() {
  const db = new DatabaseSync('like-index.sqlite')
  db.exec('PRAGMA journal_mode=WAL')
  db.exec(`
    CREATE TABLE IF NOT EXISTS likers_by_subject (
      subject TEXT,
      did TEXT,
      rkeys BLOB,
      PRIMARY KEY (subject, did)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS subject_by_like (
      did TEXT,
      rkey TEXT,
      subject TEXT,
      PRIMARY KEY (did, rkey)
    ) STRICT;
  `)
  const addLikersQuery = db.prepare(
    `INSERT INTO likers_by_subject (subject, did, rkeys)
        VALUES (:subject, :did, jsonb_array(:rkey))
      ON CONFLICT (subject, did)
        DO UPDATE SET rkeys = jsonb(json_insert(rkeys, '$[#]', :rkey))`,
  )
  const addSubjectByLikeQuery = db.prepare(
    `INSERT INTO subject_by_like (did, rkey, subject) VALUES (:did, :rkey, :subject)
      ON CONFLICT DO NOTHING`,
  )
  const removeLikersQuery = db.prepare(
    `UPDATE likers_by_subject SET rkeys = (
        SELECT jsonb(jsonb_group_array(value)) FROM json_each(rkeys) WHERE value != :rkey
      )
      WHERE subject = :subject AND did = :did`,
  )
  const removeSubjectByLikeQuery = db.prepare(
    `DELETE FROM subject_by_like WHERE did = :did AND rkey = :rkey RETURNING subject`,
  )
  const clearEmptyLikers = db.prepare(
    `DELETE FROM likers_by_subject WHERE subject = :subject AND json_array_length(rkeys) = 0`,
  )
  return {
    db,
    addLike({ subject, recordId }: { subject: string; recordId: RecordId }) {
      const { did, rkey } = recordId
      addSubjectByLikeQuery.run({ subject, did, rkey })
      addLikersQuery.run({ subject, did, rkey })
    },
    removeLike({ recordId }: { recordId: RecordId }) {
      const { did, rkey } = recordId
      const removed = removeSubjectByLikeQuery.get({ did, rkey })
      if (removed) {
        removeLikersQuery.run({ subject: removed.subject, did, rkey })
        clearEmptyLikers.run({ subject: removed.subject })
      }
    },
  }
}

// start if run directly, e.g. node like-indexer.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  likeIndexer()
}

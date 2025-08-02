import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { DatabaseSync } from 'node:sqlite'
import {
  getKvUpdate,
  keyToRecordId,
  selectCollection,
  type RecordId,
} from './util.ts'

async function main() {
  const connection = await connect({ servers: '0.0.0.0:4222' })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  await jsm.consumers.add('KV_record', {
    durable_name: 'likes-counts',
    filter_subject: selectCollection('app.bsky.feed.like'),
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
  })
  const consumer = await js.consumers.get('KV_record', 'likes-counts')
  const messages = await consumer.consume()
  const model = getModel()
  for await (const item of messages) {
    const { key, value: record } = getKvUpdate(item)
    const recordId = keyToRecordId(key)
    if (!record) {
      model.removeLike({ recordId })
    } else if (typeof record['subject']?.['uri'] === 'string') {
      const subject = record['subject']['uri']
      model.addLike({ subject, recordId })
    }
    item.ack()
  }
}

function getModel() {
  const db = new DatabaseSync('index.sqlite')
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

main()

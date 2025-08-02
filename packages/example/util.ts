import type { JsMsg } from '@nats-io/jetstream'
import { default as murmur } from 'murmurhash'

export function getKvUpdate(msg: JsMsg) {
  return {
    // replace leading $KV.{kvbucket}.
    key: msg.subject.split('.').slice(2).join('.'),
    value:
      msg.headers?.get('KV-Operation') === 'DEL'
        ? null
        : (JSON.parse(msg.data.toString()) as unknown),
  }
}

export function parseAtUri(uri: string): RecordId {
  const [did, collection, rkey] = uri.replace('at://', '').split('/')
  return { did, collection, rkey }
}

export type RecordId = { did: string; collection: string; rkey: string }

export function recordIdToKey({
  did,
  collection,
  rkey,
  partition,
}: RecordId & {
  partition: string
}) {
  return `${partition}.${collection.split('.').map(encodeNatsKeyPart).join('.')}.${encodeNatsKeyPart(did)}.${encodeNatsKeyPart(rkey)}`
}

export function keyToRecordId(key: string): RecordId & { partition: string } {
  // {partition}.{...collection}.${did}.${rkey}
  const parts = key.split('.')
  const rkey = decodeNatsKeyPart(parts.pop()!)
  const did = decodeNatsKeyPart(parts.pop()!)
  const collection = parts.splice(1).map(decodeNatsKeyPart).join('.')
  const partition = parts.pop()!
  return { did, collection, rkey, partition }
}

export function selectCollection(nsid: string) {
  return `$KV.record.*.${nsid}.>`
}

const encodeRegexp = /[^A-Za-z0-9_=/-]/g
export function encodeNatsKeyPart(str: string) {
  return str.replace(encodeRegexp, (ch) => {
    const hex = ch.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0')
    return '/' + hex
  })
}

const decodeRegexp = /\/([0-9A-F]{2})/g
export function decodeNatsKeyPart(str: string) {
  return str.replace(decodeRegexp, (_, hex) =>
    String.fromCharCode(parseInt(hex, 16)),
  )
}

// @NOTE only 16 partitions here, but could do more.
export function getPartition(str: string) {
  return murmur.v3(str).toString(16).padStart(2, '0').slice(0, 1)
}

export async function wait(ms: number) {
  return new Promise((res) => setTimeout(res, ms))
}

export const STORE_MESSAGE_FAIL_CODE = 10077

# nats-firehose
> A sync experiment with AT Proto and NATS

## Overview

This experiment builds a KV store of records from the firehose, which is able to serve a variety of needs in daily app development and in production.  This setup could also potentially bridge local development and cloud services.

The KV store is based on NATS, which allows consumers to process its contents as a stream.  Consumers transparently pick up from "live" after processing the contents of the store, so applications don't treat "backfill" as a special mode or job.  Stream processing state is maintained automatically, and processing can be scaled out horizontally on the consumer side.

### Details

A benefit of this setup is that stream filtering is performed efficiently on the server side, so consumers can choose to process specific collections or ranges of collections.  It can support both push and pull models depending on the consumer's configuration, and different strictness of delivery policies (e.g. works with or without acks).  Furthermore, we don't need to know in advance which collections we may want to process in advance.  Many important decisions are able to be deferred to consumer based on their needs, rather than known on the server or modeled ahead of time.  The server's only job is to keep this KV store accurate and up to date.

This makes it relatively straightforward to write and deploy small indexer processes that only handle records that they're interested in.

### Local Dev

In local development or testing, you might write mock data directly to the record KV rather than via the firehose.  Treating the KV store as the only data contract answers many questions around dev, debug, and testing flows.

## Usage

You're going to want four terminal windows to get the feel of this experiment.  Here's what's gonna happen in each one:
1. Run NATS: a lightweight piece of infrastructure offering streaming and key-value storage.
2. Run our record KV builder: consumes the AT Proto firehose into a key-value store of records.
3. Run our example like indexer: which live processes the record KV into like indexes (e.g. likers per post).
4. A spare terminal for poking around.

The KV builder and like indexer can be stopped and started without losing messages.  The firehose ingest will apply backpressure if the record KV processing falls behind.

### Directions

```sh
# Ensure you're using node v24 in each terminal
terminal1$ nvm install 24
terminal2$ nvm use 24
terminal3$ nvm use 24
terminal4$ nvm use 24

# Install the project
terminal1$ pnpm install

# Install and run NATS
terminal1$ curl -fsSL https://binaries.nats.dev/nats-io/nats-server/v2@v2.11.6 | sh
terminal1$ nats-server --jetstream

# Run record KV builder
terminal2$ pnpm run record-kv-builder

# Run like indexer
terminal3$ pnpm run like-indexer

# --- Now everything is running ---

# Inspect likes stream
terminal4$ nats sub '$KV.record.*.app.bsky.feed.like.>' --all

# Inspect blue.flashes namespace stream
terminal4$ nats sub '$KV.record.*.blue.flashes.>' --all

# View like indexer throughput
terminal4$ nats consumer graph KV_record like-indexer

# Check the index, e.g. try liking something then use your DID
terminal4$ echo did:plc:example | pnpm run get-likes-by

# Reset state after killing KV builder and like indexer
terminal4$ pnpm run nats-reset
```

# nats-record-kv
> A sync experiment with AT Proto and NATS

## Overview

This experiment builds a KV store of records from the firehose, which is able to serve a variety of needs in daily app development and in production.  This setup could also potentially bridge local development and cloud.

The KV store is based on NATS, which allows consumers to process its contents as a stream.  Consumers transparently pick up from "live" after processing the contents of the store, so applications don't treat "backfill" as a special mode or job.  Stream processing state is maintained automatically, and processing can be scaled out horizontally on the consumer side.  More in the [Details](#details) section below.

## Usage

You're going to want four terminal windows to get the feel of this experiment.  Here's what's gonna happen in each one:
1. Run NATS: a lightweight piece of infrastructure offering streaming and key-value storage.
2. Run our record KV builder: consumes the AT Proto firehose into a key-value store of records.
3. Run our example like indexer: live processes the record KV into like indexes (e.g. likers per post).
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

## Details

A benefit of this setup is that stream filtering is performed efficiently on the server side, so consumers can choose to process specific collections or ranges of collections.  It can support both push and pull models depending on the consumer's configuration, and different strictness of delivery policies (e.g. works with or without acks).  Furthermore, we don't need to know in advance which collections we may want to process.  Many decisions are able to be deferred to consumers based on their needs, rather than known on the server or modeled ahead of time.  The server's only job is to keep this KV store accurate and up to date.

This makes it relatively straightforward to write and deploy small indexer processes that only handle records that they're interested in.

### Local Dev

In local development or testing, you might write mock data directly to the record KV rather than via the firehose.  The application code consuming the record KV would not need to be aware.  Treating the KV store as the only data contract answers many questions around dev, debug, and testing flows.

### Horizontal Scalability, State, HA

The firehose ingester process (ingest.ts) is a singleton, maintaining a single connection with the firehose.  The ingester uses NATS to store its cursor, so its state is self-contained.  We could use NATS to also take a lock and ensure there is a single such consumer at a time.

The ingester produces a stream that is partitioned by DID, allowing the record KV builder process (kv.ts) to scale horizontally.  Though in this example we process all partitions together, without horizontal scaling.  A single record KV builder process handling messages serially seems to be fine up to 3000 evt/sec or so.

The KV store itself is also has a partition in its key, which allows downstream consumers (i.e. indexers) to scale out horizontally.

There are two NATS stream consumers in this experiment: the record KV builder and the example like indexer.  These are both pull-based streams (as opposed to push-based), which is the preferred mode in NATS.  Both use explicit acking to ensure every message is processed at least once.  All the cursor management and processing state is managed internally to NATS.  The consumers can go offline and come back without missing any messages, no need for any internal bookkeeping of their own.

The biggest open horizontal scalability questions relate to the server.  Could NATS scale to billions of records in its KV store?  The simple answer is that it can't do this on its own today.  Conventionally you move those partitions, which are currently internal to a single stream, into separate streams, potentially on separate NATS servers.  That sounds like it would be feasible to pull off.  But they are also working on this story, e.g. in [nats-server#6561](https://github.com/nats-io/nats-server/issues/6561) scheduled for version 2.12.0.

NATS does have strong support for clustering, which makes the server highly available.  Clusters can be meshed together into a "supercluster."  Streams can be mirrored to edge nodes local to a given consumer.

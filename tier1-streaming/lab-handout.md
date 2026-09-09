# Streaming Path Lab - Attendee Handout

Modernizing a Kinesis to S3 to Snowpipe pipeline with Snowpipe Streaming, then building a
medallion layer on top of it.

You will direct Cortex Code (CoCo) step by step. Each step below gives you a prompt to issue
and a way to check the result. Issue the prompt, read what comes back, judge it, and move on
when you are satisfied. **You** hold the plan today; CoCo executes it.

Work in your own schema. Pair up if the room is set up that way.

---

## The scenario

**Meridian Interactive** publishes three live games. Client telemetry - player sessions, in-app
purchases, matchmaking results, crash reports - is produced from game clients into a Kinesis
stream. Today it reaches Snowflake like this:

```
  game clients
       |
       v
  [ Kinesis ] --> [ Firehose ] --> [ S3 ] --> [ SQS ] --> [ Snowpipe ] --> table
                    buffers          files    notification   auto-ingest
                   60s / 5MB
```

Latency is a function of file arrival, not record arrival. Firehose buffers until a time or size
threshold trips, a file lands, a notification fires, and the pipe copies it.

By the end of the lab you will have built this instead:

```
  game clients
       |
       v
  [ Kinesis ] --> [ your consumer ] --> [ Snowpipe Streaming SDK ] --> PIPE --> table
                                              rows, not files
```

You are given `sample_kinesis_records.jsonl`: records exactly as a Kinesis consumer receives
them. Each line is an envelope (`shardId`, `partitionKey`, `sequenceNumber`,
`approximateArrivalTimestamp`) with the telemetry event base64-encoded in `data`.

**There is no live Kinesis stream in this lab, and that is deliberate.** You will have CoCo build
the producer with the consume side explicitly stubbed: a named function that raises, documents
what a real implementation would do, and sits behind a flag. Everything downstream of that seam
is production-shaped. The point is architectural - if the boundary between "get records" and
"send records" is drawn properly, swapping the stub for a real consumer is a one-function change
and nothing else moves.

---

## Setup

Everyone works in the shared workshop database, isolated by schema. Your facilitator has already
created the database and warehouse.

```sql
CREATE SCHEMA IF NOT EXISTS COCO_DE_STREAMING.<your_name>;
USE SCHEMA COCO_DE_STREAMING.<your_name>;
USE WAREHOUSE COCO_DE_WH;

SELECT CURRENT_SCHEMA();   -- confirm before starting
```

Install the SDK and set your connection environment:

```bash
pip install snowpipe-streaming          # needs Python 3.9+

export SNOWFLAKE_ACCOUNT="<org>-<account>"   # org-qualified, NOT the account locator
export SNOWFLAKE_PAT="<your personal access token>"
export SNOWFLAKE_ROLE="<your role>"
```

Two things that cost people time here:

- `SNOWFLAKE_ACCOUNT` must be the **org-qualified identifier** (`MYORG-MYACCOUNT`), not the
  account locator that `CURRENT_ACCOUNT()` returns. Get it with
  `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`
- The SDK authenticates with a **personal access token**. Your facilitator will give you one.
  There is no key pair to generate and no `profile.json` to distribute.

Connect Cortex Code to the account (OAuth) as normal.

---

## Step 1 - Assess the payload

Prompt CoCo:

> "Read `sample_kinesis_records.jsonl`. It is a sample of records as a Kinesis consumer
> receives them. Tell me: how many records, what the envelope fields are, what distinct event
> types are in the decoded payload, the field inventory per event type, which fields are
> candidate keys, and every data-quality problem you can see. Don't load anything yet, and
> don't propose an architecture yet."

Read the answer properly before moving on. You are looking for the shape of the data and
everything about it that will make the next four steps harder. Be skeptical: if something in the
summary looks too clean, check it yourself.

Two questions worth asking CoCo directly if its first answer does not address them:

- How many **distinct** records are there?
- Is event time encoded the same way in every event type?

Before you build anything, make the design call yourself and say it out loud to your pair or the
room: **are you landing the envelope, the decoded event, or both?** Have a reason.

---

## Step 2 - Build the producer, with Kinesis stubbed

> "Write a Python producer that streams these records into Snowflake using the
> `snowpipe-streaming` SDK (`from snowflake.ingest.streaming import StreamingIngestClient`).
> Requirements:
> - A function `consume_from_kinesis(stream_name, shard_id)` that **raises
>   NotImplementedError**, with a docstring showing what a real boto3 implementation would do
>   (`get_shard_iterator`, `get_records`, mapping the response onto our record shape) and
>   noting what it omits: checkpointing and resharding.
> - A function that reads the sample file and yields records of **identical shape**.
> - A `--source {synthetic,kinesis}` flag, defaulting to `synthetic`.
> - Everything downstream of that seam must not care which source it got.
> - One channel per shard, opened once and reused. Deterministic channel names.
> - Pass `sequenceNumber` as the `offset_token`, and land `shardId` as `CHANNEL_ID` and
>   `sequenceNumber` as `STREAM_OFFSET`.
> - Land the decoded event into a VARIANT column as a **native dict**, not a JSON string.
> - Batch appends. Authenticate with a PAT via inline `properties`, no profile file."

Verify by reading the code, not by running it:

- Does `consume_from_kinesis()` raise, and is its docstring specific enough that you could
  implement against it?
- Do both source functions yield the same keys?
- What did it choose as the channel key, and why is that the right or wrong choice?
- Is the payload assigned as a dict?

---

## Step 3 - Land the RAW layer

> "Create the target table `RAW_TELEMETRY_EVENTS` with columns CHANNEL_ID, STREAM_OFFSET,
> PARTITION_KEY, ARRIVAL_TS, PAYLOAD. Add table and column comments. Then run the producer
> against my schema and verify the load."

No `CREATE PIPE` is needed. The **default pipe** is created automatically on first use. Named
pipes exist for in-flight transformation and clustering at ingest time; you need neither today.

Run the producer:

```bash
python <your_producer>.py --database COCO_DE_STREAMING --schema <your_name>
```

Then check the load:

```sql
SELECT COUNT(*)                                          AS total_rows,
       COUNT(DISTINCT CHANNEL_ID)                        AS channels,
       COUNT(DISTINCT CHANNEL_ID || ':' || STREAM_OFFSET) AS distinct_offsets,
       COUNT_IF(TYPEOF(PAYLOAD) = 'OBJECT')              AS payload_object,
       COUNT_IF(TYPEOF(PAYLOAD) = 'VARCHAR')             AS payload_varchar
FROM RAW_TELEMETRY_EVENTS;
```

Read all five numbers, not just the first. Things to reason about:

- `payload_varchar` should be **zero**. Anything else means the producer passed a JSON string
  where the SDK wanted a native dict, so Snowflake stored escaped text instead of a structured
  object - and every downstream `PAYLOAD:field` access will silently return null. Run this check
  even though your Step 2 prompt asked for a dict. You instructed; you still verify.
- Compare `total_rows` against `distinct_offsets`. If they differ, work out why before you
  continue. It is not a bug in your producer.
- Does `total_rows` match the record count CoCo reported in Step 1?

Also look at the pipe you never created:

```sql
SHOW PIPES IN SCHEMA COCO_DE_STREAMING.<your_name>;
```

**Do not use the channel status counters as your completeness check.**
`rows_inserted_count`, `rows_parsed_count`, and `latest_committed_offset_token` lag on the server
side and will often read 0 / 0 / None right after `wait_for_flush()` returns even when every row
has landed. Confirm with `COUNT(*)` against the table. (The attribute is `rows_error_count`,
plural - several published samples say `row_error_count`, which does not exist and raises
`AttributeError`.)

Note what did **not** happen: no S3 bucket, no Firehose buffer, no SQS notification, no file.
Rows went from a process to a table.

---

## Step 4 - Build the Silver layer

> "Build the silver layer as Dynamic Tables (TARGET_LAG = '1 hour', warehouse COCO_DE_WH).
> - `SLV_TELEMETRY_EVENTS`: one row per distinct `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with
>   QUALIFY ROW_NUMBER(). Standardize every event-time encoding you found into a single
>   `EVENT_TS_UTC`. Flatten the device fields so older and newer client records land in the same
>   columns. Where an amount and its currency are fused into one string, split them into a
>   numeric amount and a currency code, reconciled with the records that already have them
>   separate. Type the rest properly and keep `PAYLOAD` for anything not promoted.
> - Then one Dynamic Table per event type off that: `SLV_PLAYER_SESSIONS`, `SLV_PURCHASES`,
>   `SLV_MATCHMAKING`, `SLV_CRASHES`. Carry comments forward."

This is the step where correctness is easiest to get wrong and hardest to notice. Work through
all four checks below and satisfy yourself on each.

**1. Did the dedupe do what you think?**

```sql
SELECT (SELECT COUNT(*) FROM RAW_TELEMETRY_EVENTS) AS raw_rows,
       (SELECT COUNT(*) FROM SLV_TELEMETRY_EVENTS) AS silver_rows,
       (SELECT COUNT(DISTINCT CHANNEL_ID || ':' || STREAM_OFFSET)
          FROM SLV_TELEMETRY_EVENTS)               AS silver_distinct_keys;
```

A lower silver count may be expected, depending on what you found in Step 1. Either way,
confirm the *key* is right: if `silver_rows` and `silver_distinct_keys` disagree, or if silver
dropped more rows than Step 1 accounts for, it deduped on something coarser and has thrown away
real records.

Worth discussing: why does the dedupe belong in silver rather than in the producer?

**2. Did every record's timestamp and device parse?**

```sql
SELECT COUNT_IF(EVENT_TS_UTC IS NULL) AS unparsed_ts,
       COUNT_IF(DEVICE_OS    IS NULL) AS no_device_os,
       COUNT_IF(DEVICE_MODEL IS NULL) AS no_device_model,
       MIN(EVENT_TS_UTC) AS ts_min,
       MAX(EVENT_TS_UTC) AS ts_max
FROM SLV_TELEMETRY_EVENTS;
```

All three counts should be zero. Check the min and max as well as the counts - a timestamp can
parse successfully and still be wrong by a factor of a thousand. If the range looks implausible,
you know which encoding was mishandled.

**3. Did every purchase resolve both an amount and a currency?**

```sql
SELECT COUNT(*) AS iap_rows, COUNT(AMOUNT) AS with_amount, COUNT(CURRENCY) AS with_currency
FROM SLV_PURCHASES;
```

All three should match. A NULL amount here is revenue you have silently deleted.

**4. Is anything missing? (the completeness check)**

This one only exists because you kept the envelope in Step 1.

```sql
WITH d AS (SELECT DISTINCT CHANNEL_ID, STREAM_OFFSET FROM RAW_TELEMETRY_EVENTS)
SELECT CHANNEL_ID, STREAM_OFFSET,
       LAG(STREAM_OFFSET) OVER (PARTITION BY CHANNEL_ID ORDER BY STREAM_OFFSET) AS prev
FROM d
QUALIFY STREAM_OFFSET != prev + 1;
```

**Must return zero rows.** A gap in the per-shard sequence means records were lost in flight.
Note that the query deduplicates first - work out for yourself why it has to.

Also check whether the four per-event-type tables account for every row in the parent. If they
do not, an event type is being silently dropped.

**One to discuss, not fix.** Some records have a null `player_id`. Find out which event type, and
how many. Then decide what silver should do about them: preserve with a null, route to a separate
table, or backfill from another identifier. There is no single right answer, and the reason it
matters is worth talking through.

---

## Step 5 - Document the pipeline

> "From live metadata, generate `PIPELINE_DOCS.md` (executive summary, source→target mapping
> per silver object, lineage diagram, business rules including the dedupe and timestamp logic,
> PII inventory, refresh pattern) and `ADD_COMMENTS.sql`. Then apply the comments."

Verify that every object has a table comment and that PII columns are labeled. Then read the
generated docs critically: is the business-rules section actually correct about what your silver
layer does, or is it describing what it assumes you did?

---

## Reference

**Envelope fields** in `sample_kinesis_records.jsonl`:

| Field | Notes |
|-------|-------|
| `shardId` | The Kinesis shard the record was read from. A real consumer knows this from the shard iterator it is reading, not from the record body; it is inlined here so the file stands alone. |
| `partitionKey` | Producer-supplied partition key. |
| `sequenceNumber` | Monotonically increasing **per shard**. |
| `approximateArrivalTimestamp` | Broker arrival time, not client event time. The two can differ by a lot on mobile. |
| `data` | The telemetry event, base64-encoded JSON. |

**Two simplifications** in the sample, both worth knowing:

- Real Kinesis sequence numbers are 56 digits and not contiguous. These are 18 digits and
  contiguous per shard, so a gap is unambiguously a lost record and the value fits
  `NUMBER(38,0)`. In production you would land the raw sequence number as a string and derive a
  numeric offset.
- `shardId` is on the record, as noted above.

**Useful commands:**

```sql
SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();  -- your SDK account identifier
SELECT CURRENT_SCHEMA();                                     -- confirm you are in your own schema
SHOW PIPES IN SCHEMA COCO_DE_STREAMING.<your_name>;          -- the pipe you never created
SHOW DYNAMIC TABLES IN SCHEMA COCO_DE_STREAMING.<your_name>;
```

If you get stuck for more than a few minutes on any step, ask your facilitator. Being stuck on
plumbing is not the exercise.

# Insurance Path - Data Engineering Workshop (Tier 1)

> **Tier:** Guided Path - *you direct, CoCo executes.*
> **Audience:** data engineers new to CoCo who own event ingestion and a refined layer on top of
> it. Maturity: Exploring.
> **Format:** hands-on lab, 180 minutes including a 10-minute break. Each attendee (or pair)
> works in their own schema under their own role.
> **Prereq:** none.
> **Account pre-req:** the customer should already be through **CoCo Customer Activation**
> (owned by the Core account team). This workshop builds on an activated account; it does not
> bootstrap one.
> **Companion:** read [`00-three-tiers.md`](../00-three-tiers.md) first; this workshop opens on it.
> **Do not hand this file to attendees.** It contains the expected findings for step 2.1, the
> three named traps and their answers, the build pitfalls and their fixes, and pointers to the
> reference implementations for both Silver paths. Attendees get `attendee/` (a handout plus the
> payload file) and nothing else. See Facilitator prep.

This is the **insurance-quote variant** of the streaming Guided Path lab. Where the
[streaming (game telemetry) version](../tier1-streaming-path/FACILITATOR-streaming-path-workshop.md)
starts from a Kinesis-specific envelope, this one starts from the most common thing a data team
is actually handed: nested JSON events off an event bus, wrapped in a thin envelope that any bus
produces. It lands them RAW through Snowpipe Streaming and then forks: each pair builds the same
Silver layer with **their choice of Dynamic Tables or dbt Projects on Snowflake**.

Use this variant when the room lives in event streams and has dbt somewhere in its stack.

---

## 0. Framing (15 min)

### The three tiers
Open with [`00-three-tiers.md`](../00-three-tiers.md). Place the room: today is **Guided
Path** - you direct CoCo step by step to build a genuine streaming pipeline - and we close by
showing what the **Skills** tier looks like.

### The personas (who's in the room)

| Persona | Cares about | Where CoCo helps on the Guided Path |
|---------|-------------|------------------------|
| **Pipeline Developer** | Building and shipping ingestion and transforms | Generates the producer, the RAW DDL, and the Silver layer as Dynamic Tables or dbt models, from your direction |
| **DevOps / Platform** | Standardization, CI/CD, environments | Draws the consume/send seam, keeps auth inline, scaffolds a dbt project that deploys and runs inside Snowflake |
| **Architect / Modernization** | Reverse-engineering and documenting legacy | Reads the inherited file-landing pipeline and produces docs, mapping, and lineage |
| **Data Quality / Validation** | Trust, audits, governance | Turns the dedupe, fanout, and offset-gap rules into verify queries or dbt tests; inventories the PII that must not reach Silver |

### The business domain
**Northpoint Insurance Marketplace**, an online marketplace where a shopper requests an auto
insurance quote once and gets offers back from several carriers. Every quote produces a short
lifecycle of events from different services, keyed by `quote_id`: the request (applicant,
vehicles, drivers, coverage asks), one response per carrier, a selection, and then either a bound
policy or an abandonment.

You are handed **6,320 records** exactly as a consumer reads them off the bus: a small envelope
(`event_id`, `source`, `partition`, `offset`, `published_at`) around a nested JSON `payload`.
Realistic mess throughout: vehicles and drivers as variable-length arrays, carriers that disagree
on how to send a premium, event times that disagree on how to encode a timestamp, shoppers who
start anonymous, records that arrive twice, a newer event version with an extra field, and
personal data in the payload that has no business in a refined layer.

> **Facilitator note - why the envelope is generic.** Kafka gives you (topic-partition,
> offset). Kinesis gives you (shard, sequenceNumber). Event Hubs and Pub/Sub have their own
> versions of the same pair. All of them map onto `partition` and `offset` here, and the lab's
> completeness check depends only on that pairing: offsets are monotonic per partition. Say it
> once: "whatever your bus is, it has these two fields, and they are the most valuable thing in
> the record."

### The current-state pipeline

```
  +------------------------------------------------------+
  | quote services                                        |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | your event bus (Kafka, Kinesis, Event Hubs, Pub/Sub, |
  | EventBridge)                                         |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | object-storage sink connector                         |
  | buffers by time or size, writes files                 |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | object storage + event notification                   |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | file-based Snowpipe, AUTO_INGEST = TRUE               |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | landing table                                         |
  +------------------------------------------------------+
```

Latency is a function of *file arrival*, not event arrival. The sink buffers until a threshold
trips, the file lands, a notification fires, and the pipe copies it. Nothing is wrong with this
pipeline. It works. It is just paying for hops it may not need.

### The target-state pipeline

```
  +------------------------------------------------------+
  | your event bus                                        |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | producer: consume (stubbed today) | send             |
  +------------------------------------------------------+
                            |   rows, not files
                            v
  +------------------------------------------------------+
  | Snowpipe Streaming default pipe                       |
  | RAW_QUOTE_EVENTS-STREAMING                            |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | RAW_QUOTE_EVENTS (envelope + PAYLOAD VARIANT)         |
  +------------------------------------------------------+
                            |
                            v
  +------------------------------------------------------+
  | Silver, your choice:                                  |
  |   2.4A Dynamic Tables   or   2.4B dbt on Snowflake    |
  | SILVER_QUOTE, SILVER_CARRIER_QUOTE,                   |
  | SILVER_QUOTE_VEHICLE, SILVER_QUOTE_DRIVER             |
  +------------------------------------------------------+
```

> **Facilitator note - a distinction worth drawing once, briefly.**
> "Snowpipe" and "Snowpipe Streaming" are different products, and teams routinely conflate
> them. **Snowpipe** is file-based: it copies files from a stage, and `AUTO_INGEST = TRUE` means
> a cloud event notification tells it a file arrived. **Snowpipe Streaming** never touches
> object storage; a client sends rows to a PIPE over the network. If someone says they are
> "using Snowpipe Streaming in auto-ingest mode off a bucket", they are describing file-based
> Snowpipe. Draw it, then move on.

### The fork, announced early
At §2.4 every pair chooses how to build Silver: **Dynamic Tables**, declared once and kept fresh
by Snowflake against a target lag, or **dbt Projects on Snowflake**, a dbt project authored in a
Workspace and run inside Snowflake with `EXECUTE DBT PROJECT`. Both paths build the same four
Silver tables with the same names and columns, and both are checked by the same verify queries,
so the debrief in §2.5 compares like with like. Ask pairs to start thinking now about which one
looks more like their day job.

---

## 1. Environment setup (20 min)

Everyone works in the shared workshop database, isolated by schema and role. The facilitator has
already run `setup/facilitator_setup.sql` (see Facilitator prep): each attendee has a role
`LAB_ATTENDEE_<n>` that owns schema `NORTHPOINT_LAB.ATTENDEE_<n>`.

```sql
USE ROLE LAB_ATTENDEE_<n>;
USE WAREHOUSE NORTHPOINT_LAB_WH;
USE SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;

SELECT CURRENT_ROLE(), CURRENT_SCHEMA();   -- confirm before starting
```

Install the SDK and set your connection environment:

```bash
pip install snowpipe-streaming          # needs Python 3.9+

export SNOWFLAKE_ACCOUNT="<org>-<account>"   # org-qualified, NOT the account locator
export SNOWFLAKE_PAT="<your personal access token>"
export SNOWFLAKE_ROLE="LAB_ATTENDEE_<n>"
```

Two things that cost people time here:

- `SNOWFLAKE_ACCOUNT` must be the **org-qualified identifier** (`MYORG-MYACCOUNT`), not the
  account locator you get back from `CURRENT_ACCOUNT()`. Use
  `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`
- The SDK authenticates with a **personal access token** restricted to your lab role, so there
  is no key pair to generate and no `profile.json` to distribute. (Key-pair auth also works; see
  Facilitator prep if your account policy requires it.)

Attendees also connect Cortex Code to the account (OAuth) as normal.

**The Workspace check (every attendee, whichever path they expect to take).** In Snowsight, open
Projects, then Workspaces, and confirm you can create a Workspace. Path B is authored there, and
pairs sometimes switch paths at §2.4.

> **Facilitator note.** Do the Workspace check now, not at §2.4. Workspaces live in the user's
> personal database, and some accounts restrict personal databases. If an attendee cannot create
> one, you want to know with two hours of slack, not with the dbt pairs waiting on you. Anyone
> blocked here goes to Path A; that is a fine outcome.

---

## 2. The lab - Guided streaming build (120 min, plus a 10-minute break)

**The point:** you drive CoCo one instruction at a time and watch a streaming ingestion path and
a refined Silver layer take shape. This is the Guided Path in its purest form.

Source: `sample_quote_events.jsonl` - 6,320 records, 4 partitions. Land it, refine it, document
it.

> **State the constraint to the room before starting 2.2.**
> There is no live event bus in this lab, no cloud account, and no object storage, and that is
> deliberate. Rather than pretend, you will have CoCo build the producer with the consume side
> **explicitly stubbed** - a named function that raises, documents what the real implementation
> does, and sits behind a flag. Everything downstream of that seam is production-shaped. The
> point is architectural: if the boundary between "get records" and "send records" is drawn
> properly, swapping the stub for a real Kafka or Kinesis consumer is a one-function change and
> nothing else moves.

### Step 2.1 - Assess the payload (you direct) - 20 min

Prompt CoCo:

> "Read `sample_quote_events.jsonl`. It is a sample of records as a consumer reads them off our
> event bus. Tell me: how many records, what the envelope fields are, what distinct event types
> are in the payload, the field inventory per event type including nested objects and arrays,
> which fields are candidate keys, and every data-quality problem you can see. Don't load
> anything yet, and don't propose an architecture yet."

Expected findings CoCo should surface:

| What | Detail |
|------|--------|
| Volume | 6,320 records across 4 partitions (0 to 3), 1,200 distinct `quote_id` |
| Envelope | `event_id`, `source`, `partition`, `offset`, `published_at`, `payload` (nested JSON, not encoded) |
| Event mix | `carrier_quote_returned` 3,486, `quote_requested` 1,236, `quote_selected` 631, `quote_abandoned` 590, `policy_bound` 377 |
| Candidate key | `(partition, offset)` - the only reliable one |
| Duplicates | 157 redelivered records; only 6,163 distinct `(partition, offset)` pairs |
| Timestamps | `event_ts` has **three** encodings, one per event type: epoch milliseconds as a JSON number (`carrier_quote_returned`, `quote_abandoned`), ISO 8601 with an offset (`quote_requested`, `policy_bound`), ISO 8601 with no offset, UTC by convention (`quote_selected`) |
| Arrays | `vehicles[]` and `drivers[]` on `quote_requested`, variable length; 71 quotes have an empty `vehicles` array |
| Premium | some carriers send a currency-formatted string like `"$1,284.50"`, others a bare number; `term` is `6mo` or `12mo` |
| Nulls | `applicant_id` null on anonymous `quote_requested` events; the id appears later on `quote_selected` |
| Schema drift | v2 `quote_requested` carries `telematics_opt_in`; v1 does not |
| PII | `dob` and `email` in the `applicant` block (fake values) |

Every count above is over all **6,320 records, duplicates included**, which is the right way to
read a raw feed. After deduplication in §2.4 the same counts come out lower: 1,200 quote requests
rather than 1,236, and 3,392 carrier quotes rather than 3,486. That difference is not an error;
it is the 157 redeliveries. Flag it now so nobody thinks Silver lost rows later.

The one CoCo may miss, and the one that matters most: **6,320 records but only 6,163 distinct
`(partition, offset)` pairs.** Event buses are at-least-once. If CoCo does not raise this, ask it
directly: *"are all 6,320 records distinct?"*

The second most likely miss is the no-offset ISO form. CoCo often reports "ISO 8601" as one
format. Ask: *"do all the ISO timestamps carry a timezone offset?"*

> **Facilitator talking point.** On the Guided Path *you* asked for the assessment and *you*
> decide what to do with it. Note how much CoCo inferred unprompted - that is foreshadowing for
> the Skills tier, where it works from your team's standards rather than your step-by-step.

> **Facilitator talking point - PII, once.** `dob` and `email` are in the payload. The design
> call for today is that they stay in RAW and do not reach Silver. Masking, classification, and
> tags are real answers in production and out of scope for this lab. One sentence, then move on.

Then make the design call yourself, out loud, before moving on:

> "We are keeping the envelope, not just the payload. `offset` is monotonic per partition, so it
> gives us a completeness check we cannot reconstruct once we throw it away, and `partition`
> gives every row a traceable origin. The payload lands as-is in one VARIANT; all flattening
> happens in Silver."

### Step 2.2 - Build the producer, source stubbed (you direct) - 25 min

> "Write a Python producer that streams these records into Snowflake using the
> `snowpipe-streaming` SDK (`from snowflake.ingest.streaming import StreamingIngestClient`).
> Requirements:
> - A function `consume_from_event_source(topic)` that **raises NotImplementedError**, with a
>   docstring sketching what a real implementation would do for both a Kafka consumer
>   (`confluent_kafka` poll and commit) and a Kinesis `get_records` loop, mapping each onto our
>   record shape, and noting what it omits: offset commit after durable write, rebalancing.
> - A function that reads the sample file and yields records of **identical shape**.
> - A `--source {synthetic,event_bus}` flag, defaulting to `synthetic`.
> - Everything downstream of that seam must not care which source it got.
> - One channel per partition, opened once and reused. Deterministic channel names of the form
>   `quotes-p0`.
> - Land `partition` as `CHANNEL_ID` (as `p0` to `p3`) and `offset` as `STREAM_OFFSET`, and pass
>   the offsets as the channel's offset tokens. Keep `event_id`, `source`, and `published_at`.
> - Land the payload into a VARIANT column as a **native dict**, not a JSON string.
> - Batch appends. Authenticate with a PAT via inline `properties`, no profile file. Print each
>   channel's `rows_error_count` at the end."

Verify by reading, not running:

- `consume_from_event_source()` raises, and its docstring is specific enough to implement from.
- Both source functions yield the same keys.
- The channel key is the **partition**, not `quote_id`. If CoCo channeled per quote, that is
  the teaching moment: 1,200 quotes means 1,200 channels, and worse, offsets are monotonic per
  *partition*, so per-quote channels destroy the ordering the completeness check depends on.
- Channel names are deterministic (`quotes-p0` to `quotes-p3`), so a restart reuses the channel
  instead of orphaning it.
- The payload is assigned a dict. If CoCo wrote `json.dumps(...)`, leave it - 2.3 catches it.
- The status attribute is `rows_error_count`, plural. Several published samples say
  `row_error_count`, which does not exist and raises `AttributeError`.

> **If a pair is stuck.** `reference/producer_reference.py` is the verified answer against SDK
> 1.8.0. Show the `to_row()` function and `channel_name()`, not the whole file.

### Step 2.3 - Land the RAW layer (you direct) - 20 min

> "Create the target table `RAW_QUOTE_EVENTS` with columns CHANNEL_ID, STREAM_OFFSET, EVENT_ID,
> SOURCE, PUBLISHED_AT, PAYLOAD. PAYLOAD is a VARIANT. Add table and column comments. Then run
> the producer against my schema and verify the load."

No `CREATE PIPE` is needed. The **default pipe** (`RAW_QUOTE_EVENTS-STREAMING`) is created
automatically on first use. Named pipes exist for in-flight transformation and clustering at
ingest time; you need neither today.

```bash
python <your_producer>.py --database NORTHPOINT_LAB --schema ATTENDEE_<n>
```

(whatever CoCo named the file)

Once the load has run, look at what the default pipe actually is:

```sql
SHOW PIPES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;
```

Its definition comes back as a Snowflake-managed
`COPY INTO RAW_QUOTE_EVENTS FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`.
There is no DDL of yours to maintain: the pipe is an implementation detail until the day you need
a transformation in flight.

Verify:

```sql
SELECT COUNT(*)                                   AS total_rows,
       COUNT(DISTINCT CHANNEL_ID)                 AS channels,
       COUNT(DISTINCT CHANNEL_ID, STREAM_OFFSET)  AS distinct_offsets,
       COUNT_IF(TYPEOF(PAYLOAD) = 'OBJECT')       AS payload_object,
       COUNT_IF(TYPEOF(PAYLOAD) = 'VARCHAR')      AS payload_varchar
FROM RAW_QUOTE_EVENTS;
```

Expect `6320 / 4 / 6163 / 6320 / 0`. RAW keeps every delivery, duplicates included; that is
correct, and it is the first half of trap 1.

> **The VARIANT-as-string check, before the fork.** If `payload_varchar` is non-zero, the
> producer passed a JSON *string* where the SDK wanted a native dict, and Snowflake stored
> escaped text rather than a structured object. Every downstream `PAYLOAD:field` access then
> silently returns null, on either Silver path. This is the single most common mistake against
> this SDK, and `TYPEOF` catches it in one query. It is checked here because it is a property of
> RAW and must be true before anyone forks.
>
> Run the check even though the 2.2 prompt explicitly asked for a native dict. That is the habit
> worth building: you instructed, and you still verify.

> **Do not trust the channel status counters for row counts.** `rows_inserted_count` and
> `latest_committed_offset_token` lag on the server side and often read 0 or None right after
> the flush returns, even when every row has landed. Confirm with `COUNT(*)` against the table.

> **Schema evolution, observed rather than configured.** Ask the room: v2 requests carry
> `telematics_opt_in` and v1 requests do not, so what did we have to change in RAW to accept
> both? Nothing. The field lives inside the VARIANT, and the default pipe took both versions
> without a DDL change. Promoting it to a typed column is a Silver decision, made in §2.4.

Note what did **not** happen: no bucket, no sink connector, no notification, no file. Rows went
from a process to a table.

### Break (10 min)

Before the room breaks, ask every pair to write down their §2.4 choice. Pairs that pick dbt
should open their Workspace when they come back, before the frame.

### Step 2.4 - Choose your path: build Silver (you direct) - 40 min

Both paths build the same four objects, with the same names and columns:

| Object | Grain |
|---|---|
| `SILVER_QUOTE` | one row per quote |
| `SILVER_CARRIER_QUOTE` | one row per carrier response |
| `SILVER_QUOTE_VEHICLE` | one row per vehicle per quote |
| `SILVER_QUOTE_DRIVER` | one row per driver per quote |

Both read from a deduplicated, conformed staging object, `STG_QUOTE_EVENTS`, never from RAW
directly. Both end on the same verify queries. Pick one:

| Choose | When |
|---|---|
| **2.4A Dynamic Tables** | you want Silver declared once and kept fresh by Snowflake against a target lag, with no orchestration to own |
| **2.4B dbt Projects on Snowflake** | your team already writes dbt, wants tests and models in version control, and wants dbt to run inside Snowflake rather than on a runner you maintain |

> **Facilitator note - the fork is cheap because the names match.** Shared object names are
> what let every verify query, the §2.5 debrief, and the documentation step be path-agnostic.
> Hold pairs to the names. A pair that calls its table `SLV_QUOTES` has opted out of the debrief.

> **Facilitator note - timing.** Both paths fit 40 minutes at median pace. Path B gets no extra
> time for being dbt. If a dbt pair is behind at the 25-minute mark, tell them to collapse the
> vehicle and driver children into one model rather than skip the tests.

#### 2.4A - Silver as Dynamic Tables

**Step A1 - staging.**

> "Create a Dynamic Table `STG_QUOTE_EVENTS` over `RAW_QUOTE_EVENTS`, warehouse
> NORTHPOINT_LAB_WH, with `TARGET_LAG = DOWNSTREAM` so it refreshes only when something reads
> it. One row per distinct `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with QUALIFY ROW_NUMBER().
> Promote the event type, event version, and quote id. Standardize every event-time encoding you
> found in 2.1 into a single `EVENT_TS_UTC`, and keep `PAYLOAD` for everything not promoted."

> **Note the prompt does not say "three formats" or name them.** That is deliberate and matches
> the attendee handout word for word. The prompt says "every encoding you found in 2.1", which
> makes 2.1's output load-bearing. If a pair's staging misses a format, that is 2.1 having been
> done carelessly, and it is worth saying so.

Verify:

```sql
SELECT COUNT(*)                       AS stg_rows,       -- 6163
       COUNT_IF(EVENT_TS_UTC IS NULL) AS unparsed_ts,    -- 0
       MIN(EVENT_TS_UTC), MAX(EVENT_TS_UTC)
FROM STG_QUOTE_EVENTS;
```

The timestamp recipe is the fiddly part. The verified form, from
`reference/silver_dt_reference.sql`:

```sql
CASE
    WHEN TYPEOF(PAYLOAD:event_ts) IN ('INTEGER', 'DECIMAL')
        THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(PAYLOAD:event_ts::VARCHAR), 3)
    WHEN REGEXP_LIKE(PAYLOAD:event_ts::VARCHAR, '.*[+-][0-9]{2}:[0-9]{2}$')
        THEN CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(PAYLOAD:event_ts::VARCHAR))::TIMESTAMP_NTZ
    ELSE TRY_TO_TIMESTAMP_NTZ(PAYLOAD:event_ts::VARCHAR)
END AS EVENT_TS_UTC
```

The no-offset form is UTC by producer convention and is parsed as NTZ on purpose; running
`TRY_TO_TIMESTAMP_TZ` on it would silently apply the session time zone instead.

> **If a pair is stuck - "Failed to cast variant value ... to FIXED" on a guarded cast.** A pair writes
> `PAYLOAD:event_ts::NUMBER` inside the `WHEN TYPEOF(...) IN ('INTEGER','DECIMAL')` branch and it
> still errors on the ISO strings. The CASE guard does not protect a hard cast: the optimizer may
> evaluate the cast before the test. Use `TRY_TO_NUMBER`, as above. This was hit during the build
> of the reference, so expect it in the room.

**Step A2 - the quote-grain table.**

> "Create a Dynamic Table `SILVER_QUOTE` over `STG_QUOTE_EVENTS`, TARGET_LAG = '5 minutes',
> warehouse NORTHPOINT_LAB_WH, one row per `quote_id`. Build it by aggregating across the events
> for each quote, not by joining event types together. Carry: when it was requested, the request's
> event version, channel, applicant id, whether the applicant was known at request time, state, zip, coverage level,
> deductible, any field present in only some event versions, vehicle and driver counts, how many carriers
> responded, the selected carrier, policy number, bound premium annualized, abandon reason, and a
> status of BOUND, ABANDONED, SELECTED, or OPEN. Do not carry date of birth or email."

Verify:

```sql
SELECT COUNT(*)                                    AS quotes,       -- 1200
       COUNT_IF(QUOTE_STATUS = 'BOUND')            AS bound,        -- 371
       COUNT_IF(QUOTE_STATUS = 'ABANDONED')        AS abandoned,    -- 578
       COUNT_IF(QUOTE_STATUS = 'SELECTED')         AS selected,     -- 251
       COUNT_IF(QUOTE_STATUS = 'OPEN')             AS open,         -- 0
       COUNT_IF(WAS_ANONYMOUS)                     AS anonymous     -- 191
FROM SILVER_QUOTE;
```

> **If a pair is stuck - anonymous count comes back 0.** The pair wrote
> `PAYLOAD:applicant:applicant_id IS NULL`. A JSON null is a VARIANT null, not a SQL NULL, so
> that predicate is never true. Cast first: `PAYLOAD:applicant:applicant_id::VARCHAR IS NULL`.
> The reference build hit exactly this, and the count was 0 until the cast went in.

> **Facilitator talking point - the anonymous quotes.** 191 quotes start anonymous. The shopper
> authenticates before selecting, so `quote_selected` carries the id, and the reference resolves
> it with `COALESCE` across the two events. A join on the request's `applicant_id` alone drops
> those quotes, which understates conversion exactly for the shoppers who were browsing before
> committing. Ask the room what they would do in their own pipeline; resolving forward is one
> answer, not the only one.

**Step A3 - the child tables.**

> "Create three more Dynamic Tables over `STG_QUOTE_EVENTS`, same lag and warehouse:
> `SILVER_CARRIER_QUOTE`, one row per carrier response, with the term, the premium typed as a
> number for every way the carriers send it, and an annualized premium; `SILVER_QUOTE_VEHICLE`
> and `SILVER_QUOTE_DRIVER`, one row per array element on the quote request, using LATERAL
> FLATTEN. Carry comments forward on all four Silver tables."

> **Same discipline on the premium.** The prompt describes "every way the carriers send it" and
> does not quote the string form. 2.1 found it; 2.4 has to use what 2.1 found.

Verify:

```sql
SELECT (SELECT COUNT(*) FROM SILVER_CARRIER_QUOTE)                        AS carrier_quotes,     -- 3392
       (SELECT COUNT_IF(TERM_PREMIUM IS NULL) FROM SILVER_CARRIER_QUOTE)  AS unparsed_premium,   -- 0
       (SELECT COUNT(*) FROM SILVER_QUOTE_VEHICLE)                        AS vehicles,           -- 1706
       (SELECT COUNT(*) FROM SILVER_QUOTE_DRIVER)                         AS drivers;            -- 2001
```

The 71 quotes with an empty `vehicles` array contribute no vehicle rows, which is correct: FLATTEN
without the `OUTER` option drops them, and the quote still exists in `SILVER_QUOTE` with a
`VEHICLE_COUNT` of 0.

**Step A4 - refresh mode.**

> "Show me the refresh mode of every Dynamic Table in my schema, and the reason if any of them
> is not incremental."

Verify: `SHOW DYNAMIC TABLES` reports five tables (`STG_QUOTE_EVENTS` and the four `SILVER_*`),
every one with `refresh_mode` = `INCREMENTAL`.

Then run the **shared verify queries** below.

#### 2.4B - Silver as dbt Projects on Snowflake

> **Facilitator note before they start.** No dbt packages in this lab, and dbt Cloud or a local dbt-core install would also work but neither is in the attendee path. Packages need `dbt deps`,
> which needs an external access integration, which is setup nobody has. Every test here is a
> dbt built-in or a singular test in SQL. If a pair adds `dbt_utils`, the run fails on deps, and
> that is expected, not a grant problem.

**Step B1 - scaffold the project in your Workspace.**

> "In my Snowflake Workspace, scaffold a dbt project named `northpoint_silver`. Create
> `dbt_project.yml` with staging models materialized as views and silver models as tables.
> Create `profiles.yml` for a Snowflake target with no password or key fields, since dbt Projects
> on Snowflake authenticate with the session, reading role, warehouse, database, and schema from
> environment variables. Create `env.yml` that resolves role and warehouse from
> `CURRENT_ROLE()` and `CURRENT_WAREHOUSE()`, and sets database `NORTHPOINT_LAB` and schema
> `ATTENDEE_<n>` as literal values. Create `models/sources.yml` with one source, `raw`, pointing
> at `RAW_QUOTE_EVENTS` in my schema."

Verify by reading:

- `profiles.yml` has no credentials, and its role is the caller's role via `env.yml`.
- `env.yml` database and schema are **literals**.
- `sources.yml` resolves to `NORTHPOINT_LAB.ATTENDEE_<n>.RAW_QUOTE_EVENTS`.

> **If a pair is stuck - the run fails with a null database or schema.** The pair let CoCo set
> `env.yml` database and schema from `CURRENT_DATABASE()` and `CURRENT_SCHEMA()`, by analogy
> with the role. Inside `EXECUTE DBT PROJECT` there is no current database or schema, so those
> return NULL and the run fails. Role and warehouse via `CURRENT_ROLE()` and
> `CURRENT_WAREHOUSE()` work, and are what make dbt run as the attendee's role. Database and
> schema must be literals. This was observed in the reference build.

> **Why the role comes from `CURRENT_ROLE()`.** dbt Projects on Snowflake runs with the role in
> `profiles.yml` *and* is limited to the calling user's privileges. A profile role that differs
> from `LAB_ATTENDEE_<n>` is the most likely Path B failure in a room. Resolving it from the
> caller removes the mismatch.

**Step B2 - the staging model.**

> "Write `models/staging/stg_quote_events.sql` over the `raw` source. One row per distinct
> `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with QUALIFY ROW_NUMBER(). Promote the event type,
> event version, and quote id. Standardize every event-time encoding you found in 2.1 into a
> single `EVENT_TS_UTC`, and keep `PAYLOAD` for everything not promoted."

Verify by reading: the `QUALIFY` partitions on `CHANNEL_ID, STREAM_OFFSET`, and the timestamp
logic has a branch per encoding found in 2.1. The same `TRY_TO_NUMBER` pitfall as Path A applies
here: a hard `::NUMBER` cast inside the guarded branch can still error on the ISO strings.

**Step B3 - the four Silver models.**

> "Write four models in `models/silver/`, all reading `stg_quote_events` through `ref()`:
> `silver_quote`, one row per quote, built by aggregating across each quote's events rather than
> joining event types together, carrying requested time, event version, channel, applicant id,
> whether the applicant was known at request time, state, zip, coverage level, deductible, any field present in only some event versions, vehicle and driver counts, carrier response count, selected carrier, policy
> number, bound premium annualized, abandon reason, and a status of BOUND, ABANDONED, SELECTED,
> or OPEN; `silver_carrier_quote`, one row per carrier response, with term, the premium typed as
> a number for every way the carriers send it, and an annualized premium;
> `silver_quote_vehicle` and `silver_quote_driver`, one row per array element using LATERAL
> FLATTEN. Do not carry date of birth or email. Use the same column names as a Dynamic Table
> build would, so the outputs are comparable."

The same VARIANT-null pitfall applies to `WAS_ANONYMOUS`: cast `applicant_id` to `VARCHAR`
before `IS NULL`, or the anonymous count is 0.

> **Ask the pair to say their materialization choice aloud.** The reference uses `table` for
> Silver. `incremental` on `STREAM_OFFSET` per channel is a legitimate choice and a better one at
> production volume; it is the pair's call, stated out loud, which keeps the design decision with
> the human.

**Step B4 - tests that encode the traps.**

> "Add tests, using only dbt built-ins and singular SQL tests, no packages. In
> `models/schema.yml`: `not_null` on `event_ts_utc` and `quote_id` in staging; `unique` and
> `not_null` on `quote_id` in `silver_quote`, which is the fanout test; `accepted_values` on
> quote status and on term; `not_null` on the typed premium; `relationships` from the vehicle and
> driver models back to `silver_quote`. In `tests/`: one singular test that staging has exactly
> one row per `(CHANNEL_ID, STREAM_OFFSET)`, one that RAW has no offset gaps per channel, and one
> that no `SILVER_` table in my schema has a `DOB` or `EMAIL` column, reading
> INFORMATION_SCHEMA."

Verify by reading: 9 generic tests in `schema.yml` and 3 singular tests in `tests/`, 12 in total.

> **If a pair is stuck - the PII test passes, but suspiciously early.** A singular test that
> reads INFORMATION_SCHEMA rather than a `ref()` has no dependency on the models, so dbt is free
> to run it *before* the Silver tables exist, and it passes vacuously on an empty schema. Fix it
> with explicit dependency hints at the top of the test, one per model:
>
> ```sql
> -- depends_on: {{ ref('silver_quote') }}
> -- depends_on: {{ ref('silver_carrier_quote') }}
> -- depends_on: {{ ref('silver_quote_vehicle') }}
> -- depends_on: {{ ref('silver_quote_driver') }}
> ```
>
> The reference build had this exact bug. It is a good one to show the room, because the test
> was green and wrong.

**Step B5 - deploy and run.**

> "Deploy this project from my Workspace as a dbt project object named `NORTHPOINT_SILVER` in
> my schema, then run it with `EXECUTE DBT PROJECT` and `ARGS = 'build'`, and show me the result
> of every model and test."

```sql
EXECUTE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER ARGS = 'build';
```

Verify: 5 models and 12 tests, **17 of 17 PASS**.

> **Facilitator talking point - deliberately break it.** If a pair finishes early, have them
> join `silver_quote_vehicle` back into `silver_quote` and rerun. The `unique` test on
> `quote_id` fails, which is the fanout trap caught by a test rather than by a human reading
> counts. Revert before the shared verify.

> **Facilitator aside - the paths are not mutually exclusive.** dbt-snowflake can materialize a
> model *as* a Dynamic Table (`materialized='dynamic_table'`). In production a team can author in
> dbt, test in dbt, and still get Snowflake-managed incremental refresh. Talking point only;
> nobody builds it today.

> **Facilitator aside - scheduling.** The attendee path runs the project by hand. In production
> you would wrap `EXECUTE DBT PROJECT` in a task on a schedule. One sentence; it is not a lab
> step.

Then run the **shared verify queries**.

#### Shared verify queries (both paths)

These are the three named traps. They are identical for 2.4A and 2.4B, because the object names
are. Go through them explicitly.

1. **At-least-once duplicates.** RAW keeps every delivery; staging keeps one per
   `(CHANNEL_ID, STREAM_OFFSET)`.
   ```sql
   SELECT (SELECT COUNT(*) FROM RAW_QUOTE_EVENTS)                           AS raw_rows,          -- 6320
          (SELECT COUNT(*) FROM STG_QUOTE_EVENTS)                           AS stg_rows,          -- 6163
          (SELECT COUNT(DISTINCT CHANNEL_ID, STREAM_OFFSET) FROM RAW_QUOTE_EVENTS) AS distinct_offsets; -- 6163
   ```
   If `stg_rows` is lower than `distinct_offsets`, staging deduped on something coarser, such as
   `quote_id` plus event type, and has thrown away real records. The dedupe belongs in Silver,
   not in the producer: you cannot make an at-least-once source exactly-once by trying harder in
   the consumer; you make it idempotent downstream.

2. **Array fanout.** Joining the flattened vehicles or drivers back to the quote multiplies
   rows. The quote grain must hold.
   ```sql
   SELECT COUNT(*) AS quote_rows, COUNT(DISTINCT QUOTE_ID) AS distinct_quotes  -- 1200, 1200
   FROM SILVER_QUOTE;
   ```
   If `quote_rows` is larger, a child was joined back in. With 1,706 vehicles and 2,001 drivers
   across 1,200 quotes, a vehicle-and-driver join inflates every quote-level total, including
   bound premium.

3. **Offset gaps.** A gap in a channel's offsets means a record was lost in flight. This check
   only exists because the envelope was kept in 2.1.
   ```sql
   WITH d AS (SELECT DISTINCT CHANNEL_ID, STREAM_OFFSET FROM RAW_QUOTE_EVENTS)
   SELECT CHANNEL_ID, STREAM_OFFSET,
          LAG(STREAM_OFFSET) OVER (PARTITION BY CHANNEL_ID ORDER BY STREAM_OFFSET) AS prev
   FROM d
   QUALIFY STREAM_OFFSET != prev + 1;
   ```
   Must return **zero rows**. Dedupe before checking, or the duplicates read as false gaps.

And one silent-null check that catches a missed encoding on either path:

```sql
SELECT (SELECT COUNT_IF(EVENT_TS_UTC IS NULL) FROM STG_QUOTE_EVENTS)     AS unparsed_ts,         -- 0
       (SELECT COUNT_IF(TERM_PREMIUM IS NULL) FROM SILVER_CARRIER_QUOTE) AS unparsed_premium,    -- 0
       (SELECT COUNT_IF(APPLICANT_ID IS NULL AND QUOTE_STATUS IN ('SELECTED','BOUND'))
          FROM SILVER_QUOTE)                                             AS unresolved_applicant; -- 0
```

> **Facilitator note - the two paths really are one lab.** In the reference build, a row-level
> `MINUS` in both directions between the Path A and Path B Silver tables returned 0 rows on all
> four tables. If a DT pair and a dbt pair both pass the shared verify, their Silver should be
> identical, and you can say so with evidence.

### Step 2.5 - Compare paths and document (you direct) - 15 min

**Cross-path debrief (5 min).** Pick one Dynamic Tables pair and one dbt pair. Ask each, briefly:

- What was easier than you expected, and what was harder?
- Where did the traps bite - which verify query or test caught something?
- How does your Silver get refreshed tomorrow, with nobody touching it?

Draw out the contrast: the DT pair's answer to the third question is `TARGET_LAG`; the dbt pair's
answer is "someone schedules `EXECUTE DBT PROJECT`", and their answer to the second is a red test.
Neither is better in the abstract. Close with the `materialized='dynamic_table'` point if it has
not come up.

**Document (10 min).**

> "From live metadata in my schema, generate `PIPELINE_DOCS.md` (executive summary,
> source-to-target mapping per Silver object, lineage diagram, business rules including the
> dedupe, timestamp, premium, and anonymous-applicant logic, PII inventory, refresh pattern) and
> `ADD_COMMENTS.sql`. Then apply the comments."

Verify: every Silver object has a table comment; the PII inventory names `dob` and `email` as
present in RAW and absent from Silver; the refresh pattern section matches the pair's path.

> **If time is short,** cut this half to a walkthrough of the shipped
> `reference/PIPELINE_DOCS.md`. Keep the debrief.

---

## 3. Close - What the future holds (15 min)

**The point:** show the room the ceiling, then point one tier up. No building here.

### 3.1 Point CoCo at a pipeline you already own (5 min, narrative by default)

Everything so far was greenfield. Most of the work is the pipeline that already exists, that
nobody documented, and that somebody now wants modernized. CoCo reads that as well as it writes
new code, and for many rooms this is the highest-value thing they take away.

The prompt, shown rather than run:

> "Read the SQL behind <a pipeline you own>. Explain what it does in plain English, produce a
> source-to-target mapping, and draw the lineage. Then flag anything risky: implicit cross
> joins, `SELECT *` in views other objects depend on, non-sargable filters, silent NULL
> handling, missing deduplication, unexplained literals. For each one, tell me what it does to
> the numbers."

What to draw out:

- **The failures that matter are the silent ones.** A legacy pipeline where `TRY_TO_DECIMAL`
  returns NULL on a currency-formatted premium, and `SUM` skips the NULL, reports a number that
  is simply wrong while every job runs green. That is the premium problem the room fixed in
  Silver an hour ago.
- **Reading is lower risk than writing.** For a team nervous about pointing an agent at
  production, comprehension and documentation is the safe first use case.

> **Facilitator options.** Run it live only if the clock allows and an attendee volunteers a
> pipeline; take the extra minutes from 3.2, not 3.3. A deliberately bad legacy pipeline ships at
> `reference/legacy_pipeline_setup.sql` as an optional prop. Skip it on a first delivery.
> Deciding *what to do* about a legacy pipeline is architecture work and belongs to the next
> tiers; park those questions.

### 3.2 Plan-mode teaser on a gold-layer BRD (7 min)

Feed CoCo a short **fake BRD** and switch to **plan mode**. Let the room watch it produce a full
plan from one paragraph of intent, *without building it.*

> **BRD: Quote-to-bind conversion by carrier.** Carrier management needs a daily view, per
> carrier, of quotes returned, quotes selected, policies bound, quote-to-bind conversion rate,
> and median annualized premium offered versus bound, with week-over-week movement. It reads off
> our Silver quote tables and must be current within the hour. Quotes that started anonymous must
> be counted, not dropped. Follow our coding standards.

While it plans, narrate the shape of the gold layer it should land on: aggregates off
`SILVER_CARRIER_QUOTE` and `SILVER_QUOTE`, one grain per business question, built the same way
the pair built Silver (Dynamic Tables or dbt models). Two things to draw out. The BRD's
anonymous-quote requirement is the null `applicant_id` problem from 2.4 arriving as a business
requirement. And "current within the hour" is where a target lag becomes a product decision.

Then name the jump: the room did not give steps, they gave an outcome, and the agent produced the
plan. That is the **Skills** tier. Packaging today's patterns (the consume/send seam, the dedupe
rule, the timestamp recipe, the trap tests) as a reusable skill is how the next stream onboards
itself; **using** that skill is Skills, and **building and hardening** it is Build Your Own.

### 3.3 Place the room on the path (3 min)

"You worked on the **Guided Path** today. Next is **Skills** - you lean on the team's encoded
standards and gate the agent. Beyond that, **Build Your Own** - the tooling that keeps the whole
team consistent." Name the Tier 2 boundary plainly: today you held the plan; in Skills the agent
holds it and your job becomes judging it.

> Keep the future state honest: a direction of travel, aligned with Product before delivery
> (roadmap, pending PM alignment), not a commitment.

---

## Facilitator prep

- **Read `setup/PERMISSIONS.md`, then run `setup/facilitator_setup.sql`.** It creates
  `NORTHPOINT_LAB`, `NORTHPOINT_LAB_WH`, and per attendee a role `LAB_ATTENDEE_<n>` that **owns**
  schema `NORTHPOINT_LAB.ATTENDEE_<n>`. No ACCOUNTADMIN: SYSADMIN for the database and warehouse,
  USERADMIN for roles and users. Hand `PERMISSIONS.md` to a customer admin if they are running
  setup for you.
- **Minimum attendee grants** (from `PERMISSIONS.md`): `USAGE` on the database, `USAGE` on the
  warehouse, and `OWNERSHIP` of their own schema. Ownership is the design, not a convenience:
  Dynamic Tables refresh as the owner role and auto-enable change tracking on RAW, and both fail
  if the attendee role does not own RAW. Do not use a shared, facilitator-owned RAW table. If the
  customer will not grant schema ownership, the setup script carries the alternative:
  `CREATE TABLE`, `CREATE VIEW`, `CREATE DYNAMIC TABLE`, and `CREATE DBT PROJECT` on the schema,
  plus `OPERATE` and `MONITOR` on future dynamic tables and `USAGE` and `MONITOR` on future dbt
  projects.
- **Not needed** (say this to the customer admin up front): ACCOUNTADMIN, SYSADMIN, or
  SECURITYADMIN for any attendee; `CREATE PIPE`, because the default pipe is auto-created; any
  integration, whether external access, storage, notification, or API; `CREATE TASK` or
  `EXECUTE TASK`; `IMPERSONATE`.
- **Workspace pre-check.** Have each attendee sign in to Snowsight before the session and
  confirm Projects, then Workspaces, lets them create one. Workspaces live in the user's personal
  database, and some accounts restrict personal databases. The setup script also checks the
  `2026_06` behavior bundle status, since the lab assumes a single mutable live version of the
  dbt project object.
- **Proven under a non-admin attendee role.** The full lab was run end to end as a test user
  holding only `LAB_ATTENDEE_01`, authenticated with a PAT restricted to that role, after running
  `setup/facilitator_setup.sql` as shipped:
  - default-pipe ingest: all 6,320 rows landed through `RAW_QUOTE_EVENTS-STREAMING` on four
    channels, payloads `OBJECT`, the later-version field arrived with no schema change configured;
  - Path A: five Dynamic Tables created by the attendee, all `INCREMENTAL` (the attendee owns RAW,
    so enabling change tracking succeeds);
  - Path B: the attendee deployed the dbt project object and `dbt build` passed 17 of 17, with the
    role resolved from `CURRENT_ROLE()`;
  - the two paths' Silver tables were identical (row-level `MINUS` both ways, 0 rows);
  - introducing the fanout join made `unique_silver_quote_quote_id` fail with 460 duplicates.
- **Not proven: deploying from a Workspace.** The test deployed with
  `CREATE DBT PROJECT ... FROM` a stage, because a Workspace is tied to an interactive user. The
  Workspace UI path and personal-database access in the customer account are the remaining
  unknowns. Confirm both in your dry run.
- **Dry-run both paths before the session,** under an attendee role, not your own. Path A should
  end with five Dynamic Tables all `INCREMENTAL`; Path B with 17 of 17 PASS. The reference
  `env.yml` ships with `DBT_CURRENT_SCHEMA: ATTENDEE_01`; change it to the dry-run schema.
- **Personal access tokens.** Issue one per attendee, restricted to their lab role:
  ```sql
  ALTER USER <attendee> ADD PROGRAMMATIC ACCESS TOKEN NORTHPOINT_LAB
    ROLE_RESTRICTION = 'LAB_ATTENDEE_<n>' DAYS_TO_EXPIRY = 2;
  ```
  The secret is shown **once**, on creation. Capture it then and distribute privately.
  *Key-pair fallback:* if account policy forbids PATs, have attendees generate keys with
  `openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt`, set
  `RSA_PUBLIC_KEY` on their user, and switch the producer to `authorization_type: "JWT"` with
  `private_key_file`. Pre-generate before the session; otherwise budget an extra 10 minutes in §1.
- **SDK prerequisites.** `pip install snowpipe-streaming`, Python 3.9+, glibc 2.26+ on Linux.
  Have attendees install before the session; on a conference network this is the single most
  likely thing to eat 20 minutes.
- **If you deploy the reference dbt project from a laptop,** note that snow CLI 3.16 rejects
  `env_var()` in `profiles.yml` client-side. Use CLI 3.22 or later, or
  `CREATE DBT PROJECT ... FROM` a stage. Attendees deploy from a Workspace, which is unaffected.
- **Distribute the attendee package.** Attendees get `attendee/` and nothing else:
  `lab-handout.md` and `sample_quote_events.jsonl`. The handout carries the setup steps, the
  prompts for **both** §2.4 paths verbatim, and the verify queries **without expected values**, so
  2.1 stays a genuine discovery. Do not hand out this guide, `reference/`, or `setup/`.
  - Because the handout has no expected values, **you are the answer key.** Have the 2.1 table,
    the shared verify expectations, and the five "if a pair is stuck" notes in front of you.
- **The payload** also lives in `dataset/` alongside its generator. Regenerate with
  `python dataset/generate_sample_payload.py` if needed, then copy it into `attendee/`. It is
  seeded, so output is byte-identical and every count in this guide stays true.
- **Reference outputs on hand:** `reference/producer_reference.py`,
  `reference/silver_dt_reference.sql` (Path A), `reference/dbt_project/` (Path B),
  `reference/PIPELINE_DOCS.md`, `reference/ADD_COMMENTS.sql`. Use them to unblock a stuck pair
  and to compare, not to hand out.
- **Optional prop for 3.1:** `reference/legacy_pipeline_setup.sql`. Skip it on a first delivery.
- **The fake BRD** in 3.2 is ready to paste.
- **Afterwards, run `setup/teardown.sql`** and revoke the PATs. Teardown drops each attendee
  schema with `CASCADE`, which removes Dynamic Tables, dbt tables, the dbt project object, RAW,
  its default pipe, and the channels, whichever path the pair took. It deliberately does not drop
  objects by type: in a mixed room `DROP DYNAMIC TABLE` errors on a pair's dbt table of the same
  name. Verified to leave nothing behind.

## Facilitator checklist

- [ ] `PERMISSIONS.md` shared with the customer admin; `facilitator_setup.sql` run; database,
      warehouse, per-attendee roles, and owned schemas exist.
- [ ] Both paths dry-run under an attendee role: Path A five DTs `INCREMENTAL`, Path B 17 of 17
      PASS. Workspace deploy confirmed in this account.
- [ ] PATs issued, restricted to `LAB_ATTENDEE_<n>`, and distributed (secrets captured at
      creation).
- [ ] Attendees have `snowpipe-streaming` installed and a working Cortex Code connection.
- [ ] Every attendee confirmed they can create a Workspace.
- [ ] Each attendee confirmed `SELECT CURRENT_ROLE(), CURRENT_SCHEMA()` returns their own role
      and schema.
- [ ] `attendee/` distributed (handout and payload). This guide, `reference/`, and `setup/`
      **not** distributed.
- [ ] 2.1 expected findings, shared verify expectations, and the stuck-pair notes to hand.
- [ ] Fake BRD prepared for 3.2.
- [ ] Asked in advance whether anyone will volunteer a pipeline of their own for 3.1.
- [ ] Framing ([`00-three-tiers.md`](../00-three-tiers.md)) opened at start, referenced per lab,
      closed at end.
- [ ] `teardown.sql` run and PATs revoked afterward.

## Changes from the streaming-path (game telemetry) lab

- **Domain.** Game telemetry to an insurance-quote marketplace, with five lifecycle events keyed
  by `quote_id`.
- **Source envelope is generic.** The Kinesis-specific envelope with a base64 `data` field is
  replaced by `event_id`, `source`, `partition`, `offset`, `published_at`, and a nested JSON
  `payload`. Partition and offset replace shard and sequence number one for one, and the
  current-state diagram is a generic event bus to object-storage sink to file-based Snowpipe.
- **Nested arrays.** `vehicles[]` and `drivers[]` make LATERAL FLATTEN and child Silver tables a
  core step, and introduce the fanout trap.
- **Silver is a fork.** Pairs choose Dynamic Tables (2.4A) or dbt Projects on Snowflake (2.4B).
  Both build the same four `SILVER_*` objects and share one set of verify queries; the streaming
  lab was Dynamic Tables only.
- **Traps rearranged.** Duplicates and offset gaps carry over. Array fanout replaces
  VARIANT-as-string as the third named trap; the VARIANT check moves to §2.3's Verify, because it
  is a RAW property that must hold before the fork.
- **New silent-failure content.** Currency-formatted premium strings with mixed 6- and 12-month
  terms, three event-time encodings chosen per event type, anonymous applicants resolved forward,
  and the VARIANT-null predicate.
- **Four build pitfalls added as stuck-pair notes,** all found while building the reference: the
  guarded hard cast, the VARIANT null, literal database and schema in `env.yml`, and the vacuous
  INFORMATION_SCHEMA test. The CLI `env_var()` rejection is in Facilitator prep.
- **Permissions model.** One owner role per attendee schema, with `setup/PERMISSIONS.md` as the
  reference, replacing a shared lab role.
- **Timing.** 180 minutes including a 10-minute break, against the streaming lab's 195-minute plan (180 plus a 15-minute
  break). §1 grows to 20 for the Workspace check, §2.4 to 40 for the fork, and §3 shrinks to 15:
  the gold-layer narrative and skill packaging fold into the 3.2 plan-mode teaser, and 3.1 is
  narrative by default.
- **PII is one talking point.** `dob` and `email` are excluded from Silver and tested for in Path
  B; masking and classification are out of scope.

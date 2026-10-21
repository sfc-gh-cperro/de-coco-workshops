# Insurance Path Lab - Attendee Handout

Landing nested insurance-quote events from an event bus with Snowpipe Streaming, then building
a refined Silver layer on top of them with your choice of Dynamic Tables or dbt Projects on
Snowflake.

You will direct Cortex Code (CoCo) step by step. Each step below gives you a prompt to issue and
a way to check the result. Issue the prompt, read what comes back, judge it, and move on when you
are satisfied. **You** hold the plan today; CoCo executes it.

Work in your own schema, under your own role. Pair up if the room is set up that way.

---

## The scenario

**Northpoint Insurance Marketplace** is an online marketplace where a shopper requests an auto
insurance quote once and gets offers back from several carriers. Every quote produces a short
lifecycle of events from different services, keyed by `quote_id`: the request (applicant,
vehicles, drivers, coverage asks), one response per carrier, a selection, and then either a bound
policy or an abandonment.

You are given `sample_quote_events.jsonl`: records exactly as a consumer reads them off the bus. A
small envelope (`event_id`, `source`, `partition`, `offset`, `published_at`) wraps a nested JSON
`payload`. Expect realistic mess; finding it is your first job.

Today the events reach Snowflake like this:

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

Latency is a function of file arrival, not event arrival. The sink buffers until a threshold
trips, the file lands, a notification fires, and the pipe copies it.

By the end of the lab you will have built this instead:

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

**There is no live event bus in this lab, no cloud account, and no object storage, and that is
deliberate.** You will have CoCo build the producer with the consume side explicitly stubbed: a
named function that raises, documents what the real implementation does, and sits behind a flag.
Everything downstream of that seam is production-shaped. If the boundary between "get records"
and "send records" is drawn properly, swapping the stub for a real Kafka or Kinesis consumer is a
one-function change and nothing else moves.

At step 2.4 you choose how to build Silver. Start thinking now about which option looks more like
your day job.

---

## 1. Setup

Everyone works in the shared workshop database, isolated by schema and role. Your facilitator has
already created a role `LAB_ATTENDEE_<n>` for you that owns schema `NORTHPOINT_LAB.ATTENDEE_<n>`.

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
  account locator that `CURRENT_ACCOUNT()` returns. Get it with
  `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`
- The SDK authenticates with a **personal access token** restricted to your lab role. Your
  facilitator will give you one. There is no key pair to generate and no `profile.json` to
  distribute.

Connect Cortex Code to the account (OAuth) as normal.

**Workspace check, whichever path you expect to take.** In Snowsight, open Projects, then
Workspaces, and confirm you can create a Workspace. Path B is authored there, and pairs sometimes
switch paths at 2.4. If you cannot create one, tell your facilitator now.

---

## 2. The lab

### Step 2.1 - Assess the payload

Prompt CoCo:

> "Read `sample_quote_events.jsonl`. It is a sample of records as a consumer reads them off our
> event bus. Tell me: how many records, what the envelope fields are, what distinct event types
> are in the payload, the field inventory per event type including nested objects and arrays,
> which fields are candidate keys, and every data-quality problem you can see. Don't load
> anything yet, and don't propose an architecture yet."

Read the answer properly before moving on. You are looking for the shape of the data and
everything about it that will make the later steps harder. Steps 2.4A and 2.4B refer back to what
you found here, so be thorough. Be skeptical: if something in the summary looks too clean, check
it yourself.

Before you build anything, make the design call yourself and say it out loud to your pair or the
room: **are you landing the envelope, the payload, or both, and how will the payload be stored?**
Have a reason.

### Step 2.2 - Build the producer, source stubbed

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
- What is the channel keyed on? Ask yourself why that choice matters for ordering.
- Channel names are deterministic (`quotes-p0` to `quotes-p3`), so a restart reuses the channel
  instead of orphaning it.
- How is the payload assigned: a dict, or a string? Note it; step 2.3 checks it.

### Step 2.3 - Land the RAW layer

> "Create the target table `RAW_QUOTE_EVENTS` with columns CHANNEL_ID, STREAM_OFFSET, EVENT_ID,
> SOURCE, PUBLISHED_AT, PAYLOAD. PAYLOAD is a VARIANT. Add table and column comments. Then run
> the producer against my schema and verify the load."

No `CREATE PIPE` is needed. The **default pipe** (`RAW_QUOTE_EVENTS-STREAMING`) is created
automatically on first use.

```bash
python <your_producer>.py --database NORTHPOINT_LAB --schema ATTENDEE_<n>
```

(whatever CoCo named the file)

Once the load has run, look at the pipe you never created:

```sql
SHOW PIPES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;
```

Verify:

```sql
SELECT COUNT(*)                                   AS total_rows,
       COUNT(DISTINCT CHANNEL_ID)                 AS channels,
       COUNT(DISTINCT CHANNEL_ID, STREAM_OFFSET)  AS distinct_offsets,
       COUNT_IF(TYPEOF(PAYLOAD) = 'OBJECT')       AS payload_object,
       COUNT_IF(TYPEOF(PAYLOAD) = 'VARCHAR')      AS payload_varchar
FROM RAW_QUOTE_EVENTS;
```

Pass criteria: `TYPEOF(PAYLOAD)` should be `OBJECT` for every row, so `payload_object` should
match `total_rows` and `payload_varchar` must be zero. Compare `total_rows` with
`distinct_offsets` against what you found in 2.1. Do not trust the channel status counters for
row counts; confirm with `COUNT(*)` against the table.

Question for your pair: did 2.1 find more than one event version? If so, what did you have to change
in RAW to accept them?

### Break

Before the break, write down your 2.4 choice. If you pick dbt, open your Workspace when you come
back.

### Step 2.4 - Choose your path: build Silver

Both paths build the same four objects, with the same names and columns:

| Object | Grain |
|---|---|
| `SILVER_QUOTE` | one row per quote |
| `SILVER_CARRIER_QUOTE` | one row per carrier response |
| `SILVER_QUOTE_VEHICLE` | one row per vehicle per quote |
| `SILVER_QUOTE_DRIVER` | one row per driver per quote |

Both read from a deduplicated, conformed staging object, `STG_QUOTE_EVENTS`, never from RAW
directly. Both end on the same verify queries. Keep the names exactly as shown. Pick one:

| Choose | When |
|---|---|
| **2.4A Dynamic Tables** | you want Silver declared once and kept fresh by Snowflake against a target lag, with no orchestration to own |
| **2.4B dbt Projects on Snowflake** | your team already writes dbt, wants tests and models in version control, and wants dbt to run inside Snowflake rather than on a runner you maintain |

#### 2.4A - Silver as Dynamic Tables

**Step A1 - staging.**

> "Create a Dynamic Table `STG_QUOTE_EVENTS` over `RAW_QUOTE_EVENTS`, warehouse
> NORTHPOINT_LAB_WH, with `TARGET_LAG = DOWNSTREAM` so it refreshes only when something reads
> it. One row per distinct `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with QUALIFY ROW_NUMBER().
> Promote the event type, event version, and quote id. Standardize every event-time encoding you
> found in 2.1 into a single `EVENT_TS_UTC`, and keep `PAYLOAD` for everything not promoted."

Verify:

```sql
SELECT COUNT(*)                       AS stg_rows,
       COUNT_IF(EVENT_TS_UTC IS NULL) AS unparsed_ts,
       MIN(EVENT_TS_UTC), MAX(EVENT_TS_UTC)
FROM STG_QUOTE_EVENTS;
```

Pass criteria: `unparsed_ts` must be zero, and `stg_rows` should match `distinct_offsets` from
2.3. Check the min and max as well: a timestamp can parse and still be wrong.

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
SELECT COUNT(*)                                    AS quotes,
       COUNT_IF(QUOTE_STATUS = 'BOUND')            AS bound,
       COUNT_IF(QUOTE_STATUS = 'ABANDONED')        AS abandoned,
       COUNT_IF(QUOTE_STATUS = 'SELECTED')         AS selected,
       COUNT_IF(QUOTE_STATUS = 'OPEN')             AS open,
       COUNT_IF(WAS_ANONYMOUS)                     AS anonymous
FROM SILVER_QUOTE;
```

Pass criteria: `quotes` should match the distinct quote count you found in 2.1, and the four
status counts should sum to `quotes`. Does the `anonymous` count agree with what 2.1 told you?

**Step A3 - the child tables.**

> "Create three more Dynamic Tables over `STG_QUOTE_EVENTS`, same lag and warehouse:
> `SILVER_CARRIER_QUOTE`, one row per carrier response, with the term, the premium typed as a
> number for every way the carriers send it, and an annualized premium; `SILVER_QUOTE_VEHICLE`
> and `SILVER_QUOTE_DRIVER`, one row per array element on the quote request, using LATERAL
> FLATTEN. Carry comments forward on all four Silver tables."

Verify:

```sql
SELECT (SELECT COUNT(*) FROM SILVER_CARRIER_QUOTE)                        AS carrier_quotes,
       (SELECT COUNT_IF(TERM_PREMIUM IS NULL) FROM SILVER_CARRIER_QUOTE)  AS unparsed_premium,
       (SELECT COUNT(*) FROM SILVER_QUOTE_VEHICLE)                        AS vehicles,
       (SELECT COUNT(*) FROM SILVER_QUOTE_DRIVER)                         AS drivers;
```

Pass criteria: `unparsed_premium` must be zero. A NULL premium is money you have silently
deleted.

**Step A4 - refresh mode.**

> "Show me the refresh mode of every Dynamic Table in my schema, and the reason if any of them
> is not incremental."

Pass criteria: `SHOW DYNAMIC TABLES` lists five tables (`STG_QUOTE_EVENTS` and the four
`SILVER_*`), and every `refresh_mode` should be `INCREMENTAL`.

Then run the **shared verify queries** below.

#### 2.4B - Silver as dbt Projects on Snowflake

No dbt packages in this lab: packages need `dbt deps`, which needs an external access
integration nobody has set up. Use dbt built-in tests and singular SQL tests only.

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

**Step B2 - the staging model.**

> "Write `models/staging/stg_quote_events.sql` over the `raw` source. One row per distinct
> `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with QUALIFY ROW_NUMBER(). Promote the event type,
> event version, and quote id. Standardize every event-time encoding you found in 2.1 into a
> single `EVENT_TS_UTC`, and keep `PAYLOAD` for everything not promoted."

Verify by reading: the `QUALIFY` partitions on `CHANNEL_ID, STREAM_OFFSET`, and the timestamp
logic has a branch for every encoding you found in 2.1.

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

Decide your Silver materialization and say it out loud to your pair, with a reason.

**Step B4 - tests.**

> "Add tests, using only dbt built-ins and singular SQL tests, no packages. In
> `models/schema.yml`: `not_null` on `event_ts_utc` and `quote_id` in staging; `unique` and
> `not_null` on `quote_id` in `silver_quote`, which is the fanout test; `accepted_values` on
> quote status and on term; `not_null` on the typed premium; `relationships` from the vehicle and
> driver models back to `silver_quote`. In `tests/`: one singular test that staging has exactly
> one row per `(CHANNEL_ID, STREAM_OFFSET)`, one that RAW has no offset gaps per channel, and one
> that no `SILVER_` table in my schema has a `DOB` or `EMAIL` column, reading
> INFORMATION_SCHEMA."

Verify by reading: every test named in the prompt exists, in `schema.yml` or `tests/`. 

**Step B5 - deploy and run.**

> "Deploy this project from my Workspace as a dbt project object named `NORTHPOINT_SILVER` in
> my schema, then run it with `EXECUTE DBT PROJECT` and `ARGS = 'build'`, and show me the result
> of every model and test."

```sql
EXECUTE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER ARGS = 'build';
```

Pass criteria: all five models build and all dbt tests pass.

Then run the **shared verify queries**.

#### Shared verify queries (both paths)

Run all four, whichever path you took.

**1. Did the dedupe do what you think?**

```sql
SELECT (SELECT COUNT(*) FROM RAW_QUOTE_EVENTS)                           AS raw_rows,
       (SELECT COUNT(*) FROM STG_QUOTE_EVENTS)                           AS stg_rows,
       (SELECT COUNT(DISTINCT CHANNEL_ID, STREAM_OFFSET) FROM RAW_QUOTE_EVENTS) AS distinct_offsets;
```

Pass criteria: `stg_rows` and `distinct_offsets` should match.

**2. Does the quote grain hold?**

```sql
SELECT COUNT(*) AS quote_rows, COUNT(DISTINCT QUOTE_ID) AS distinct_quotes
FROM SILVER_QUOTE;
```

Pass criteria: these two counts should match.

**3. Is anything missing?**

```sql
WITH d AS (SELECT DISTINCT CHANNEL_ID, STREAM_OFFSET FROM RAW_QUOTE_EVENTS)
SELECT CHANNEL_ID, STREAM_OFFSET,
       LAG(STREAM_OFFSET) OVER (PARTITION BY CHANNEL_ID ORDER BY STREAM_OFFSET) AS prev
FROM d
QUALIFY STREAM_OFFSET != prev + 1;
```

Pass criteria: **must return zero rows.** Work out for yourself why the query deduplicates first.

**4. Any silent nulls?**

```sql
SELECT (SELECT COUNT_IF(EVENT_TS_UTC IS NULL) FROM STG_QUOTE_EVENTS)     AS unparsed_ts,
       (SELECT COUNT_IF(TERM_PREMIUM IS NULL) FROM SILVER_CARRIER_QUOTE) AS unparsed_premium,
       (SELECT COUNT_IF(APPLICANT_ID IS NULL AND QUOTE_STATUS IN ('SELECTED','BOUND'))
          FROM SILVER_QUOTE)                                             AS unresolved_applicant;
```

Pass criteria: all three must be zero.

### Step 2.5 - Compare paths and document

Your facilitator will pick one Dynamic Tables pair and one dbt pair for a short debrief. Be ready
to answer:

- What was easier than you expected, and what was harder?
- Which verify query or test caught something?
- How does your Silver get refreshed tomorrow, with nobody touching it?

Then document:

> "From live metadata in my schema, generate `PIPELINE_DOCS.md` (executive summary,
> source-to-target mapping per Silver object, lineage diagram, business rules including the
> dedupe, timestamp, premium, and anonymous-applicant logic, PII inventory, refresh pattern) and
> `ADD_COMMENTS.sql`. Then apply the comments."

Verify: every Silver object has a table comment; the PII inventory says where `dob` and `email`
live and confirms they are absent from Silver; the refresh pattern section matches your path. Then
read the generated docs critically: is the business-rules section correct about what your Silver
does, or is it describing what it assumes you did?

---

## Reference

**Envelope fields** in `sample_quote_events.jsonl`:

| Field | Notes |
|-------|-------|
| `event_id` | Producer-assigned UUID. |
| `source` | The emitting service. |
| `partition` | Stream partition the record was read from (a Kafka partition or Kinesis shard). |
| `offset` | Monotonic **per partition** (a Kafka offset or Kinesis sequence number). |
| `published_at` | Broker timestamp, ISO 8601 UTC. Not the same thing as event time. |
| `payload` | The nested event, as JSON (not base64). |

**Two simplifications** in the sample, both worth knowing:

- Offsets are contiguous per partition, so a gap is unambiguously a lost record. Real Kafka
  offsets are contiguous per partition too; real Kinesis sequence numbers are not.
- `partition` is carried on the record. A real consumer knows it from the assignment it reads,
  not from the record body. It is inlined so the sample file stands alone.

**Useful commands:**

```sql
SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();     -- your SDK account identifier
SELECT CURRENT_ROLE(), CURRENT_SCHEMA();                        -- confirm your own role and schema
SHOW PIPES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;               -- the pipe you never created
SHOW DYNAMIC TABLES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;      -- Path A refresh mode
SHOW DBT PROJECTS IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;        -- Path B project object
EXECUTE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER ARGS = 'build';
```

If you get stuck for more than a few minutes on any step, ask your facilitator. Being stuck on
plumbing is not the exercise.

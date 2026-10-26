# Insurance Path Lab - Attendee Handout (dbt Projects on Snowflake)

Landing nested insurance-quote events from an event bus with Snowpipe Streaming, then building
a refined Silver layer on top of them with dbt Projects on Snowflake.

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

You are given `sample_quote_events.jsonl`, in the same folder as this handout: records exactly as a consumer reads them off the bus. A
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
  | Silver as dbt Projects on Snowflake                  |
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
  `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`
- The SDK authenticates with a **personal access token** restricted to your lab role. Your
  facilitator will give you one. There is no key pair to generate and no `profile.json` to
  distribute.

Connect Cortex Code to the account (OAuth), set its role to `LAB_ATTENDEE_<n>` and its warehouse
to `NORTHPOINT_LAB_WH`, and open the folder that holds `sample_quote_events.jsonl`. Ask CoCo to run
`SELECT CURRENT_ROLE(), CURRENT_SCHEMA();` and confirm both before step 2.1. Anything CoCo creates
under another role will be owned by that role, and later steps will fail on it.

**Workspace check.** In Snowsight, open Projects, then Workspaces, and confirm you can create a
Workspace. This path is authored there. If you cannot create one, tell your facilitator now.

---

## 2. The lab

Before you paste any prompt, replace every `<n>` with your attendee number. CoCo will not guess it.

### Step 2.1 - Assess the payload

Prompt CoCo:

> "Read `./sample_quote_events.jsonl`. It is a sample of records as a consumer reads them off our
> event bus. Tell me: how many records, what the envelope fields are, what distinct event types
> are in the payload, the field inventory per event type including nested objects and arrays,
> which fields are candidate keys, and every data-quality problem you can see. Don't load
> anything yet, and don't propose an architecture yet."

Read the answer properly before moving on. You are looking for the shape of the data and
everything about it that will make the later steps harder. Step 2.4 refers back to what
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
> - Required CLI flags `--database` and `--schema`, and `--file` defaulting to
>   `sample_quote_events.jsonl`.
> - On open, read each channel's latest committed offset token and skip records at or below it,
>   so a rerun appends nothing.
> - Batch appends. Authenticate with a PAT via inline `properties`, no profile file. Before
>   exiting, wait for each channel to commit the highest offset it sent, not the last one sent.
>   Print each channel's `rows_error_count` at the end."

Verify by reading, not running:

- `consume_from_event_source()` raises, and its docstring is specific enough to implement from.
- Both source functions yield the same keys.
- What is the channel keyed on? Ask yourself why that choice matters for ordering.
- Channel names are deterministic (`quotes-p0` to `quotes-p3`), so a restart reuses the channel
  instead of orphaning it. Reusing the channel is not enough on its own: check that the producer
  resumes from the committed offset token, or a restart re-sends everything.
- How is the payload assigned: a dict, or a string? Note it; step 2.3 checks it.

### Step 2.3 - Land the RAW layer

> "Create the target table `RAW_QUOTE_EVENTS` with columns CHANNEL_ID VARCHAR, STREAM_OFFSET
> NUMBER, EVENT_ID VARCHAR, SOURCE VARCHAR, PUBLISHED_AT TIMESTAMP_TZ, and PAYLOAD VARIANT. Add
> table and column comments. The producer's row keys must match these column names exactly,
> because the default pipe maps by name. Then give me the command to run the producer against my
> schema. Do not run it yourself."

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

Now run the producer a second time and repeat the query a minute later. `total_rows` must not
change. If it doubles, the producer is not resuming from its committed offsets; fix that before
you move on. The second run reports success either way, which is the point of checking.

Question for your pair: did 2.1 find more than one event version? If so, why did RAW not need to
change to accept them?

### Break

When you come back, open your Workspace.

### Step 2.4 - Build Silver as dbt Projects on Snowflake

The four Silver objects, with fixed names and columns:

| Object | Grain |
|---|---|
| `SILVER_QUOTE` | one row per quote |
| `SILVER_CARRIER_QUOTE` | one row per carrier response |
| `SILVER_QUOTE_VEHICLE` | one row per vehicle per quote |
| `SILVER_QUOTE_DRIVER` | one row per driver per quote |

All four read from a deduplicated, conformed staging object, `STG_QUOTE_EVENTS`, never from RAW
directly. Keep the names exactly as shown.

No dbt packages in this lab: packages need `dbt deps`, which needs an external access
integration nobody has set up. Use dbt built-in tests and singular SQL tests only.

Use these exact column names wherever they apply. The verify queries in step 2.5 depend on them,
and CoCo has not seen step 2.5:

| Object | Required columns |
|---|---|
| `STG_QUOTE_EVENTS` | `CHANNEL_ID`, `STREAM_OFFSET`, `EVENT_TYPE`, `EVENT_VERSION`, `QUOTE_ID`, `EVENT_TS_UTC`, `PUBLISHED_AT_UTC`, `PAYLOAD` |
| `SILVER_QUOTE` | `QUOTE_ID`, `QUOTE_STATUS`, `APPLICANT_ID`, `APPLICANT_KNOWN_AT_REQUEST`, `BOUND_PREMIUM_ANNUAL` |
| `SILVER_CARRIER_QUOTE` | `QUOTE_ID`, `TERM`, `TERM_PREMIUM`, `ANNUAL_PREMIUM` |
| `SILVER_QUOTE_VEHICLE`, `SILVER_QUOTE_DRIVER` | `QUOTE_ID` |

**Step 2.4.1 - scaffold the project in your Workspace.**

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

**Step 2.4.2 - the staging model.**

> "Write `models/staging/stg_quote_events.sql` over the `raw` source. One row per distinct
> `(CHANNEL_ID, STREAM_OFFSET)` - dedupe with QUALIFY ROW_NUMBER(). Promote the event type,
> event version, and quote id. Standardize every event-time encoding you found in 2.1 into a
> single `EVENT_TS_UTC`, add `PUBLISHED_AT` converted to UTC as `PUBLISHED_AT_UTC`, and keep
> `PAYLOAD` for everything not promoted. Use the column names in the table above."

Verify by reading: the `QUALIFY` partitions on `CHANNEL_ID, STREAM_OFFSET`, and the timestamp
logic has a branch for every encoding you found in 2.1.

**Step 2.4.3 - the four Silver models.**

> "Write four models in `models/silver/`, all reading `stg_quote_events` through `ref()`:
> `silver_quote`, one row per quote, built by aggregating across each quote's events rather than
> joining event types together, carrying requested time, event version, channel, applicant id,
> whether the applicant was known at request time, state, zip, coverage level, deductible, any field present in only some event versions, vehicle and driver counts, carrier response count, selected carrier, policy
> number, bound premium annualized, abandon reason, and a status of BOUND, ABANDONED, SELECTED,
> or OPEN; `silver_carrier_quote`, one row per carrier response, with term, the premium typed as
> a number for every way the carriers send it, and an annualized premium;
> `silver_quote_vehicle` and `silver_quote_driver`, one row per array element using LATERAL
> FLATTEN. Do not carry date of birth or email. Use the column names in the table above."

Decide your Silver materialization and say it out loud to your pair, with a reason.

**Step 2.4.4 - tests.**

> "Add tests, using only dbt built-ins and singular SQL tests, no packages. In
> `models/schema.yml`: `not_null` on `event_ts_utc` and `quote_id` in staging; `unique` and
> `not_null` on `quote_id` in `silver_quote`, which is the fanout test; `accepted_values` on
> `quote_status` and on `term`; `not_null` on `term_premium`; `relationships` from the vehicle and
> driver models back to `silver_quote`. In `tests/`: one singular test that staging has exactly
> one row per `(CHANNEL_ID, STREAM_OFFSET)`, one that RAW has no offset gaps per channel, and one
> that no `SILVER_` table in my schema has a column whose name contains `DOB` or `EMAIL`, or any
> VARIANT column, reading INFORMATION_SCHEMA. That test has no `ref()`, so start it with one
> `-- depends_on: {{ ref('...') }}` line per Silver model, or it runs before Silver exists and
> passes on nothing."

Verify by reading: every test named in the prompt exists, in `schema.yml` or `tests/`, and the
PII test has a `depends_on` line for all four Silver models.

**Step 2.4.5 - deploy and run.**

> "Deploy this project from my Workspace as a dbt project object named `NORTHPOINT_SILVER` in
> my schema, using `CREATE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER FROM
> 'snow://workspace/USER$<your_user>.PUBLIC."DEFAULT$"/versions/live/northpoint_silver'`, then run
> it with `EXECUTE DBT PROJECT` and `ARGS = 'build'`, and show me the result of every model and
> test."

If your project files are on your laptop rather than in the Workspace, tell your facilitator
before you deploy.

```sql
EXECUTE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER ARGS = 'build';
```

Pass criteria: all five models build and all dbt tests pass.

Then run the verify queries in step 2.5.

### Step 2.5 - Verify

Run all five.

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

Zero rows only proves nothing was lost in the middle of a channel: the first offset has no `prev`,
so a lost offset 0 is invisible, and so is a lost tail. Also confirm `MIN(STREAM_OFFSET) = 0` per
channel and that `MAX(STREAM_OFFSET)` matches the last offset per partition you found in 2.1. This
sample has no gaps, so if you want to see the check catch one, delete a single RAW row and rerun
it, then reload.

**4. Any silent nulls?**

```sql
SELECT (SELECT COUNT_IF(EVENT_TS_UTC IS NULL) FROM STG_QUOTE_EVENTS)     AS unparsed_ts,
       (SELECT COUNT_IF(TERM_PREMIUM IS NULL) FROM SILVER_CARRIER_QUOTE) AS unparsed_premium,
       (SELECT COUNT_IF(APPLICANT_ID IS NULL AND QUOTE_STATUS IN ('SELECTED','BOUND'))
          FROM SILVER_QUOTE)                                             AS unresolved_applicant;
```

Pass criteria: all three must be zero.

**5. Are the timestamps right, not just present?**

```sql
SELECT COUNT_IF(ABS(DATEDIFF('second', EVENT_TS_UTC, PUBLISHED_AT_UTC)) > 5) AS skewed_ts
FROM STG_QUOTE_EVENTS;
```

Pass criteria: must be zero. In this sample, event time and broker time agree to the second for
every encoding. A non-zero count usually means a zoneless timestamp was read in the session time
zone instead of UTC; query 4 cannot catch that, because nothing is null.

### Step 2.6 - Debrief and document

Your facilitator will pick a pair or two for a short debrief. Be ready
to answer:

- What was easier than you expected, and what was harder?
- Which verify query or test caught something?
- How does your Silver get refreshed tomorrow, with nobody touching it?

Then document:

> "From live metadata in my schema, generate `PIPELINE_DOCS.md` (executive summary,
> source-to-target mapping per Silver object, lineage diagram, business rules including the
> dedupe, timestamp, premium, and anonymous-applicant logic, PII inventory, refresh pattern). Put
> every table and column description into `models/schema.yml`, and add
> `+persist_docs: {relation: true, columns: true}` to `dbt_project.yml`. Then redeploy and run
> `build`."

Comments applied with `COMMENT ON` after the build do not last: every `build` recreates the Silver
tables and drops them. Putting the descriptions in dbt makes each build reapply them.

Verify: run `build` a second time, then confirm every Silver object still has a table comment; the PII inventory says where `dob` and `email`
live and confirms they are absent from Silver; the refresh pattern section describes how the dbt project is run. Then
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
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME(); -- your SDK account identifier
SELECT CURRENT_ROLE(), CURRENT_SCHEMA();                        -- confirm your own role and schema
SHOW PIPES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;               -- the pipe you never created
SHOW DBT PROJECTS IN SCHEMA NORTHPOINT_LAB.ATTENDEE_<n>;        -- the dbt project object
EXECUTE DBT PROJECT NORTHPOINT_LAB.ATTENDEE_<n>.NORTHPOINT_SILVER ARGS = 'build';
```

If you get stuck for more than a few minutes on any step, ask your facilitator. Being stuck on
plumbing is not the exercise.

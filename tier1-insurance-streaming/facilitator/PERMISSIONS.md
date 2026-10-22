# Permissions for the Insurance Streaming Lab

Facilitator reference. Covers every privilege an attendee role needs to run the full lab on
either Silver path, and what the facilitator needs to provision it. `setup/facilitator_setup.sql`
implements this file.

Status column: **Doc** means taken from Snowflake product documentation; **Proven** means run
end to end for this lab as a test user holding only `LAB_ATTENDEE_01`, authenticated with a PAT
restricted to that role, after `facilitator_setup.sql` ran as shipped. One item remains open:
Workspace deploy (see Path B).

## Design: one role per attendee, owning one schema

Each attendee gets a role `LAB_ATTENDEE_<n>` that **owns** their schema `NORTHPOINT_LAB.ATTENDEE_<n>`.
Because the role owns the schema and creates every object in it, it automatically owns RAW, the
Dynamic Tables, the dbt project object, and the tables dbt materializes. That removes most of the
privilege surface below, and it matters for two specific reasons:

- **Dynamic Tables refresh as the owner role.** The owner must keep `SELECT` on RAW and `USAGE` on
  the warehouse, database, and schema, or scheduled refreshes fail and the DT can auto-suspend.
  Owning RAW guarantees this.
- **Dynamic Tables auto-enable change tracking on the base table.** If the creating role cannot
  alter the base table, creation fails with "Insufficient privileges to operate on base table to
  automatically enable CHANGE_TRACKING". Owning RAW avoids it.

A shared, facilitator-owned RAW table would break both. Do not use one.

## Attendee role grants

| # | Privilege | On | Needed for | Status |
|---|---|---|---|---|
| 1 | `USAGE` | database `NORTHPOINT_LAB` | everything | Doc |
| 2 | `OWNERSHIP` | schema `NORTHPOINT_LAB.ATTENDEE_<n>` | everything below without per-object grants | Doc |
| 3 | `USAGE` | warehouse `NORTHPOINT_LAB_WH` | queries, DT refresh, dbt runs | Doc |
| 4 | `CREATE TABLE` | own schema | RAW landing table, dbt `table`/`incremental` models | Doc (implied by ownership) |
| 5 | `CREATE DYNAMIC TABLE` | own schema | Path A | Doc (implied by ownership) |
| 6 | `CREATE DBT PROJECT` | own schema | Path B: deploy from Workspace or `snow dbt deploy` | Doc (implied by ownership) |
| 7 | `CREATE VIEW` | own schema | dbt staging model if materialized as a view | Doc (implied by ownership) |
| 8 | Default-pipe ingest rights on RAW | own RAW table | Snowpipe Streaming via the auto-created `<TABLE>-STREAMING` pipe | **Proven** (6,320 rows, four channels, no extra grant needed beyond ownership) |
| 9 | PAT for the attendee user, restricted to `LAB_ATTENDEE_<n>` | user | SDK auth in §1 | **Proven** (SDK and SQL both authenticated with it) |

Rows 4 to 7 are listed explicitly so a customer admin who refuses to grant schema ownership knows
the exact schema-level grants to give instead. In that case also grant `OPERATE` and `MONITOR` on
future dynamic tables and `USAGE` and `MONITOR` on future dbt projects in the schema.

## What each path needs at run time

**Path A, Dynamic Tables** (`CREATE DYNAMIC TABLE ... TARGET_LAG ... WAREHOUSE = NORTHPOINT_LAB_WH`):
- create: `CREATE DYNAMIC TABLE` on schema, `SELECT` on RAW, `USAGE` on warehouse
- a DT over another DT with `INITIALIZE = ON_CREATE` also needs `OPERATE` on the upstream DT
  (covered by ownership; matters only if ownership is replaced by row-level grants)
- `SHOW DYNAMIC TABLES` refresh-mode check and refresh history: `MONITOR` (covered by ownership)

**Path B, dbt Projects on Snowflake:**
- deploy: `CREATE DBT PROJECT` on schema; the role active in the Workspace deploy dialog or the CLI
  connection becomes the owner
- compile and run use the role in `profiles.yml` **and** are limited to the calling user's
  privileges. Both must be `LAB_ATTENDEE_<n>`. A mismatched role in `profiles.yml` is the most
  likely Path B failure in the room. **Mitigated in the reference project:** `profiles.yml` reads
  the role from `env.yml`, which resolves it to `CURRENT_ROLE()`, so dbt runs as the caller.
  **Proven** under the attendee role: deploy and `dbt build` 17 of 17 PASS.
- `env.yml` database and schema must be **literals**. Inside `EXECUTE DBT PROJECT` there is no
  current database or schema, so `CURRENT_DATABASE()` returns NULL and the run fails (observed).
- `snow dbt deploy` on CLI 3.16 rejects `env_var()` in `profiles.yml` client-side. Attendees deploy
  from the Workspace, which is unaffected; facilitators deploying from a laptop need CLI 3.22+ or
  `CREATE DBT PROJECT ... FROM '@stage'`.
- `EXECUTE DBT PROJECT`: `USAGE` on the dbt project object (covered by ownership)
- run history and artifacts in Snowsight: `MONITOR` on the dbt project object (covered by ownership)
- **no external access integration**, because the lab uses no dbt packages (spec R12). If a pair
  adds a package, `dbt deps` fails; that is expected, not a grant problem
- **Workspaces** live in the user's personal database (`USER$<name>.PUBLIC`). Confirm attendees can
  create a Workspace in the customer account before the session; some accounts restrict personal
  databases. **Open:** not testable without an interactive user; the lab test deployed with
  `CREATE DBT PROJECT ... FROM` a stage instead, which the attendee role can also do.

## Not needed (state this to a customer admin up front)

- `ACCOUNTADMIN`, `SYSADMIN`, or `SECURITYADMIN` for any attendee
- `CREATE PIPE` (the default pipe is auto-created)
- any integration: external access, storage, notification, or API
- `CREATE TASK` / `EXECUTE TASK` (scheduling dbt is a facilitator aside, spec NG12)
- `IMPERSONATE` (no `EXECUTE AS USER` on the DTs)

## Facilitator role

Provisioning needs a role that can create a database, a warehouse, roles, and users, and grant
roles to users: typically `SYSADMIN` for the database and warehouse plus `USERADMIN` (or
`SECURITYADMIN`) for roles and users. Teardown needs the same, plus ownership of any dbt project
objects and DTs, which it gets by dropping each attendee schema with `CASCADE` after the attendee
role is dropped or its objects reassigned.

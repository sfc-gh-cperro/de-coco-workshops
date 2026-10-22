# Snowflake permissions for the AI readiness scan

**For:** Parekh and the LendingTree Snowflake admin team
**Purpose:** set up one role, before the workshop, that can run Snowflake's AI readiness scan against the LendingTree account. ACCOUNTADMIN is not required to run the scan. An ACCOUNTADMIN only has to run the grant script below once.

## What the scan does

The scan reads Snowflake's account usage metadata (`SNOWFLAKE.ACCOUNT_USAGE`) for the last 7 days and scores three things:

- **Demand coverage:** how much analytical query traffic lands on well-modeled, consumption-ready tables rather than raw tables.
- **Semantic view coverage:** how many of those consumption-ready tables have a semantic view on top.
- **Semantic view quality:** whether the semantic views have primary keys, relationships, synonyms, comments, metrics, and verified queries.

It produces a 0-100 score, a gap classification, and an HTML report with the top improvement targets.

The scan reads **metadata and query history only**. It does not read table data, and it creates no objects in your databases. Its only write is a small results cache in the running user's personal stage (`@~/ai_readiness_cache/`), which every Snowflake user already has.

## Recommended setup (least privilege)

Run once as ACCOUNTADMIN. Replace the placeholders in angle brackets.

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS AI_READINESS_SCAN
  COMMENT = 'Read-only role for the Snowflake AI readiness scan';

-- 1. Account usage views the scan reads
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE AI_READINESS_SCAN;  -- ACCESS_HISTORY, QUERY_HISTORY
GRANT DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER   TO ROLE AI_READINESS_SCAN;  -- SESSIONS (BI tool detection)
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER     TO ROLE AI_READINESS_SCAN;  -- TABLES, SEMANTIC_* views

-- 2. Compute (an XS warehouse is enough)
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE AI_READINESS_SCAN;

-- 3. Visibility on semantic views, so verified queries are counted.
--    Repeat for each database/schema that holds semantic views.
GRANT USAGE ON DATABASE <db_with_semantic_views> TO ROLE AI_READINESS_SCAN;
GRANT USAGE ON SCHEMA <db_with_semantic_views>.<schema> TO ROLE AI_READINESS_SCAN;
GRANT REFERENCES ON ALL SEMANTIC VIEWS IN SCHEMA <db_with_semantic_views>.<schema> TO ROLE AI_READINESS_SCAN;

-- 4. Who runs it
GRANT ROLE AI_READINESS_SCAN TO USER <user_running_the_scan>;
```

### What each grant exposes

| Grant | What the role can see | Why the scan needs it |
|---|---|---|
| `GOVERNANCE_VIEWER` | Query history and access history (which objects and columns each query touched), plus governance views such as masking and row access policies | Core of the demand coverage score |
| `SECURITY_VIEWER` | Sessions, login history, users, roles, and grants | Identifies which reads come from BI tools (Tableau, Power BI, and others) |
| `OBJECT_VIEWER` | Object metadata: tables, views, columns, semantic view definitions | Table freshness and semantic view quality signals |
| Warehouse `USAGE` | Ability to run queries on that warehouse | Compute for the scan |
| Semantic view `REFERENCES` | Semantic view definitions only, not the underlying data | Counting verified queries |

`SECURITY_VIEWER` is the grant your security team is most likely to ask about, because it covers login history and grants as well as sessions. The scan only reads `SESSIONS` from it, to detect BI-tool traffic.

## Simpler alternative

If your team prefers one grant over three database roles, this also works. It exposes every view in the `SNOWFLAKE` database, which is broader than the scan needs:

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE AI_READINESS_SCAN;
```

Steps 2-4 above still apply.

## Things to know

- **The scan covers the whole account.** Account usage views are not filtered by what the role can query, so the scan sees query history across every database. That is how the score reflects real demand, and it is also the main reason these grants get reviewed.
- **Missing semantic view visibility fails silently.** Without step 3, the scan still runs but counts zero verified queries, which understates semantic view quality. In our testing it reported 0 semantic views until we added the `REFERENCES` grants.
- **Data lag:** access history can lag up to about 3 hours. Usage from the last few hours may not show up yet.
- **Runtime:** under a minute on our test account. Larger accounts take longer. The scan supports sampling (10/30/50%) if a full scan is slow.
- **Secondary roles:** if the running user has secondary roles enabled, they may see more than this role grants. The role above is sufficient on its own.

## How we tested this

We created this exact role in a Snowflake test account, turned off secondary roles so only this role applied, and ran the scan end to end. It completed with no permission errors. The consumption-readiness results matched an ACCOUNTADMIN run exactly. Semantic view counts matched only after the step 3 grants were added.

## Checklist before the workshop

- [ ] Role created and granted (steps 1-4)
- [ ] The user running the scan can run `USE ROLE AI_READINESS_SCAN;` and `SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE query_start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP());`
- [ ] Step 3 repeated for every schema that holds semantic views

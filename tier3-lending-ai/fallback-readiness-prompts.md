# Fallback readiness check: CoCo prompts

Use these when the role running the check has no access to `SNOWFLAKE.ACCOUNT_USAGE`. Paste the prompts into Cortex Code in order, in one chat, connected with the role that will run the check.

Before starting, replace `<DB>` with the database to assess (one database per run). Replace `<BI_PATTERN>` with names that identify BI traffic in your account: the service users, roles, or warehouses that Tableau, Power BI, or other BI tools connect with (for example `TABLEAU|POWERBI|BI_`).

What the role needs: USAGE on `<DB>` and its schemas, REFERENCES (or SELECT) on the tables and semantic views, a warehouse to run on, and MONITOR or OPERATE on the warehouses whose query history should count. Without MONITOR, query history shows only your own queries.

---

## Prompt 0: ground rules and truncation check

> I want to assess how AI-ready the tables in `<DB>` are, but my role cannot read SNOWFLAKE.ACCOUNT_USAGE. For this whole conversation, use only `<DB>.INFORMATION_SCHEMA` views and table functions. Do not query ACCOUNT_USAGE, and do not suggest granting it.
>
> First, check how much query history I can see. Call `<DB>.INFORMATION_SCHEMA.QUERY_HISTORY()` with END_TIME_RANGE_START 167 hours ago (not 7 days, which errors), END_TIME_RANGE_END now, and RESULT_LIMIT 10000. Tell me the total row count, the oldest and newest start times, and how many distinct users and warehouses are visible. If the count is exactly 10000, the window is truncated: list the warehouses ranked by query count, so we can run the demand analysis per warehouse with QUERY_HISTORY_BY_WAREHOUSE instead.

## Prompt 1: structural readiness

> For every base table in `<DB>` (excluding INFORMATION_SCHEMA), show schema, table, row count, size in MB, days since last altered, whether the table has a comment, whether it has a primary key constraint, column count, and the percentage of columns with comments. Use INFORMATION_SCHEMA.TABLES, COLUMNS, and TABLE_CONSTRAINTS. Order by schema and table.

## Prompt 2: demand and an approximate consumption-readiness score

> Using the same QUERY_HISTORY call as before (or QUERY_HISTORY_BY_WAREHOUSE per warehouse if the window was truncated), keep only successful SELECT queries that ran on a warehouse and took more than 0 ms.
>
> Attribute each query to a table in `<DB>` when the uppercase query text contains the table name as a whole word, and either the text includes `SCHEMA.TABLE` or the query ran with that schema as its session context. Name matching is the only option here, because INFORMATION_SCHEMA has no access history.
>
> Per table, compute: distinct analytical reads, distinct users, reads where the user, role, or warehouse name matches the regex `<BI_PATTERN>` (case-insensitive), median execution time in ms, and days since the table was last altered. Drop tables with fewer than 5 reads.
>
> Then score each table the way Snowflake's AI readiness scan does:
> - activity = PERCENT_RANK of reads
> - consumption = the greater of PERCENT_RANK of distinct users and PERCENT_RANK of BI-pattern reads
> - speed = 1 / (1 + (p50_ms / 5000)^3)
> - freshness = exp(-ln(2) / 30 * days_since_update)
> - score = activity^0.35 * consumption^0.30 * speed^0.20 * freshness^0.15, with each factor floored at 0.001
>
> A table is consumption-ready if the score is at least 0.80. Show the table sorted by score. Then give me demand coverage: reads on consumption-ready tables divided by all matched reads, as a percentage, with the count of consumption-ready tables out of tables scored.

## Prompt 3: semantic view coverage

> For every base table in `<DB>`, use INFORMATION_SCHEMA.SEMANTIC_TABLES to show how many semantic views use it as a base table, their names, and whether any of those semantic tables define primary keys, synonyms, or a comment. Then list the consumption-ready tables from the previous step that have no semantic view, ordered by read count. Those are the top candidates for new semantic views.

## Prompt 4: summary

> Summarize the results in a short scorecard: demand coverage, consumption-ready tables out of tables scored, the share of consumption-ready tables covered by a semantic view, and the three biggest gaps. Gaps can be busy tables that are not consumption-ready and why (no comments, stale, slow, narrow audience), consumption-ready tables with no semantic view, and semantic views missing primary keys or synonyms. Flag that these numbers are approximate: they come from name matching on query text, a query history capped at 10,000 rows per call, and a name-pattern proxy for BI tools.

---

## If something looks off

- If CoCo starts querying SNOWFLAKE.ACCOUNT_USAGE, remind it of the rule in Prompt 0.
- If the check in Prompt 0 returns exactly 10,000 queries, run Prompt 2 once per BI and analyst warehouse, so busy warehouses do not crowd out the rest.
- These numbers are directional. Use them to rank targets, not to compare against the full scan.

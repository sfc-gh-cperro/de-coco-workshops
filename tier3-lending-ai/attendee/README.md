# AI Readiness Lab: measure, pick, fix, repeat

This guide is for walking through the AI readiness improvment loop. It walks through the same loop we ran in the room so you can repeat it on your own data, as often as you like, with Cortex Code (CoCo).

## Why this matters

AI agents and natural-language tools (Cortex Analyst, Snowflake Intelligence) only give trustworthy answers when the data underneath is modeled the way the business asks questions, and described well enough that an agent can find and use it. When that foundation is missing, people and agents fall back to raw tables. They get inconsistent numbers, duplicate logic, and access that is hard to govern.

AI readiness is a way to measure that foundation from how your account is actually used, so the next modeling investment goes where the demand is.

## The loop

1. **Measure:** run the AI readiness scan, or the fallback if the scan's permissions are not in place.
2. **Pick:** take the scan's gap classification and choose one concrete target from its improvement list.
3. **Fix:** use CoCo to extend the existing pipeline or semantic view that closes the gap.
4. **Re-measure:** after a few days of normal use, run it again and check that the score moved.

One target per pass. Small, measured fixes compound. A big remodel stalls.

## Step 1: Measure

### Option A: the AI readiness scan (preferred)

In CoCo, connected with a role that has the grants in `PERMISSIONS-ai-readiness-scan.md`, type:

> AI readiness score

CoCo runs the scan against the last 7 days of account usage and produces an HTML report with a 0-100 score, the scores behind it, a gap classification, and a list of top improvement targets. It reads metadata and query history only, not table data.

The scan covers the whole account. If you want to focus on one domain, read the improvement list for that domain's databases.

### Option B: the fallback (no ACCOUNT_USAGE access)

If the grants are not approved, use `fallback-readiness-prompts.md`. It runs the same three measurements against one database at a time using only INFORMATION_SCHEMA. The numbers are approximate. It matches tables by name in query text, history is capped at 10,000 queries per call, and BI tools are identified by a name pattern. It still shows the same shape: which tables are consumption-ready, where demand lands, and where semantic views are missing.

## Step 2: Pick a recommendation

The scan assigns one gap class, using this order. The first rule that matches wins:

| If | Gap class | What it means |
|---|---|---|
| Demand Coverage is under 50%, or there are no consumption-ready tables | `BUILD_CR_TABLES` | Most real queries land on tables that are not modeled for consumption. Fix the modeled layer first. |
| No semantic views on consumption-ready tables | `BUILD_SVS` | The modeled layer exists, but agents have no business-level map of it. |
| Semantic views cover under 50% of consumption-ready tables | `EXPAND_SV_COVERAGE` | Some well-used tables have semantic views and others do not. |
| Average semantic view quality is under 50% | `IMPROVE_SV_QUALITY` | Semantic views exist but are thin, so agents guess at joins and meanings. |
| None of the above | `HEALTHY` | Keep the loop running as demand shifts. |

Then choose **one** target from the improvement list, using these rules:

- Prefer the target with the most reads or the broadest set of users. That is where a fix pays off soonest.
- If analysts are going to raw tables for it, ask them why. The answer is usually the modeling gap.
- Pick something you can finish and verify in a day or two, on the existing pipeline.

## Step 3: Fix it with CoCo

The three levers below each map to gap classes. Each includes starter prompts. Replace the names in angle brackets.

## The three levers

### Lever 1: Consumption-ready tables (Demand Coverage)

**What it is.** The share of real analytical reads that land on tables the scan rates consumption-ready. A table's score blends four signals, weighted 35/30/20/15: how often it is read, how broad its audience is (distinct users or BI-tool reads), how fast its typical query is, and how recently it was updated. Scores are percentile ranks, so a table is "ready" relative to the rest of your account.

**Why it matters.** This is the most direct measure of whether your modeled layer matches how the business asks questions. When demand coverage is low, people are answering questions from raw tables. Every one of those queries is logic that lives outside your pipelines and that an agent cannot reuse.

**How to move it.** Find the questions that send people to raw, and model them into the curated layer on the pipelines you already run.

> Which raw tables in `<DB>` are queried most in the last 7 days, by whom, and for what? Group the queries by pattern and tell me which business questions the curated layer cannot answer today.

> Add a curated table `<NAME>` to `<SCHEMA>` that answers `<question>` at `<grain>` grain. Build it on the existing refresh pipeline, add column comments, and reconcile it against the raw source.

### Lever 2: Semantic view coverage

**What it is.** The share of consumption-ready tables that have at least one semantic view on top.

**Why it matters.** A semantic view is the business map an agent reads: which tables join, what the metrics mean, what users call things. A well-modeled table without one is invisible to Cortex Analyst and to agents built on it. Coverage tells you whether your best tables are reachable in natural language at all.

**How to move it.** Start with the busiest consumption-ready table that has no semantic view.

> Create a semantic view over `<TABLE>` and the dimension tables it joins to. Include primary keys, relationships, the five metrics analysts compute most often from this table, and dimensions with synonyms. Base it on the queries people actually run against it.

### Lever 3: Semantic view quality

**What it is.** A 9-point score per semantic view. Six binary signals (primary keys, synonyms, unique keys, distinct ranges, relationships, metrics), plus how many verified queries it has (worth up to 2 points), plus the depth of its comments (up to 1 point).

**Why it matters.** Quality is what makes natural-language answers correct, not just possible. Primary keys and relationships stop wrong joins. Synonyms match the words people use. Verified queries anchor the agent on known-good answers to common questions. A low-quality semantic view gives fluent answers that are sometimes wrong, which is worse than no answer.

**How to move it.** Take the lowest-scoring semantic view on a busy table and fill in what is missing.

> The semantic view `<SV>` scored low on AI readiness quality. Review it and add what is missing: primary keys, relationships, synonyms, comments on every dimension and metric, and three to five verified queries for the most common questions asked of it. Validate that each verified query runs.

## Step 4: Re-measure

Run the scan or the fallback again after the fix has seen a few days of real use. The scan reads a 7-day window, and access history can lag by a few hours, so a change made today will not show up in today's score. Expect movement on the lever you worked on. Because scores are relative, other tables can shift too.

Keep a short log of each pass (date, score, gap class, target, what changed). It becomes the backlog and the evidence for restricting raw access once the curated layer covers the demand.

## Files in this folder

| File | Use |
|---|---|
| `README.md` | This guide |
| `PERMISSIONS-ai-readiness-scan.md` | The grants needed to run the AI readiness scan, and why |
| `fallback-readiness-prompts.md` | CoCo prompts for the fallback when ACCOUNT_USAGE is not available |

Questions: Chris Perro, Snowflake (chris.perro@snowflake.com).

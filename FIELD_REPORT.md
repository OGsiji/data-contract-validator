# Data Contract Validator — Field Report

_data-contract-validator v1.1.6 · MIT · [github.com/OGsiji/data-contract-validator](https://github.com/OGsiji/data-contract-validator)_

> Catch breaking changes between your dbt models and your FastAPI/Pydantic APIs — before they hit production.

## 1. What a data contract actually is

Every API that serves data from a warehouse is making a promise it didn't write down anywhere a machine can check.

A Pydantic or SQLModel class declares a shape — `customer_id: str`, `lifetime_value: Optional[Decimal]` — and that shape is a claim about what the underlying data will look like. Somewhere upstream, a dbt model is what actually makes that claim true, by selecting and transforming columns out of the warehouse. Those two things — the promise and the thing that fulfills it — live in different repositories, are maintained by different teams, and are connected by nothing stronger than convention.

```
  dbt model                                          API response
  sem_customer_360.sql   ⋯⋯⋯⋯⋯ contract ⋯⋯⋯⋯⋯   class Customer(SQLModel)
  (what the pipeline produces)                  (what the API promises)
```

Nothing enforces that agreement automatically. A data engineer can rename `lifetime_value` to `ltv`, run `dbt run`, watch the pipeline go green, and have no way of knowing that three repositories away, a FastAPI endpoint is about to start throwing 500s. The dbt test suite has no idea the API exists. The API's test suite mocks the database, so it has no idea the column is gone. The first signal anyone gets is a page at 2am, or worse, a client silently getting `null` where they used to get a number.

This is the same class of problem a type checker solves inside one codebase — except the "two sides" here are two systems that never compile against each other. A data contract is what closes that gap: an explicit, checkable statement that *whatever the API promises, the data layer currently provides*.

## 2. How the validator closes it

It extracts both sides independently, then compares them structurally — never by running either system.

On the dbt side, it prefers the most accurate source available and degrades gracefully when that's not present:

| Tier | Source | Confidence | Behavior |
|---|---|---|---|
| 1 | `catalog.json` | high | Real warehouse column types, from `dbt docs generate`. |
| 2 | sqlglot SQL parse | medium | A real SQL parser — trusts column names; flags `SELECT *` as incomplete rather than guessing. |
| 3 | regex parse | low | Last resort. Never used to hard-fail a build. |

On the API side, Pydantic and SQLModel classes are parsed with Python's `ast` module — no code is imported or executed, so it's safe to point at a repository you don't otherwise trust.

The two sides never get compared as raw strings. `varchar` and `str`, or `timestamp` and `datetime.datetime`, are normalized into one shared vocabulary first — a canonical type system — so the tool isn't drowning teams in false "type mismatch" noise over things that are actually identical. Table and column names are matched the same way: casing, snake/camel, and plural/singular variants are bridged automatically, and an explicit `mapping` config covers the cases convention genuinely can't reach.

> The design principle underneath all of this: **a check that cries wolf gets muted, and a check that stays quiet on a real gap is worse than no check at all.** Section 4 is what happens when that principle gets tested against real schemas instead of trusted on faith.

## 3. What it takes to run

Adopting it is minutes, not a migration project — there's no schema to write by hand and nothing to install into the warehouse.

1. **Install** into the same environment dbt runs in.
   ```bash
   pip install data-contract-validator
   ```
2. **Run the wizard.** It asks where your dbt project is, which framework you use, and whether your API models live locally or in another GitHub repo — then writes the config, a CI workflow, and (if you want one) a pre-commit hook in one pass.
   ```bash
   contract-validator init --interactive
   ```
3. **Validate.** Runs the same way locally, in a pre-commit hook, or as a PR check.
   ```bash
   contract-validator validate
   ```

From there it runs at the same trust boundary as the rest of a team's guardrails — a red check on a pull request, not a spreadsheet someone has to remember to update.

## 4. Field notes: hardening this release

"Never cry wolf" is a claim, not a default. These are real defects found and fixed by pointing the tool at real dbt models and real SQLModel classes — in the order they surfaced.

**Finding 1 — A real, dbt-backed table was silently exempted from checking**
- *Symptom:* validation reported `0 target schemas` and a trivial "✅ PASSED" against a repo where every class was a genuine, dbt-fed table.
- *Cause:* every `SQLModel(table=True)` class was blanket-skipped, on the assumption it was a database mirror, not an API contract.
- *The catch:* `table=True` alone can't tell two structurally identical classes apart — one genuinely has no dbt model (populated by a Kafka stream), another is a normal dbt-fed table an API also returns directly. That's business knowledge, not something recoverable from a class definition.
- *Fix:* `table=True` classes are now validated like anything else. The genuine "no source model on purpose" case is now a deliberate, human-stated `mapping.exclude` entry — not an inference.

**Finding 2 — A correct schema flagged as a type mismatch**
- *Symptom:* `source provides 'bigint' but target expects 'Optional[int]'` — on ordinary count and ID columns, the most common numeric shape in the warehouse.
- *Cause:* Python's `int` was mapped to the same canonical rank as a fixed-width SQL `INTEGER`, which is narrower than `BIGINT`.
- *The catch:* Python's `int` is arbitrary-precision — there is no actual truncation risk consuming a `bigint` into it, unlike a real fixed-width column.
- *Fix:* `int` now maps to the wider rank. A genuinely fractional source (`DECIMAL`/`FLOAT`) is still correctly flagged.

**Finding 3 — The wizard misread its own suggested answer**
- *Symptom:* accepting the setup wizard's own default (`app/models`) produced a GitHub target of `app/models/app/models`.
- *Cause:* local-vs-GitHub was guessed from the path's shape — and a local relative path is syntactically identical to a GitHub `org/repo` string.
- *Fix:* the wizard now asks directly — "local project, or a different GitHub repo?" — before it asks for the path at all.

**Finding 4 — CI silently assumed it could read a private target repo**
- *Symptom:* a generated CI workflow validating against a *different*, private API repo would fail every PR with a 404 indistinguishable from a typo'd path.
- *Cause:* the workflow wired up `secrets.GITHUB_TOKEN`, but that token only has access to the repo the workflow itself runs in — never a different repo, private or not.
- *Fix:* the default stays `secrets.GITHUB_TOKEN` — zero setup for the common case of a public target repo — but the generated workflow now carries a hard-to-miss comment recommending a user-created `secrets.API_REPO_TOKEN` for a private target, instead of the risk sitting undocumented. Skipped entirely for a `local` target, which never calls the GitHub API at all. The workflow also now scaffolds (commented, since it needs your warehouse credentials) the `dbt docs generate` step needed to unlock Tier 1 real warehouse types in CI, instead of that only being mentioned in prose docs.

## 5. Why this matters for teams shipping operational APIs

Most teams already gate a deploy on unit tests, type checks, and linting. Almost none of them gate it on the one dependency that's hardest to see from either side of the seam: whether the warehouse still produces what the API promised. That gap tends to get discovered in production, by whoever is paged first.

The cost of getting this wrong isn't abstract. It's a client integration that silently starts receiving `null` for a field it depends on. It's an analytics rename that ships clean because nothing downstream was watching. It's the debugging session that starts with "the API says 500 but nothing changed on our side" — because nothing did, on *their* side.

A data contract check turns that failure mode from a production incident into a pull-request comment — on the commit that actually caused it, reviewed by the person who can actually fix it, before anything ships.

---

```bash
pip install data-contract-validator==1.1.6
```

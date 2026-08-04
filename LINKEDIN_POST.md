# LinkedIn post — data-contract-validator v1.3.0

---

A year ago I stood up at the dbt Meetup in Lagos and talked about a problem
I kept running into: nothing checks the seam between your dbt models and the
things that consume them.

I finally went back and did the work. **data-contract-validator v1.3.0 is out.**

---

**The problem, in one sentence:**

A data engineer renames a column, `dbt run` goes green, and three
repositories away a FastAPI endpoint starts throwing 500s at 2am.

The dbt tests don't know the API exists. The API tests mock the database, so
they don't know the column is gone. The first person who finds out is
whoever is on call.

It's the same thing a type checker solves inside one codebase — except these
two sides never compile against each other.

---

**What the tool does:**

It reads what your dbt models actually produce, reads what your Pydantic /
SQLModel classes actually promise, and fails the PR when the data side can no
longer satisfy the consumer side.

```bash
pip install data-contract-validator
contract-validator init --interactive
contract-validator validate
```

No schema to hand-write. Nothing to install into your warehouse. It parses
Python with `ast` (nothing is imported or executed) and SQL with a real
parser, not regex.

---

**What's new in this release:**

🔗 **Reverse ETL is now covered, not just APIs.** dbt models don't only feed
services — they get synced into HubSpot, and that destination has a schema
too. Except it lives in an admin UI that anyone can edit, with no code review
and no git history. A property renamed in a settings page breaks a sync
exactly like a dropped dbt column does, and nothing in your repo would show
it. Now it's validated the same way.

🔀 **Branch auto-matching.** A dbt PR into `dev` now checks against the API
repo's `dev` branch automatically. Each environment validates against its own
counterpart — zero config.

🎯 **Fewer false positives, which is the whole game.** A check that cries
wolf gets muted, and a muted check is worse than none. Real fixes this cycle:
`bigint` columns no longer false-flag against Python `int` (Python ints are
arbitrary-precision — there's no truncation risk). `SQLModel(table=True)`
classes are no longer silently skipped, which was quietly exempting real,
dbt-backed tables from checking entirely.

💡 **"Did you mean?" on renames.** `lifetime_value` → `ltv` isn't a
mechanical transform, so name-matching can't bridge it. The error now names
the closest actual column and the exact config line to add.

---

**Why I think this matters:**

Most teams already gate deploys on unit tests, type checks, and linting.
Almost nobody gates them on the one dependency that's invisible from both
sides of the seam: whether the warehouse still produces what the consumer
promised.

That gap gets discovered in production, by whoever is paged first.

---

**Every fix in this release came from pointing the tool at real dbt models
and real SQLModel classes and finding it was wrong.** Not hypotheticals —
actual false passes and false alarms, found and fixed.

Which is exactly why I'd love more eyes on it. If you run dbt alongside an
API or a reverse-ETL sync, point it at your project and tell me where it's
wrong. That feedback is worth more than a star.

Especially interested in contributors for:
→ Salesforce (the obvious next destination)
→ Django / SQLAlchemy targets
→ More warehouse type edge cases

MIT licensed. 97 tests. Issues and PRs genuinely welcome.

🔗 github.com/OGsiji/data-contract-validator
📦 pip install data-contract-validator

#dbt #DataEngineering #Analytics #DataContracts #Python #FastAPI #OpenSource #ReverseETL #DataQuality

---

## Shorter variant (if you want something punchier)

A year ago I talked about this at the dbt Meetup in Lagos. I finally shipped it.

**data-contract-validator v1.3.0**

The problem: a data engineer renames a column, `dbt run` goes green, and
three repos away a FastAPI endpoint starts 500-ing at 2am. dbt's tests don't
know the API exists. The API's tests mock the DB. Nobody finds out until
production does.

The tool reads what your dbt models actually produce, reads what your
Pydantic/SQLModel classes actually promise, and fails the PR when they've
drifted apart.

New in this release:
🔗 HubSpot — because reverse ETL destinations have schemas too, and theirs
live in an admin UI with no code review and no git history
🔀 Branch auto-matching — a dbt PR into `dev` checks against the API repo's
`dev` branch, no config
🎯 A pile of false-positive fixes, each one found by pointing it at real
schemas and watching it be wrong

```bash
pip install data-contract-validator
contract-validator init --interactive
```

If you run dbt next to an API or a CRM sync: point it at your project and
tell me where it breaks. MIT, 97 tests, PRs welcome — Salesforce and Django
targets are wide open if anyone wants them.

🔗 github.com/OGsiji/data-contract-validator

#dbt #DataEngineering #DataContracts #Python #OpenSource

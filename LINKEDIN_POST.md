# LinkedIn post — data-contract-validator v1.3.0

## ⭐ Primary (short)

Your dbt model feeds a HubSpot sync. Someone in Sales renames a property in
the HubSpot settings page.

Nothing breaks loudly. The sync just quietly starts writing into a field that
no longer means what it used to — and nobody finds out until a campaign goes
out against bad data.

That's the part of reverse ETL nobody guards. Your dbt tests don't know
HubSpot exists. HubSpot has no code review, no git history, no PR for
"renamed a field." The contract between them lives entirely in someone's
memory.

At the dbt Meetup Lagos I talked about why data contracts matter. **This is
the shipped version of that argument.**

**data-contract-validator v1.3.0** now validates CRM destinations, not just
APIs:

```yaml
target:
  hubspot:
    type: "hubspot"
    object_type: "contacts"
    fields: [email, lifecyclestage, lifetime_value]
```

```bash
pip install data-contract-validator
contract-validator validate
```

It reads what your dbt models actually produce, reads what the destination
actually expects, and fails the PR when they've drifted apart. Same check for
FastAPI/Pydantic services. Now also for the CRM at the end of your reverse
ETL pipeline.

Also new: branch auto-matching (a dbt PR into `dev` checks against the API
repo's `dev` branch, zero config), and a pile of false-positive fixes — every
one of them found by pointing the tool at real schemas and watching it be
wrong.

If you run dbt next to an API or a CRM sync: point it at your project and
tell me where it breaks. That feedback is worth more than a star.

Salesforce is the obvious next destination. PRs genuinely welcome.

🔗 github.com/OGsiji/data-contract-validator
📦 `pip install data-contract-validator`

#dbt #DataEngineering #DataContracts #ReverseETL #Analytics #Python #OpenSource

---

## Alternate (slightly longer — if you want the origin story in)

A year ago I posted about a problem that kept biting me: nothing checks the
seam between your dbt models and the things that consume them.

At the dbt Meetup Lagos I made the case for data contracts. This is the
shipped version of that argument — and the newest part is the one I think is
most under-guarded.

**Reverse ETL destinations have schemas too.**

Your dbt model syncs into HubSpot. Someone renames a property in a settings
page. No PR, no review, no git history — and the sync quietly starts writing
into a field that no longer means what it did yesterday.

Your dbt tests don't know HubSpot exists. HubSpot doesn't know your dbt
project exists. The contract between them lives in someone's memory.

**data-contract-validator v1.3.0** now covers it:

```bash
pip install data-contract-validator
contract-validator init --interactive
```

It reads what dbt actually produces, reads what the destination actually
expects — a FastAPI/Pydantic service, or now a HubSpot object — and fails the
PR when the data side can no longer satisfy the consumer side.

A few things I care about in this release:

🔗 **CRM targets.** Scoped to the properties your sync actually writes,
because a stock HubSpot object has 100–400+ of them and comparing against all
of them is just noise.

🔀 **Branch auto-matching.** A dbt PR into `dev` validates against the API
repo's `dev` branch automatically. Each environment checks against its own
counterpart.

🎯 **Fewer false positives — which is the whole game.** A check that cries
wolf gets muted, and a muted check is worse than none. Real fixes this cycle:
`bigint` columns no longer false-flag against Python `int`, and
`SQLModel(table=True)` classes are no longer silently skipped — that one was
quietly exempting real dbt-backed tables from checking entirely.

Every fix came from pointing it at real dbt models and finding it was wrong.
Not hypotheticals.

Which is exactly why I want more eyes on it. Point it at your project and
tell me where it breaks.

Open for contributors: **Salesforce**, Django/SQLAlchemy targets, warehouse
type edge cases. MIT licensed, 97 tests.

🔗 github.com/OGsiji/data-contract-validator

#dbt #DataEngineering #DataContracts #ReverseETL #Python #OpenSource

---

## Notes before posting

- Tag **David Adejumo** and **dbt Labs Meetup Lagos** if you want the
  organizer/community reach — his recap post is recent enough that this reads
  as a natural follow-up.
- Worth tagging co-speakers **Adedamola Onabanjo** and **Israel Odeajo** only
  if you're framing it as a community thread rather than a project launch.
- The repo has **no CONTRIBUTING.md** — if this post drives contributors,
  that's the first thing they'll look for.

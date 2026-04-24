# OmniRoute — Schema Evolution Strategy

## Overview

This document defines how the OmniRoute pipeline handles changes to source data schemas over time, ensuring backward compatibility and zero-downtime upgrades.

---

## Types of Schema Changes

| Change Type | Impact | Handling Strategy |
|---|---|---|
| New column added to CSV | Low | Bronze ignestion rejects (schema mismatch) → update `EXPECTED_SCHEMA` |
| Column removed from CSV | High | Bronze ingestion rejects → update schema + backfill |
| Data type change (e.g., INT→STRING) | Medium | Schema enforcement catches → quarantine + manual migration |
| Column renamed | High | Treated as remove + add → requires migration |

---

## Bronze Layer: Strict Schema Enforcement

Every Bronze ingestion job uses a **two-pass validation** strategy:

1. **Pass 1 — Header Validation**: Read CSV without schema enforcement and compare column names against `EXPECTED_SCHEMA`
2. **Pass 2 — Type Enforcement**: Re-read with `schema()` applied to cast types

If Pass 1 fails (column mismatch), the entire file is moved to `quarantine/` and the job fails loudly. This is intentional — **silent schema drift is more dangerous than a failed job**.

### Adding a New Column

```
1. Update EXPECTED_SCHEMA in the spark job
2. Set nullable=True for the new field (backward-safe)
3. Deploy updated spark job
4. Re-run the DAG for the affected date
5. Silver/Gold layers ignore unknown columns (additive-safe)
```

### Removing a Column

```
1. Verify no Silver/Gold jobs depend on the column
2. Update EXPECTED_SCHEMA to remove the field
3. Update Silver transform if the column was used
4. Deploy and re-run
```

---

## Silver Layer: Additive-Only Contract

Silver transforms reference Bronze columns explicitly. New Bronze columns are ignored unless a Silver job is updated to consume them. This ensures **additive changes to Bronze never break Silver**.

## Gold Layer: Business Logic Contracts

Gold tables have fixed schemas tied to business requirements. Schema changes here require:
1. Update to BRD documentation
2. Migration script for existing data
3. Coordinated deployment with reporting layer

---

## Monitoring

- Schema mismatches trigger a quarantine + job failure
- DQ metrics JSON files log the exact error for each run
- Airflow `on_failure_callback` provides immediate alerting

## Best Practices

1. **Never silently ignore schema changes** — always fail fast
2. **Use nullable fields** for new columns to maintain backward compatibility
3. **Version your schemas** — document each change in this file
4. **Test migrations** on a staging S3 bucket before production

# PostgreSQL Backup and Restore Procedure

Representative custom-format `pg_dump` and restore drill for the distributed-engine
PostgreSQL database.

## Pre-Backup: Record Source Invariants

Before taking the backup, record these invariants from the source database:

```sql
-- Migration count
SELECT count(*) AS migration_count FROM schema_migrations;

-- Task count
SELECT count(*) AS task_count FROM tasks;

-- Task status distribution
SELECT status, count(*) FROM tasks GROUP BY status ORDER BY status;

-- Outbox event count
SELECT count(*) AS outbox_count FROM outbox_events;

-- Tenant count (if populated)
SELECT count(*) AS tenant_count FROM tenants;

-- Schedule count (if populated)
SELECT count(*) AS schedule_count FROM recurring_schedules;

-- CHECK constraints on tasks
SELECT conname FROM pg_constraint
WHERE contype = 'c' AND conrelid = 'tasks'::regclass
ORDER BY conname;

-- Task indexes
SELECT indexname FROM pg_indexes
WHERE tablename = 'tasks'
ORDER BY indexname;

-- Foreign-key constraints
SELECT conname FROM pg_constraint
WHERE contype = 'f'
ORDER BY conname;
```

Record all results before proceeding.

## Backup

```bash
# From the PostgreSQL pod or a host with psql/pg_dump access
pg_dump -U postgres distributed_engine \
  --format=custom \
  --file=/tmp/distributed_engine_backup.dump

# Verify file is non-empty
ls -la /tmp/distributed_engine_backup.dump
```

If running from outside the cluster:

```bash
kubectl exec -n distributed-engine postgres-0 -- \
  pg_dump -U postgres distributed_engine --format=custom \
  > distributed_engine_backup.dump

# Verify file size
ls -la distributed_engine_backup.dump
```

## Restore to Temporary Database

Restore into a clean target database to verify backup integrity:

```bash
# Create temporary restore database
kubectl exec -n distributed-engine postgres-0 -- \
  psql -U postgres -c "CREATE DATABASE distributed_engine_restore_test;"

# Restore
kubectl exec -i -n distributed-engine postgres-0 -- \
  pg_restore -U postgres -d distributed_engine_restore_test \
  < distributed_engine_backup.dump
```

Or from inside the pod:

```bash
pg_restore -U postgres -d distributed_engine_restore_test \
  /tmp/distributed_engine_backup.dump
```

## Post-Restore: Compare Invariants

Run the same invariant queries against the restored database and compare:

```bash
kubectl exec -n distributed-engine postgres-0 -- \
  psql -U postgres -d distributed_engine_restore_test -c \
  "SELECT count(*) AS migration_count FROM schema_migrations;"
```

| Invariant | Source value | Restored value | Match? |
|---|---|---|---|
| Migration count | | | |
| Task count | | | |
| Task status distribution | | | |
| Outbox event count | | | |
| Tenant count | | | |
| Schedule count | | | |
| CHECK constraints | | | |
| Task indexes | | | |
| Foreign-key constraints | | | |

All invariants must match exactly.

## Record Timing

When the drill is performed live, record:
- Backup start time
- Backup end time
- Backup file size
- Restore start time
- Restore end time
- Verification duration

Do not claim an RTO or RPO guarantee from an unexecuted procedure or a
single successful drill.

## Cleanup

```bash
kubectl exec -n distributed-engine postgres-0 -- \
  psql -U postgres -c "DROP DATABASE distributed_engine_restore_test;"
```

## What This Drill Demonstrates

Successful logical backup and restore with verification of selected
representative schema and data invariants.

## What This Drill Does NOT Prove

- High availability
- Disaster recovery
- Point-in-time recovery
- Backup scheduling or automation
- Cross-region backup
- Guaranteed RTO or RPO
- Exhaustive data fidelity beyond the checked invariants

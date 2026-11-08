#!/bin/sh
# Idempotent bootstrap: never re-seed or overwrite physical-owner guard after cutover.
set -eu
if [ "$(psql -X -At -v ON_ERROR_STOP=1 -c "SELECT COALESCE(to_regclass('public.shard_metadata')::text,'')")" = "" ]; then
  echo B10E4D_B_NEW_DB_BOOTSTRAP
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/shard.sql
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/capture.sql
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/physical.sql
else
  echo B10E4D_B_EXISTING_DB_VERIFY_ONLY
fi
psql -X -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE n INTEGER; hi INTEGER; who CHAR(1); e INTEGER;
BEGIN
  SELECT count(*) INTO n FROM accounts;
  SELECT last_account INTO hi FROM shard_metadata
  WHERE singleton=1 AND shard_id='B' AND first_account=1001 AND schema_version=1;
  IF NOT ((hi=2000 AND n=1000) OR (hi=1500 AND n=500)) THEN
    RAISE EXCEPTION 'B10E4D_B_BOOTSTRAP_STATE_INVALID';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM bank_migration_fence WHERE singleton=1) OR
     NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='bank_migration_fence_before'
                 AND tgrelid='accounts'::regclass AND tgenabled='O') OR
     NOT EXISTS (SELECT 1 FROM pg_proc
                   WHERE proname='bank_migration_write_guard'
                     AND pg_get_functiondef(oid) LIKE '%physical_shard%') THEN
    RAISE EXCEPTION 'B10E4D_B_FENCE_OR_PHYSICAL_GUARD_MISSING';
  END IF;
  IF hi=1500 AND EXISTS (SELECT 1 FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000') THEN
    RAISE EXCEPTION 'B10E4D_B_STALE_OLD_OWNER_ROWS';
  END IF;
END $$;
SQL
echo BONUS10E4D_B_INIT_SAFE_PASS

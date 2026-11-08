#!/bin/sh
set -eu
if [ "$(psql -X -At -v ON_ERROR_STOP=1 -c "SELECT COALESCE(to_regclass('public.shard_metadata')::text,'')")" = "" ]; then
  echo B10E4D_C_NEW_DB_BOOTSTRAP
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/shard_c.sql
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/capture.sql
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/physical.sql
  psql -X -v ON_ERROR_STOP=1 -f /bootstrap/staging.sql
else
  echo B10E4D_C_EXISTING_DB_VERIFY_ONLY
fi
psql -X -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE n INTEGER; owner CHAR(1); epoch INTEGER; fenced BOOLEAN;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM shard_metadata WHERE singleton=1 AND shard_id='C'
       AND first_account=1501 AND last_account=2000 AND schema_version=1) THEN
    RAISE EXCEPTION 'B10E4D_C_IDENTITY_INVALID';
  END IF;
  SELECT count(*) INTO n FROM accounts;
  SELECT owner_shard,owner_epoch,fence_enabled INTO owner,epoch,fenced
     FROM bank_migration_fence WHERE singleton=1;
  IF NOT ((n=0 AND owner='B' AND epoch=1 AND fenced=FALSE)
       OR (n=500 AND owner='C' AND epoch=2 AND fenced=TRUE)) THEN
    RAISE EXCEPTION 'B10E4D_C_BOOTSTRAP_STATE_INVALID';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='bank_migration_fence_before'
                  AND tgrelid='accounts'::regclass AND tgenabled='O') OR
     NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname='bank_migration_write_guard'
                  AND pg_get_functiondef(oid) LIKE '%physical_shard%') OR
     to_regclass('bank_migration_stage_accounts') IS NULL OR
     to_regclass('bank_migration_stage_receipts') IS NULL THEN
    RAISE EXCEPTION 'B10E4D_C_FENCE_OR_STAGE_MISSING';
  END IF;
END $$;
SQL
echo BONUS10E4D_C_INIT_SAFE_PASS

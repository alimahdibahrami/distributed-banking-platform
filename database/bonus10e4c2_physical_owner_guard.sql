-- 10E-4C2: fail-closed physical-owner guard for B/C migration databases.
-- Run ONLY after bonus10e2_migration_capture.sql on isolated/backup-restorable
-- B and C replicas. This file does NOT modify rows, routes or shard_metadata.
-- Source B after fence cannot be written even if a client spoofs C/epoch 2.
-- Shard C standby rejects writes before activation. Do NOT run in production.
BEGIN;

DO $$
DECLARE physical CHAR(1);
BEGIN
  SELECT shard_id INTO physical FROM shard_metadata WHERE singleton=1;
  IF physical NOT IN ('B', 'C') OR physical IS NULL THEN
    RAISE EXCEPTION 'BONUS10E4C2_REQUIRES_B_OR_C_DATABASE';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM bank_migration_fence WHERE singleton=1) THEN
    RAISE EXCEPTION 'BONUS10E4C2_REQUIRES_INITIALIZED_FENCE';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger
    WHERE tgrelid = 'accounts'::regclass AND tgname = 'bank_migration_fence_before'
      AND NOT tgisinternal) THEN
    RAISE EXCEPTION 'BONUS10E4C2_REQUIRES_10E2_FENCE_TRIGGER';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION bank_migration_write_guard()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_key VARCHAR(5);
    physical_shard CHAR(1);
    current_owner CHAR(1);
    current_epoch BIGINT;
    enforced BOOLEAN;
    capturing BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        account_key := NEW.account_id;
    ELSE
        account_key := OLD.account_id;
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.account_id <> OLD.account_id THEN
        RAISE EXCEPTION 'MIGRATION_ACCOUNT_REKEY_DENIED' USING ERRCODE='P0001';
    END IF;
    IF account_key < 'A1501' OR account_key > 'A2000' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    SELECT shard_id INTO physical_shard
       FROM shard_metadata WHERE singleton=1;
    IF NOT FOUND OR physical_shard NOT IN ('B', 'C') THEN
        RAISE EXCEPTION 'PHYSICAL_OWNER_IDENTITY_MISSING' USING ERRCODE='P0001';
    END IF;
    SELECT owner_shard,owner_epoch,fence_enabled,capture_enabled
       INTO current_owner,current_epoch,enforced,capturing
       FROM bank_migration_fence WHERE singleton=1 FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'MIGRATION_FENCE_NOT_INITIALIZED' USING ERRCODE='P0001';
    END IF;

    -- Unlike a client-provided session variable, shard_metadata is durable.
    -- After a B->C handoff, the B database must reject ALL moving-range writes.
    IF current_owner <> physical_shard THEN
        RAISE EXCEPTION 'STALE_OWNER_EPOCH: physical shard %, owner %, epoch %',
          physical_shard,current_owner,current_epoch USING ERRCODE='P0001';
    END IF;
    -- Standby C cannot accept writes before activation, even with a forged GUC.
    IF physical_shard = 'C' AND (NOT enforced OR current_epoch <> 2) THEN
        RAISE EXCEPTION 'STALE_OWNER_EPOCH: shard C is standby' USING ERRCODE='P0001';
    END IF;
    IF enforced AND (
        current_setting('bank.owner_shard', true) IS DISTINCT FROM trim(current_owner)
        OR current_setting('bank.owner_epoch', true) IS DISTINCT FROM current_epoch::TEXT
    ) THEN
        RAISE EXCEPTION 'STALE_OWNER_EPOCH: account=%, owner=%, epoch=%',
          account_key,current_owner,current_epoch USING ERRCODE='P0001';
    END IF;
    IF capturing AND TG_OP IN ('INSERT','DELETE') THEN
        RAISE EXCEPTION 'MIGRATION_CAPTURE_REQUIRES_UPDATE_ONLY' USING ERRCODE='P0001';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF capturing THEN
            IF OLD.migration_revision = 9223372036854775807 THEN
                RAISE EXCEPTION 'MIGRATION_REVISION_OVERFLOW';
            END IF;
            NEW.migration_revision := OLD.migration_revision + 1;
        ELSE
            NEW.migration_revision := OLD.migration_revision;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
COMMIT;

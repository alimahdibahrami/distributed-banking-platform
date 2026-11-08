-- Bonus 10E-2: ADDITIVE, OPT-IN migration capture and epoch fence for B/C.
-- NEVER apply to the live A/B deployment without tested backups and 10E-3.
-- One schema per database. The initial row assumes B owns the moving range.
-- The target database C must be initialized separately before this migration.
-- PostgreSQL 17. This DDL performs NO cutover or account copy.
BEGIN;

CREATE TABLE IF NOT EXISTS bank_migration_fence (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    first_account VARCHAR(5) NOT NULL DEFAULT 'A1501',
    last_account VARCHAR(5) NOT NULL DEFAULT 'A2000',
    owner_shard CHAR(1) NOT NULL CHECK (owner_shard IN ('B','C')),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    fence_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    capture_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT moving_range_fixed CHECK (first_account = 'A1501' AND last_account = 'A2000')
);
-- Start disabled. A migration controller, not this initializer, will enable it.
INSERT INTO bank_migration_fence (singleton, owner_shard, owner_epoch)
VALUES (1, 'B', 1) ON CONFLICT (singleton) DO NOTHING;

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS migration_revision BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS bank_migration_cdc (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id VARCHAR(5) NOT NULL,
    account_revision BIGINT NOT NULL CHECK (account_revision > 0),
    resulting_balance BIGINT NOT NULL CHECK (resulting_balance >= 0),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_id, account_revision)
);
CREATE INDEX IF NOT EXISTS bank_migration_cdc_account_revision
    ON bank_migration_cdc (account_id, account_revision);

-- BEFORE trigger acquires a transaction-level SHARE row lock on the fence.
-- A PREPARED 2PC transaction retains this lock: the migration controller
-- cannot flip the owner epoch until every in-flight PREPARE is resolved.
CREATE OR REPLACE FUNCTION bank_migration_write_guard()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_key VARCHAR(5);
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

    SELECT owner_shard, owner_epoch, fence_enabled, capture_enabled
        INTO current_owner, current_epoch, enforced, capturing
        FROM bank_migration_fence WHERE singleton = 1 FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'MIGRATION_FENCE_NOT_INITIALIZED' USING ERRCODE='P0001';
    END IF;

    IF enforced AND (
        current_setting('bank.owner_shard', true) IS DISTINCT FROM trim(current_owner)
        OR current_setting('bank.owner_epoch', true) IS DISTINCT FROM current_epoch::TEXT
    ) THEN
        RAISE EXCEPTION 'STALE_OWNER_EPOCH: account=%, owner=%, epoch=%',
             account_key, current_owner, current_epoch USING ERRCODE='P0001';
    END IF;

    -- During capture no accounts may be created or deleted. E3's dedicated
    -- post-cutover cleanup must use a separately reviewed mechanism.
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

CREATE OR REPLACE FUNCTION bank_migration_capture_update()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    -- A version increment is part of the same transaction as balance UPDATE.
    -- An aborted or ROLLBACK PREPARED transaction leaves no event or balance.
    IF NEW.account_id BETWEEN 'A1501' AND 'A2000'
       AND NEW.migration_revision > OLD.migration_revision THEN
        INSERT INTO bank_migration_cdc(account_id, account_revision, resulting_balance)
        VALUES(NEW.account_id, NEW.migration_revision, NEW.balance);
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS bank_migration_fence_before ON accounts;
CREATE TRIGGER bank_migration_fence_before
BEFORE INSERT OR UPDATE OR DELETE ON accounts
FOR EACH ROW EXECUTE FUNCTION bank_migration_write_guard();

DROP TRIGGER IF EXISTS bank_migration_cdc_after ON accounts;
CREATE TRIGGER bank_migration_cdc_after
AFTER UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION bank_migration_capture_update();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bank_migration_fence WHERE singleton=1
          AND first_account='A1501' AND last_account='A2000'
    ) THEN
        RAISE EXCEPTION 'Unexpected migration fence identity';
    END IF;
END $$;
COMMIT;

-- Additional staging-only tables for a DISPOSABLE Shard C database.
-- Does NOT touch accounts, shard_metadata, ownership fence, or routes.
-- Must NOT be installed on the current production databases.
BEGIN;
CREATE TABLE IF NOT EXISTS bank_migration_stage_accounts (
    account_id VARCHAR(5) PRIMARY KEY
        CHECK (account_id ~ '^A[0-9]{4}$' AND account_id BETWEEN 'A1501' AND 'A2000'),
    balance BIGINT NOT NULL CHECK (balance >= 0),
    account_revision BIGINT NOT NULL CHECK (account_revision >= 0),
    snapshot_revision BIGINT NOT NULL CHECK (snapshot_revision >= 0
                                             AND snapshot_revision <= account_revision),
    snapshot_balance BIGINT NOT NULL CHECK (snapshot_balance >= 0)
);
CREATE TABLE IF NOT EXISTS bank_migration_stage_receipts (
    account_id VARCHAR(5) NOT NULL
        REFERENCES bank_migration_stage_accounts(account_id),
    account_revision BIGINT NOT NULL CHECK (account_revision > 0),
    balance BIGINT NOT NULL CHECK (balance >= 0),
    PRIMARY KEY (account_id, account_revision)
);
COMMIT;

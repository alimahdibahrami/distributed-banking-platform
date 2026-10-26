-- Idempotent initialization for Shard B; never resets existing balances.
-- Run by the one-shot shard_b_init service against bank_b.
BEGIN;
CREATE TABLE IF NOT EXISTS shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_id CHAR(1) NOT NULL CHECK (shard_id = 'B'),
    first_account INTEGER NOT NULL CHECK (first_account = 1001),
    last_account INTEGER NOT NULL CHECK (last_account = 2000),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1)
);
INSERT INTO shard_metadata (singleton, shard_id, first_account, last_account, schema_version)
VALUES (1, 'B', 1001, 2000, 1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    CONSTRAINT account_id_in_shard_b CHECK (
        account_id ~ '^A[0-9]{4}$' AND substring(account_id from 2)::INTEGER BETWEEN 1001 AND 2000
    )
);
-- Phase 5: committed participant receipt belongs to the SAME prepared transaction.
-- The receipt is invisible until COMMIT PREPARED and guards duplicate application.
CREATE TABLE IF NOT EXISTS tpc_participant_commits (
    transaction_id UUID PRIMARY KEY,
    account_id VARCHAR(5) NOT NULL REFERENCES accounts(account_id),
    role VARCHAR(6) NOT NULL CHECK (role IN ('debit', 'credit')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Seed history prevents a future restart from re-creating accounts moved by live resharding.
CREATE TABLE IF NOT EXISTS bootstrap_history (
    seed_id TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- A1500 begins with 500 as in the instructor's 2PC example.
WITH first_seed AS (
    INSERT INTO bootstrap_history (seed_id) VALUES ('initial-accounts-v1')
    ON CONFLICT (seed_id) DO NOTHING
    RETURNING seed_id
)
INSERT INTO accounts (account_id, balance)
SELECT 'A' || lpad(n::TEXT, 4, '0'),
       CASE WHEN n = 1500 THEN 500::BIGINT ELSE 1000::BIGINT END
FROM generate_series(1001, 2000) AS n
WHERE EXISTS (SELECT 1 FROM first_seed)
ON CONFLICT (account_id) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM shard_metadata
        WHERE singleton = 1 AND shard_id = 'B' AND first_account = 1001
              AND last_account = 2000 AND schema_version = 1
    ) THEN
        RAISE EXCEPTION 'Shard B database identity mismatch';
    END IF;
    IF (SELECT count(*) FROM accounts) <> 1000 THEN
        RAISE EXCEPTION 'Shard B account fixture not complete';
    END IF;
    IF EXISTS (
        SELECT 1 FROM accounts
        WHERE account_id !~ '^A[0-9]{4}$'
           OR substring(account_id from 2)::INTEGER NOT BETWEEN 1001 AND 2000
    ) THEN
        RAISE EXCEPTION 'Shard B has an account owned by another shard';
    END IF;
END $$;
COMMIT;

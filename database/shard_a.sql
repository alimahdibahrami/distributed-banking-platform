-- Idempotent initialization for Shard A; never resets existing balances.
-- Run by the one-shot shard_a_init service against bank_a.
BEGIN;
CREATE TABLE IF NOT EXISTS shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_id CHAR(1) NOT NULL CHECK (shard_id = 'A'),
    first_account INTEGER NOT NULL CHECK (first_account = 1),
    last_account INTEGER NOT NULL CHECK (last_account = 1000),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1)
);
INSERT INTO shard_metadata (singleton, shard_id, first_account, last_account, schema_version)
VALUES (1, 'A', 1, 1000, 1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    CONSTRAINT account_id_in_shard_a CHECK (
        account_id ~ '^A[0-9]{4}$' AND substring(account_id from 2)::INTEGER BETWEEN 1 AND 1000
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
-- Phase 6: durable abort tombstone.  A timed-out PREPARE cannot
-- resurrect a transaction after its ABORT has been acknowledged.
CREATE TABLE IF NOT EXISTS tpc_participant_aborts (
    transaction_id UUID PRIMARY KEY,
    aborted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Seed history prevents a future restart from re-creating accounts moved by live resharding.
CREATE TABLE IF NOT EXISTS bootstrap_history (
    seed_id TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Deterministic fixture, including the A0500 example in the assignment.
WITH first_seed AS (
    INSERT INTO bootstrap_history (seed_id) VALUES ('initial-accounts-v1')
    ON CONFLICT (seed_id) DO NOTHING
    RETURNING seed_id
)
INSERT INTO accounts (account_id, balance)
SELECT 'A' || lpad(n::TEXT, 4, '0'), 1000::BIGINT
FROM generate_series(1, 1000) AS n
WHERE EXISTS (SELECT 1 FROM first_seed)
ON CONFLICT (account_id) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM shard_metadata
        WHERE singleton = 1 AND shard_id = 'A' AND first_account = 1
              AND last_account = 1000 AND schema_version = 1
    ) THEN
        RAISE EXCEPTION 'Shard A database identity mismatch';
    END IF;
    IF (SELECT count(*) FROM accounts) <> 1000 THEN
        RAISE EXCEPTION 'Shard A account fixture not complete';
    END IF;
    IF EXISTS (
        SELECT 1 FROM accounts
        WHERE account_id !~ '^A[0-9]{4}$'
           OR substring(account_id from 2)::INTEGER NOT BETWEEN 1 AND 1000
    ) THEN
        RAISE EXCEPTION 'Shard A has an account owned by another shard';
    END IF;
END $$;
COMMIT;

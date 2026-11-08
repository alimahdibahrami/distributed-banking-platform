-- B10E4D only. Empty migration target: no synthetic balances.
BEGIN;
CREATE TABLE shard_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    shard_id CHAR(1) NOT NULL CHECK (shard_id='C'),
    first_account INTEGER NOT NULL CHECK (first_account=1501),
    last_account INTEGER NOT NULL CHECK (last_account=2000),
    schema_version INTEGER NOT NULL CHECK (schema_version=1)
);
INSERT INTO shard_metadata VALUES (1,'C',1501,2000,1);
CREATE TABLE accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance BIGINT NOT NULL CHECK (balance>=0),
    CONSTRAINT account_id_in_shard_c CHECK (
        account_id ~ '^A[0-9]{4}$' AND substring(account_id from 2)::INTEGER BETWEEN 1501 AND 2000)
);
CREATE TABLE tpc_participant_commits (
    transaction_id UUID PRIMARY KEY,
    account_id VARCHAR(5) NOT NULL REFERENCES accounts(account_id),
    role VARCHAR(6) NOT NULL CHECK (role IN ('debit','credit')),
    amount BIGINT NOT NULL CHECK (amount>0),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE tpc_participant_aborts (
    transaction_id UUID PRIMARY KEY,
    aborted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE bootstrap_history (
    seed_id TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO bootstrap_history VALUES ('empty-migration-target-v1');
COMMIT;

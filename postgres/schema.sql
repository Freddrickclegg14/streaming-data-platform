CREATE TABLE IF NOT EXISTS ground_truth_labels (
    transaction_id TEXT PRIMARY KEY,
    is_fraud       BOOLEAN NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fraud_flags (
    transaction_id TEXT PRIMARY KEY,
    flagged_fraud  BOOLEAN NOT NULL,
    flagged_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rule_triggered TEXT
);

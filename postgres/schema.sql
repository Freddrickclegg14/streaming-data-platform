CREATE TABLE IF NOT EXISTS ground_truth_labels (
    transaction_id TEXT PRIMARY KEY,
    is_fraud       BOOLEAN NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fraud_flags (
    transaction_id TEXT NOT NULL,
    flagged_fraud  BOOLEAN NOT NULL,
    flagged_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rule_triggered TEXT NOT NULL,
    PRIMARY KEY (transaction_id, rule_triggered)
);


CREATE OR REPLACE VIEW rule_performance AS
SELECT
    f.rule_triggered,
    COUNT(*) FILTER (WHERE g.is_fraud) AS true_positives,
    COUNT(*) FILTER (WHERE NOT g.is_fraud) AS false_positives,
    ROUND(
        COUNT(*) FILTER (WHERE g.is_fraud)::numeric / NULLIF(COUNT(*), 0) * 100, 2
    ) AS precision_pct,
    ROUND(
        COUNT(*) FILTER (WHERE g.is_fraud)::numeric
        / NULLIF((SELECT COUNT(*) FROM ground_truth_labels WHERE is_fraud), 0) * 100, 2
    ) AS recall_pct
FROM fraud_flags f
JOIN ground_truth_labels g USING (transaction_id)
WHERE f.flagged_fraud
GROUP BY f.rule_triggered;
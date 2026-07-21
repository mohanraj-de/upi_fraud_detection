CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              BIGSERIAL PRIMARY KEY,
    txn_id          VARCHAR(64) NOT NULL,
    sender_upi      VARCHAR(64) NOT NULL,
    receiver_upi    VARCHAR(64) NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    txn_timestamp   TIMESTAMP NOT NULL,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    fraud_reason    VARCHAR(128) NOT NULL,
    txn_count       INTEGER,
    total_amount    NUMERIC(14, 2),
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_txn_timestamp ON fraud_alerts (txn_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_sender_upi ON fraud_alerts (sender_upi);

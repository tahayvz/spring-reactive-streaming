CREATE TABLE IF NOT EXISTS price_ticks (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(16)     NOT NULL,
    price       NUMERIC(19, 4)  NOT NULL,
    source      VARCHAR(32)     NOT NULL,
    observed_at TIMESTAMPTZ     NOT NULL
);

-- Sorgu her zaman "bir sembolün son kayıtları" biçiminde; indeks bu erişimi izler.
CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol_time
    ON price_ticks (symbol, observed_at DESC);

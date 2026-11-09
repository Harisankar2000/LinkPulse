-- Each nextval() reserves a block of 1000 IDs for one API instance.
-- Keep INCREMENT BY in sync with store.IDBlockSize (checked at startup).
CREATE SEQUENCE IF NOT EXISTS link_id_blocks INCREMENT BY 1000 START WITH 1000;

CREATE TABLE IF NOT EXISTS links (
    code        text        PRIMARY KEY,
    long_url    text        NOT NULL,
    is_custom   boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz
);

CREATE INDEX IF NOT EXISTS links_expires_at_idx
    ON links (expires_at) WHERE expires_at IS NOT NULL;

-- Analytics are stored as hourly rollups, not one row per click: a link with a
-- million clicks from 20 countries in a day is ~500 rows instead of 1,000,000.
CREATE TABLE IF NOT EXISTS link_clicks_hourly (
    code      text        NOT NULL,
    bucket    timestamptz NOT NULL,
    country   text        NOT NULL,
    device    text        NOT NULL,
    referrer  text        NOT NULL,
    clicks    bigint      NOT NULL,
    PRIMARY KEY (code, bucket, country, device, referrer)
);

-- Apply once to databases created from the pre-campaign-window schema.
-- Fresh installations receive these columns and indexes from ../schema.sql.
ALTER TABLE campaigns
    ADD COLUMN starts_at BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN ends_at BIGINT NOT NULL DEFAULT 0,
    ADD INDEX idx_campaigns_sku_state (sku_id, state);

ALTER TABLE orders
    ADD COLUMN campaign_id VARCHAR(64) NOT NULL DEFAULT '',
    ADD INDEX idx_orders_campaign_id (campaign_id);

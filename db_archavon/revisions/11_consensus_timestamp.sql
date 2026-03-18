ALTER TABLE cache_creatures ADD COLUMN consensus_at DATETIME NULL DEFAULT NULL AFTER is_consensus;
ALTER TABLE cache_quests ADD COLUMN consensus_at DATETIME NULL DEFAULT NULL AFTER is_consensus;
ALTER TABLE cache_gameobjects ADD COLUMN consensus_at DATETIME NULL DEFAULT NULL AFTER is_consensus;
ALTER TABLE cache_pagetext ADD COLUMN consensus_at DATETIME NULL DEFAULT NULL AFTER is_consensus;

-- backfill existing consensus rows with first_seen as approximation
UPDATE cache_creatures SET consensus_at = first_seen WHERE is_consensus = 1;
UPDATE cache_quests SET consensus_at = first_seen WHERE is_consensus = 1;
UPDATE cache_gameobjects SET consensus_at = first_seen WHERE is_consensus = 1;
UPDATE cache_pagetext SET consensus_at = first_seen WHERE is_consensus = 1;

-- index for consensus_since queries
CREATE INDEX idx_consensus_at ON cache_creatures (consensus_at);
CREATE INDEX idx_consensus_at ON cache_quests (consensus_at);
CREATE INDEX idx_consensus_at ON cache_gameobjects (consensus_at);
CREATE INDEX idx_consensus_at ON cache_pagetext (consensus_at);

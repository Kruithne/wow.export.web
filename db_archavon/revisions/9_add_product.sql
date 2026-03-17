ALTER TABLE cache_creatures ADD COLUMN product VARCHAR(32) NOT NULL DEFAULT 'wow' AFTER content_hash;
ALTER TABLE cache_quests ADD COLUMN product VARCHAR(32) NOT NULL DEFAULT 'wow' AFTER content_hash;
ALTER TABLE cache_gameobjects ADD COLUMN product VARCHAR(32) NOT NULL DEFAULT 'wow' AFTER content_hash;
ALTER TABLE cache_pagetext ADD COLUMN product VARCHAR(32) NOT NULL DEFAULT 'wow' AFTER content_hash;

ALTER TABLE cache_creatures DROP INDEX uq_creature, ADD UNIQUE KEY uq_creature (record_id, locale, content_hash, product);
ALTER TABLE cache_quests DROP INDEX uq_quest, ADD UNIQUE KEY uq_quest (record_id, locale, content_hash, product);
ALTER TABLE cache_gameobjects DROP INDEX uq_gameobject, ADD UNIQUE KEY uq_gameobject (record_id, locale, content_hash, product);
ALTER TABLE cache_pagetext DROP INDEX uq_pagetext, ADD UNIQUE KEY uq_pagetext (record_id, locale, content_hash, product);

ALTER TABLE cache_creatures ADD INDEX idx_creatures_product (product);
ALTER TABLE cache_quests ADD INDEX idx_quests_product (product);
ALTER TABLE cache_gameobjects ADD INDEX idx_gameobjects_product (product);
ALTER TABLE cache_pagetext ADD INDEX idx_pagetext_product (product);

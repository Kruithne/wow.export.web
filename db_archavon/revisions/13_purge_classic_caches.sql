-- purge incorrectly-parsed classic cache data
-- these products were being parsed with the retail format prior to classic support

SET @classic_era = 'wow_classic_era';
SET @anniversary = 'wow_anniversary';
SET @classic = 'wow_classic';
SET @titan = 'wow_classic_titan';

-- wdb_attestations has no FK, clean up orphaned rows for each entity type
DELETE a FROM wdb_attestations a
INNER JOIN cache_creatures c ON a.entity_type = 'creature' AND a.entry_id = c.entry_id
WHERE c.product IN (@classic_era, @anniversary, @classic, @titan);

DELETE a FROM wdb_attestations a
INNER JOIN cache_quests q ON a.entity_type = 'quest' AND a.entry_id = q.entry_id
WHERE q.product IN (@classic_era, @anniversary, @classic, @titan);

DELETE a FROM wdb_attestations a
INNER JOIN cache_gameobjects g ON a.entity_type = 'gameobject' AND a.entry_id = g.entry_id
WHERE g.product IN (@classic_era, @anniversary, @classic, @titan);

DELETE a FROM wdb_attestations a
INNER JOIN cache_pagetext p ON a.entity_type = 'pagetext' AND a.entry_id = p.entry_id
WHERE p.product IN (@classic_era, @anniversary, @classic, @titan);

-- cache_quest_objectives and cache_quest_conditional_texts cascade from cache_quests

-- purge cache entity tables
DELETE FROM cache_creatures WHERE product IN (@classic_era, @anniversary, @classic, @titan);
DELETE FROM cache_quests WHERE product IN (@classic_era, @anniversary, @classic, @titan);
DELETE FROM cache_gameobjects WHERE product IN (@classic_era, @anniversary, @classic, @titan);
DELETE FROM cache_pagetext WHERE product IN (@classic_era, @anniversary, @classic, @titan);

-- purge hotfix entries
DELETE FROM hotfix_entries WHERE product IN (@classic_era, @anniversary, @classic, @titan);

-- purge submissions (cascade deletes cache_submission_files)
DELETE FROM cache_submissions WHERE product IN (@classic_era, @anniversary, @classic, @titan);

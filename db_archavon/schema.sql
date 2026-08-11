-- archavon sqlite schema (collapsed from mysql revisions 1-14)
--
-- dialect notes:
--   ENUM            -> TEXT + CHECK constraint
--   AUTO_INCREMENT  -> INTEGER PRIMARY KEY AUTOINCREMENT (prevents rowid reuse; entry_id is
--                      referenced by wdb_attestations without a foreign key, so reuse after
--                      a prune would silently re-associate stale attestations)
--   DATETIME        -> TEXT 'YYYY-MM-DD HH:MM:SS' in UTC via datetime('now')
--                      mysql CURRENT_TIMESTAMP was server-local; all comparisons must use UTC
--   INT/BIGINT      -> INTEGER (no unsigned concept)
--   FLOAT           -> REAL
--   VARCHAR/CHAR    -> TEXT (no length limits enforced)
--   BOOLEAN         -> INTEGER 0/1
--
-- index names are global in sqlite, so per-table names are prefixed.

PRAGMA foreign_keys = ON;

-- ============================================================
-- submissions
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_submissions (
	submission_id TEXT NOT NULL PRIMARY KEY,
	machine_id TEXT NOT NULL,
	product TEXT NOT NULL,
	patch TEXT NOT NULL,
	build_number INTEGER NOT NULL,
	build_key TEXT NOT NULL,
	cdn_key TEXT NOT NULL,
	binary_hash TEXT NOT NULL,
	client_ip TEXT NULL,
	submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
	finalized_at TEXT NULL,
	processed_at TEXT NULL,
	status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'finalized', 'processing', 'completed', 'partial', 'failed')),
	status_reason TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_submissions_finalized ON cache_submissions (finalized_at);
CREATE INDEX IF NOT EXISTS idx_submissions_processed ON cache_submissions (processed_at);
CREATE INDEX IF NOT EXISTS idx_submissions_status ON cache_submissions (status);
CREATE INDEX IF NOT EXISTS idx_submissions_machine_submitted ON cache_submissions (machine_id, submitted_at);

CREATE TABLE IF NOT EXISTS cache_submission_files (
	submission_id TEXT NOT NULL,
	file_name TEXT NOT NULL,
	locale TEXT NOT NULL,
	file_size INTEGER NOT NULL,
	modified_at TEXT NULL,
	object_id TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'rejected')),
	failure_reason TEXT NULL CHECK (failure_reason IS NULL OR failure_reason IN ('download_failed', 'checksum_mismatch', 'invalid_magic', 'parse_error', 'no_records', 'unknown_signature', 'purged')),
	records_added INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (submission_id, file_name, locale),
	FOREIGN KEY (submission_id) REFERENCES cache_submissions(submission_id) ON DELETE CASCADE
);

-- object_id is how the vps addresses a file row (status writes, purge); without
-- this every such write scans the table
CREATE INDEX IF NOT EXISTS idx_submission_files_object ON cache_submission_files (object_id);

-- absent in mysql; ledger of applied worker deltas. a delta is identified by its sha256,
-- so a retried upload short-circuits instead of re-running the merge.
CREATE TABLE IF NOT EXISTS delta_applications (
	submission_id TEXT NOT NULL,
	delta_hash TEXT NOT NULL,
	rows_applied INTEGER NOT NULL DEFAULT 0,
	applied_at TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY (submission_id, delta_hash),
	FOREIGN KEY (submission_id) REFERENCES cache_submissions(submission_id) ON DELETE CASCADE
);

-- ============================================================
-- machines
-- ============================================================
CREATE TABLE IF NOT EXISTS machines (
	machine_id TEXT NOT NULL PRIMARY KEY,
	hardware_hash TEXT NULL,
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	last_seen TEXT NOT NULL DEFAULT (datetime('now')),
	trust_score REAL NOT NULL DEFAULT 1.0,
	blocked INTEGER NOT NULL DEFAULT 0,
	block_reason TEXT NULL
);

-- ============================================================
-- cache_creatures
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_creatures (
	entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	attestation_count INTEGER NOT NULL DEFAULT 1,
	is_consensus INTEGER NOT NULL DEFAULT 0,
	consensus_at TEXT NULL DEFAULT NULL,

	title TEXT NOT NULL DEFAULT '',
	title_alt TEXT NOT NULL DEFAULT '',
	cursor_name TEXT NOT NULL DEFAULT '',
	leader INTEGER NOT NULL DEFAULT 0,
	creature_type INTEGER NOT NULL DEFAULT 0,
	creature_family INTEGER NOT NULL DEFAULT 0,
	classification INTEGER NOT NULL DEFAULT 0,
	num_displays INTEGER NOT NULL DEFAULT 0,
	total_probability REAL NOT NULL DEFAULT 0,
	hp_multiplier REAL NOT NULL DEFAULT 1,
	energy_multiplier REAL NOT NULL DEFAULT 1,
	movement_info_id INTEGER NOT NULL DEFAULT 0,
	required_expansion INTEGER NOT NULL DEFAULT 0,
	tracking_quest_id INTEGER NOT NULL DEFAULT 0,
	vignette_id INTEGER NOT NULL DEFAULT 0,
	creature_class_mask INTEGER NOT NULL DEFAULT 0,
	creature_difficulty_id INTEGER NOT NULL DEFAULT 0,
	widget_parent_set_id INTEGER NOT NULL DEFAULT 0,
	widget_set_unit_condition_id INTEGER NOT NULL DEFAULT 0,

	name_0 TEXT NOT NULL DEFAULT '',
	name_1 TEXT NOT NULL DEFAULT '',
	name_2 TEXT NOT NULL DEFAULT '',
	name_3 TEXT NOT NULL DEFAULT '',

	name_alt_0 TEXT NOT NULL DEFAULT '',
	name_alt_1 TEXT NOT NULL DEFAULT '',
	name_alt_2 TEXT NOT NULL DEFAULT '',
	name_alt_3 TEXT NOT NULL DEFAULT '',

	flag_0 INTEGER NOT NULL DEFAULT 0,
	flag_1 INTEGER NOT NULL DEFAULT 0,

	proxy_creature_id_0 INTEGER NOT NULL DEFAULT 0,
	proxy_creature_id_1 INTEGER NOT NULL DEFAULT 0,

	display_id_0 INTEGER NOT NULL DEFAULT 0,
	display_scale_0 REAL NOT NULL DEFAULT 0,
	display_probability_0 REAL NOT NULL DEFAULT 0,
	display_id_1 INTEGER NOT NULL DEFAULT 0,
	display_scale_1 REAL NOT NULL DEFAULT 0,
	display_probability_1 REAL NOT NULL DEFAULT 0,
	display_id_2 INTEGER NOT NULL DEFAULT 0,
	display_scale_2 REAL NOT NULL DEFAULT 0,
	display_probability_2 REAL NOT NULL DEFAULT 0,
	display_id_3 INTEGER NOT NULL DEFAULT 0,
	display_scale_3 REAL NOT NULL DEFAULT 0,
	display_probability_3 REAL NOT NULL DEFAULT 0,

	quest_item_0 INTEGER NOT NULL DEFAULT 0,
	quest_item_1 INTEGER NOT NULL DEFAULT 0,
	quest_item_2 INTEGER NOT NULL DEFAULT 0,
	quest_item_3 INTEGER NOT NULL DEFAULT 0,
	quest_item_4 INTEGER NOT NULL DEFAULT 0,
	quest_item_5 INTEGER NOT NULL DEFAULT 0,

	currency_id_0 INTEGER NOT NULL DEFAULT 0,
	currency_id_1 INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_creature ON cache_creatures (record_id, locale, content_hash, product);
CREATE INDEX IF NOT EXISTS idx_creatures_product ON cache_creatures (product);
CREATE INDEX IF NOT EXISTS idx_creatures_consensus_at ON cache_creatures (consensus_at);

-- ============================================================
-- cache_quests
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_quests (
	entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	attestation_count INTEGER NOT NULL DEFAULT 1,
	is_consensus INTEGER NOT NULL DEFAULT 0,
	consensus_at TEXT NULL DEFAULT NULL,

	quest_type INTEGER NOT NULL DEFAULT 0,
	quest_package_id INTEGER NOT NULL DEFAULT 0,
	content_tuning_id INTEGER NOT NULL DEFAULT 0,
	quest_sort_id INTEGER NOT NULL DEFAULT 0,
	quest_info_id INTEGER NOT NULL DEFAULT 0,
	suggested_group_num INTEGER NOT NULL DEFAULT 0,
	reward_next_quest INTEGER NOT NULL DEFAULT 0,
	reward_xp_difficulty INTEGER NOT NULL DEFAULT 0,
	reward_xp_multiplier REAL NOT NULL DEFAULT 0,
	reward_money INTEGER NOT NULL DEFAULT 0,
	reward_money_difficulty INTEGER NOT NULL DEFAULT 0,
	reward_money_multiplier REAL NOT NULL DEFAULT 0,
	reward_bonus_money INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_count INTEGER NOT NULL DEFAULT 0,
	reward_spell INTEGER NOT NULL DEFAULT 0,
	reward_honor_addition INTEGER NOT NULL DEFAULT 0,
	reward_honor_multiplier REAL NOT NULL DEFAULT 0,
	reward_artifact_xp_difficulty INTEGER NOT NULL DEFAULT 0,
	reward_artifact_xp_multiplier REAL NOT NULL DEFAULT 0,
	reward_artifact_category_id INTEGER NOT NULL DEFAULT 0,
	provided_item INTEGER NOT NULL DEFAULT 0,
	poi_continent INTEGER NOT NULL DEFAULT 0,
	poi_x REAL NOT NULL DEFAULT 0,
	poi_y REAL NOT NULL DEFAULT 0,
	poi_priority INTEGER NOT NULL DEFAULT 0,
	reward_title INTEGER NOT NULL DEFAULT 0,
	reward_arena_points INTEGER NOT NULL DEFAULT 0,
	reward_skill_line_id INTEGER NOT NULL DEFAULT 0,
	reward_num_skill_ups INTEGER NOT NULL DEFAULT 0,
	portrait_giver_display_id INTEGER NOT NULL DEFAULT 0,
	portrait_giver_mount_display_id INTEGER NOT NULL DEFAULT 0,
	portrait_turn_in_display_id INTEGER NOT NULL DEFAULT 0,
	portrait_model_scene_id INTEGER NOT NULL DEFAULT 0,
	reward_faction_flags INTEGER NOT NULL DEFAULT 0,
	accepted_sound_kit_id INTEGER NOT NULL DEFAULT 0,
	complete_sound_kit_id INTEGER NOT NULL DEFAULT 0,
	area_group_id INTEGER NOT NULL DEFAULT 0,
	time_allowed INTEGER NOT NULL DEFAULT 0,
	num_objectives INTEGER NOT NULL DEFAULT 0,
	race_flags INTEGER NOT NULL DEFAULT 0,
	expansion_id INTEGER NOT NULL DEFAULT 0,
	managed_world_state_id INTEGER NOT NULL DEFAULT 0,
	quest_session_bonus INTEGER NOT NULL DEFAULT 0,
	quest_giver_creature_id INTEGER NOT NULL DEFAULT 0,
	ready_for_translation INTEGER NOT NULL DEFAULT 0,
	reset_by_scheduler INTEGER NOT NULL DEFAULT 0,

	flag_0 INTEGER NOT NULL DEFAULT 0,
	flag_1 INTEGER NOT NULL DEFAULT 0,
	flag_2 INTEGER NOT NULL DEFAULT 0,
	flag_3 INTEGER NOT NULL DEFAULT 0,

	reward_fixed_item_id_0 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_qty_0 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_id_1 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_qty_1 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_id_2 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_qty_2 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_id_3 INTEGER NOT NULL DEFAULT 0,
	reward_fixed_item_qty_3 INTEGER NOT NULL DEFAULT 0,

	item_drop_item_id_0 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_qty_0 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_id_1 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_qty_1 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_id_2 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_qty_2 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_id_3 INTEGER NOT NULL DEFAULT 0,
	item_drop_item_qty_3 INTEGER NOT NULL DEFAULT 0,

	reward_choice_item_id_0 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_0 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_0 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_id_1 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_1 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_1 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_id_2 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_2 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_2 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_id_3 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_3 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_3 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_id_4 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_4 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_4 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_id_5 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_qty_5 INTEGER NOT NULL DEFAULT 0,
	reward_choice_item_display_id_5 INTEGER NOT NULL DEFAULT 0,

	faction_reward_id_0 INTEGER NOT NULL DEFAULT 0,
	faction_reward_value_0 INTEGER NOT NULL DEFAULT 0,
	faction_reward_override_0 INTEGER NOT NULL DEFAULT 0,
	faction_reward_max_rank_0 INTEGER NOT NULL DEFAULT 0,
	faction_reward_id_1 INTEGER NOT NULL DEFAULT 0,
	faction_reward_value_1 INTEGER NOT NULL DEFAULT 0,
	faction_reward_override_1 INTEGER NOT NULL DEFAULT 0,
	faction_reward_max_rank_1 INTEGER NOT NULL DEFAULT 0,
	faction_reward_id_2 INTEGER NOT NULL DEFAULT 0,
	faction_reward_value_2 INTEGER NOT NULL DEFAULT 0,
	faction_reward_override_2 INTEGER NOT NULL DEFAULT 0,
	faction_reward_max_rank_2 INTEGER NOT NULL DEFAULT 0,
	faction_reward_id_3 INTEGER NOT NULL DEFAULT 0,
	faction_reward_value_3 INTEGER NOT NULL DEFAULT 0,
	faction_reward_override_3 INTEGER NOT NULL DEFAULT 0,
	faction_reward_max_rank_3 INTEGER NOT NULL DEFAULT 0,
	faction_reward_id_4 INTEGER NOT NULL DEFAULT 0,
	faction_reward_value_4 INTEGER NOT NULL DEFAULT 0,
	faction_reward_override_4 INTEGER NOT NULL DEFAULT 0,
	faction_reward_max_rank_4 INTEGER NOT NULL DEFAULT 0,

	currency_reward_id_0 INTEGER NOT NULL DEFAULT 0,
	currency_reward_qty_0 INTEGER NOT NULL DEFAULT 0,
	currency_reward_id_1 INTEGER NOT NULL DEFAULT 0,
	currency_reward_qty_1 INTEGER NOT NULL DEFAULT 0,
	currency_reward_id_2 INTEGER NOT NULL DEFAULT 0,
	currency_reward_qty_2 INTEGER NOT NULL DEFAULT 0,
	currency_reward_id_3 INTEGER NOT NULL DEFAULT 0,
	currency_reward_qty_3 INTEGER NOT NULL DEFAULT 0,

	reward_display_spell_id_0 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_condition_0 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_type_0 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_id_1 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_condition_1 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_type_1 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_id_2 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_condition_2 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_type_2 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_id_3 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_condition_3 INTEGER NOT NULL DEFAULT 0,
	reward_display_spell_type_3 INTEGER NOT NULL DEFAULT 0,

	treasure_picker_id_0 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_1 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_3 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_4 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_5 INTEGER NOT NULL DEFAULT 0,

	treasure_picker_id_2_0 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2_1 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2_2 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2_3 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2_4 INTEGER NOT NULL DEFAULT 0,
	treasure_picker_id_2_5 INTEGER NOT NULL DEFAULT 0,

	log_title TEXT NOT NULL,
	log_description TEXT NOT NULL,
	quest_description TEXT NOT NULL,
	area_description TEXT NOT NULL,
	portrait_giver_text TEXT NOT NULL,
	portrait_giver_name TEXT NOT NULL,
	portrait_turn_in_text TEXT NOT NULL,
	portrait_turn_in_name TEXT NOT NULL,
	quest_completion_log TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_quest ON cache_quests (record_id, locale, content_hash, product);
CREATE INDEX IF NOT EXISTS idx_quests_product ON cache_quests (product);
CREATE INDEX IF NOT EXISTS idx_quests_consensus_at ON cache_quests (consensus_at);

-- ============================================================
-- cache_gameobjects
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_gameobjects (
	entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	attestation_count INTEGER NOT NULL DEFAULT 1,
	is_consensus INTEGER NOT NULL DEFAULT 0,
	consensus_at TEXT NULL DEFAULT NULL,

	type INTEGER NOT NULL DEFAULT 0,
	display_id INTEGER NOT NULL DEFAULT 0,
	icon TEXT NOT NULL DEFAULT '',
	action TEXT NOT NULL DEFAULT '',
	"condition" TEXT NOT NULL DEFAULT '',
	scale REAL NOT NULL DEFAULT 1,
	content_tuning_id INTEGER NOT NULL DEFAULT 0,

	name_0 TEXT NOT NULL DEFAULT '',
	name_1 TEXT NOT NULL DEFAULT '',
	name_2 TEXT NOT NULL DEFAULT '',
	name_3 TEXT NOT NULL DEFAULT '',

	game_data_0 INTEGER NOT NULL DEFAULT 0,
	game_data_1 INTEGER NOT NULL DEFAULT 0,
	game_data_2 INTEGER NOT NULL DEFAULT 0,
	game_data_3 INTEGER NOT NULL DEFAULT 0,
	game_data_4 INTEGER NOT NULL DEFAULT 0,
	game_data_5 INTEGER NOT NULL DEFAULT 0,
	game_data_6 INTEGER NOT NULL DEFAULT 0,
	game_data_7 INTEGER NOT NULL DEFAULT 0,
	game_data_8 INTEGER NOT NULL DEFAULT 0,
	game_data_9 INTEGER NOT NULL DEFAULT 0,
	game_data_10 INTEGER NOT NULL DEFAULT 0,
	game_data_11 INTEGER NOT NULL DEFAULT 0,
	game_data_12 INTEGER NOT NULL DEFAULT 0,
	game_data_13 INTEGER NOT NULL DEFAULT 0,
	game_data_14 INTEGER NOT NULL DEFAULT 0,
	game_data_15 INTEGER NOT NULL DEFAULT 0,
	game_data_16 INTEGER NOT NULL DEFAULT 0,
	game_data_17 INTEGER NOT NULL DEFAULT 0,
	game_data_18 INTEGER NOT NULL DEFAULT 0,
	game_data_19 INTEGER NOT NULL DEFAULT 0,
	game_data_20 INTEGER NOT NULL DEFAULT 0,
	game_data_21 INTEGER NOT NULL DEFAULT 0,
	game_data_22 INTEGER NOT NULL DEFAULT 0,
	game_data_23 INTEGER NOT NULL DEFAULT 0,
	game_data_24 INTEGER NOT NULL DEFAULT 0,
	game_data_25 INTEGER NOT NULL DEFAULT 0,
	game_data_26 INTEGER NOT NULL DEFAULT 0,
	game_data_27 INTEGER NOT NULL DEFAULT 0,
	game_data_28 INTEGER NOT NULL DEFAULT 0,
	game_data_29 INTEGER NOT NULL DEFAULT 0,
	game_data_30 INTEGER NOT NULL DEFAULT 0,
	game_data_31 INTEGER NOT NULL DEFAULT 0,
	game_data_32 INTEGER NOT NULL DEFAULT 0,
	game_data_33 INTEGER NOT NULL DEFAULT 0,
	game_data_34 INTEGER NOT NULL DEFAULT 0,

	quest_item_0 INTEGER NOT NULL DEFAULT 0,
	quest_item_1 INTEGER NOT NULL DEFAULT 0,
	quest_item_2 INTEGER NOT NULL DEFAULT 0,
	quest_item_3 INTEGER NOT NULL DEFAULT 0,
	quest_item_4 INTEGER NOT NULL DEFAULT 0,
	quest_item_5 INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_gameobject ON cache_gameobjects (record_id, locale, content_hash, product);
CREATE INDEX IF NOT EXISTS idx_gameobjects_product ON cache_gameobjects (product);
CREATE INDEX IF NOT EXISTS idx_gameobjects_consensus_at ON cache_gameobjects (consensus_at);

-- ============================================================
-- cache_pagetext
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_pagetext (
	entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	attestation_count INTEGER NOT NULL DEFAULT 1,
	is_consensus INTEGER NOT NULL DEFAULT 0,
	consensus_at TEXT NULL DEFAULT NULL,

	next_page_text_id INTEGER NOT NULL DEFAULT 0,
	player_condition_id INTEGER NOT NULL DEFAULT 0,
	flags INTEGER NOT NULL DEFAULT 0,
	text TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_pagetext ON cache_pagetext (record_id, locale, content_hash, product);
CREATE INDEX IF NOT EXISTS idx_pagetext_product ON cache_pagetext (product);
CREATE INDEX IF NOT EXISTS idx_pagetext_consensus_at ON cache_pagetext (consensus_at);

-- ============================================================
-- quest junction tables
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_quest_objectives (
	quest_entry_id INTEGER NOT NULL,
	objective_index INTEGER NOT NULL,
	objective_id INTEGER NOT NULL DEFAULT 0,
	type INTEGER NOT NULL DEFAULT 0,
	storage_index INTEGER NOT NULL DEFAULT 0,
	object_id INTEGER NOT NULL DEFAULT 0,
	amount INTEGER NOT NULL DEFAULT 0,
	flags INTEGER NOT NULL DEFAULT 0,
	flags2 INTEGER NOT NULL DEFAULT 0,
	percent_amount REAL NOT NULL DEFAULT 0,
	description TEXT NOT NULL,
	visual_effect_0 INTEGER NOT NULL DEFAULT 0,
	visual_effect_1 INTEGER NOT NULL DEFAULT 0,
	visual_effect_2 INTEGER NOT NULL DEFAULT 0,
	visual_effect_3 INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (quest_entry_id, objective_index),
	FOREIGN KEY (quest_entry_id) REFERENCES cache_quests(entry_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cache_quest_conditional_texts (
	quest_entry_id INTEGER NOT NULL,
	text_type TEXT NOT NULL CHECK (text_type IN ('description', 'completion')),
	text_index INTEGER NOT NULL,
	player_condition_id INTEGER NOT NULL DEFAULT 0,
	quest_giver_creature_id INTEGER NOT NULL DEFAULT 0,
	text TEXT NOT NULL,
	PRIMARY KEY (quest_entry_id, text_type, text_index),
	FOREIGN KEY (quest_entry_id) REFERENCES cache_quests(entry_id) ON DELETE CASCADE
);

-- ============================================================
-- attestations
-- ============================================================
CREATE TABLE IF NOT EXISTS wdb_attestations (
	entity_type TEXT NOT NULL CHECK (entity_type IN ('creature', 'quest', 'gameobject', 'pagetext')),
	entry_id INTEGER NOT NULL,
	machine_id TEXT NOT NULL,
	submission_id TEXT NOT NULL,
	attested_at TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY (entity_type, entry_id, machine_id)
);

-- absent in mysql; required by the attestation prune
CREATE INDEX IF NOT EXISTS idx_attestations_attested_at ON wdb_attestations (attested_at);

-- ============================================================
-- hotfixes
-- ============================================================
CREATE TABLE IF NOT EXISTS db2_table_hashes (
	table_hash INTEGER NOT NULL PRIMARY KEY,
	table_name TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_db2_table_name ON db2_table_hashes (table_name);

CREATE TABLE IF NOT EXISTS hotfix_entries (
	table_hash INTEGER NOT NULL,
	record_id INTEGER NOT NULL,
	push_id INTEGER NOT NULL,
	unique_id INTEGER NOT NULL DEFAULT 0,
	region_id INTEGER NOT NULL DEFAULT 0,
	status INTEGER NOT NULL,
	game_build INTEGER NOT NULL,
	data_blob BLOB,
	product TEXT NOT NULL DEFAULT 'wow',
	first_seen TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY (table_hash, record_id, push_id, product)
);

CREATE INDEX IF NOT EXISTS idx_hotfix_game_build ON hotfix_entries (game_build);
CREATE INDEX IF NOT EXISTS idx_hotfix_product ON hotfix_entries (product);

-- ============================================================
-- binary hashes
-- ============================================================
CREATE TABLE IF NOT EXISTS cache_binary_hashes (
	build_key TEXT NOT NULL,
	file_name TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	file_size INTEGER NOT NULL,
	fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY (build_key, file_name, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_binary_hashes_file_name ON cache_binary_hashes (file_name);
CREATE INDEX IF NOT EXISTS idx_binary_hashes_build_key ON cache_binary_hashes (build_key);

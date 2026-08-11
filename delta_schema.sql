-- archavon delta schema; one file per processed submission (POST /api/v1/delta)
--
-- mirrors the main schema minus the columns the apply endpoint owns; entry_id is
-- delta-local and exists only to link quest junction rows.
--
-- GENERATED from db/schema.sql by tools/gen_delta_schema.php -- do not edit by hand.

CREATE TABLE IF NOT EXISTS delta_meta (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT NOT NULL
);

-- per-file outcome for the submission; drives cache_submission_files and the derived
-- overall submission status
CREATE TABLE IF NOT EXISTS delta_submission_files (
	file_name TEXT NOT NULL,
	locale TEXT NOT NULL,
	status TEXT NOT NULL CHECK (status IN ('completed', 'rejected')),
	failure_reason TEXT NULL,
	records_added INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (file_name, locale)
);

CREATE TABLE IF NOT EXISTS cache_creatures (
	entry_id INTEGER NOT NULL PRIMARY KEY,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,

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

CREATE UNIQUE INDEX IF NOT EXISTS uq_delta_cache_creatures ON cache_creatures (record_id, locale, content_hash, product);

CREATE TABLE IF NOT EXISTS cache_quests (
	entry_id INTEGER NOT NULL PRIMARY KEY,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,

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

CREATE UNIQUE INDEX IF NOT EXISTS uq_delta_cache_quests ON cache_quests (record_id, locale, content_hash, product);

CREATE TABLE IF NOT EXISTS cache_gameobjects (
	entry_id INTEGER NOT NULL PRIMARY KEY,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,

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

CREATE UNIQUE INDEX IF NOT EXISTS uq_delta_cache_gameobjects ON cache_gameobjects (record_id, locale, content_hash, product);

CREATE TABLE IF NOT EXISTS cache_pagetext (
	entry_id INTEGER NOT NULL PRIMARY KEY,
	record_id INTEGER NOT NULL,
	locale TEXT NOT NULL,
	content_hash TEXT NOT NULL,
	product TEXT NOT NULL DEFAULT 'wow',
	game_build INTEGER NOT NULL,

	next_page_text_id INTEGER NOT NULL DEFAULT 0,
	player_condition_id INTEGER NOT NULL DEFAULT 0,
	flags INTEGER NOT NULL DEFAULT 0,
	text TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_delta_cache_pagetext ON cache_pagetext (record_id, locale, content_hash, product);

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
	PRIMARY KEY (quest_entry_id, objective_index)
);

CREATE TABLE IF NOT EXISTS cache_quest_conditional_texts (
	quest_entry_id INTEGER NOT NULL,
	text_type TEXT NOT NULL CHECK (text_type IN ('description', 'completion')),
	text_index INTEGER NOT NULL,
	player_condition_id INTEGER NOT NULL DEFAULT 0,
	quest_giver_creature_id INTEGER NOT NULL DEFAULT 0,
	text TEXT NOT NULL,
	PRIMARY KEY (quest_entry_id, text_type, text_index)
);

CREATE TABLE IF NOT EXISTS db2_table_hashes (
	table_hash INTEGER NOT NULL PRIMARY KEY,
	table_name TEXT NOT NULL
);

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
	PRIMARY KEY (table_hash, record_id, push_id, product)
);

/*!
	delta database builder; the worker accumulates parsed records into an in-memory
	sqlite db matching ./delta_schema.sql and ships db.serialize() to POST /api/v1/delta.

	content_hash must stay byte-identical to the historic wdb_store.ts hashing
	(stable_json sorts keys, stringifies bigints) or dedup against migrated rows breaks.
	entry_id is delta-local; the apply endpoint re-keys junction rows via
	(record_id, locale, content_hash, product) and derives attestations/consensus itself.
 */

import { Database } from 'bun:sqlite';
import type { WdbRecord, CreatureRecord, QuestRecord, GameObjectRecord, PageTextRecord } from './wdb';
import type { DbCacheEntry } from './dbcache';

const DELTA_VERSION = '1';
const SCHEMA_PATH = new URL('delta_schema.sql', import.meta.url);

const MAX_CREATURE_DISPLAYS = 4;
const MAX_QUEST_ITEMS = 6;
const MAX_CURRENCY_IDS = 2;
const MAX_QUEST_FLAGS = 4;
const MAX_REWARD_DISPLAY_SPELLS = 4;
const MAX_TREASURE_PICKER_IDS = 6;
const MAX_VISUAL_EFFECTS = 4;
const MAX_GAMEOBJECT_QUEST_ITEMS = 6;
const GAME_DATA_SIZE = 35;

const CREATURE_COLUMNS = [
	'record_id', 'locale', 'content_hash', 'product', 'game_build',
	'title', 'title_alt', 'cursor_name', 'leader',
	'creature_type', 'creature_family', 'classification',
	'num_displays', 'total_probability', 'hp_multiplier', 'energy_multiplier',
	'movement_info_id', 'required_expansion', 'tracking_quest_id',
	'vignette_id', 'creature_class_mask', 'creature_difficulty_id',
	'widget_parent_set_id', 'widget_set_unit_condition_id',
	'name_0', 'name_1', 'name_2', 'name_3',
	'name_alt_0', 'name_alt_1', 'name_alt_2', 'name_alt_3',
	'flag_0', 'flag_1',
	'proxy_creature_id_0', 'proxy_creature_id_1',
	'display_id_0', 'display_scale_0', 'display_probability_0',
	'display_id_1', 'display_scale_1', 'display_probability_1',
	'display_id_2', 'display_scale_2', 'display_probability_2',
	'display_id_3', 'display_scale_3', 'display_probability_3',
	'quest_item_0', 'quest_item_1', 'quest_item_2', 'quest_item_3', 'quest_item_4', 'quest_item_5',
	'currency_id_0', 'currency_id_1'
];

const QUEST_COLUMNS = [
	'record_id', 'locale', 'content_hash', 'product', 'game_build',
	'quest_type', 'quest_package_id', 'content_tuning_id', 'quest_sort_id',
	'quest_info_id', 'suggested_group_num', 'reward_next_quest',
	'reward_xp_difficulty', 'reward_xp_multiplier',
	'reward_money', 'reward_money_difficulty', 'reward_money_multiplier',
	'reward_bonus_money', 'reward_display_spell_count',
	'reward_spell', 'reward_honor_addition', 'reward_honor_multiplier',
	'reward_artifact_xp_difficulty', 'reward_artifact_xp_multiplier',
	'reward_artifact_category_id', 'provided_item',
	'poi_continent', 'poi_x', 'poi_y', 'poi_priority',
	'reward_title', 'reward_arena_points',
	'reward_skill_line_id', 'reward_num_skill_ups',
	'portrait_giver_display_id', 'portrait_giver_mount_display_id',
	'portrait_turn_in_display_id', 'portrait_model_scene_id',
	'reward_faction_flags',
	'accepted_sound_kit_id', 'complete_sound_kit_id', 'area_group_id',
	'time_allowed', 'num_objectives', 'race_flags',
	'expansion_id', 'managed_world_state_id', 'quest_session_bonus',
	'quest_giver_creature_id',
	'ready_for_translation', 'reset_by_scheduler',
	'flag_0', 'flag_1', 'flag_2', 'flag_3',
	'reward_fixed_item_id_0', 'reward_fixed_item_qty_0',
	'reward_fixed_item_id_1', 'reward_fixed_item_qty_1',
	'reward_fixed_item_id_2', 'reward_fixed_item_qty_2',
	'reward_fixed_item_id_3', 'reward_fixed_item_qty_3',
	'item_drop_item_id_0', 'item_drop_item_qty_0',
	'item_drop_item_id_1', 'item_drop_item_qty_1',
	'item_drop_item_id_2', 'item_drop_item_qty_2',
	'item_drop_item_id_3', 'item_drop_item_qty_3',
	'reward_choice_item_id_0', 'reward_choice_item_qty_0', 'reward_choice_item_display_id_0',
	'reward_choice_item_id_1', 'reward_choice_item_qty_1', 'reward_choice_item_display_id_1',
	'reward_choice_item_id_2', 'reward_choice_item_qty_2', 'reward_choice_item_display_id_2',
	'reward_choice_item_id_3', 'reward_choice_item_qty_3', 'reward_choice_item_display_id_3',
	'reward_choice_item_id_4', 'reward_choice_item_qty_4', 'reward_choice_item_display_id_4',
	'reward_choice_item_id_5', 'reward_choice_item_qty_5', 'reward_choice_item_display_id_5',
	'faction_reward_id_0', 'faction_reward_value_0', 'faction_reward_override_0', 'faction_reward_max_rank_0',
	'faction_reward_id_1', 'faction_reward_value_1', 'faction_reward_override_1', 'faction_reward_max_rank_1',
	'faction_reward_id_2', 'faction_reward_value_2', 'faction_reward_override_2', 'faction_reward_max_rank_2',
	'faction_reward_id_3', 'faction_reward_value_3', 'faction_reward_override_3', 'faction_reward_max_rank_3',
	'faction_reward_id_4', 'faction_reward_value_4', 'faction_reward_override_4', 'faction_reward_max_rank_4',
	'currency_reward_id_0', 'currency_reward_qty_0',
	'currency_reward_id_1', 'currency_reward_qty_1',
	'currency_reward_id_2', 'currency_reward_qty_2',
	'currency_reward_id_3', 'currency_reward_qty_3',
	'reward_display_spell_id_0', 'reward_display_spell_condition_0', 'reward_display_spell_type_0',
	'reward_display_spell_id_1', 'reward_display_spell_condition_1', 'reward_display_spell_type_1',
	'reward_display_spell_id_2', 'reward_display_spell_condition_2', 'reward_display_spell_type_2',
	'reward_display_spell_id_3', 'reward_display_spell_condition_3', 'reward_display_spell_type_3',
	'treasure_picker_id_0', 'treasure_picker_id_1', 'treasure_picker_id_2',
	'treasure_picker_id_3', 'treasure_picker_id_4', 'treasure_picker_id_5',
	'treasure_picker_id_2_0', 'treasure_picker_id_2_1', 'treasure_picker_id_2_2',
	'treasure_picker_id_2_3', 'treasure_picker_id_2_4', 'treasure_picker_id_2_5',
	'log_title', 'log_description', 'quest_description', 'area_description',
	'portrait_giver_text', 'portrait_giver_name',
	'portrait_turn_in_text', 'portrait_turn_in_name',
	'quest_completion_log'
];

const GAMEOBJECT_COLUMNS = [
	'record_id', 'locale', 'content_hash', 'product', 'game_build',
	'type', 'display_id', 'icon', 'action', '"condition"', 'scale', 'content_tuning_id',
	'name_0', 'name_1', 'name_2', 'name_3',
	...Array.from({ length: GAME_DATA_SIZE }, (_, i) => `game_data_${i}`),
	'quest_item_0', 'quest_item_1', 'quest_item_2', 'quest_item_3', 'quest_item_4', 'quest_item_5'
];

const PAGETEXT_COLUMNS = [
	'record_id', 'locale', 'content_hash', 'product', 'game_build',
	'next_page_text_id', 'player_condition_id', 'flags', 'text'
];

const OBJECTIVE_COLUMNS = [
	'quest_entry_id', 'objective_index',
	'objective_id', 'type', 'storage_index', 'object_id', 'amount',
	'flags', 'flags2', 'percent_amount', 'description',
	'visual_effect_0', 'visual_effect_1', 'visual_effect_2', 'visual_effect_3'
];

const CONDITIONAL_TEXT_COLUMNS = [
	'quest_entry_id', 'text_type', 'text_index',
	'player_condition_id', 'quest_giver_creature_id', 'text'
];

const HOTFIX_COLUMNS = [
	'table_hash', 'record_id', 'push_id', 'unique_id', 'region_id',
	'status', 'game_build', 'data_blob', 'product'
];

// incoming row only wins when it carries a blob and the stored one does not; the
// apply endpoint re-applies the same rule against the main table
const HOTFIX_CONFLICT_SQL = `ON CONFLICT (table_hash, record_id, push_id, product) DO UPDATE SET
	unique_id = CASE WHEN excluded.data_blob IS NOT NULL AND hotfix_entries.data_blob IS NULL THEN excluded.unique_id ELSE hotfix_entries.unique_id END,
	region_id = CASE WHEN excluded.data_blob IS NOT NULL AND hotfix_entries.data_blob IS NULL THEN excluded.region_id ELSE hotfix_entries.region_id END,
	status = CASE WHEN excluded.data_blob IS NOT NULL AND hotfix_entries.data_blob IS NULL THEN excluded.status ELSE hotfix_entries.status END,
	game_build = CASE WHEN excluded.data_blob IS NOT NULL AND hotfix_entries.data_blob IS NULL THEN excluded.game_build ELSE hotfix_entries.game_build END,
	data_blob = CASE WHEN excluded.data_blob IS NOT NULL AND hotfix_entries.data_blob IS NULL THEN excluded.data_blob ELSE hotfix_entries.data_blob END`;

export type DeltaFileStatus = 'completed' | 'rejected';

export type EntityTable = 'cache_creatures' | 'cache_quests' | 'cache_gameobjects' | 'cache_pagetext';

type DeltaValue = string | number | bigint | boolean | Uint8Array | null;

function stable_json(obj: unknown): string {
	return JSON.stringify(obj, (_, value) => {
		if (typeof value === 'bigint')
			return value.toString();

		if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
			const sorted: Record<string, unknown> = {};
			for (const key of Object.keys(value).sort())
				sorted[key] = value[key];
			return sorted;
		}

		return value;
	});
}

function compute_hash(data: unknown): string {
	const hasher = new Bun.CryptoHasher('sha256');
	hasher.update(stable_json(data));
	return hasher.digest('hex');
}

function pad_array<T>(arr: T[], max: number, fill: T): T[] {
	const result = arr.slice(0, max);
	while (result.length < max)
		result.push(fill);
	return result;
}

function insert_sql(table: string, columns: string[], extra = ''): string {
	const placeholders = columns.map(() => '?').join(', ');
	return `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders}) ${extra}`.trimEnd();
}

const SCHEMA_SQL = await Bun.file(SCHEMA_PATH).text();

export class WdbDelta {
	readonly submission_id: string;

	private db: Database;
	private next_entry_id = new Map<EntityTable, number>();
	private entry_ids = new Map<string, number>();
	private statements = new Map<string, ReturnType<Database['prepare']>>();

	constructor(submission_id: string, machine_id: string) {
		this.submission_id = submission_id;

		this.db = new Database(':memory:');
		this.db.run('PRAGMA journal_mode = MEMORY');
		this.db.run(SCHEMA_SQL);

		const meta = this.db.prepare('INSERT INTO delta_meta (key, value) VALUES (?, ?)');
		meta.run('delta_version', DELTA_VERSION);
		meta.run('submission_id', submission_id);
		meta.run('machine_id', machine_id);
	}

	private stmt(key: string, sql: string) {
		let cached = this.statements.get(key);
		if (cached === undefined) {
			cached = this.db.prepare(sql);
			this.statements.set(key, cached);
		}

		return cached;
	}

	// inserts one entity row and returns its delta-local entry_id; byte-identical
	// records across files dedup on (record_id, locale, content_hash, product)
	private add_entity(table: EntityTable, columns: string[], record_id: number, hash: string, locale: string, product: string, params: DeltaValue[]): number {
		const key = `${table}\u0000${record_id}\u0000${locale}\u0000${hash}\u0000${product}`;
		const existing = this.entry_ids.get(key);

		if (existing !== undefined)
			return existing;

		const entry_id = (this.next_entry_id.get(table) ?? 0) + 1;
		this.next_entry_id.set(table, entry_id);
		this.entry_ids.set(key, entry_id);

		this.stmt(`entity:${table}`, insert_sql(table, ['entry_id', ...columns])).run(entry_id, ...params as any[]);

		return entry_id;
	}

	add_creatures(records: WdbRecord[], locale: string, product: string, game_build: number): number {
		const insert = this.db.transaction((batch: WdbRecord[]) => {
			for (const record of batch) {
				const d = record.data as CreatureRecord;
				const hash = compute_hash(d);

				const names = pad_array(d.names, 4, '');
				const name_alts = pad_array(d.name_alts, 4, '');
				const flags = pad_array(d.flags, 2, 0);
				const proxy = pad_array(d.proxy_creature_ids, 2, 0);
				const displays = pad_array(d.displays, MAX_CREATURE_DISPLAYS, { id: 0, scale: 0, probability: 0 });
				const quest_items = pad_array(d.quest_items, MAX_QUEST_ITEMS, 0);
				const currency_ids = pad_array(d.currency_ids, MAX_CURRENCY_IDS, 0);

				this.add_entity('cache_creatures', CREATURE_COLUMNS, record.id, hash, locale, product, [
					record.id, locale, hash, product, game_build,
					d.title, d.title_alt, d.cursor_name, d.leader,
					d.creature_type, d.creature_family, d.classification,
					d.num_displays, d.total_probability, d.hp_multiplier, d.energy_multiplier,
					d.movement_info_id, d.required_expansion, d.tracking_quest_id,
					d.vignette_id, d.creature_class_mask, d.creature_difficulty_id,
					d.widget_parent_set_id, d.widget_set_unit_condition_id,
					...names,
					...name_alts,
					...flags,
					...proxy,
					...displays.flatMap(disp => [disp.id, disp.scale, disp.probability]),
					...quest_items,
					...currency_ids
				]);
			}
		});

		insert(records);

		return records.length;
	}

	add_quests(records: WdbRecord[], locale: string, product: string, game_build: number): number {
		const insert = this.db.transaction((batch: WdbRecord[]) => {
			for (const record of batch) {
				const d = record.data as QuestRecord;
				const hash = compute_hash(d);

				const flags = pad_array(d.flags, MAX_QUEST_FLAGS, 0);
				const rfi = pad_array(d.reward_fixed_items, 4, { item_id: 0, quantity: 0 });
				const idi = pad_array(d.item_drop_items, 4, { item_id: 0, quantity: 0 });
				const rci = pad_array(d.reward_choice_items, 6, { item_id: 0, quantity: 0, display_id: 0 });
				const fr = pad_array(d.faction_rewards, 5, { faction_id: 0, value: 0, override: 0, gain_max_rank: 0 });
				const cr = pad_array(d.currency_rewards, 4, { currency_id: 0, quantity: 0 });
				const rds = pad_array(d.reward_display_spells, MAX_REWARD_DISPLAY_SPELLS, { spell_id: 0, player_condition_id: 0, spell_type: 0 });
				const tpi = pad_array(d.treasure_picker_ids, MAX_TREASURE_PICKER_IDS, 0);
				const tpi2 = pad_array(d.treasure_picker_ids_2, MAX_TREASURE_PICKER_IDS, 0);

				// time_allowed / race_flags are u64; sqlite wraps them to signed int64 with
				// the bit pattern intact. compute_hash sees the original value via stable_json
				const entry_id = this.add_entity('cache_quests', QUEST_COLUMNS, record.id, hash, locale, product, [
					record.id, locale, hash, product, game_build,
					d.quest_type, d.quest_package_id, d.content_tuning_id, d.quest_sort_id,
					d.quest_info_id, d.suggested_group_num, d.reward_next_quest,
					d.reward_xp_difficulty, d.reward_xp_multiplier,
					d.reward_money, d.reward_money_difficulty, d.reward_money_multiplier,
					d.reward_bonus_money, d.reward_display_spell_count,
					d.reward_spell, d.reward_honor_addition, d.reward_honor_multiplier,
					d.reward_artifact_xp_difficulty, d.reward_artifact_xp_multiplier,
					d.reward_artifact_category_id, d.provided_item,
					d.poi_continent, d.poi_x, d.poi_y, d.poi_priority,
					d.reward_title, d.reward_arena_points,
					d.reward_skill_line_id, d.reward_num_skill_ups,
					d.portrait_giver_display_id, d.portrait_giver_mount_display_id,
					d.portrait_turn_in_display_id, d.portrait_model_scene_id,
					d.reward_faction_flags,
					d.accepted_sound_kit_id, d.complete_sound_kit_id, d.area_group_id,
					d.time_allowed, d.num_objectives, d.race_flags,
					d.expansion_id, d.managed_world_state_id, d.quest_session_bonus,
					d.quest_giver_creature_id,
					d.ready_for_translation, d.reset_by_scheduler,
					...flags,
					...rfi.flatMap(x => [x.item_id, x.quantity]),
					...idi.flatMap(x => [x.item_id, x.quantity]),
					...rci.flatMap(x => [x.item_id, x.quantity, x.display_id]),
					...fr.flatMap(x => [x.faction_id, x.value, x.override, x.gain_max_rank]),
					...cr.flatMap(x => [x.currency_id, x.quantity]),
					...rds.flatMap(x => [x.spell_id, x.player_condition_id, x.spell_type]),
					...tpi,
					...tpi2,
					d.log_title, d.log_description, d.quest_description, d.area_description,
					d.portrait_giver_text, d.portrait_giver_name,
					d.portrait_turn_in_text, d.portrait_turn_in_name,
					d.quest_completion_log
				]);

				const objective_stmt = this.stmt('objectives', insert_sql('cache_quest_objectives', OBJECTIVE_COLUMNS, 'ON CONFLICT DO NOTHING'));
				for (let i = 0; i < d.objectives.length; i++) {
					const obj = d.objectives[i]!;
					const ve = pad_array(obj.visual_effects, MAX_VISUAL_EFFECTS, 0);
					objective_stmt.run(entry_id, i, obj.id, obj.type, obj.storage_index, obj.object_id, obj.amount, obj.flags, obj.flags2, obj.percent_amount, obj.description, ...ve);
				}

				const text_stmt = this.stmt('conditional_texts', insert_sql('cache_quest_conditional_texts', CONDITIONAL_TEXT_COLUMNS, 'ON CONFLICT DO NOTHING'));
				for (let i = 0; i < d.conditional_quest_descriptions.length; i++) {
					const ct = d.conditional_quest_descriptions[i]!;
					text_stmt.run(entry_id, 'description', i, ct.player_condition_id, ct.quest_giver_creature_id, ct.text);
				}

				for (let i = 0; i < d.conditional_quest_completions.length; i++) {
					const ct = d.conditional_quest_completions[i]!;
					text_stmt.run(entry_id, 'completion', i, ct.player_condition_id, ct.quest_giver_creature_id, ct.text);
				}
			}
		});

		insert(records);

		return records.length;
	}

	add_gameobjects(records: WdbRecord[], locale: string, product: string, game_build: number): number {
		const insert = this.db.transaction((batch: WdbRecord[]) => {
			for (const record of batch) {
				const d = record.data as GameObjectRecord;
				const hash = compute_hash(d);

				const names = pad_array(d.names, 4, '');
				const game_data = pad_array(d.game_data, GAME_DATA_SIZE, 0);
				const quest_items = pad_array(d.quest_items, MAX_GAMEOBJECT_QUEST_ITEMS, 0);

				this.add_entity('cache_gameobjects', GAMEOBJECT_COLUMNS, record.id, hash, locale, product, [
					record.id, locale, hash, product, game_build,
					d.type, d.display_id, d.icon, d.action, d.condition, d.scale, d.content_tuning_id,
					...names,
					...game_data,
					...quest_items
				]);
			}
		});

		insert(records);

		return records.length;
	}

	add_pagetext(records: WdbRecord[], locale: string, product: string, game_build: number): number {
		const insert = this.db.transaction((batch: WdbRecord[]) => {
			for (const record of batch) {
				const d = record.data as PageTextRecord;
				const hash = compute_hash(d);

				this.add_entity('cache_pagetext', PAGETEXT_COLUMNS, record.id, hash, locale, product, [
					record.id, locale, hash, product, game_build,
					d.next_page_text_id, d.player_condition_id, d.flags, d.text
				]);
			}
		});

		insert(records);

		return records.length;
	}

	add_hotfixes(entries: DbCacheEntry[], product: string, game_build: number): number {
		const stmt = this.stmt('hotfixes', insert_sql('hotfix_entries', HOTFIX_COLUMNS, HOTFIX_CONFLICT_SQL));

		const insert = this.db.transaction((batch: DbCacheEntry[]) => {
			for (const entry of batch) {
				stmt.run(
					entry.table_hash >>> 0,
					entry.record_id >>> 0,
					entry.push_id >>> 0,
					entry.unique_id >>> 0,
					entry.region_id >>> 0,
					entry.status,
					game_build,
					entry.record_data ? new Uint8Array(entry.record_data) : null,
					product
				);
			}
		});

		insert(entries);

		return entries.length;
	}

	// per-file outcome; the apply endpoint derives the submission roll-up from these
	set_file_result(file_name: string, locale: string, status: DeltaFileStatus, failure_reason: string | null, records_added: number) {
		this.stmt(
			'file_result',
			insert_sql('delta_submission_files', ['file_name', 'locale', 'status', 'failure_reason', 'records_added'], 'ON CONFLICT (file_name, locale) DO UPDATE SET status = excluded.status, failure_reason = excluded.failure_reason, records_added = excluded.records_added')
		).run(file_name, locale, status, failure_reason, records_added);
	}

	serialize(): Uint8Array {
		return this.db.serialize();
	}

	close() {
		this.db.close();
	}
}

import type { SQL } from 'bun';
import type { WdbRecord, CreatureRecord, QuestRecord, GameObjectRecord, PageTextRecord } from './wdb';

const BATCH_SIZE = 100;

type EntityType = 'creature' | 'quest' | 'gameobject' | 'pagetext';

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

export async function upsert_machine(db: SQL, machine_id: string): Promise<void> {
	await db.unsafe(
		'INSERT INTO machines (machine_id) VALUES (?) ON DUPLICATE KEY UPDATE last_seen = CURRENT_TIMESTAMP',
		[machine_id]
	);
}

interface HashedRecord {
	record_id: number;
	hash: string;
	params: unknown[];
}

async function store_batch(
	db: SQL,
	entity_type: EntityType,
	table_name: string,
	columns: string[],
	records: HashedRecord[],
	locale: string,
	machine_id: string,
	submission_id: string
): Promise<void> {
	if (records.length === 0)
		return;

	const placeholder = `(${columns.map(() => '?').join(', ')})`;
	const placeholders = records.map(() => placeholder).join(', ');
	const params = records.flatMap(r => r.params);

	await db.unsafe(
		`INSERT IGNORE INTO ${table_name} (${columns.join(', ')}) VALUES ${placeholders}`,
		params
	);

	const tuple_placeholders = records.map(() => '(?, ?, ?)').join(', ');
	const tuple_params = records.flatMap(r => [r.record_id, locale, r.hash]);

	const entries = await db.unsafe(
		`SELECT entry_id FROM ${table_name} WHERE (record_id, locale, content_hash) IN (${tuple_placeholders})`,
		tuple_params
	);

	if (entries.length === 0)
		return;

	const entry_ids = entries.map((e: any) => e.entry_id);

	const att_placeholders = entry_ids.map(() => '(?, ?, ?, ?)').join(', ');
	const att_params = entry_ids.flatMap((id: any) => [entity_type, id, machine_id, submission_id]);

	await db.unsafe(
		`INSERT IGNORE INTO wdb_attestations (entity_type, entry_id, machine_id, submission_id) VALUES ${att_placeholders}`,
		att_params
	);

	const id_placeholders = entry_ids.map(() => '?').join(', ');
	await db.unsafe(
		`UPDATE ${table_name} SET attestation_count = (SELECT COUNT(*) FROM wdb_attestations a WHERE a.entity_type = ? AND a.entry_id = ${table_name}.entry_id) WHERE entry_id IN (${id_placeholders})`,
		[entity_type, ...entry_ids]
	);
}

export async function store_creatures(db: SQL, records: WdbRecord[], locale: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'game_build',
		'title', 'title_alt', 'cursor_name', 'leader',
		'creature_type', 'creature_family', 'classification',
		'num_displays', 'total_probability', 'hp_multiplier', 'energy_multiplier',
		'movement_info_id', 'required_expansion', 'tracking_quest_id',
		'vignette_id', 'creature_class_mask', 'creature_difficulty_id',
		'widget_parent_set_id', 'widget_set_unit_condition_id',
		'names', 'name_alts', 'flags', 'proxy_creature_ids',
		'displays', 'quest_items', 'currency_ids'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as CreatureRecord;
			const hash = compute_hash(d);
			return {
				record_id: record.id,
				hash,
				params: [
					record.id, locale, hash, game_build,
					d.title, d.title_alt, d.cursor_name, d.leader,
					d.creature_type, d.creature_family, d.classification,
					d.num_displays, d.total_probability, d.hp_multiplier, d.energy_multiplier,
					d.movement_info_id, d.required_expansion, d.tracking_quest_id,
					d.vignette_id, d.creature_class_mask, d.creature_difficulty_id,
					d.widget_parent_set_id, d.widget_set_unit_condition_id,
					JSON.stringify(d.names), JSON.stringify(d.name_alts),
					JSON.stringify(d.flags), JSON.stringify(d.proxy_creature_ids),
					JSON.stringify(d.displays), JSON.stringify(d.quest_items),
					JSON.stringify(d.currency_ids)
				]
			};
		});

		await store_batch(db, 'creature', 'cache_creatures', columns, hashed, locale, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

export async function store_quests(db: SQL, records: WdbRecord[], locale: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'game_build',
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
		'flags', 'reward_fixed_items', 'item_drop_items', 'reward_choice_items',
		'faction_rewards', 'currency_rewards', 'reward_display_spells',
		'treasure_picker_ids', 'treasure_picker_ids_2',
		'objectives', 'conditional_quest_descriptions', 'conditional_quest_completions',
		'log_title', 'log_description', 'quest_description', 'area_description',
		'portrait_giver_text', 'portrait_giver_name',
		'portrait_turn_in_text', 'portrait_turn_in_name',
		'quest_completion_log'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as QuestRecord;
			const hash = compute_hash(d);
			return {
				record_id: record.id,
				hash,
				params: [
					record.id, locale, hash, game_build,
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
					d.time_allowed.toString(), d.num_objectives, d.race_flags.toString(),
					d.expansion_id, d.managed_world_state_id, d.quest_session_bonus,
					d.quest_giver_creature_id,
					d.ready_for_translation, d.reset_by_scheduler,
					JSON.stringify(d.flags), JSON.stringify(d.reward_fixed_items),
					JSON.stringify(d.item_drop_items), JSON.stringify(d.reward_choice_items),
					JSON.stringify(d.faction_rewards), JSON.stringify(d.currency_rewards),
					JSON.stringify(d.reward_display_spells),
					JSON.stringify(d.treasure_picker_ids), JSON.stringify(d.treasure_picker_ids_2),
					JSON.stringify(d.objectives),
					JSON.stringify(d.conditional_quest_descriptions),
					JSON.stringify(d.conditional_quest_completions),
					d.log_title, d.log_description, d.quest_description, d.area_description,
					d.portrait_giver_text, d.portrait_giver_name,
					d.portrait_turn_in_text, d.portrait_turn_in_name,
					d.quest_completion_log
				]
			};
		});

		await store_batch(db, 'quest', 'cache_quests', columns, hashed, locale, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

export async function store_gameobjects(db: SQL, records: WdbRecord[], locale: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'game_build',
		'type', 'display_id', 'icon', 'action', '`condition`', 'scale', 'content_tuning_id',
		'names', 'game_data', 'quest_items'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as GameObjectRecord;
			const hash = compute_hash(d);
			return {
				record_id: record.id,
				hash,
				params: [
					record.id, locale, hash, game_build,
					d.type, d.display_id, d.icon, d.action, d.condition, d.scale, d.content_tuning_id,
					JSON.stringify(d.names), JSON.stringify(d.game_data), JSON.stringify(d.quest_items)
				]
			};
		});

		await store_batch(db, 'gameobject', 'cache_gameobjects', columns, hashed, locale, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

export async function store_pagetext(db: SQL, records: WdbRecord[], locale: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'game_build',
		'next_page_text_id', 'player_condition_id', 'flags', 'text'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as PageTextRecord;
			const hash = compute_hash(d);
			return {
				record_id: record.id,
				hash,
				params: [
					record.id, locale, hash, game_build,
					d.next_page_text_id, d.player_condition_id, d.flags, d.text
				]
			};
		});

		await store_batch(db, 'pagetext', 'cache_pagetext', columns, hashed, locale, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

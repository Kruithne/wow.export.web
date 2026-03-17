import type { SQL } from 'bun';
import type { WdbRecord, CreatureRecord, QuestRecord, GameObjectRecord, PageTextRecord } from './wdb';

const BATCH_SIZE = 100;
const CONSENSUS_THRESHOLD = 3;

const MAX_CREATURE_DISPLAYS = 4;
const MAX_QUEST_ITEMS = 6;
const MAX_CURRENCY_IDS = 2;
const MAX_QUEST_FLAGS = 4;
const MAX_REWARD_DISPLAY_SPELLS = 4;
const MAX_TREASURE_PICKER_IDS = 6;
const MAX_VISUAL_EFFECTS = 4;
const MAX_GAMEOBJECT_QUEST_ITEMS = 6;
const GAME_DATA_SIZE = 35;

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

function pad_array<T>(arr: T[], max: number, fill: T): T[] {
	const result = arr.slice(0, max);
	while (result.length < max)
		result.push(fill);
	return result;
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
	product: string,
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

	const tuple_placeholders = records.map(() => '(?, ?, ?, ?)').join(', ');
	const tuple_params = records.flatMap(r => [r.record_id, locale, r.hash, product]);

	const entries = await db.unsafe(
		`SELECT entry_id FROM ${table_name} WHERE (record_id, locale, content_hash, product) IN (${tuple_placeholders})`,
		tuple_params
	);

	if (entries.length === 0)
		return;

	const entry_ids = entries.map((e: any) => e.entry_id);

	const att_placeholders = entry_ids.map(() => '(?, ?, ?, ?)').join(', ');
	const att_params = entry_ids.flatMap((id: any) => [entity_type, id, machine_id, submission_id]);

	await db.unsafe(
		`INSERT INTO wdb_attestations (entity_type, entry_id, machine_id, submission_id)
		 VALUES ${att_placeholders}
		 ON DUPLICATE KEY UPDATE attested_at = CURRENT_TIMESTAMP, submission_id = VALUES(submission_id)`,
		att_params
	);

	const id_placeholders = entry_ids.map(() => '?').join(', ');
	await db.unsafe(
		`UPDATE ${table_name} SET attestation_count = (SELECT COUNT(*) FROM wdb_attestations a WHERE a.entity_type = ? AND a.entry_id = ${table_name}.entry_id AND a.attested_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)) WHERE entry_id IN (${id_placeholders})`,
		[entity_type, ...entry_ids]
	);

	// recount sibling entries (same record_id/locale/product) to reflect expired attestations
	await db.unsafe(
		`UPDATE ${table_name} t SET attestation_count = (
			SELECT COUNT(*) FROM wdb_attestations a
			WHERE a.entity_type = ? AND a.entry_id = t.entry_id
			AND a.attested_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
		) WHERE (record_id, locale, product) IN (
			SELECT record_id, locale, product FROM ${table_name} WHERE entry_id IN (${id_placeholders})
		) AND entry_id NOT IN (${id_placeholders})`,
		[entity_type, ...entry_ids, ...entry_ids]
	);

	// demote entries that dropped below consensus threshold
	await db.unsafe(
		`UPDATE ${table_name} SET is_consensus = 0
		 WHERE is_consensus = 1 AND attestation_count < ?
		 AND (record_id, locale, product) IN (
			 SELECT record_id, locale, product FROM (
				 SELECT record_id, locale, product FROM ${table_name} WHERE entry_id IN (${id_placeholders})
			 ) sub
		 )`,
		[CONSENSUS_THRESHOLD, ...entry_ids]
	);

	// promote entries that just crossed the consensus threshold
	const candidates = await db.unsafe(
		`SELECT entry_id, record_id, locale, product, game_build FROM ${table_name}
		 WHERE entry_id IN (${id_placeholders}) AND attestation_count >= ? AND is_consensus = 0`,
		[...entry_ids, CONSENSUS_THRESHOLD]
	);

	for (const c of candidates) {
		await db.unsafe(
			`UPDATE ${table_name} SET is_consensus = 0
			 WHERE record_id = ? AND locale = ? AND product = ? AND is_consensus = 1 AND game_build <= ?`,
			[c.record_id, c.locale, c.product, c.game_build]
		);

		await db.unsafe(
			`UPDATE ${table_name} SET is_consensus = 1
			 WHERE entry_id = ? AND NOT EXISTS (
				 SELECT 1 FROM (SELECT 1 FROM ${table_name}
				 WHERE record_id = ? AND locale = ? AND product = ? AND is_consensus = 1) t
			 )`,
			[c.entry_id, c.record_id, c.locale, c.product]
		);
	}
}

export async function store_creatures(db: SQL, records: WdbRecord[], locale: string, product: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
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

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as CreatureRecord;
			const hash = compute_hash(d);

			const names = pad_array(d.names, 4, '');
			const name_alts = pad_array(d.name_alts, 4, '');
			const flags = pad_array(d.flags, 2, 0);
			const proxy = pad_array(d.proxy_creature_ids, 2, 0);
			const displays = pad_array(d.displays, MAX_CREATURE_DISPLAYS, { id: 0, scale: 0, probability: 0 });
			const quest_items = pad_array(d.quest_items, MAX_QUEST_ITEMS, 0);
			const currency_ids = pad_array(d.currency_ids, MAX_CURRENCY_IDS, 0);

			return {
				record_id: record.id,
				hash,
				params: [
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
				]
			};
		});

		await store_batch(db, 'creature', 'cache_creatures', columns, hashed, locale, product, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

export async function store_quests(db: SQL, records: WdbRecord[], locale: string, product: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
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

	const obj_columns = [
		'quest_entry_id', 'objective_index',
		'objective_id', 'type', 'storage_index', 'object_id', 'amount',
		'flags', 'flags2', 'percent_amount', 'description',
		'visual_effect_0', 'visual_effect_1', 'visual_effect_2', 'visual_effect_3'
	];

	const cond_columns = [
		'quest_entry_id', 'text_type', 'text_index',
		'player_condition_id', 'quest_giver_creature_id', 'text'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
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

			return {
				record_id: record.id,
				hash,
				params: [
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
					d.time_allowed.toString(), d.num_objectives, d.race_flags.toString(),
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
				]
			};
		});

		await store_batch(db, 'quest', 'cache_quests', columns, hashed, locale, product, machine_id, submission_id);

		// insert junction table rows for objectives and conditional texts
		const tuple_placeholders = batch.map(() => '(?, ?, ?, ?)').join(', ');
		const tuple_params = hashed.flatMap(r => [r.record_id, locale, r.hash, product]);

		const entries = await db.unsafe(
			`SELECT entry_id, record_id, content_hash FROM cache_quests WHERE (record_id, locale, content_hash, product) IN (${tuple_placeholders})`,
			tuple_params
		);

		const entry_map = new Map<string, number>();
		for (const e of entries)
			entry_map.set(`${e.record_id}:${e.content_hash}`, Number(e.entry_id));

		for (let j = 0; j < batch.length; j++) {
			const d = batch[j]!.data as QuestRecord;
			const h = hashed[j]!;
			const entry_id = entry_map.get(`${h.record_id}:${h.hash}`);
			if (entry_id === undefined)
				continue;

			// objectives
			if (d.objectives.length > 0) {
				const obj_placeholder = `(${obj_columns.map(() => '?').join(', ')})`;
				const obj_placeholders = d.objectives.map(() => obj_placeholder).join(', ');
				const obj_params = d.objectives.flatMap((obj, idx) => {
					const ve = pad_array(obj.visual_effects, MAX_VISUAL_EFFECTS, 0);
					return [entry_id, idx, obj.id, obj.type, obj.storage_index, obj.object_id, obj.amount, obj.flags, obj.flags2, obj.percent_amount, obj.description, ...ve];
				});

				await db.unsafe(
					`INSERT IGNORE INTO cache_quest_objectives (${obj_columns.join(', ')}) VALUES ${obj_placeholders}`,
					obj_params
				);
			}

			// conditional texts
			const cond_texts: unknown[][] = [];
			for (let ci = 0; ci < d.conditional_quest_descriptions.length; ci++) {
				const ct = d.conditional_quest_descriptions[ci]!;
				cond_texts.push([entry_id, 'description', ci, ct.player_condition_id, ct.quest_giver_creature_id, ct.text]);
			}
			for (let ci = 0; ci < d.conditional_quest_completions.length; ci++) {
				const ct = d.conditional_quest_completions[ci]!;
				cond_texts.push([entry_id, 'completion', ci, ct.player_condition_id, ct.quest_giver_creature_id, ct.text]);
			}

			if (cond_texts.length > 0) {
				const cond_placeholder = `(${cond_columns.map(() => '?').join(', ')})`;
				const cond_placeholders = cond_texts.map(() => cond_placeholder).join(', ');
				const cond_params = cond_texts.flat();

				await db.unsafe(
					`INSERT IGNORE INTO cache_quest_conditional_texts (${cond_columns.join(', ')}) VALUES ${cond_placeholders}`,
					cond_params
				);
			}
		}

		stored += batch.length;
	}

	return stored;
}

export async function store_gameobjects(db: SQL, records: WdbRecord[], locale: string, product: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'product', 'game_build',
		'type', 'display_id', 'icon', 'action', '`condition`', 'scale', 'content_tuning_id',
		'name_0', 'name_1', 'name_2', 'name_3',
		...Array.from({ length: GAME_DATA_SIZE }, (_, i) => `game_data_${i}`),
		'quest_item_0', 'quest_item_1', 'quest_item_2', 'quest_item_3', 'quest_item_4', 'quest_item_5'
	];

	let stored = 0;
	for (let i = 0; i < records.length; i += BATCH_SIZE) {
		const batch = records.slice(i, i + BATCH_SIZE);
		const hashed: HashedRecord[] = batch.map(record => {
			const d = record.data as GameObjectRecord;
			const hash = compute_hash(d);

			const names = pad_array(d.names, 4, '');
			const game_data = pad_array(d.game_data, GAME_DATA_SIZE, 0);
			const quest_items = pad_array(d.quest_items, MAX_GAMEOBJECT_QUEST_ITEMS, 0);

			return {
				record_id: record.id,
				hash,
				params: [
					record.id, locale, hash, product, game_build,
					d.type, d.display_id, d.icon, d.action, d.condition, d.scale, d.content_tuning_id,
					...names,
					...game_data,
					...quest_items
				]
			};
		});

		await store_batch(db, 'gameobject', 'cache_gameobjects', columns, hashed, locale, product, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

export async function store_pagetext(db: SQL, records: WdbRecord[], locale: string, product: string, game_build: number, machine_id: string, submission_id: string): Promise<number> {
	const columns = [
		'record_id', 'locale', 'content_hash', 'product', 'game_build',
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
					record.id, locale, hash, product, game_build,
					d.next_page_text_id, d.player_condition_id, d.flags, d.text
				]
			};
		});

		await store_batch(db, 'pagetext', 'cache_pagetext', columns, hashed, locale, product, machine_id, submission_id);
		stored += batch.length;
	}

	return stored;
}

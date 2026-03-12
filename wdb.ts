import BufferReader from './buffer';

const WDB_HEADER_SIZE = 24;

interface GameVersion {
	expansion: number;
	major: number;
	minor: number;
	build: number;
}

function parse_game_version(patch: string, build: number): GameVersion {
	const parts = patch.split('.').map(Number);
	return {
		expansion: parts[0] ?? 0,
		major: parts[1] ?? 0,
		minor: parts[2] ?? 0,
		build
	};
}

function ver_gte(ver: GameVersion, exp: number, maj: number, min: number): boolean {
	if (ver.expansion !== exp)
		return ver.expansion > exp;
	if (ver.major !== maj)
		return ver.major > maj;
	return ver.minor >= min;
}

interface WdbHeader {
	signature: string;
	build: number;
	locale: string;
	record_size: number;
	record_version: number;
	cache_version: number;
}

interface CreatureDisplay {
	id: number;
	scale: number;
	probability: number;
}

interface CreatureRecord {
	names: string[];
	name_alts: string[];
	title: string;
	title_alt: string;
	cursor_name: string;
	leader: boolean;
	flags: number[];
	creature_type: number;
	creature_family: number;
	classification: number;
	proxy_creature_ids: number[];
	num_displays: number;
	total_probability: number;
	displays: CreatureDisplay[];
	hp_multiplier: number;
	energy_multiplier: number;
	quest_items: number[];
	currency_ids: number[];
	movement_info_id: number;
	required_expansion: number;
	tracking_quest_id: number;
	vignette_id: number;
	creature_class_mask: number;
	creature_difficulty_id: number;
	widget_parent_set_id: number;
	widget_set_unit_condition_id: number;
}

interface QuestObjective {
	id: number;
	type: number;
	storage_index: number;
	object_id: number;
	amount: number;
	flags: number;
	flags2: number;
	percent_amount: number;
	visual_effects: number[];
	description: string;
}

interface QuestRewardFixedItem {
	item_id: number;
	quantity: number;
}

interface QuestRewardChoiceItem {
	item_id: number;
	quantity: number;
	display_id: number;
}

interface QuestFactionReward {
	faction_id: number;
	value: number;
	override: number;
	gain_max_rank: number;
}

interface QuestCurrencyReward {
	currency_id: number;
	quantity: number;
}

interface QuestRewardDisplaySpell {
	spell_id: number;
	player_condition_id: number;
	spell_type: number;
}

interface ConditionalQuestText {
	player_condition_id: number;
	quest_giver_creature_id: number;
	text: string;
}

interface QuestRecord {
	quest_id: number;
	quest_type: number;
	quest_package_id: number;
	content_tuning_id: number;
	quest_sort_id: number;
	quest_info_id: number;
	suggested_group_num: number;
	reward_next_quest: number;
	reward_xp_difficulty: number;
	reward_xp_multiplier: number;
	reward_money: number;
	reward_money_difficulty: number;
	reward_money_multiplier: number;
	reward_bonus_money: number;
	reward_display_spell_count: number;
	reward_spell: number;
	reward_honor_addition: number;
	reward_honor_multiplier: number;
	reward_artifact_xp_difficulty: number;
	reward_artifact_xp_multiplier: number;
	reward_artifact_category_id: number;
	provided_item: number;
	flags: number[];
	reward_fixed_items: QuestRewardFixedItem[];
	item_drop_items: QuestRewardFixedItem[];
	reward_choice_items: QuestRewardChoiceItem[];
	poi_continent: number;
	poi_x: number;
	poi_y: number;
	poi_priority: number;
	reward_title: number;
	reward_arena_points: number;
	reward_skill_line_id: number;
	reward_num_skill_ups: number;
	portrait_giver_display_id: number;
	portrait_giver_mount_display_id: number;
	portrait_turn_in_display_id: number;
	portrait_model_scene_id: number;
	faction_rewards: QuestFactionReward[];
	reward_faction_flags: number;
	currency_rewards: QuestCurrencyReward[];
	accepted_sound_kit_id: number;
	complete_sound_kit_id: number;
	area_group_id: number;
	time_allowed: bigint;
	num_objectives: number;
	race_flags: bigint;
	expansion_id: number;
	managed_world_state_id: number;
	quest_session_bonus: number;
	quest_giver_creature_id: number;
	reward_display_spells: QuestRewardDisplaySpell[];
	treasure_picker_ids: number[];
	treasure_picker_ids_2: number[];
	objectives: QuestObjective[];
	log_title: string;
	log_description: string;
	quest_description: string;
	area_description: string;
	portrait_giver_text: string;
	portrait_giver_name: string;
	portrait_turn_in_text: string;
	portrait_turn_in_name: string;
	quest_completion_log: string;
	ready_for_translation: boolean;
	reset_by_scheduler: boolean;
	conditional_quest_descriptions: ConditionalQuestText[];
	conditional_quest_completions: ConditionalQuestText[];
}

interface GameObjectRecord {
	type: number;
	display_id: number;
	names: string[];
	icon: string;
	action: string;
	condition: string;
	game_data: number[];
	scale: number;
	quest_items: number[];
	content_tuning_id: number;
}

interface PageTextRecord {
	page_text_id: number;
	next_page_text_id: number;
	player_condition_id: number;
	flags: number;
	text: string;
}

type RecordData = CreatureRecord | QuestRecord | GameObjectRecord | PageTextRecord | Record<string, unknown>;

interface WdbRecord {
	id: number;
	data: RecordData;
}

interface WdbResult {
	header: WdbHeader;
	records: WdbRecord[];
}

// MSB-first bit reader matching DataStore.cs behavior
class BitReader {
	private buf: BufferReader;
	private bit_pos: number = 8;
	private curr: number = 0;

	constructor(buf: BufferReader) {
		this.buf = buf;
	}

	read_bit(): number {
		if (this.bit_pos === 8) {
			this.curr = this.buf.readUInt8();
			this.bit_pos = 0;
		}

		const result = (this.curr >> 7);
		this.curr = (this.curr << 1) & 0xff;
		this.bit_pos++;
		return result;
	}

	read_bits(count: number): number {
		let result = 0;
		for (let i = count - 1; i >= 0; i--)
			result |= (this.read_bit() << i);
		return result;
	}

	read_bool(): boolean {
		return this.read_bit() !== 0;
	}

	read_string(length: number): string {
		this.flush();
		if (length > 0)
			return this.buf.readStringFixed(length);
		return '';
	}

	flush(): void {
		this.bit_pos = 8;
	}
}

type BodyParser = (buf: BufferReader, length: number, ver: GameVersion) => RecordData;

function reverse_chars(str: string): string {
	return str.split('').reverse().join('');
}

function read_header(buf: BufferReader): WdbHeader {
	const sig_bytes = buf.readBytes(4);
	const signature = reverse_chars(String.fromCharCode(...sig_bytes));

	const build = buf.readUInt32LE();

	const locale_bytes = buf.readBytes(4);
	const locale = reverse_chars(String.fromCharCode(...locale_bytes));

	const record_size = buf.readUInt32LE();
	const record_version = buf.readUInt32LE();
	const cache_version = buf.readUInt32LE();

	return { signature, build, locale, record_size, record_version, cache_version };
}

function parse_creature(buf: BufferReader, length: number, ver: GameVersion): CreatureRecord {
	const ds = new BitReader(buf);

	const title_len = ds.read_bits(11);
	const title_alt_len = ds.read_bits(11);
	const cursor_name_len = ds.read_bits(6);
	const leader = ds.read_bool();

	const name_lens: number[] = [];
	const name_alt_lens: number[] = [];
	for (let i = 0; i < 4; i++) {
		name_lens.push(ds.read_bits(11));
		name_alt_lens.push(ds.read_bits(11));
	}

	const names: string[] = [];
	const name_alts: string[] = [];
	for (let i = 0; i < 4; i++) {
		names.push(ds.read_string(name_lens[i]!).replace(/\0+$/, ''));
		name_alts.push(ds.read_string(name_alt_lens[i]!).replace(/\0+$/, ''));
	}

	const flags_0 = buf.readUInt32LE();
	const flags_1 = buf.readUInt32LE();
	const flags = [flags_0, flags_1];

	if (ver_gte(ver, 11, 2, 0))
		buf.readUInt32LE(); // TWW_112_Int

	const creature_type = ver_gte(ver, 11, 2, 0) ? buf.readUInt8() : buf.readUInt32LE();
	const creature_family = buf.readUInt32LE();
	const classification = ver_gte(ver, 11, 2, 0) ? buf.readUInt8() : buf.readUInt32LE();

	const proxy_creature_ids = [buf.readUInt32LE(), buf.readUInt32LE()];

	const num_displays = buf.readUInt32LE();
	const total_probability = buf.readFloatLE();

	const displays: CreatureDisplay[] = [];
	for (let i = 0; i < num_displays; i++) {
		displays.push({
			id: buf.readUInt32LE(),
			scale: buf.readFloatLE(),
			probability: buf.readFloatLE()
		});
	}

	const hp_multiplier = buf.readFloatLE();
	const energy_multiplier = buf.readFloatLE();

	const num_quest_items = buf.readUInt32LE();
	const num_currency_ids = buf.readUInt32LE();

	const movement_info_id = buf.readInt32LE();
	const required_expansion = buf.readUInt32LE();
	const tracking_quest_id = buf.readUInt32LE();
	const vignette_id = buf.readUInt32LE();
	const creature_class_mask = buf.readUInt32LE();
	const creature_difficulty_id = buf.readUInt32LE();
	const widget_parent_set_id = buf.readUInt32LE();
	const widget_set_unit_condition_id = buf.readUInt32LE();

	const title = ds.read_string(title_len).replace(/\0+$/, '');
	const title_alt = ds.read_string(title_alt_len).replace(/\0+$/, '');
	const cursor_name = cursor_name_len !== 1 ? ds.read_string(cursor_name_len).replace(/\0+$/, '') : '';

	const quest_items = buf.readUInt32Array(num_quest_items);
	const currency_ids = buf.readUInt32Array(num_currency_ids);

	return {
		names, name_alts, title, title_alt, cursor_name, leader,
		flags, creature_type, creature_family, classification,
		proxy_creature_ids, num_displays, total_probability, displays,
		hp_multiplier, energy_multiplier,
		quest_items, currency_ids,
		movement_info_id, required_expansion, tracking_quest_id,
		vignette_id, creature_class_mask, creature_difficulty_id,
		widget_parent_set_id, widget_set_unit_condition_id
	};
}

function parse_quest(buf: BufferReader, length: number, ver: GameVersion): QuestRecord {
	const quest_id = buf.readUInt32LE();
	const quest_type = buf.readUInt32LE();
	const quest_package_id = buf.readUInt32LE();
	const content_tuning_id = buf.readUInt32LE();
	const quest_sort_id = buf.readInt32LE();
	const quest_info_id = buf.readUInt32LE();
	const suggested_group_num = buf.readUInt32LE();
	const reward_next_quest = buf.readUInt32LE();
	const reward_xp_difficulty = buf.readUInt32LE();
	const reward_xp_multiplier = buf.readFloatLE();
	const reward_money = buf.readUInt32LE();
	const reward_money_difficulty = buf.readUInt32LE();
	const reward_money_multiplier = buf.readFloatLE();
	const reward_bonus_money = buf.readUInt32LE();

	const reward_display_spell_count = buf.readUInt32LE();

	const reward_spell = buf.readUInt32LE();
	const reward_honor_addition = buf.readUInt32LE();
	const reward_honor_multiplier = buf.readFloatLE();

	let reward_favor = 0;
	if (ver_gte(ver, 12, 0, 0) && ver.build >= 64611)
		reward_favor = buf.readUInt32LE();

	const reward_artifact_xp_difficulty = buf.readUInt32LE();
	const reward_artifact_xp_multiplier = buf.readFloatLE();
	const reward_artifact_category_id = buf.readUInt32LE();
	const provided_item = buf.readUInt32LE();

	const flags = [buf.readUInt32LE(), buf.readUInt32LE(), buf.readUInt32LE()];
	if (ver_gte(ver, 11, 2, 0))
		flags.push(buf.readUInt32LE());

	const reward_fixed_items: QuestRewardFixedItem[] = [];
	for (let i = 0; i < 4; i++) {
		reward_fixed_items.push({
			item_id: buf.readUInt32LE(),
			quantity: buf.readUInt32LE()
		});
	}

	const item_drop_items: QuestRewardFixedItem[] = [];
	for (let i = 0; i < 4; i++) {
		item_drop_items.push({
			item_id: buf.readUInt32LE(),
			quantity: buf.readUInt32LE()
		});
	}

	const reward_choice_items: QuestRewardChoiceItem[] = [];
	for (let i = 0; i < 6; i++) {
		reward_choice_items.push({
			item_id: buf.readUInt32LE(),
			quantity: buf.readUInt32LE(),
			display_id: buf.readUInt32LE()
		});
	}

	const poi_continent = buf.readUInt32LE();
	const poi_x = buf.readFloatLE();
	const poi_y = buf.readFloatLE();
	const poi_priority = buf.readUInt32LE();
	const reward_title = buf.readUInt32LE();
	const reward_arena_points = buf.readUInt32LE();
	const reward_skill_line_id = buf.readUInt32LE();
	const reward_num_skill_ups = buf.readUInt32LE();
	const portrait_giver_display_id = buf.readUInt32LE();
	const portrait_giver_mount_display_id = buf.readUInt32LE();
	const portrait_turn_in_display_id = buf.readUInt32LE();
	const portrait_model_scene_id = buf.readUInt32LE();

	const faction_rewards: QuestFactionReward[] = [];
	for (let i = 0; i < 5; i++) {
		faction_rewards.push({
			faction_id: buf.readUInt32LE(),
			value: buf.readUInt32LE(),
			override: buf.readUInt32LE(),
			gain_max_rank: buf.readUInt32LE()
		});
	}

	const reward_faction_flags = buf.readUInt32LE();

	const currency_rewards: QuestCurrencyReward[] = [];
	for (let i = 0; i < 4; i++) {
		currency_rewards.push({
			currency_id: buf.readUInt32LE(),
			quantity: buf.readUInt32LE()
		});
	}

	const accepted_sound_kit_id = buf.readUInt32LE();
	const complete_sound_kit_id = buf.readUInt32LE();
	const area_group_id = buf.readUInt32LE();
	const time_allowed = buf.readUInt64LE();
	const num_objectives = buf.readUInt32LE();
	const race_flags = buf.readUInt64LE();

	const treasure_picker_id_count = buf.readUInt32LE();
	const treasure_picker_id_2_count = buf.readUInt32LE();

	const expansion_id = buf.readUInt32LE();
	const managed_world_state_id = buf.readUInt32LE();
	const quest_session_bonus = buf.readUInt32LE();
	const quest_giver_creature_id = buf.readUInt32LE();

	let num_conditional_quest_description = buf.readUInt32LE();
	let num_conditional_quest_completion = buf.readUInt32LE();

	let house_room_reward_ids: number[] = [];
	let decor_reward_ids: number[] = [];

	if (ver_gte(ver, 11, 2, 7)) {
		const num_house_room_rewards = buf.readUInt32LE();
		const num_decor_rewards = buf.readUInt32LE();
		house_room_reward_ids = buf.readUInt32Array(num_house_room_rewards);
		decor_reward_ids = buf.readUInt32Array(num_decor_rewards);
	}

	const reward_display_spells: QuestRewardDisplaySpell[] = [];
	for (let i = 0; i < reward_display_spell_count; i++) {
		reward_display_spells.push({
			spell_id: buf.readUInt32LE(),
			player_condition_id: buf.readUInt32LE(),
			spell_type: buf.readUInt32LE()
		});
	}

	const treasure_picker_ids = buf.readUInt32Array(treasure_picker_id_count);
	const treasure_picker_ids_2 = buf.readUInt32Array(treasure_picker_id_2_count);

	// bitpacked string lengths
	const ds = new BitReader(buf);

	const log_title_len = ds.read_bits(9);
	const log_description_len = ds.read_bits(12);
	const quest_description_len = ds.read_bits(12);
	const area_description_len = ds.read_bits(9);
	const portrait_giver_text_len = ds.read_bits(10);
	const portrait_giver_name_len = ds.read_bits(8);
	const portrait_turn_in_text_len = ds.read_bits(10);
	const portrait_turn_in_name_len = ds.read_bits(8);
	const quest_completion_log_len = ds.read_bits(11);

	const ready_for_translation = ds.read_bool();
	const reset_by_scheduler = ds.read_bool();

	ds.flush();

	// objectives
	const objectives: QuestObjective[] = [];
	for (let i = 0; i < num_objectives; i++) {
		const obj_id = buf.readUInt32LE();
		const obj_type = buf.readUInt32LE();
		const storage_index = buf.readUInt8();
		const object_id = buf.readUInt32LE();
		const amount = buf.readUInt32LE();

		if (ver_gte(ver, 11, 2, 7))
			buf.readUInt32LE(); // ObjectiveUNK

		const obj_flags = buf.readUInt32LE();
		const obj_flags2 = buf.readUInt32LE();
		const percent_amount = buf.readFloatLE();

		const num_visual_effects = buf.readUInt32LE();
		const visual_effects = buf.readUInt32Array(num_visual_effects);

		if (ver_gte(ver, 11, 2, 7))
			buf.readUInt32LE(); // WorldEffectID

		const description_length = buf.readUInt8();

		if (ver_gte(ver, 11, 2, 7) && ver.build >= 64228)
			buf.readUInt8(); // padding

		const description = ds.read_string(description_length).replace(/\0+$/, '');

		objectives.push({
			id: obj_id,
			type: obj_type,
			storage_index,
			object_id,
			amount,
			flags: obj_flags,
			flags2: obj_flags2,
			percent_amount,
			visual_effects,
			description
		});
	}

	// trailing strings
	const log_title = ds.read_string(log_title_len).replace(/\0+$/, '');
	const log_description = ds.read_string(log_description_len).replace(/\0+$/, '');
	const quest_description = ds.read_string(quest_description_len).replace(/\0+$/, '');
	const area_description = ds.read_string(area_description_len).replace(/\0+$/, '');
	const portrait_giver_text = ds.read_string(portrait_giver_text_len).replace(/\0+$/, '');
	const portrait_giver_name = ds.read_string(portrait_giver_name_len).replace(/\0+$/, '');
	const portrait_turn_in_text = ds.read_string(portrait_turn_in_text_len).replace(/\0+$/, '');
	const portrait_turn_in_name = ds.read_string(portrait_turn_in_name_len).replace(/\0+$/, '');
	const quest_completion_log = ds.read_string(quest_completion_log_len).replace(/\0+$/, '');
	ds.flush();

	// conditional descriptions
	const conditional_quest_descriptions: ConditionalQuestText[] = [];
	for (let i = 0; i < num_conditional_quest_description; i++) {
		const player_condition_id = buf.readUInt32LE();
		const cond_quest_giver_creature_id = buf.readUInt32LE();
		const cond_len = ds.read_bits(12);
		ds.flush();
		const text = ds.read_string(cond_len).replace(/\0+$/, '');
		conditional_quest_descriptions.push({ player_condition_id, quest_giver_creature_id: cond_quest_giver_creature_id, text });
	}

	const conditional_quest_completions: ConditionalQuestText[] = [];
	for (let i = 0; i < num_conditional_quest_completion; i++) {
		const player_condition_id = buf.readUInt32LE();
		const cond_quest_giver_creature_id = buf.readUInt32LE();
		const cond_len = ds.read_bits(12);
		ds.flush();
		const text = ds.read_string(cond_len).replace(/\0+$/, '');
		conditional_quest_completions.push({ player_condition_id, quest_giver_creature_id: cond_quest_giver_creature_id, text });
	}

	return {
		quest_id, quest_type, quest_package_id, content_tuning_id,
		quest_sort_id, quest_info_id, suggested_group_num,
		reward_next_quest, reward_xp_difficulty, reward_xp_multiplier,
		reward_money, reward_money_difficulty, reward_money_multiplier,
		reward_bonus_money, reward_display_spell_count,
		reward_spell, reward_honor_addition, reward_honor_multiplier,
		reward_artifact_xp_difficulty, reward_artifact_xp_multiplier,
		reward_artifact_category_id, provided_item,
		flags, reward_fixed_items, item_drop_items, reward_choice_items,
		poi_continent, poi_x, poi_y, poi_priority,
		reward_title, reward_arena_points,
		reward_skill_line_id, reward_num_skill_ups,
		portrait_giver_display_id, portrait_giver_mount_display_id,
		portrait_turn_in_display_id, portrait_model_scene_id,
		faction_rewards, reward_faction_flags, currency_rewards,
		accepted_sound_kit_id, complete_sound_kit_id, area_group_id,
		time_allowed, num_objectives, race_flags, expansion_id,
		managed_world_state_id, quest_session_bonus,
		quest_giver_creature_id, reward_display_spells,
		treasure_picker_ids, treasure_picker_ids_2,
		objectives,
		log_title, log_description, quest_description, area_description,
		portrait_giver_text, portrait_giver_name,
		portrait_turn_in_text, portrait_turn_in_name,
		quest_completion_log,
		ready_for_translation, reset_by_scheduler,
		conditional_quest_descriptions, conditional_quest_completions
	};
}

function parse_gameobject(buf: BufferReader, length: number, ver: GameVersion): GameObjectRecord {
	const type = buf.readUInt32LE();
	const display_id = buf.readUInt32LE();

	const names: string[] = [];
	for (let i = 0; i < 4; i++)
		names.push(buf.readNullTermString());

	const icon = buf.readNullTermString();
	const action = buf.readNullTermString();
	const condition = buf.readNullTermString();

	const game_data_size = 35;
	const game_data: number[] = [];
	for (let i = 0; i < game_data_size; i++)
		game_data.push(buf.readUInt32LE());

	const scale = buf.readFloatLE();

	const num_quest_items = buf.readUInt8();
	const quest_items = buf.readUInt32Array(num_quest_items);
	const content_tuning_id = buf.readUInt32LE();

	if (ver_gte(ver, 11, 2, 0))
		buf.readUInt32LE(); // TWW_112_Int

	return { type, display_id, names, icon, action, condition, game_data, scale, quest_items, content_tuning_id };
}

function parse_pagetext(buf: BufferReader, _length: number, _ver: GameVersion): PageTextRecord {
	const page_text_id = buf.readUInt32LE();
	const next_page_text_id = buf.readUInt32LE();
	const player_condition_id = buf.readUInt32LE();
	const flags = buf.readUInt8();

	const ds = new BitReader(buf);
	const text_len = ds.read_bits(12);
	const text = ds.read_string(text_len).replace(/\0+$/, '');

	return { page_text_id, next_page_text_id, player_condition_id, flags, text };
}

function parse_fallback(_buf: BufferReader, length: number, _ver: GameVersion): Record<string, unknown> {
	return { raw_size: length };
}

const BODY_PARSERS: Record<string, BodyParser> = {
	'WGOB': parse_gameobject,
	'WMOB': parse_creature,
	'WQST': parse_quest,
	'WPTX': parse_pagetext,
};

export function parse_wdb(data: ArrayBuffer, patch: string): WdbResult | null {
	if (data.byteLength < WDB_HEADER_SIZE)
		return null;

	const buf = new BufferReader(data);
	const header = read_header(buf);
	const ver = parse_game_version(patch, header.build);

	const body_parser = BODY_PARSERS[header.signature] ?? parse_fallback;

	const records: WdbRecord[] = [];

	while (buf.remainingBytes >= 8) {
		const id = buf.readUInt32LE();
		const length = buf.readUInt32LE();

		if (id === 0 && length === 0)
			break;

		if (length > buf.remainingBytes)
			break;

		const record_start = buf.offset;

		let parsed: RecordData;
		try {
			parsed = body_parser(buf, length, ver);
		} catch {
			parsed = { raw_size: length, parse_error: true };
		}

		buf.seek(record_start + length);
		records.push({ id, data: parsed });
	}

	return { header, records };
}

export type { WdbResult, WdbHeader, WdbRecord, RecordData };
export type { CreatureRecord, QuestRecord, GameObjectRecord, PageTextRecord };

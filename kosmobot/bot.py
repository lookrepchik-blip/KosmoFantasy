from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from .config import Settings
from .db import Database, UserProfile
from .excel_sync import ExcelRepository

CHOOSING_PARTICIPANT = 1
ENTER_SCORE_ADJUST = 2

LINEUP_PAGE_SIZE = 8
SCORE_PAGE_SIZE = 6
PARTICIPANT_PAGE_SIZE = 10


@dataclass(slots=True)
class BotServices:
    settings: Settings
    db: Database
    excel: ExcelRepository

    def is_admin(self, user_id: int) -> bool:
        return user_id in self.settings.admin_ids

    def get_stage(self) -> str:
        return self.db.get_state("current_stage", self.settings.current_stage) or self.settings.current_stage

    def get_round(self) -> int:
        raw = self.db.get_state("current_round", str(self.settings.current_round))
        return int(raw or self.settings.current_round)

    def get_deadline(self) -> datetime:
        raw = self.db.get_state("lineup_deadline")
        if raw:
            return datetime.fromisoformat(raw).astimezone(self.settings.tzinfo)
        now = datetime.now(self.settings.tzinfo)
        deadline = now.replace(
            hour=self.settings.lineup_deadline_hour,
            minute=self.settings.lineup_deadline_minute,
            second=0,
            microsecond=0,
        )
        if deadline < now:
            deadline += timedelta(days=1)
        return deadline

    def lineup_locked(self) -> bool:
        if self.db.get_state("lineup_locked", "0") == "1":
            return True
        return datetime.now(self.settings.tzinfo) >= self.get_deadline()

    def get_limits(self) -> dict[str, int]:
        raw = self.db.get_state("lineup_limits", self.settings.default_lineup_limits) or self.settings.default_lineup_limits
        return parse_limits(raw)

    def is_playoff(self) -> bool:
        stage = self.get_stage().lower()
        return "плей" in stage or "play" in stage

    def transfer_state_for(self, participant_name: str) -> tuple[str, int, int]:
        stage = self.get_stage()
        round_no = self.get_round()
        if self.is_playoff():
            used = len(self.db.get_transfers(participant_name, stage, round_no))
            return ("playoff", 2, max(0, 2 - used))
        used = self.db.count_regular_transfers_used(participant_name)
        return ("regular", 1, max(0, 1 - used))


def parse_limits(raw: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        try:
            result[name] = int(value.strip())
        except ValueError:
            continue
    return result


def position_bucket(position: str | None) -> str:
    pos = (position or "").strip().lower()
    if "врат" in pos or pos in {"gk", "goalkeeper", "goalie"}:
        return "Вратарь"
    if "защ" in pos or "def" in pos:
        return "Защитник"
    if "нап" in pos or "форв" in pos or "fwd" in pos or "forw" in pos:
        return "Нападающий"
    return "Другое"


def _user_full_name(update: Update) -> str:
    user = update.effective_user
    if not user:
        return "Unknown"
    full_name = " ".join(x for x in [user.first_name, user.last_name] if x).strip()
    return full_name or user.username or str(user.id)


def _calc_points(position: str | None, goals: int, assists: int, goals_allowed: int | None, shutout: bool, manual_adj: float) -> float:
    bucket = position_bucket(position)
    if bucket == "Вратарь":
        if shutout:
            base = 3
        elif goals_allowed == 1:
            base = 2
        elif goals_allowed == 2:
            base = 1
        else:
            base = 0
        return round(base + manual_adj, 2)
    return round(goals + assists + manual_adj, 2)


def _item_name(item):
    return item[0]


def _item_position(item):
    return item[1] if len(item) > 1 else None


def _item_team(item):
    return item[2] if len(item) > 2 else None


def _lineup_counts(selected) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in selected:
        bucket = position_bucket(_item_position(item))
        counts[bucket] = counts.get(bucket, 0) + 1
    return counts


def _club_conflict(selected, candidate_name: str, candidate_position: str | None, candidate_team: str | None) -> tuple[str, str] | None:
    if position_bucket(candidate_position) == "Вратарь" or not candidate_team:
        return None
    for item in selected:
        if _item_name(item) == candidate_name:
            continue
        if position_bucket(_item_position(item)) == "Вратарь":
            continue
        if _item_team(item) and _item_team(item) == candidate_team:
            return candidate_team, _item_name(item)
    return None


def _format_limits_status(selected, limits: dict[str, int]) -> str:
    counts = _lineup_counts(selected)
    parts = []
    for bucket, limit in limits.items():
        actual = counts.get(bucket, 0)
        mark = "✅" if actual == limit else ("⚠️" if actual < limit else "❌")
        parts.append(f"{mark} {bucket}: {actual}/{limit}")
    return "\n".join(parts) if parts else "Лимиты не заданы"


def _selected_lookup(selected) -> set[str]:
    return {_item_name(item) for item in selected}


def _transfer_diff(previous, new_selected):
    prev_map = {_item_name(item): item for item in previous}
    new_map = {_item_name(item): item for item in new_selected}
    outs = [name for name in prev_map if name not in new_map]
    ins = [name for name in new_map if name not in prev_map]
    return outs, ins


def _transfer_plan(previous, new_selected):
    outs, ins = _transfer_diff(previous, new_selected)
    pairs = []
    for idx in range(max(len(outs), len(ins))):
        pairs.append((outs[idx] if idx < len(outs) else None, ins[idx] if idx < len(ins) else None))
    return pairs


def _transfer_validation(services: BotServices, participant_name: str, previous, new_selected) -> tuple[bool, str, list[tuple[str | None, str | None]], str]:
    pairs = _transfer_plan(previous, new_selected)
    transfers_used_now = len([1 for out_name, in_name in pairs if out_name or in_name])
    if not previous:
        return True, "", pairs, "initial"
    if transfers_used_now == 0:
        return True, "", [], "none"

    same_size = len(previous) == len(new_selected)
    if not same_size:
        return False, "🚫 Нельзя менять размер состава. Делай замены внутри действующих лимитов.", pairs, "invalid"

    prev_counts = _lineup_counts(previous)
    new_counts = _lineup_counts(new_selected)
    if prev_counts != new_counts:
        return False, "🚫 Замена не должна ломать позиционные лимиты. Поменяй игрока на игрока той же позиции.", pairs, "invalid"

    stage = services.get_stage()
    round_no = services.get_round()
    if services.is_playoff():
        if transfers_used_now > 2:
            return False, f"🚫 В плей-офф можно сделать максимум 2 замены за раунд. Сейчас: {transfers_used_now}.", pairs, "playoff"
        return True, "", pairs, "playoff"

    used_before = services.db.count_regular_transfers_used(participant_name, stage, round_no)
    remaining = 1 - used_before
    if transfers_used_now > remaining:
        return False, f"🚫 В регулярке доступна только 1 замена за сезон. Осталось: {max(0, remaining)}.", pairs, "regular"
    return True, "", pairs, "regular"


def _roster_keyboard(pool, selected, page: int) -> InlineKeyboardMarkup:
    pages = max(1, (len(pool) + LINEUP_PAGE_SIZE - 1) // LINEUP_PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * LINEUP_PAGE_SIZE
    end = start + LINEUP_PAGE_SIZE
    selected_names = _selected_lookup(selected)
    buttons = []
    for offset, item in enumerate(pool[start:end]):
        name, position, team = _item_name(item), _item_position(item), _item_team(item)
        prefix = "✅" if name in selected_names else "▫️"
        team_part = f" [{team}]" if team else ""
        label = f"{prefix} {name}{team_part} ({position or '-'})"
        absolute_idx = start + offset
        buttons.append([InlineKeyboardButton(label[:64], callback_data=f"lu:toggle:{page}:{absolute_idx}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"lu:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="lu:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"lu:page:{page+1}"))
    buttons.append(nav)
    buttons.append([
        InlineKeyboardButton("♻️ Сброс", callback_data=f"lu:reset:{page}"),
        InlineKeyboardButton("💾 Сохранить", callback_data=f"lu:save:{page}"),
    ])
    return InlineKeyboardMarkup(buttons)


def _participant_keyboard(participants: list[str], page: int, prefix: str) -> InlineKeyboardMarkup:
    pages = max(1, (len(participants) + PARTICIPANT_PAGE_SIZE - 1) // PARTICIPANT_PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * PARTICIPANT_PAGE_SIZE
    end = start + PARTICIPANT_PAGE_SIZE
    buttons = [[InlineKeyboardButton(name[:64], callback_data=f"{prefix}:pick:{page}:{start+idx}")] for idx, name in enumerate(participants[start:end])]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"{prefix}:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data=f"{prefix}:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"{prefix}:page:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("Закрыть", callback_data=f"{prefix}:close")])
    return InlineKeyboardMarkup(buttons)


def _score_keyboard(players: list[tuple[str, str | None]], page: int) -> InlineKeyboardMarkup:
    pages = max(1, (len(players) + SCORE_PAGE_SIZE - 1) // SCORE_PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * SCORE_PAGE_SIZE
    end = start + SCORE_PAGE_SIZE
    buttons = []
    for offset, (name, position) in enumerate(players[start:end]):
        absolute_idx = start + offset
        buttons.append([InlineKeyboardButton(f"{name} ({position or '-'})"[:64], callback_data=f"sc:pick:{page}:{absolute_idx}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"sc:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="sc:noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"sc:page:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("⬅️ К участникам", callback_data="sc:participants"), InlineKeyboardButton("Закрыть", callback_data="sc:close")])
    return InlineKeyboardMarkup(buttons)


def _editor_keyboard(position: str | None) -> InlineKeyboardMarkup:
    bucket = position_bucket(position)
    if bucket == "Вратарь":
        rows = [
            [InlineKeyboardButton("🥅 GA -1", callback_data="ed:gaminus"), InlineKeyboardButton("🥅 GA +1", callback_data="ed:gaplus")],
            [InlineKeyboardButton("🧱 Шатаут вкл/выкл", callback_data="ed:shutout")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("🏒 Гол -1", callback_data="ed:gminus"), InlineKeyboardButton("🏒 Гол +1", callback_data="ed:gplus")],
            [InlineKeyboardButton("🎯 Пас -1", callback_data="ed:aminus"), InlineKeyboardButton("🎯 Пас +1", callback_data="ed:aplus")],
        ]
    rows.extend(
        [
            [InlineKeyboardButton("➖ Поправка -0.5", callback_data="ed:mminus"), InlineKeyboardButton("➕ Поправка +0.5", callback_data="ed:mplus")],
            [InlineKeyboardButton("✅ Учитывать: да/нет", callback_data="ed:counted")],
            [InlineKeyboardButton("✍️ Ввести поправку текстом", callback_data="ed:manual")],
            [InlineKeyboardButton("💾 Сохранить", callback_data="ed:save"), InlineKeyboardButton("⬅️ Назад", callback_data="ed:back")],
        ]
    )
    return InlineKeyboardMarkup(rows)


def _render_score_editor(name: str, position: str | None, data: dict[str, object], participant_name: str | None = None) -> str:
    bucket = position_bucket(position)
    points = _calc_points(
        position,
        int(data.get("goals", 0)),
        int(data.get("assists", 0)),
        int(data.get("goals_allowed", 0)),
        bool(data.get("shutout", False)),
        float(data.get("manual_adj", 0.0)),
    )
    lines = ["📊 <b>Редактор статистики</b>"]
    if participant_name:
        lines.append(f"👤 <b>Участник:</b> {participant_name}")
    lines.extend([f"🏒 <b>Игрок:</b> {name}", f"🧩 <b>Позиция:</b> {position or '-'}", f"📦 <b>Категория:</b> {bucket}"])
    if bucket == "Вратарь":
        lines.extend([
            f"🥅 Пропущено: {int(data.get('goals_allowed', 0))}",
            f"🧱 Шатаут: {'да' if data.get('shutout') else 'нет'}",
        ])
    else:
        lines.extend([
            f"🏒 Голы: {int(data.get('goals', 0))}",
            f"🎯 Передачи: {int(data.get('assists', 0))}",
        ])
    lines.extend([
        f"🧮 Ручная поправка: {float(data.get('manual_adj', 0.0)):+.1f}",
        f"✅ Учитывать: {'да' if data.get('counted', True) else 'нет'}",
        f"🏆 Итог очков: {points}",
    ])
    return "\n".join(lines)


def _medal(index: int) -> str:
    return {1: "🥇", 2: "🥈", 3: "🥉"}.get(index, f"{index}.")


def _deadline_human(deadline: datetime, now: datetime) -> str:
    delta = deadline - now
    total_minutes = int(delta.total_seconds() // 60)
    if total_minutes <= 0:
        return "⏳ дедлайн уже наступил"
    days, rem = divmod(total_minutes, 60 * 24)
    hours, minutes = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes or not parts:
        parts.append(f"{minutes}м")
    return "через " + " ".join(parts)


def _leaderboard_text(rows) -> str:
    lines = ["🏆 <b>Рейтинг лиги</b>", ""]
    leader_points = rows[0]["total_points"] if rows else 0
    for idx, row in enumerate(rows, start=1):
        gap = "" if idx == 1 else f" <i>(-{round(leader_points - row['total_points'], 2)})</i>"
        lines.append(f"{_medal(idx)} <b>{row['participant_name']}</b> — {row['total_points']} очк.{gap}")
    return "\n".join(lines)


def _transfer_summary(services: BotServices, participant_name: str) -> str:
    quota_kind, limit, remaining = services.transfer_state_for(participant_name)
    if quota_kind == "playoff":
        used = limit - remaining
        return f"🔁 Плей-офф: {used}/{limit} замен в этом раунде"
    used = limit - remaining
    return f"🔁 Регулярка: {used}/{limit} замен за сезон"


def _effective_lineup_for_stage(services: BotServices, participant_name: str, round_no: int | None = None):
    return services.db.get_effective_lineup(participant_name, services.get_stage(), round_no or services.get_round())


def _lineup_text(selected, limits: dict[str, int], participant_name: str | None = None, stage: str | None = None, round_no: int | None = None, transfer_text: str | None = None) -> str:
    header = ["🧩 <b>Состав этапа</b>"]
    if participant_name:
        header.append(f"👤 <b>{participant_name}</b>")
    if stage is not None and round_no is not None:
        header.append(f"🎯 {stage} • Раунд {round_no}")
    if transfer_text:
        header.append(transfer_text)
    header.append("")
    header.append(_format_limits_status(selected, limits))
    header.append("")
    if selected:
        grouped: dict[str, list[str]] = {}
        for name, position, *rest in selected:
            bucket = position_bucket(position)
            team = rest[0] if rest else None
            team_part = f" [{team}]" if team else ""
            grouped.setdefault(bucket, []).append(f"• {name}{team_part}")
        for bucket in ["Вратарь", "Защитник", "Нападающий", "Другое"]:
            items = grouped.get(bucket)
            if items:
                emoji = {"Вратарь": "🥅", "Защитник": "🛡️", "Нападающий": "⚡", "Другое": "📦"}.get(bucket, "•")
                header.append(f"{emoji} <b>{bucket}</b>")
                header.extend(items)
                header.append("")
    else:
        header.append("🫥 Пока пусто — выбери игроков ниже.")
    return "\n".join(header).strip()


def _profile_text(user, services: BotServices) -> str:
    limits = services.get_limits()
    now = datetime.now(services.settings.tzinfo)
    deadline = services.get_deadline()
    transfer_text = _transfer_summary(services, user["participant_name"]) if user["participant_name"] else "🔁 Замены: недоступны"
    return (
        "👤 <b>Профиль участника</b>\n\n"
        f"🏷 <b>Участник:</b> {user['participant_name'] or 'не привязан'}\n"
        f"🎯 <b>Этап:</b> {services.get_stage()}\n"
        f"🔢 <b>Раунд:</b> {services.get_round()}\n"
        f"⏰ <b>Дедлайн:</b> {deadline.strftime('%Y-%m-%d %H:%M %Z')}\n"
        f"⌛ <b>До дедлайна:</b> {_deadline_human(deadline, now)}\n"
        f"🔒 <b>Составы:</b> {'закрыты' if services.lineup_locked() else 'открыты'}\n"
        f"{transfer_text}\n"
        f"📐 <b>Лимиты:</b> {', '.join(f'{k}={v}' for k, v in limits.items()) if limits else 'не заданы'}"
    )


def _status_text(services: BotServices) -> str:
    now = datetime.now(services.settings.tzinfo)
    deadline = services.get_deadline()
    locked = services.lineup_locked()
    score_mode = "только администратор" if not services.settings.allow_self_scoring else "участники и администратор"
    return (
        "📌 <b>Статус лиги</b>\n\n"
        f"🏒 <b>Лига:</b> {services.settings.league_name}\n"
        f"🎯 <b>Этап:</b> {services.get_stage()}\n"
        f"🔢 <b>Раунд:</b> {services.get_round()}\n"
        f"⏰ <b>Дедлайн:</b> {deadline.strftime('%Y-%m-%d %H:%M %Z')}\n"
        f"⌛ <b>До дедлайна:</b> {_deadline_human(deadline, now)}\n"
        f"🔐 <b>Статус составов:</b> {'закрыты' if locked else 'открыты'}\n"
        f"🧮 <b>Ввод очков:</b> {score_mode}"
    )


def _main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📖 Правила", callback_data="rules:show"),
            InlineKeyboardButton("🆘 Помощь", callback_data="rules:help"),
        ],
        [
            InlineKeyboardButton("📌 Статус", callback_data="menu:status"),
            InlineKeyboardButton("🏆 Рейтинг", callback_data="menu:leaderboard"),
        ],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("🧮 Ввод очков (админ)", callback_data="menu:scoreadmin")])
    return InlineKeyboardMarkup(rows)


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    await update.message.reply_text(
        "🎛 <b>Главное меню</b>\n\nВыбери, что хочешь посмотреть или сделать:",
        reply_markup=_main_menu_keyboard(services.is_admin(update.effective_user.id)),
        parse_mode="HTML",
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    await update.message.reply_text(_status_text(services), reply_markup=_main_menu_keyboard(services.is_admin(update.effective_user.id)), parse_mode="HTML")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services: BotServices = context.bot_data["services"]
    user = update.effective_user
    if not user or not update.message:
        return ConversationHandler.END
    services.db.upsert_user(
        UserProfile(
            telegram_id=user.id,
            username=user.username,
            full_name=_user_full_name(update),
            participant_name=None,
            is_admin=services.is_admin(user.id),
        )
    )
    participants = services.excel.load_participants()
    if not participants:
        await update.message.reply_text("⚠️ Не нашёл участников в Excel. Проверь лист Participants/Участники.")
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(name, callback_data=f"bind:{name}")] for name in participants]
    await update.message.reply_text(
        f"🏒 Привет! Я бот лиги <b>{services.settings.league_name}</b>.\n\n👇 Выбери себя:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML",
    )
    await update.message.reply_text("📌 Нужны правила или помощь? Нажми кнопку ниже.", reply_markup=_main_menu_keyboard(services.is_admin(user.id)))
    return CHOOSING_PARTICIPANT


async def bind_participant(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services: BotServices = context.bot_data["services"]
    query = update.callback_query
    if not query or not query.data:
        return ConversationHandler.END
    await query.answer()
    participant = query.data.split(":", 1)[1]
    services.db.set_participant(query.from_user.id, participant)
    transfer_text = _transfer_summary(services, participant)
    await query.edit_message_text(
        f"✅ Готово! Ты привязан к участнику: <b>{participant}</b>\n\n{transfer_text}\n\n📋 Команды:\n/lineup — состав\n/mylineup — мой состав\n/leaderboard — рейтинг\n/help — помощь",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    admin_note = "\n🧮 /score — ввести или обновить статистику (админ)\n🧪 /flashscore — заготовка под автоимпорт" if services.is_admin(update.effective_user.id) else ""
    msg = (
        "🤖 <b>Команды бота</b>\n\n"
        "👤 /me — мой профиль\n"
        "🧩 /lineup — выбрать состав\n"
        "📋 /mylineup — мой состав на текущий раунд\n"
        "🔁 /transfers — статус замен\n"
        "🗂 /myscores — мои записи\n"
        "🏆 /leaderboard — рейтинг\n"
        "📖 /rules — правила\n"
        "📌 /status — статус лиги\n"
        f"{admin_note}"
    )
    await update.message.reply_text(msg, reply_markup=_main_menu_keyboard(services.is_admin(update.effective_user.id)), parse_mode="HTML")


async def rules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    text = services.excel.load_rules_text()
    await update.message.reply_text("📖 <b>Правила лиги</b>\n\n" + text, reply_markup=_main_menu_keyboard(services.is_admin(update.effective_user.id)), parse_mode="HTML")


async def rules_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    query = update.callback_query
    if not query:
        return
    await query.answer()
    if query.data == "rules:help":
        await query.message.reply_text(
            "ℹ️ <b>Быстрые команды</b>\n\n🧩 /lineup — выбрать состав\n📋 /mylineup — посмотреть состав\n🔁 /transfers — статус замен\n🏆 /leaderboard — общий рейтинг\n👤 /me — профиль\n📌 /status — статус лиги",
            reply_markup=_main_menu_keyboard(services.is_admin(query.from_user.id)),
            parse_mode="HTML",
        )
    else:
        await query.message.reply_text("📖 <b>Правила лиги</b>\n\n" + services.excel.load_rules_text(), reply_markup=_main_menu_keyboard(services.is_admin(query.from_user.id)), parse_mode="HTML")


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    query = update.callback_query
    if not query:
        return
    await query.answer()
    if query.data == "menu:status":
        await query.message.reply_text(_status_text(services), reply_markup=_main_menu_keyboard(services.is_admin(query.from_user.id)), parse_mode="HTML")
    elif query.data == "menu:leaderboard":
        rows = services.db.leaderboard()
        if not rows:
            await query.message.reply_text("🏆 Лидерборд пока пуст.")
            return
        await query.message.reply_text(_leaderboard_text(rows), reply_markup=_main_menu_keyboard(services.is_admin(query.from_user.id)), parse_mode="HTML")
    elif query.data == "menu:scoreadmin":
        if not services.is_admin(query.from_user.id):
            await query.message.reply_text("⛔ Это меню только для администратора.")
            return
        await _open_score_participants(query.message, context, page=0)


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    user = services.db.get_user(update.effective_user.id)
    if not user:
        await update.message.reply_text("👋 Сначала используй /start")
        return
    await update.message.reply_text(_profile_text(user, services), parse_mode="HTML")


def _require_bound_participant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    services: BotServices = context.bot_data["services"]
    user = services.db.get_user(update.effective_user.id)
    if not user or not user["participant_name"]:
        return None, services, "👋 Сначала используй /start и выбери себя."
    return user, services, None


async def transfers_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user, services, err = _require_bound_participant(update, context)
    if err:
        await update.message.reply_text(err)
        return
    rows = services.db.get_transfers(user["participant_name"])
    text = ["🔁 <b>Замены</b>", "", _transfer_summary(services, user["participant_name"])]
    recent = [r for r in rows if r["stage"] == services.get_stage() and r["round_no"] == services.get_round()]
    if recent:
        text.extend(["", "📝 В текущем окне замен:"])
        for row in recent:
            text.append(f"• {row['player_out'] or '—'} → {row['player_in'] or '—'}")
    await update.message.reply_text("\n".join(text), parse_mode="HTML")


async def lineup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user, services, err = _require_bound_participant(update, context)
    if err:
        await update.message.reply_text(err)
        return
    if services.lineup_locked() and not services.is_admin(update.effective_user.id):
        await update.message.reply_text("🔒 Составы уже закрыты для текущего окна изменений.")
        return
    rosters = services.excel.load_rosters()
    players = rosters.get(user["participant_name"], [])
    if not players:
        await update.message.reply_text("⚠️ Не нашёл твой список игроков в Excel.")
        return
    stage = services.get_stage()
    current_round = services.get_round()
    if services.db.has_stage_lineup(user["participant_name"], stage):
        current_rows = _effective_lineup_for_stage(services, user["participant_name"], current_round)
        selected = [(r["player_name"], r["position"], next((p.team_name for p in players if p.name == r["player_name"]), None)) for r in current_rows]
        await update.message.reply_text(
            _lineup_text(selected, services.get_limits(), user["participant_name"], stage, current_round, _transfer_summary(services, user["participant_name"]))
            + "\n\nℹ️ Базовый состав на этот этап уже собран. Для изменений используй /transfer.",
            parse_mode="HTML",
        )
        return

    context.user_data["lineup_mode"] = "initial"
    context.user_data["roster_pool"] = [(p.name, p.position, p.team_name) for p in players]
    context.user_data["lineup_selected"] = []
    context.user_data["lineup_page"] = 0
    limits = services.get_limits()
    await update.message.reply_text(
        _lineup_text([], limits, user["participant_name"], stage, current_round, _transfer_summary(services, user["participant_name"]))
        + "\n\n🆕 Это стартовый состав на весь этап.",
        reply_markup=_roster_keyboard(context.user_data["roster_pool"], [], 0),
        parse_mode="HTML",
    )


async def transfer_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user, services, err = _require_bound_participant(update, context)
    if err:
        await update.message.reply_text(err)
        return
    if services.lineup_locked() and not services.is_admin(update.effective_user.id):
        await update.message.reply_text("🔒 Сейчас окно изменений закрыто.")
        return
    stage = services.get_stage()
    current_round = services.get_round()
    if not services.db.has_stage_lineup(user["participant_name"], stage):
        await update.message.reply_text("⚠️ Сначала собери базовый состав этапа через /lineup.")
        return
    rosters = services.excel.load_rosters()
    players = rosters.get(user["participant_name"], [])
    if not players:
        await update.message.reply_text("⚠️ Не нашёл твой список игроков в Excel.")
        return
    current_rows = _effective_lineup_for_stage(services, user["participant_name"], current_round)
    selected = [(r["player_name"], r["position"], next((p.team_name for p in players if p.name == r["player_name"]), None)) for r in current_rows]
    context.user_data["lineup_mode"] = "transfer"
    context.user_data["roster_pool"] = [(p.name, p.position, p.team_name) for p in players]
    context.user_data["lineup_selected"] = selected
    context.user_data["lineup_page"] = 0
    await update.message.reply_text(
        _lineup_text(selected, services.get_limits(), user["participant_name"], stage, current_round, _transfer_summary(services, user["participant_name"]))
        + "\n\n🔁 Выбери, кого убрать и кого добавить. Замена применяется сразу после сохранения.",
        reply_markup=_roster_keyboard(context.user_data["roster_pool"], selected, 0),
        parse_mode="HTML",
    )


async def lineup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    if query.data == "lu:noop":
        return
    _, action, *rest = query.data.split(":")
    page = int(rest[0]) if rest else 0
    pool = context.user_data.get("roster_pool", [])
    selected = context.user_data.get("lineup_selected", [])
    selected_names = _selected_lookup(selected)
    services: BotServices = context.bot_data["services"]
    limits = services.get_limits()
    user = services.db.get_user(query.from_user.id)

    if action == "page":
        context.user_data["lineup_page"] = page
    elif action == "reset":
        previous = _effective_lineup_for_stage(services, user["participant_name"], services.get_round())
        prev_team_map = {name: team for name, _pos, team in pool}
        selected = [(r["player_name"], r["position"], prev_team_map.get(r["player_name"])) for r in previous]
    elif action == "toggle":
        idx = int(rest[1])
        if idx < 0 or idx >= len(pool):
            await query.answer("⚠️ Игрок не найден", show_alert=True)
            return
        name, position, team = pool[idx]
        if name in selected_names:
            selected = [item for item in selected if _item_name(item) != name]
        else:
            conflict = _club_conflict(selected, name, position, team)
            if conflict:
                conflict_team, conflict_player = conflict
                await query.answer(
                    f"🚫 Нельзя выбрать второго полевого из клуба {conflict_team}.\n\n✅ Уже выбран: {conflict_player}.\n🥅 Вратарь — исключение.",
                    show_alert=True,
                )
                return
            selected = selected + [(name, position, team)]
    elif action == "save":
        counts = _lineup_counts(selected)
        for bucket, limit in limits.items():
            if counts.get(bucket, 0) != limit:
                await query.answer(f"🚫 Нельзя сохранить: {bucket} = {counts.get(bucket, 0)}/{limit}", show_alert=True)
                return
        mode = context.user_data.get("lineup_mode", "initial")
        previous_rows = _effective_lineup_for_stage(services, user["participant_name"], services.get_round())
        previous = [(r["player_name"], r["position"]) for r in previous_rows]
        if mode == "initial" and services.db.has_stage_lineup(user["participant_name"], services.get_stage()):
            await query.answer("⚠️ Стартовый состав на этап уже существует. Используй /transfer.", show_alert=True)
            return
        validation_ok, validation_text, transfer_pairs, quota_kind = _transfer_validation(services, user["participant_name"], previous, [(n, p) for n, p, _t in selected])
        if mode == "initial":
            quota_kind = "initial"
            transfer_pairs = []
            validation_ok = True
            validation_text = ""
        if not validation_ok:
            await query.answer(validation_text, show_alert=True)
            return
        services.db.replace_lineup(user["participant_name"], services.get_stage(), services.get_round(), [(n, p) for n, p, _t in selected], query.from_user.id)
        if quota_kind in {"regular", "playoff"}:
            services.db.replace_transfers(user["participant_name"], services.get_stage(), services.get_round(), transfer_pairs, query.from_user.id, quota_kind)
        else:
            services.db.replace_transfers(user["participant_name"], services.get_stage(), services.get_round(), [], query.from_user.id, "initial")
        transfer_note = ""
        if transfer_pairs:
            transfer_note = "\n\n🔁 Замены:\n" + "\n".join(f"• {old or '—'} → {new or '—'}" for old, new in transfer_pairs)
        await query.edit_message_text(
            _lineup_text(selected, limits, user["participant_name"], services.get_stage(), services.get_round(), _transfer_summary(services, user["participant_name"]))
            + "\n\n✅ <b>Состав сохранён</b>"
            + transfer_note,
            parse_mode="HTML",
        )
        return

    context.user_data["lineup_selected"] = selected
    current_page = context.user_data.get("lineup_page", page)
    await query.edit_message_text(
        _lineup_text(selected, limits, user["participant_name"], services.get_stage(), services.get_round(), _transfer_summary(services, user["participant_name"])),
        reply_markup=_roster_keyboard(pool, selected, current_page),
        parse_mode="HTML",
    )


async def mylineup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user, services, err = _require_bound_participant(update, context)
    if err:
        await update.message.reply_text(err)
        return
    rows = _effective_lineup_for_stage(services, user["participant_name"], services.get_round())
    if not rows:
        await update.message.reply_text("📝 На текущем этапе состав ещё не сохранён.")
        return
    selected = [(r["player_name"], r["position"]) for r in rows]
    await update.message.reply_text(
        _lineup_text(selected, services.get_limits(), user["participant_name"], services.get_stage(), services.get_round(), _transfer_summary(services, user["participant_name"])),
        parse_mode="HTML",
    )


async def _open_score_participants(target_message, context: ContextTypes.DEFAULT_TYPE, page: int) -> None:
    services: BotServices = context.bot_data["services"]
    participants = services.excel.load_participants()
    context.user_data["score_participants"] = participants
    await target_message.reply_text(
        "🧮 <b>Админ-ввод очков</b>\n\nСначала выбери участника:",
        reply_markup=_participant_keyboard(participants, page, "scp"),
        parse_mode="HTML",
    )


async def score(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ввод очков доступен только администратору.")
        return
    await _open_score_participants(update.message, context, page=0)


async def score_participant_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(query.from_user.id):
        await query.message.reply_text("⛔ Недостаточно прав.")
        return
    _, action, *rest = query.data.split(":")
    participants = context.user_data.get("score_participants", services.excel.load_participants())
    if action == "close":
        await query.edit_message_text("👌 Экран выбора участника закрыт.")
        return
    if action == "page":
        page = int(rest[0])
        await query.edit_message_reply_markup(reply_markup=_participant_keyboard(participants, page, "scp"))
        return
    if action == "pick":
        idx = int(rest[1])
        if idx < 0 or idx >= len(participants):
            await query.edit_message_text("⚠️ Участник не найден.")
            return
        participant_name = participants[idx]
        rows = _effective_lineup_for_stage(services, participant_name, services.get_round())
        if not rows:
            await query.edit_message_text(f"⚠️ У участника {participant_name} нет активного состава на текущий этап.")
            return
        players = [(r["player_name"], r["position"]) for r in rows]
        context.user_data["score_target_participant"] = participant_name
        context.user_data["score_players"] = players
        await query.edit_message_text(
            f"🧮 <b>Админ-ввод очков</b>\n\n👤 Участник: <b>{participant_name}</b>\n🏒 Выбери игрока:",
            reply_markup=_score_keyboard(players, 0),
            parse_mode="HTML",
        )


async def score_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    query = update.callback_query
    if not query or not query.data:
        return None
    await query.answer()
    if query.data in {"sc:noop", "ed:noop"}:
        return None

    services: BotServices = context.bot_data["services"]
    if not services.is_admin(query.from_user.id):
        await query.message.reply_text("⛔ Ввод очков доступен только администратору.")
        return None
    participant_name = context.user_data.get("score_target_participant")

    if query.data.startswith("sc:"):
        _, action, *rest = query.data.split(":")
        players = context.user_data.get("score_players", [])
        if action == "close":
            await query.edit_message_text("✅ Экран ввода очков закрыт.")
            return None
        if action == "participants":
            participants = context.user_data.get("score_participants", services.excel.load_participants())
            await query.edit_message_text(
                "🧮 <b>Админ-ввод очков</b>\n\nСначала выбери участника:",
                reply_markup=_participant_keyboard(participants, 0, "scp"),
                parse_mode="HTML",
            )
            return None
        if action == "page":
            page = int(rest[0])
            await query.edit_message_reply_markup(reply_markup=_score_keyboard(players, page))
            return None
        if action == "pick":
            idx = int(rest[1])
            if idx < 0 or idx >= len(players):
                await query.edit_message_text("⚠️ Игрок не найден. Запусти /score ещё раз.")
                return None
            name, position = players[idx]
            existing = services.db.get_score(participant_name, services.get_stage(), services.get_round(), name)
            editor = {
                "player_name": name,
                "position": position,
                "goals": int(existing["goals"]) if existing else 0,
                "assists": int(existing["assists"]) if existing else 0,
                "goals_allowed": int(existing["goals_allowed"] or 0) if existing else 0,
                "shutout": bool(existing["shutout"]) if existing else False,
                "manual_adj": float(existing["manual_adj"]) if existing else 0.0,
                "counted": bool(existing["counted"]) if existing else True,
            }
            context.user_data["score_editor"] = editor
            await query.edit_message_text(_render_score_editor(name, position, editor, participant_name), reply_markup=_editor_keyboard(position), parse_mode="HTML")
            return None

    if query.data.startswith("ed:"):
        editor = context.user_data.get("score_editor")
        if not editor:
            await query.edit_message_text("⌛ Редактор очков устарел. Запусти /score ещё раз.")
            return ConversationHandler.END
        action = query.data.split(":", 1)[1]
        if action == "gplus":
            editor["goals"] = int(editor.get("goals", 0)) + 1
        elif action == "gminus":
            editor["goals"] = max(0, int(editor.get("goals", 0)) - 1)
        elif action == "aplus":
            editor["assists"] = int(editor.get("assists", 0)) + 1
        elif action == "aminus":
            editor["assists"] = max(0, int(editor.get("assists", 0)) - 1)
        elif action == "gaplus":
            editor["goals_allowed"] = int(editor.get("goals_allowed", 0)) + 1
        elif action == "gaminus":
            editor["goals_allowed"] = max(0, int(editor.get("goals_allowed", 0)) - 1)
        elif action == "shutout":
            editor["shutout"] = not bool(editor.get("shutout", False))
        elif action == "mplus":
            editor["manual_adj"] = round(float(editor.get("manual_adj", 0.0)) + 0.5, 1)
        elif action == "mminus":
            editor["manual_adj"] = round(float(editor.get("manual_adj", 0.0)) - 0.5, 1)
        elif action == "counted":
            editor["counted"] = not bool(editor.get("counted", True))
        elif action == "manual":
            context.user_data["awaiting_manual_adjust"] = True
            await query.message.reply_text("✍️ Отправь число для ручной поправки, например: 1 или -0.5")
            return ENTER_SCORE_ADJUST
        elif action == "back":
            players = context.user_data.get("score_players", [])
            await query.edit_message_text(
                f"🧮 <b>Админ-ввод очков</b>\n\n👤 Участник: <b>{participant_name}</b>\n🏒 Выбери игрока:",
                reply_markup=_score_keyboard(players, 0),
                parse_mode="HTML",
            )
            return None
        elif action == "save":
            points = _calc_points(
                editor.get("position"),
                int(editor.get("goals", 0)),
                int(editor.get("assists", 0)),
                int(editor.get("goals_allowed", 0)),
                bool(editor.get("shutout", False)),
                float(editor.get("manual_adj", 0.0)),
            )
            services.db.upsert_score(
                participant_name=participant_name,
                stage=services.get_stage(),
                round_no=services.get_round(),
                player_name=str(editor["player_name"]),
                position=editor.get("position"),
                goals=int(editor.get("goals", 0)),
                assists=int(editor.get("assists", 0)),
                goals_allowed=int(editor.get("goals_allowed", 0)),
                shutout=bool(editor.get("shutout", False)),
                manual_adj=float(editor.get("manual_adj", 0.0)),
                counted=bool(editor.get("counted", True)),
                points=points,
                created_by=query.from_user.id,
            )
            await query.edit_message_text(_render_score_editor(str(editor["player_name"]), editor.get("position"), editor, participant_name) + "\n\n✅ Сохранено", parse_mode="HTML")
            return ConversationHandler.END
        context.user_data["score_editor"] = editor
        await query.edit_message_text(_render_score_editor(str(editor["player_name"]), editor.get("position"), editor, participant_name), reply_markup=_editor_keyboard(editor.get("position")), parse_mode="HTML")
        return None
    return None


async def manual_adjust_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    editor = context.user_data.get("score_editor")
    participant_name = context.user_data.get("score_target_participant")
    if not editor:
        await update.message.reply_text("⚠️ Редактор не найден. Запусти /score ещё раз.")
        return ConversationHandler.END
    try:
        value = float((update.message.text or "0").strip().replace(",", "."))
    except ValueError:
        await update.message.reply_text("⚠️ Не смог разобрать число. Попробуй ещё раз, например: -0.5")
        return ENTER_SCORE_ADJUST
    editor["manual_adj"] = value
    context.user_data["score_editor"] = editor
    context.user_data["awaiting_manual_adjust"] = False
    await update.message.reply_text(_render_score_editor(str(editor["player_name"]), editor.get("position"), editor, participant_name), reply_markup=_editor_keyboard(editor.get("position")), parse_mode="HTML")
    return ConversationHandler.END


async def myscores(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user, services, err = _require_bound_participant(update, context)
    if err:
        await update.message.reply_text(err)
        return
    rows = services.db.get_scores(user["participant_name"], services.get_stage(), services.get_round())
    if not rows:
        await update.message.reply_text("🗂 Для текущего раунда записей пока нет.")
        return
    lines = ["🗂 <b>Твои записи</b>"]
    for row in rows:
        lines.append(f"• {row['player_name']}: {row['points']} очк. | counted={'да' if row['counted'] else 'нет'}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    rows = services.db.leaderboard()
    if not rows:
        await update.message.reply_text("🏆 Лидерборд пока пуст.")
        return
    await update.message.reply_text(_leaderboard_text(rows), parse_mode="HTML")


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    await update.message.reply_text(
        "🛠 <b>Команды админа</b>\n\n"
        "/setround &lt;номер&gt;\n"
        "/setstage &lt;название&gt;\n"
        "/deadline YYYY-MM-DD HH:MM\n"
        "/locklineup /unlocklineup\n"
        "/setlimits Вратарь=1,Защитник=2,Нападающий=2\n"
        "/score — ввод очков\n"
        "/flashscore — заготовка под автоимпорт\n"
        "/export",
        parse_mode="HTML",
    )


async def setround(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    try:
        value = int(context.args[0])
    except Exception:
        await update.message.reply_text("Использование: /setround 3")
        return
    services.db.set_state("current_round", str(value))
    await update.message.reply_text(f"🔢 Текущий раунд = {value}")


async def setstage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    stage = " ".join(context.args).strip()
    if not stage:
        await update.message.reply_text("Использование: /setstage Плей-офф")
        return
    services.db.set_state("current_stage", stage)
    await update.message.reply_text(f"🎯 Текущий этап = {stage}")


async def deadline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    raw = " ".join(context.args).strip()
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(tzinfo=services.settings.tzinfo)
    except Exception:
        await update.message.reply_text("Использование: /deadline 2026-04-25 18:00")
        return
    services.db.set_state("lineup_deadline", dt.isoformat())
    await update.message.reply_text(f"⏰ Дедлайн обновлён: {dt.strftime('%Y-%m-%d %H:%M %Z')}")


async def locklineup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    services.db.set_state("lineup_locked", "1")
    await update.message.reply_text("🔒 Составы заблокированы.")


async def unlocklineup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    services.db.set_state("lineup_locked", "0")
    await update.message.reply_text("🔓 Составы разблокированы.")


async def setlimits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    raw = " ".join(context.args).strip()
    limits = parse_limits(raw)
    if not limits:
        await update.message.reply_text("Использование: /setlimits Вратарь=1,Защитник=2,Нападающий=2")
        return
    services.db.set_state("lineup_limits", raw)
    await update.message.reply_text("📐 Лимиты обновлены: " + ", ".join(f"{k}={v}" for k, v in limits.items()))


async def flashscore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Команда доступна только администратору.")
        return
    await update.message.reply_text(
        "🧪 <b>Flashscore-импорт: подготовка</b>\n\n"
        "Сейчас в боте включён надёжный режим: статистику заносит администратор.\n"
        "Модуль автоимпорта лучше подключать как подтверждаемый импорт, а не как единственный источник.\n\n"
        "Следующий шаг: привязать матчи и сделать кнопку 'Подтвердить импорт'.",
        parse_mode="HTML",
    )


async def export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services: BotServices = context.bot_data["services"]
    if not services.is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Недостаточно прав.")
        return
    output = services.excel.export_bot_data(
        users=services.db.list_users(),
        lineups=services.db.all_lineups(),
        scores=services.db.get_scores(),
        leaderboard=services.db.leaderboard(),
        output_path=services.settings.excel_path.with_name(services.settings.excel_path.stem + "_bot_export.xlsx"),
        transfers=services.db.get_transfers(),
    )
    await update.message.reply_document(document=Path(output).open("rb"), filename=output.name)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("👌 Действие отменено.")
    return ConversationHandler.END


def build_application(services: BotServices) -> Application:
    app = Application.builder().token(services.settings.bot_token).build()
    app.bot_data["services"] = services

    start_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={CHOOSING_PARTICIPANT: [CallbackQueryHandler(bind_participant, pattern=r"^bind:")]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    score_handler = ConversationHandler(
        entry_points=[CommandHandler("score", score)],
        states={ENTER_SCORE_ADJUST: [MessageHandler(filters.TEXT & ~filters.COMMAND, manual_adjust_input)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    app.add_handler(start_handler)
    app.add_handler(score_handler)
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(CommandHandler("lineup", lineup))
    app.add_handler(CommandHandler("transfer", transfer_cmd))
    app.add_handler(CommandHandler("mylineup", mylineup))
    app.add_handler(CommandHandler("myscores", myscores))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("rules", rules_cmd))
    app.add_handler(CommandHandler("transfers", transfers_cmd))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("setround", setround))
    app.add_handler(CommandHandler("setstage", setstage))
    app.add_handler(CommandHandler("deadline", deadline))
    app.add_handler(CommandHandler("locklineup", locklineup))
    app.add_handler(CommandHandler("unlocklineup", unlocklineup))
    app.add_handler(CommandHandler("setlimits", setlimits))
    app.add_handler(CommandHandler("flashscore", flashscore_cmd))
    app.add_handler(CommandHandler("export", export))
    app.add_handler(CallbackQueryHandler(lineup_callback, pattern=r"^lu:"))
    app.add_handler(CallbackQueryHandler(score_participant_callback, pattern=r"^scp:"))
    app.add_handler(CallbackQueryHandler(score_callback, pattern=r"^(sc:|ed:)") )
    app.add_handler(CallbackQueryHandler(rules_callback, pattern=r"^rules:(show|help)$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu:(status|leaderboard|scoreadmin)$"))
    return app

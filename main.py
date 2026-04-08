"""
Telegram Task Manager Bot
Stack: python-telegram-bot 21.5, SQLite, APScheduler
Deploy: Render free tier
"""

import logging
import sqlite3
import os
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── CONFIG ──────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TIMEZONE = ZoneInfo(os.environ.get("TZ", "Asia/Ho_Chi_Minh"))
DB_PATH = "tasks.db"

# Status labels & emoji
STATUS_EMOJI = {
    "todo": "📋",
    "doing": "🔄",
    "done": "✅",
    "stuck": "🚧",
}

PRIORITY_EMOJI = {
    "low": "🟢",
    "medium": "🟡",
    "high": "🔴",
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── DATABASE ─────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                display_name TEXT NOT NULL,
                chat_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                assignee_id INTEGER,
                created_by INTEGER,
                status TEXT DEFAULT 'todo',
                priority TEXT DEFAULT 'medium',
                deadline DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (assignee_id) REFERENCES members(id),
                FOREIGN KEY (created_by) REFERENCES members(id)
            );

            CREATE TABLE IF NOT EXISTS group_chats (
                id INTEGER PRIMARY KEY,
                chat_id INTEGER UNIQUE NOT NULL,
                title TEXT
            );
        """)


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def now():
    return datetime.now(TIMEZONE)


def ensure_member(user, chat_id=None):
    """Auto-register member on first interaction."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM members WHERE telegram_id = ?", (user.id,)
        ).fetchone()
        if not existing:
            display = user.full_name or user.username or f"User{user.id}"
            conn.execute(
                "INSERT INTO members (telegram_id, username, display_name, chat_id) VALUES (?,?,?,?)",
                (user.id, user.username, display, chat_id),
            )
        elif chat_id and not existing["chat_id"]:
            conn.execute(
                "UPDATE members SET chat_id = ? WHERE telegram_id = ?",
                (chat_id, user.id),
            )
        return conn.execute(
            "SELECT * FROM members WHERE telegram_id = ?", (user.id,)
        ).fetchone()


def get_member_by_id(member_id):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM members WHERE id = ?", (member_id,)).fetchone()


def get_all_members():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM members").fetchall()


def get_task(task_id):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


def format_task(task, show_id=True):
    with get_conn() as conn:
        assignee = conn.execute(
            "SELECT display_name FROM members WHERE id = ?", (task["assignee_id"],)
        ).fetchone()
    name = assignee["display_name"] if assignee else "Chưa giao"
    deadline_str = f"⏰ {task['deadline']}" if task["deadline"] else "⏰ Không có deadline"
    status_str = f"{STATUS_EMOJI.get(task['status'], '?')} {task['status'].upper()}"
    priority_str = f"{PRIORITY_EMOJI.get(task['priority'], '?')} {task['priority'].upper()}"
    id_str = f"[#{task['id']}] " if show_id else ""
    desc = f"\n📝 {task['description']}" if task["description"] else ""
    return (
        f"{id_str}*{task['title']}*{desc}\n"
        f"👤 {name} | {status_str} | {priority_str}\n"
        f"{deadline_str}"
    )


def register_group_chat(chat_id, title):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO group_chats (chat_id, title) VALUES (?,?)",
            (chat_id, title),
        )


def get_group_chats():
    with get_conn() as conn:
        return conn.execute("SELECT chat_id FROM group_chats").fetchall()


# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    member = ensure_member(user, chat.id if chat.type != "private" else None)

    if chat.type in ("group", "supergroup"):
        register_group_chat(chat.id, chat.title)

    await update.message.reply_text(
        f"👋 Chào *{member['display_name']}*\\! Bot quản lý task đã sẵn sàng\\.\n\n"
        "📌 *Lệnh nhanh:*\n"
        "`/new` \\- Tạo task mới\n"
        "`/list` \\- Xem tất cả task\n"
        "`/my` \\- Task của tôi\n"
        "`/done <id>` \\- Đánh dấu hoàn thành\n"
        "`/doing <id>` \\- Đang làm\n"
        "`/stuck <id>` \\- Bị kẹt\n"
        "`/task <id>` \\- Chi tiết task\n"
        "`/report` \\- Báo cáo hôm nay\n"
        "`/week` \\- Báo cáo tuần\n"
        "`/members` \\- Danh sách thành viên\n"
        "`/help` \\- Trợ giúp",
        parse_mode="MarkdownV2",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Task Manager Bot* — Hướng dẫn\n\n"
        "*Tạo & Quản lý Task:*\n"
        "`/new` — Tạo task mới \\(interactive\\)\n"
        "`/task <id>` — Xem chi tiết & menu hành động\n"
        "`/edit <id>` — Chỉnh sửa task\n"
        "`/delete <id>` — Xóa task\n\n"
        "*Cập nhật nhanh:*\n"
        "`/done <id>` — Hoàn thành ✅\n"
        "`/doing <id>` — Đang làm 🔄\n"
        "`/stuck <id>` — Bị kẹt 🚧\n"
        "`/todo <id>` — Quay về chờ 📋\n\n"
        "*Xem danh sách:*\n"
        "`/list` — Tất cả task đang mở\n"
        "`/list all` — Tất cả kể cả done\n"
        "`/my` — Task của tôi\n"
        "`/deadline` — Sắp đến hạn \\(7 ngày\\)\n\n"
        "*Báo cáo:*\n"
        "`/report` — Tổng kết hôm nay\n"
        "`/week` — Tổng kết tuần này",
        parse_mode="MarkdownV2",
    )


# ─── CREATE TASK (multi-step conversation) ─────────────────────────────────

async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user, update.effective_chat.id)
    context.user_data["creating_task"] = {"step": "title"}
    await update.message.reply_text(
        "📝 *Tạo task mới*\n\nNhập *tiêu đề* task:",
        parse_mode="Markdown",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle multi-step task creation conversation."""
    user = update.effective_user
    ensure_member(user)
    text = update.message.text.strip()
    state = context.user_data.get("creating_task")

    if not state:
        return

    step = state.get("step")

    if step == "title":
        state["title"] = text
        state["step"] = "description"
        await update.message.reply_text(
            "📋 Nhập *mô tả* task \\(hoặc gõ `-` để bỏ qua\\):",
            parse_mode="MarkdownV2",
        )

    elif step == "description":
        state["description"] = None if text == "-" else text
        state["step"] = "assignee"
        members = get_all_members()
        if not members:
            await update.message.reply_text("❌ Chưa có thành viên nào. Mọi người hãy /start trước.")
            context.user_data.pop("creating_task", None)
            return
        keyboard = [
            [InlineKeyboardButton(f"👤 {m['display_name']}", callback_data=f"assign_{m['id']}")]
            for m in members
        ]
        await update.message.reply_text(
            "👥 Giao task cho ai?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif step == "deadline":
        if text.lower() in ("-", "skip", "bỏ qua"):
            state["deadline"] = None
        else:
            try:
                deadline = datetime.strptime(text, "%d/%m/%Y").date()
                state["deadline"] = str(deadline)
            except ValueError:
                await update.message.reply_text("❌ Sai định dạng. Nhập lại `DD/MM/YYYY` hoặc `-`:")
                return
        state["step"] = "priority"
        keyboard = [
            [
                InlineKeyboardButton("🟢 Thấp", callback_data="priority_low"),
                InlineKeyboardButton("🟡 Vừa", callback_data="priority_medium"),
                InlineKeyboardButton("🔴 Cao", callback_data="priority_high"),
            ]
        ]
        await update.message.reply_text(
            "⚡ Chọn *độ ưu tiên*:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = update.effective_user
    ensure_member(user)

    # ── Assign member during task creation
    if data.startswith("assign_"):
        assignee_id = int(data.split("_")[1])
        state = context.user_data.get("creating_task")
        if not state:
            return
        state["assignee_id"] = assignee_id
        state["step"] = "deadline"
        await query.edit_message_text(
            "📅 Nhập *deadline* \\(định dạng `DD/MM/YYYY`\\) hoặc `-` để bỏ qua:",
            parse_mode="MarkdownV2",
        )

    # ── Priority during task creation
    elif data.startswith("priority_"):
        priority = data.split("_")[1]
        state = context.user_data.get("creating_task")
        if not state:
            return
        state["priority"] = priority

        member = ensure_member(user)
        with get_conn() as conn:
            conn.execute(
                """INSERT INTO tasks (title, description, assignee_id, created_by, priority, deadline)
                   VALUES (?,?,?,?,?,?)""",
                (
                    state["title"],
                    state.get("description"),
                    state["assignee_id"],
                    member["id"],
                    priority,
                    state.get("deadline"),
                ),
            )
            task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        context.user_data.pop("creating_task", None)
        task = get_task(task_id)
        assignee = get_member_by_id(state["assignee_id"])

        await query.edit_message_text(
            f"✅ *Task #{task_id} đã tạo\\!*\n\n{format_task(task)}",
            parse_mode="MarkdownV2",
        )

        # Notify assignee if different from creator
        if assignee and assignee["telegram_id"] != user.id and assignee["chat_id"]:
            try:
                await context.bot.send_message(
                    chat_id=assignee["chat_id"],
                    text=f"🔔 *Bạn được giao task mới\\!*\n\n{format_task(task)}",
                    parse_mode="MarkdownV2",
                )
            except Exception:
                pass

    # ── Task action buttons
    elif data.startswith("status_"):
        parts = data.split("_")
        task_id = int(parts[1])
        new_status = parts[2]
        with get_conn() as conn:
            conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                (new_status, now().isoformat(), task_id),
            )
        task = get_task(task_id)
        await query.edit_message_text(
            f"✅ Cập nhật thành công\\!\n\n{format_task(task)}",
            parse_mode="MarkdownV2",
            reply_markup=task_action_keyboard(task_id, task["status"]),
        )

    elif data.startswith("delete_confirm_"):
        task_id = int(data.split("_")[2])
        with get_conn() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await query.edit_message_text(f"🗑️ Task #{task_id} đã bị xóa.")

    elif data.startswith("delete_cancel_"):
        await query.edit_message_text("❌ Hủy xóa.")

    elif data.startswith("delete_"):
        task_id = int(data.split("_")[1])
        keyboard = [
            [
                InlineKeyboardButton("✅ Xác nhận xóa", callback_data=f"delete_confirm_{task_id}"),
                InlineKeyboardButton("❌ Hủy", callback_data=f"delete_cancel_{task_id}"),
            ]
        ]
        await query.edit_message_text(
            f"⚠️ Bạn chắc muốn xóa Task \\#{task_id}?",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


def task_action_keyboard(task_id: int, current_status: str):
    statuses = [s for s in ["todo", "doing", "done", "stuck"] if s != current_status]
    buttons = [
        InlineKeyboardButton(
            f"{STATUS_EMOJI[s]} {s.capitalize()}",
            callback_data=f"status_{task_id}_{s}",
        )
        for s in statuses
    ]
    rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
    rows.append([InlineKeyboardButton("🗑️ Xóa task", callback_data=f"delete_{task_id}")])
    return InlineKeyboardMarkup(rows)


# ─── STATUS SHORTCUTS ─────────────────────────────────────────────────────────

async def quick_status(update: Update, context: ContextTypes.DEFAULT_TYPE, status: str):
    ensure_member(update.effective_user)
    if not context.args:
        await update.message.reply_text(f"Usage: /{status} <task_id>")
        return
    try:
        task_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID phải là số nguyên.")
        return
    task = get_task(task_id)
    if not task:
        await update.message.reply_text(f"❌ Không tìm thấy task #{task_id}")
        return
    with get_conn() as conn:
        conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
            (status, now().isoformat(), task_id),
        )
    task = get_task(task_id)
    await update.message.reply_text(
        f"{STATUS_EMOJI[status]} Cập nhật Task \\#{task_id} → *{status.upper()}*\n\n{format_task(task)}",
        parse_mode="MarkdownV2",
    )


async def cmd_done(u, c): await quick_status(u, c, "done")
async def cmd_doing(u, c): await quick_status(u, c, "doing")
async def cmd_stuck(u, c): await quick_status(u, c, "stuck")
async def cmd_todo(u, c): await quick_status(u, c, "todo")


# ─── LIST & VIEW ──────────────────────────────────────────────────────────────

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    show_all = context.args and context.args[0].lower() == "all"

    with get_conn() as conn:
        if show_all:
            tasks = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
        else:
            tasks = conn.execute(
                "SELECT * FROM tasks WHERE status != 'done' ORDER BY priority DESC, deadline ASC"
            ).fetchall()

    if not tasks:
        await update.message.reply_text("📭 Không có task nào." + (" " if show_all else " Thêm `/list all` để xem cả done."))
        return

    lines = [f"📋 *{'Tất cả' if show_all else 'Task đang mở'} ({len(tasks)})*\n"]
    for t in tasks:
        lines.append(format_task(t))
        lines.append("")

    # Send in chunks to avoid Telegram message length limit
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 3800:
            await update.message.reply_text(chunk, parse_mode="Markdown")
            chunk = line + "\n"
        else:
            chunk += line + "\n"
    if chunk.strip():
        await update.message.reply_text(chunk, parse_mode="Markdown")


async def cmd_my(update: Update, context: ContextTypes.DEFAULT_TYPE):
    member = ensure_member(update.effective_user)
    with get_conn() as conn:
        tasks = conn.execute(
            "SELECT * FROM tasks WHERE assignee_id=? AND status!='done' ORDER BY deadline ASC",
            (member["id"],),
        ).fetchall()

    if not tasks:
        await update.message.reply_text("🎉 Bạn không có task nào đang chờ!")
        return

    lines = [f"👤 *Task của bạn ({len(tasks)})*\n"]
    for t in tasks:
        lines.append(format_task(t))
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    if not context.args:
        await update.message.reply_text("Usage: /task <id>")
        return
    try:
        task_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID phải là số.")
        return
    task = get_task(task_id)
    if not task:
        await update.message.reply_text(f"❌ Không tìm thấy task #{task_id}")
        return

    with get_conn() as conn:
        creator = conn.execute("SELECT display_name FROM members WHERE id=?", (task["created_by"],)).fetchone()

    creator_name = creator["display_name"] if creator else "?"
    text = (
        f"{format_task(task)}\n"
        f"🧑‍💻 Tạo bởi: {creator_name}\n"
        f"📅 Tạo lúc: {task['created_at'][:16]}\n"
        f"🔄 Cập nhật: {task['updated_at'][:16]}"
    )
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=task_action_keyboard(task_id, task["status"]),
    )


async def cmd_deadline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    cutoff = (now().date() + timedelta(days=7)).isoformat()
    today = now().date().isoformat()
    with get_conn() as conn:
        tasks = conn.execute(
            "SELECT * FROM tasks WHERE deadline IS NOT NULL AND deadline <= ? AND deadline >= ? AND status != 'done' ORDER BY deadline ASC",
            (cutoff, today),
        ).fetchall()

    if not tasks:
        await update.message.reply_text("✅ Không có task nào sắp đến hạn trong 7 ngày tới.")
        return

    lines = [f"⏰ *Sắp đến hạn ({len(tasks)} task)*\n"]
    for t in tasks:
        days_left = (date.fromisoformat(t["deadline"]) - now().date()).days
        urgency = "🔴" if days_left <= 1 else "🟡" if days_left <= 3 else "🟢"
        lines.append(f"{urgency} Còn {days_left} ngày | {format_task(t)}")
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── MEMBERS ──────────────────────────────────────────────────────────────────

async def cmd_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    members = get_all_members()
    if not members:
        await update.message.reply_text("Chưa có thành viên nào. Mọi người hãy /start!")
        return

    lines = ["👥 *Thành viên nhóm*\n"]
    for m in members:
        with get_conn() as conn:
            open_tasks = conn.execute(
                "SELECT COUNT(*) as c FROM tasks WHERE assignee_id=? AND status!='done'", (m["id"],)
            ).fetchone()["c"]
        uname = f"@{m['username']}" if m["username"] else ""
        lines.append(f"• *{m['display_name']}* {uname} — {open_tasks} task đang mở")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── DELETE ───────────────────────────────────────────────────────────────────

async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    if not context.args:
        await update.message.reply_text("Usage: /delete <task_id>")
        return
    try:
        task_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID phải là số.")
        return
    task = get_task(task_id)
    if not task:
        await update.message.reply_text(f"❌ Không tìm thấy task #{task_id}")
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Xác nhận", callback_data=f"delete_confirm_{task_id}"),
            InlineKeyboardButton("❌ Hủy", callback_data=f"delete_cancel_{task_id}"),
        ]
    ])
    await update.message.reply_text(
        f"⚠️ Xóa Task *#{task_id}: {task['title']}*?",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


# ─── REPORTS ──────────────────────────────────────────────────────────────────

def build_daily_report():
    today = now().date().isoformat()
    with get_conn() as conn:
        done_today = conn.execute(
            "SELECT t.*, m.display_name FROM tasks t LEFT JOIN members m ON t.assignee_id=m.id "
            "WHERE t.status='done' AND DATE(t.updated_at)=?",
            (today,),
        ).fetchall()

        open_tasks = conn.execute(
            "SELECT t.*, m.display_name FROM tasks t LEFT JOIN members m ON t.assignee_id=m.id "
            "WHERE t.status != 'done' ORDER BY t.priority DESC"
        ).fetchall()

        overdue = conn.execute(
            "SELECT t.*, m.display_name FROM tasks t LEFT JOIN members m ON t.assignee_id=m.id "
            "WHERE t.deadline < ? AND t.status != 'done'",
            (today,),
        ).fetchall()

    report = [f"📊 *Báo cáo ngày {today}*\n"]

    report.append(f"✅ *Hoàn thành hôm nay ({len(done_today)})*")
    for t in done_today:
        report.append(f"  • #{t['id']} {t['title']} — {t['display_name']}")

    report.append(f"\n📋 *Đang mở ({len(open_tasks)})*")
    for t in open_tasks:
        dl = f" | ⏰{t['deadline']}" if t["deadline"] else ""
        report.append(f"  • #{t['id']} {t['title']} [{t['status']}]{dl} — {t['display_name']}")

    if overdue:
        report.append(f"\n🚨 *Quá hạn ({len(overdue)})*")
        for t in overdue:
            report.append(f"  • #{t['id']} {t['title']} | ⏰{t['deadline']} — {t['display_name']}")

    return "\n".join(report)


def build_weekly_report():
    today = now().date()
    week_start = (today - timedelta(days=today.weekday())).isoformat()
    week_end = today.isoformat()

    with get_conn() as conn:
        done_week = conn.execute(
            "SELECT t.*, m.display_name FROM tasks t LEFT JOIN members m ON t.assignee_id=m.id "
            "WHERE t.status='done' AND DATE(t.updated_at) BETWEEN ? AND ?",
            (week_start, week_end),
        ).fetchall()

        open_tasks = conn.execute(
            "SELECT COUNT(*) as c FROM tasks WHERE status != 'done'"
        ).fetchone()["c"]

        members = conn.execute("SELECT * FROM members").fetchall()

    report = [f"📊 *Báo cáo tuần ({week_start} → {week_end})*\n"]
    report.append(f"✅ Hoàn thành: *{len(done_week)} task*")
    report.append(f"📋 Còn đang mở: *{open_tasks} task*\n")

    report.append("👤 *Theo thành viên:*")
    for m in members:
        done = len([t for t in done_week if t["display_name"] == m["display_name"]])
        with get_conn() as conn:
            pending = conn.execute(
                "SELECT COUNT(*) as c FROM tasks WHERE assignee_id=? AND status!='done'", (m["id"],)
            ).fetchone()["c"]
        report.append(f"  • {m['display_name']}: {done} done, {pending} đang chờ")

    return "\n".join(report)


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    await update.message.reply_text(build_daily_report(), parse_mode="Markdown")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_member(update.effective_user)
    await update.message.reply_text(build_weekly_report(), parse_mode="Markdown")


# ─── SCHEDULED JOBS ───────────────────────────────────────────────────────────

async def job_deadline_reminder(app):
    """Run every morning at 8AM — remind about tasks due today & tomorrow."""
    today = now().date().isoformat()
    tomorrow = (now().date() + timedelta(days=1)).isoformat()

    with get_conn() as conn:
        tasks = conn.execute(
            "SELECT t.*, m.display_name, m.telegram_id FROM tasks t "
            "LEFT JOIN members m ON t.assignee_id=m.id "
            "WHERE t.deadline IN (?,?) AND t.status != 'done'",
            (today, tomorrow),
        ).fetchall()

    if not tasks:
        return

    for task in tasks:
        days_left = (date.fromisoformat(task["deadline"]) - now().date()).days
        urgency = "🔴 HÔM NAY!" if days_left == 0 else "🟡 Ngày mai"
        msg = (
            f"⏰ *Nhắc deadline* — {urgency}\n\n"
            f"Task: *{task['title']}*\n"
            f"Deadline: {task['deadline']}\n"
            f"Status: {STATUS_EMOJI.get(task['status'], '?')} {task['status']}\n\n"
            f"Dùng `/doing {task['id']}` hoặc `/done {task['id']}` để cập nhật!"
        )
        # Send to group chats
        groups = get_group_chats()
        for g in groups:
            try:
                await app.bot.send_message(chat_id=g["chat_id"], text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Cannot send to group {g['chat_id']}: {e}")

        # DM assignee
        if task["telegram_id"]:
            try:
                await app.bot.send_message(chat_id=task["telegram_id"], text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Cannot DM user {task['telegram_id']}: {e}")


async def job_daily_report(app):
    """Send daily report to all groups at 6PM."""
    report = build_daily_report()
    groups = get_group_chats()
    for g in groups:
        try:
            await app.bot.send_message(chat_id=g["chat_id"], text=report, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Daily report to {g['chat_id']}: {e}")


async def job_weekly_report(app):
    """Send weekly report every Friday at 5PM."""
    report = build_weekly_report()
    groups = get_group_chats()
    for g in groups:
        try:
            await app.bot.send_message(chat_id=g["chat_id"], text=report, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Weekly report to {g['chat_id']}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("my", cmd_my))
    app.add_handler(CommandHandler("task", cmd_task))
    app.add_handler(CommandHandler("done", cmd_done))
    app.add_handler(CommandHandler("doing", cmd_doing))
    app.add_handler(CommandHandler("stuck", cmd_stuck))
    app.add_handler(CommandHandler("todo", cmd_todo))
    app.add_handler(CommandHandler("deadline", cmd_deadline))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("members", cmd_members))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("week", cmd_week))

    # Callbacks & messages
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Scheduler
    scheduler = AsyncIOScheduler(timezone=str(TIMEZONE))
    scheduler.add_job(
        lambda: app.create_task(job_deadline_reminder(app)),
        "cron", hour=8, minute=0,
    )
    scheduler.add_job(
        lambda: app.create_task(job_daily_report(app)),
        "cron", hour=18, minute=0,
    )
    scheduler.add_job(
        lambda: app.create_task(job_weekly_report(app)),
        "cron", day_of_week="fri", hour=17, minute=0,
    )
    scheduler.start()

    logger.info("Bot started. Polling...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

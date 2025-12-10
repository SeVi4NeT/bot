# -*- coding: utf-8 -*-
# Совместим с python-telegram-bot v13.x (sync API, Updater/Dispatcher)

import csv, hashlib, json, re, requests, time, warnings, os, sys, tempfile, logging, platform
from io import BytesIO
from pathlib import Path
from bs4 import BeautifulSoup
from telegram import InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

from datetime import datetime, date, time as dtime, timedelta
import pytz   # pip install pytz
TZ = pytz.timezone("Europe/Kyiv")

# Playwright для рендера JS (опциональный fallback)
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_OK = True
except Exception:
    _PLAYWRIGHT_OK = False

warnings.filterwarnings("ignore", category=UserWarning, module="telegram.utils.request")

# ─────────────────────────── НАСТРОЙКИ ───────────────────────────

# !!! ВСТАВЬ СВОЙ ТОКЕН !!!
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip() or "8328849866:AAEL0hvWYv-esVYVXTHVQ9rnl-kc-IImAIY"

# Адрес страницы с таблицей
PAGE_URL = os.getenv("PAGE_URL", "https://off.energy.mk.ua").strip()

# Период проверки (мин)
CHECK_INTERVAL_MIN = int(os.getenv("CHECK_INTERVAL_MIN", "1"))

# Если знаешь точный селектор таблицы — укажи; иначе оставь пустым
TABLE_SELECTOR = os.getenv("TABLE_SELECTOR", "").strip()   # например: "#tabSchedule table"

# Папка с данными (можно переопределить через DATA_DIR)
def _default_data_dir() -> Path:
    if os.getenv("DATA_DIR"):
        return Path(os.getenv("DATA_DIR"))
    # Кросс-платформенный дефолт
    if platform.system().lower().startswith("win"):
        base = Path(os.getenv("LOCALAPPDATA", str(Path.home())))
    else:
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return Path(base) / "offenergy-bot"

DATA_DIR = _default_data_dir()
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state_table.json"
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"

# Логирование
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("offenergy-bot")

# ─────────────────────────── ВСПОМОГАТЕЛЬНОЕ ───────────────────────────

_whitespace_re = re.compile(r"\s+")

def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("load_json(%s) failed: %s", path, e)
            return default
    return default

def save_json(path: Path, data):
    """Атомная запись JSON с запасным путём."""
    txt = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(txt, encoding="utf-8")
        tmp.replace(path)
    except PermissionError:
        fallback = Path(tempfile.gettempdir()) / ("offenergy-bot_" + path.name)
        fallback.write_text(txt, encoding="utf-8")
    except Exception as e:
        log.error("save_json(%s) failed: %s", path, e)

STATE = load_json(STATE_FILE, {})
SUBSCRIBERS = set(load_json(SUBSCRIBERS_FILE, []))

def _clean_text(s: str) -> str:
    if not s:
        return ""
    return _whitespace_re.sub(" ", s.replace("\xa0", " ")).strip()

def normalize_cell_text(tag, include_class=False):
    text = _clean_text(tag.get_text(separator=" ", strip=True))
    if include_class:
        classes = " ".join(sorted(tag.get("class", [])))
        style = tag.get("style", "")
        if classes or style:
            text = f"{text}{{{classes}|{style}}}"
    return text

def _make_soup(html: str):
    # Сначала быстрый парсер lxml, затем встроенный html.parser
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(html, parser)
        except Exception as e:
            log.debug("_make_soup with parser=%s failed: %s", parser, e)
            continue
    raise RuntimeError("Нет рабочего HTML-парсера (нужно установить 'lxml').")

def _extract_table_from_soup(soup):
    table = soup.select_one(TABLE_SELECTOR) if TABLE_SELECTOR else None
    if not table:
        for t in soup.find_all("table"):
            ths = [normalize_cell_text(th) for th in t.find_all("th")]
            if ths and ("Час" in ths[0] or ths[0].lower().startswith("час")):
                table = t
                break
        if not table:
            table = soup.find("table")
    if not table:
        return None, None

    headers = [normalize_cell_text(th) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        rows.append([normalize_cell_text(td, include_class=True) for td in tds])

    if not headers and rows:
        headers = [f"col{i+1}" for i in range(len(rows[0]))]
    return headers, rows

def _looks_unrendered(headers, rows):
    joined_h = " ".join(headers or [])
    if "{{" in joined_h or "}}" in joined_h:
        return True
    if not headers or len(headers) <= 1:
        return True
    count_tpl = 0
    for r in (rows or [])[:10]:
        if any("{{" in c or "}}" in c for c in r):
            count_tpl += 1
    return count_tpl >= 2

# ─────────────────────────── ЗАГРУЗКА ТАБЛИЦЫ ───────────────────────────

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"

def fetch_table():
    # 1) обычный GET
    r = requests.get(PAGE_URL, timeout=60, headers={"User-Agent": _UA})
    r.raise_for_status()
    soup = _make_soup(r.text)
    headers, rows = _extract_table_from_soup(soup)
    need_render = (headers is None or _looks_unrendered(headers, rows))

    # 2) если нужно — пробуем Playwright (мягко)
    if need_render and _PLAYWRIGHT_OK:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                ctx = browser.new_context()
                page = ctx.new_page()
                page.goto(PAGE_URL, wait_until="networkidle", timeout=45000)
                try:
                    sel = TABLE_SELECTOR or "table"
                    page.wait_for_selector(sel, timeout=20000)
                except Exception:
                    pass
                html = page.content()
                browser.close()
            soup2 = _make_soup(html)
            headers2, rows2 = _extract_table_from_soup(soup2)
            if headers2 and rows2:
                headers, rows = headers2, rows2
        except Exception as e:
            log.warning("Playwright fallback failed: %s", e)

    if not headers or not rows:
        raise RuntimeError("Не удалось найти таблицу (после GET и Playwright).")

    return headers, rows

def table_signature(headers, rows):
    payload = ("\n".join(headers) + "\n" + "\n".join("|".join(r) for r in rows)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def diff_tables(prev_headers, prev_rows, headers, rows, cap=30):
    changes_preview, changes_all = [], []
    nrows = max(len(prev_rows), len(rows))
    for i in range(nrows):
        prev_row = prev_rows[i] if i < len(prev_rows) else []
        row = rows[i] if i < len(rows) else []
        time_val = (row[0] if row else (prev_row[0] if prev_row else f"row{i+1}")).split("{",1)[0]
        ncols = max(len(prev_row), len(row))
        for j in range(ncols):
            old = prev_row[j] if j < len(prev_row) else ""
            new = row[j] if j < len(row) else ""
            if old != new:
                col = headers[j] if j < len(headers) else f"col{j+1}"
                changes_all.append([time_val, col, old, new])
                if len(changes_preview) < cap:
                    changes_preview.append((time_val, col, old.split("{")[0], new.split("{")[0]))
    return changes_preview, changes_all

# ─────────────────────────── ЛОГИКА /when ───────────────────────────

def clean_cell(val: str) -> str:
    return val.split("{", 1)[0].strip() if val else ""

def _parse_cell_meta(val: str):
    classes, style = "", ""
    if val and "{" in val and "}" in val:
        meta = val.split("{", 1)[1].split("}", 1)[0]
        parts = meta.split("|", 1)
        classes = (parts[0] if len(parts) > 0 else "").lower()
        style   = (parts[1] if len(parts) > 1 else "").lower()
    return classes, style

def _is_on_by_color(classes: str, style: str) -> bool:
    c = classes.lower().strip()
    s = style.lower().replace(" ", "")
    if "item-enable" in c:
        return True
    if "#a1eebd" in s or "rgb(161,238,189)" in s:
        return True
    if any(k in c for k in ("table-success", "bg-success", "text-bg-success", "green")):
        return True
    if any(h in s for h in ("#28a745", "#198754", "#2ecc71", "#00ff00", "background:green", "background-color:green")):
        return True
    return False

def _is_off_by_color(classes: str, style: str) -> bool:
    c = classes.lower().strip()
    s = style.lower().replace(" ", "")
    if "item-off" in c or "item-probably" in c:
        return True
    if ("#f6d6d6" in s or "rgb(246,214,214)" in s or
        "#f6f7c4" in s or "rgb(246,247,196)" in s):
        return True
    if any(k in c for k in ("table-warning", "table-danger", "bg-warning", "bg-danger",
                            "text-bg-warning", "text-bg-danger", "warning", "danger", "yellow", "red")):
        return True
    if any(h in s for h in ("#ffc107", "#ffcc00", "#f1c40f", "#dc3545", "#ff0000",
                            "background:yellow", "background-color:yellow",
                            "background:red", "background-color:red")):
        return True
    return False

def _cell_state_by_color(queue_name: str, val: str) -> str:
    text = clean_cell(val)
    classes, style = _parse_cell_meta(val)
    if _is_off_by_color(classes, style):
        return "off"
    if _is_on_by_color(classes, style):
        return "on"
    return "off" if text == queue_name else "on"

def parse_time_range(s: str):
    s = str(s).strip().replace("–", "-").replace("—", "-").replace(" ", "").replace("\xa0", "")
    if not s or "-" not in s:
        return None, None
    try:
        a, b = s.split("-", 1)
        today = date.today()
        h1, m1 = map(int, a.split(":"))
        h2, m2 = map(int, b.split(":"))
        start = TZ.localize(datetime.combine(today, dtime(h1, m1)))
        end = TZ.localize(datetime.combine(today, dtime(h2, m2)))
        if end <= start:
            end += timedelta(days=1)
        return start, end
    except Exception:
        return None, None

def build_schedule_map(headers, rows):
    if not headers or not rows:
        return [], {}
    times = []
    norm_headers = [_clean_text(h) for h in headers]
    cols = {h: [] for h in norm_headers[1:]}  # без первого столбца времени
    for r in rows:
        tr = clean_cell(r[0]) if r else ""
        start, end = parse_time_range(tr)
        if not start:
            continue
        times.append((start, end))
        for j, h in enumerate(norm_headers[1:], start=1):
            val = r[j] if j < len(r) else ""
            cols[h].append(_clean_text(val))
    return times, cols

def _column_index(headers, q):
    for i, h in enumerate(headers):
        if _clean_text(h) == q:
            return i
    return -1

def intervals_for_queue(queue_name: str, headers, rows):
    times, cols = build_schedule_map(headers, rows)
    q = _clean_text(queue_name)
    if q not in cols or not times:
        return [], list(cols.keys())

    col_idx = _column_index(headers, q)
    if col_idx < 0:
        return [], list(cols.keys())

    merged = []
    cur_state, cur_start = None, None
    for i, (tstart, tend) in enumerate(times):
        val_raw = rows[i][col_idx] if (i < len(rows) and col_idx < len(rows[i])) else ""
        state = _cell_state_by_color(q, val_raw)
        if cur_state is None:
            cur_state, cur_start = state, tstart
        elif state != cur_state:
            merged.append((cur_start, tstart, cur_state))
            cur_state, cur_start = state, tstart
        if i == len(times) - 1:
            merged.append((cur_start, tend, cur_state))
    return merged, list(cols.keys())

def format_intervals_readable(items, limit=16, from_now_only=True):
    now = datetime.now(TZ)
    out = []
    shown = 0
    for s, e, state in items:
        if from_now_only and e <= now:
            continue
        mark = "🚫 немає" if state == "off" else "⚡ є"
        out.append(f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')} — {mark}")
        shown += 1
        if shown >= limit:
            break
    if not out:
        return "На сьогодні інтервали не знайдені."
    return "\n".join(out)

# ─────────────────────────── КНОПКИ / МЕНЮ ───────────────────────────

def build_main_menu(chat_id: int):
    is_sub = chat_id in SUBSCRIBERS
    sub_text = "🔔 Слідкувати за оновленнями (увімкнено)" if is_sub else "🔕 Слідкувати за оновленнями (вимкнено)"
    keyboard = [
        [InlineKeyboardButton(sub_text, callback_data="sub:toggle")],
        [InlineKeyboardButton("🔎 Перевірити світло за чергою", callback_data="menu:queues")],
        [InlineKeyboardButton("🔄 Перевірити оновлення зараз", callback_data="action:check")],
    ]
    return InlineKeyboardMarkup(keyboard)

def _known_columns():
    cols = []
    if STATE.get("headers"):
        cols = [_clean_text(h) for h in STATE["headers"][1:]]
    return cols

def build_queue_keyboard():
    cols = _known_columns()
    if not cols:
        try:
            headers, rows = fetch_table()
            STATE.update({"headers": headers, "rows": rows})
            save_json(STATE_FILE, STATE)
            cols = [_clean_text(h) for h in headers[1:]]
        except Exception as e:
            log.warning("build_queue_keyboard: fetch_table failed: %s", e)
            cols = []
    if not cols:
        cols = [f"{a}.{b}" for a in range(1,7) for b in (1,2)]
    rows = []
    row = []
    for i, c in enumerate(cols, 1):
        row.append(InlineKeyboardButton(c, callback_data=f"qsel:{c}"))
        if i % 4 == 0:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)

def notify(context, text, csv_rows=None):
    bot = context.bot
    dead = []
    for chat_id in list(SUBSCRIBERS):
        try:
            if csv_rows:
                bio = BytesIO()
                w = csv.writer(bio)
                w.writerow(["time","column","old","new"])
                for r in csv_rows:
                    w.writerow(r)
                bio.seek(0)
                bot.send_document(
                    chat_id=chat_id,
                    document=InputFile(bio, filename="table_diff.csv"),
                    caption=text[:1024],
                    parse_mode='HTML'
                )
            else:
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML',
                    disable_web_page_preview=False
                )
        except Exception as e:
            log.warning("notify to %s failed: %s", chat_id, e)
            dead.append(chat_id)
    for d in dead:
        SUBSCRIBERS.discard(d)
    save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))

def check_job(context):
    global STATE
    try:
        headers, rows = fetch_table()
    except Exception as e:
        log.warning("check_job: fetch_table failed: %s", e)
        return
    sig = table_signature(headers, rows)
    if not STATE:
        STATE = {"sha256": sig, "headers": headers, "rows": rows, "ts": int(time.time())}
        save_json(STATE_FILE, STATE)
        return
    if sig != STATE.get("sha256"):
        preview, csv_rows = diff_tables(STATE.get("headers", []), STATE.get("rows", []), headers, rows)
        STATE = {"sha256": sig, "headers": headers, "rows": rows, "ts": int(time.time())}
        save_json(STATE_FILE, STATE)
        msg = [f"<b>Таблиця оновлена</b>\n<a href='{PAGE_URL}'>Відкрити</a>",
               f"Змінено ячейок: {len(csv_rows)}"]
        if preview:
            msg.append("<b>Перші зміни:</b>")
            for t, c, o, n in preview:
                msg.append(f"• <code>{t}</code> — <b>{c}</b>: <code>{o or '—'}</code> → <code>{n or '—'}</code>")
        notify(context, "\n".join(msg), csv_rows)

# ─────────────────────────── КОМАНДЫ ───────────────────────────

def start_cmd(update, context):
    chat_id = update.effective_chat.id
    text = (f"Привіт! Відстежую таблицю {PAGE_URL}. Перевірка кожні {CHECK_INTERVAL_MIN} хв.\n"
            f"Вибери дію кнопками нижче.")
    update.message.reply_text(text, reply_markup=build_main_menu(chat_id), disable_web_page_preview=True)

def stop_cmd(update, context):
    SUBSCRIBERS.discard(update.effective_chat.id)
    save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))
    update.message.reply_text("Слідкування вимкнено.", reply_markup=build_main_menu(update.effective_chat.id))

def check_cmd(update, context):
    check_job(context)
    update.message.reply_text("Перевірив.", reply_markup=build_main_menu(update.effective_chat.id))

def when_cmd(update, context):
    """Текстовая версия: /when 4.1"""
    global STATE
    args = context.args or []
    if not args:
        update.message.reply_text("Використання: /when <черга>\nНапр.: /when 1.1 або /when 5.2")
        return
    queue = args[0].strip()
    if not STATE:
        try:
            headers, rows = fetch_table()
            STATE.update({"headers": headers, "rows": rows})
            save_json(STATE_FILE, STATE)
        except Exception as e:
            log.warning("when_cmd: fetch_table failed: %s", e)
            update.message.reply_text("Не вдалось отримати таблицю зараз. Спробуйте ще раз.")
            return
    headers = STATE.get("headers", [])
    rows = STATE.get("rows", [])
    intervals, known_cols = intervals_for_queue(queue, headers, rows)
    if not intervals:
        sample = ", ".join(list(known_cols)[:12])
        update.message.reply_text(f"Чергу «{queue}» не знайдено.\nСпробуйте: {sample} ...")
        return
    text = f"<b>Черга {queue} — сьогодні</b>\n" + format_intervals_readable(intervals)
    update.message.reply_text(text, parse_mode='HTML', reply_markup=build_main_menu(update.effective_chat.id))

# ─────────────────────────── CALLBACKS ───────────────────────────

def button_cb(update, context):
    global STATE, SUBSCRIBERS
    q = update.callback_query
    data = q.data or ""
    chat_id = q.message.chat.id
    try:
        if data == "sub:toggle":
            if chat_id in SUBSCRIBERS:
                SUBSCRIBERS.discard(chat_id)
                save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))
                q.answer("Сповіщення вимкнено")
            else:
                SUBSCRIBERS.add(chat_id)
                save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))
                q.answer("Сповіщення увімкнено")
            q.edit_message_reply_markup(reply_markup=build_main_menu(chat_id))
            return

        if data == "menu:queues":
            q.answer()
            q.edit_message_text("Оберіть вашу чергу:", reply_markup=build_queue_keyboard())
            return

        if data == "menu:main":
            q.answer()
            q.edit_message_text(
                f"Привіт! Відстежую таблицю {PAGE_URL}. Перевірка кожні {CHECK_INTERVAL_MIN} хв.\n"
                f"Вибери дію кнопками нижче.",
                reply_markup=build_main_menu(chat_id),
                disable_web_page_preview=True
            )
            return

        if data == "action:check":
            q.answer("Перевіряю…")
            check_job(context)
            q.edit_message_reply_markup(reply_markup=build_main_menu(chat_id))
            return

        if data.startswith("qsel:"):
            queue = data.split(":", 1)[1]
            if not STATE.get("headers"):
                try:
                    headers, rows = fetch_table()
                    STATE.update({"headers": headers, "rows": rows})
                    save_json(STATE_FILE, STATE)
                except Exception as e:
                    log.warning("button_cb: fetch_table failed: %s", e)
                    q.answer("Не вдалося завантажити таблицю")
                    return
            headers = STATE.get("headers", [])
            rows = STATE.get("rows", [])
            intervals, known_cols = intervals_for_queue(queue, headers, rows)
            if not intervals:
                q.answer("Чергу не знайдено")
                return
            text = f"<b>Черга {queue} — сьогодні</b>\n" + format_intervals_readable(intervals)
            q.edit_message_text(text, parse_mode='HTML', reply_markup=build_queue_keyboard())
            return

        q.answer()
    except Exception as e:
        log.exception("button_cb failed: %s", e)
        q.answer("Сталася помилка")

# ─────────────────────────── MAIN ───────────────────────────

def main():
    if not BOT_TOKEN or "PASTE_YOUR_TELEGRAM_BOT_TOKEN" in BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN (переменная окружения или в коде).")
    updater = Updater(token=BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("stop", stop_cmd))
    dp.add_handler(CommandHandler("check", check_cmd))
    dp.add_handler(CommandHandler("when", when_cmd))
    dp.add_handler(CallbackQueryHandler(button_cb))

    updater.job_queue.run_repeating(check_job, interval=CHECK_INTERVAL_MIN*60, first=0)
    log.info("Bot started. PAGE_URL=%s, DATA_DIR=%s", PAGE_URL, DATA_DIR)
    updater.start_polling(drop_pending_updates=True)
    updater.idle()

if __name__ == "__main__":
    main()

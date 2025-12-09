import csv, hashlib, json, re, requests, time, warnings, os, tempfile
from io import BytesIO
from pathlib import Path
from bs4 import BeautifulSoup
from telegram import InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

from datetime import datetime, date, time as dtime, timedelta
import pytz   # pip install pytz
TZ = pytz.timezone("Europe/Kyiv")

warnings.filterwarnings("ignore", category=UserWarning, module="telegram.utils.request")

# === НАСТРОЙКИ ===
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8328849866:AAEL0hvWYv-esVYVXTHVQ9rnl-kc-IImAIY"
PAGE_URL = "https://off.energy.mk.ua"
CHECK_INTERVAL_MIN = 1
TABLE_SELECTOR = ""   # пусть бот сам найдёт таблицу по "Час"

# --- папка данных в профиле пользователя ---
DATA_DIR = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "offenergy-bot"
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = DATA_DIR / "state_table.json"
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
_whitespace_re = re.compile(r"\s+")
_tpl_re = re.compile(r"\{\{.*?\}\}")

def _strip_tpl(s: str) -> str:
    return _tpl_re.sub("", s or "")

def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default

def save_json(path: Path, data):
    """Атомная запись JSON с запасным путём на случай запрета записи."""
    txt = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(txt, encoding="utf-8")
        tmp.replace(path)
    except PermissionError:
        fallback = Path(tempfile.gettempdir()) / ("offenergy-bot_" + path.name)
        fallback.write_text(txt, encoding="utf-8")

STATE = load_json(STATE_FILE, {})
SUBSCRIBERS = set(load_json(SUBSCRIBERS_FILE, []))

def _clean_text(s: str) -> str:
    if not s: return ""
    return _whitespace_re.sub(" ", s.replace("\xa0", " ")).strip()

def normalize_cell_text(tag, include_class=False):
    text = _clean_text(_strip_tpl(tag.get_text(separator=" ", strip=True)))
    if include_class:
        classes = " ".join(sorted(tag.get("class", [])))
        style = tag.get("style", "")
        if classes or style:
            text = f"{text}{{{classes}|{style}}}"
    return text

def _extract_table_from_soup(soup):
    candidates = []
    if TABLE_SELECTOR:
        t = soup.select_one(TABLE_SELECTOR)
        if t: candidates.append(t)
    candidates.extend(soup.find_all("table"))

    def good(headers, rows):
        if not headers or not rows: return False
        if any(("{{" in h) or ("}}" in h) for h in headers): return False
        if any(any(("{{" in c) or ("}}" in c) for c in r) for r in rows[:10]): return False
        h0 = (headers[0] or "").lower()
        return ("час" in h0) or h0.startswith("час")

    for t in candidates:
        headers = [normalize_cell_text(th) for th in t.find_all("th")]
        rows = []
        for tr in t.find_all("tr"):
            tds = tr.find_all("td")
            if not tds: continue
            rows.append([normalize_cell_text(td, include_class=True) for td in tds])

        if not headers and rows:
            headers = [f"col{i+1}" for i in range(len(rows[0]))]

        if good(headers, rows):
            return headers, rows

    return None, None

def fetch_table():
    r = requests.get(
        PAGE_URL, timeout=60,
        headers={"User-Agent": "Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 Chrome/124 Mobile Safari/537.36"}
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    headers, rows = _extract_table_from_soup(soup)
    if not headers or not rows:
        raise RuntimeError("Не удалось найти корректную таблицу на странице")
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

# ========= /when: цвет -> состояние =========
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
    if "item-enable" in c or "#a1eebd" in s or "rgb(161,238,189)" in s:
        return True
    if any(k in c for k in ("table-success","bg-success","text-bg-success","green")):
        return True
    if any(h in s for h in ("#28a745","#198754","#2ecc71","#00ff00","background:green","background-color:green")):
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
    if any(k in c for k in ("table-warning","table-danger","bg-warning","bg-danger",
                            "text-bg-warning","text-bg-danger","warning","danger","yellow","red")):
        return True
    if any(h in s for h in ("#ffc107","#ffcc00","#f1c40f","#dc3545","#ff0000",
                            "background:yellow","background-color:yellow",
                            "background:red","background-color:red")):
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
    s = str(s).strip().replace("–","-").replace("—","-").replace(" ","").replace("\xa0","")
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
    norm_headers = [_clean_text(_strip_tpl(h)) for h in headers]
    cols = {h: [] for h in norm_headers[1:]}
    for r in rows:
        tr = clean_cell(r[0]) if r else ""
        start, end = parse_time_range(tr)
        if not start:
            continue
        times.append((start, end))
        for j, h in enumerate(norm_headers[1:], start=1):
            val = r[j] if j < len(r) else ""
            cols[h].append(_clean_text(_strip_tpl(val)))
    return times, cols

def _column_index(headers, q):
    for i, h in enumerate(headers):
        if _clean_text(_strip_tpl(h)) == q:
            return i
    return -1

def intervals_for_queue(queue_name: str, headers, rows):
    times, cols = build_schedule_map(headers, rows)
    q = _clean_text(_strip_tpl(queue_name))
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
    out, shown = [], 0
    for s, e, state in items:
        if from_now_only and e <= now:
            continue
        mark = "🚫 немає" if state == "off" else "⚡ є"
        out.append(f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')} — {mark}")
        shown += 1
        if shown >= limit:
            break
    return "\n".join(out) if out else "На сьогодні інтервали не знайдені."

# -------- КНОПКИ / МЕНЮ --------
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
        cols = [_clean_text(_strip_tpl(h)) for h in STATE["headers"][1:]]
        cols = [c for c in cols if c and "{{" not in c and "}}" not in c]
    return cols

def build_queue_keyboard():
    cols = _known_columns()
    if not cols:
        try:
            headers, rows = fetch_table()
            STATE.update({"headers": headers, "rows": rows})
            save_json(STATE_FILE, STATE)
            cols = [_clean_text(_strip_tpl(h)) for h in headers[1:]]
        except Exception:
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

# -------- уведомления и джоб --------
def notify(context, text, csv_rows=None):
    bot = context.bot
    dead = []
    for chat_id in list(SUBSCRIBERS):
        try:
            if csv_rows:
                bio = BytesIO()
                w = csv.writer(bio); w.writerow(["time","column","old","new"])
                for r in csv_rows: w.writerow(r)
                bio.seek(0)
                bot.send_document(chat_id=chat_id, document=InputFile(bio, filename="table_diff.csv"),
                                  caption=text[:1024], parse_mode='HTML')
            else:
                bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', disable_web_page_preview=False)
        except Exception:
            dead.append(chat_id)
    for d in dead:
        SUBSCRIBERS.discard(d)
    save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))

def check_job(context):
    global STATE
    try:
        headers, rows = fetch_table()
    except Exception:
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

# ---------- команды ----------
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
    args = context.args or []
    if not args:
        update.message.reply_text("Використання: /when <черга>\nНапр.: /when 1.1 або /when 5.2")
        return
    queue = args[0].strip()
    if not STATE.get("headers"):
        try:
            headers, rows = fetch_table()
            STATE.update({"headers": headers, "rows": rows})
            save_json(STATE_FILE, STATE)
        except Exception:
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

def simulate_change_cmd(update, context):
    """Локальна симуляція зміни таблиці та розсилка повідомлення."""
    global STATE
    try:
        if not STATE or not STATE.get("headers") or not STATE.get("rows"):
            headers, rows = fetch_table()
            STATE = {"sha256": table_signature(headers, rows),
                     "headers": headers, "rows": rows, "ts": int(time.time())}
            save_json(STATE_FILE, STATE)

        old_headers = STATE["headers"]; old_rows = STATE["rows"]
        headers = old_headers[:]; rows = [r[:] for r in old_rows]
        if not rows or len(rows[0]) < 2:
            update.message.reply_text("Недостатньо даних для симуляції.")
            return
        cell = rows[0][1]
        parts = cell.split("{", 1)
        text = parts[0].strip()
        meta = "{" + parts[1] if len(parts) > 1 else ""
        marker = " (test)"
        text = text[:-len(marker)] if text.endswith(marker) else (text + marker)
        rows[0][1] = (text + meta).strip()

        preview, csv_rows = diff_tables(old_headers, old_rows, headers, rows)
        STATE = {"sha256": table_signature(headers, rows),
                 "headers": headers, "rows": rows, "ts": int(time.time())}
        save_json(STATE_FILE, STATE)

        msg = [f"<b>Таблиця оновлена (симуляція)</b>\n<a href='{PAGE_URL}'>Відкрити</a>",
               f"Змінено ячейок: {len(csv_rows)}"]
        if preview:
            msg.append("<b>Перші зміни:</b>")
            for t, c, o, n in preview[:10]:
                msg.append(f"• <code>{t}</code> — <b>{c}</b>: <code>{o or '—'}</code> → <code>{n or '—'}</code>")
        notify(context, "\n".join(msg), csv_rows)
        update.message.reply_text("Симуляцію виконано. Надіслано сповіщення ✅", parse_mode='HTML')
    except Exception as e:
        update.message.reply_text(f"Помилка симуляції: {e}")

# ---------- CALLBACK кнопок ----------
def button_cb(update, context):
    global STATE, SUBSCRIBERS
    q = update.callback_query
    data = q.data or ""
    chat_id = q.message.chat.id
    try:
        if data == "sub:toggle":
            if chat_id in SUBSCRIBERS:
                SUBSCRIBERS.discard(chat_id); q.answer("Сповіщення вимкнено")
            else:
                SUBSCRIBERS.add(chat_id); q.answer("Сповіщення увімкнено")
            save_json(SUBSCRIBERS_FILE, list(SUBSCRIBERS))
            q.edit_message_reply_markup(reply_markup=build_main_menu(chat_id)); return
        if data == "menu:queues":
            q.answer(); q.edit_message_text("Оберіть вашу чергу:", reply_markup=build_queue_keyboard()); return
        if data == "menu:main":
            q.answer()
            q.edit_message_text(
                f"Привіт! Відстежую таблицю {PAGE_URL}. Перевірка кожні {CHECK_INTERVAL_MIN} хв.\n"
                f"Вибери дію кнопками нижче.",
                reply_markup=build_main_menu(chat_id), disable_web_page_preview=True
            ); return
        if data == "action:check":
            q.answer("Перевіряю…"); check_job(context)
            q.edit_message_reply_markup(reply_markup=build_main_menu(chat_id)); return
        if data.startswith("qsel:"):
            queue = data.split(":", 1)[1]
            if not STATE.get("headers"):
                try:
                    headers, rows = fetch_table()
                    STATE.update({"headers": headers, "rows": rows}); save_json(STATE_FILE, STATE)
                except Exception:
                    q.answer("Не вдалося завантажити таблицю"); return
            headers = STATE.get("headers", []); rows = STATE.get("rows", [])
            intervals, _ = intervals_for_queue(queue, headers, rows)
            if not intervals: q.answer("Чергу не знайдено"); return
            text = f"<b>Черга {queue} — сьогодні</b>\n" + format_intervals_readable(intervals)
            q.edit_message_text(text, parse_mode='HTML', reply_markup=build_queue_keyboard()); return
        q.answer()
    except Exception:
        q.answer("Сталася помилка")

def main():
    if not BOT_TOKEN:
        raise SystemExit("Укажи токен через переменную окружения BOT_TOKEN.")
    updater = Updater(token=BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_cmd))
    dp.add_handler(CommandHandler("stop", stop_cmd))
    dp.add_handler(CommandHandler("check", check_cmd))
    dp.add_handler(CommandHandler("when", when_cmd))
    dp.add_handler(CommandHandler("simulate_change", simulate_change_cmd))
    dp.add_handler(CallbackQueryHandler(button_cb))

    updater.job_queue.run_repeating(check_job, interval=CHECK_INTERVAL_MIN*60, first=0)
    updater.start_polling(drop_pending_updates=True)
    updater.idle()

if __name__ == "__main__":
    # страховка: требуем совместимую версию urllib3
    try:
        import urllib3
        v = tuple(int(x) for x in urllib3.__version__.split(".")[:2])
        if v >= (2,0):
            raise SystemExit("Нужен urllib3<2 для PTB 13.15. Установи: pip install 'urllib3<2'")
    except Exception:
        pass
    main()

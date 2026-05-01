import os
import re
import json
import time
import queue
import sqlite3
import threading
import urllib.parse
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import requests
import customtkinter as ctk
from tkinter import filedialog, messagebox

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# =========================================================
# CONFIG / CONSTANTS
# =========================================================
APP_TITLE = "TELE SHOP BOT - VietQR (A) + AutoBank V3 (PC 24/7)"
DB_FILE = "tele_shop.sqlite3"
CONFIG_FILE = "tele_shop_config.json"

# VietQR Quick Link: https://img.vietqr.io/image/<BANK_ID>-<STK>-<TEMPLATE>.png?amount=...&addInfo=...&accountName=...
DEFAULT_VIETQR_TEMPLATE = "compact2"

# VietQR bank_id list (quick-link). Bạn có thể thêm nếu muốn.
VIETQR_BANKS = [
    ("Vietcombank", "vietcombank"),
    ("Techcombank", "techcombank"),
    ("MB Bank", "mbbank"),
    ("ACB", "acb"),
    ("VietinBank", "vietinbank"),
    ("BIDV", "bidv"),
    ("Sacombank", "sacombank"),
    ("TPBank", "tpbank"),
]

# Sieuthicode API V3 endpoints
SIEUTHICODE_V3_ENDPOINTS = {
    "TPBank": "historyapitpbv3",
    "Vietcombank": "historyapivcbv3",
    "ACB": "historyapiacbv3",
    "MBBank": "historyapimbv3",
    "BIDV": "historyapibidvv3",
    "VietinBank": "historyapiviettinv3",
    "SeaBank": "historyapiseabankv3",
    "MSB": "historyapimsbv3",
    "Timo": "historyapitimov3",
}

# Regex parse Napid <id>
NAPID_RE = re.compile(r"\bNapid\s+(\d+)\b", re.IGNORECASE)

# Conversation states
BUY_WAIT_COUPON_CHOICE, BUY_WAIT_COUPON_CODE, DEPOSIT_WAIT_AMOUNT = range(3)

# In-memory admin mode toggle
ADMIN_MODE = set()

# =========================================================
# HELPERS
# =========================================================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_int(x, default=0) -> int:
    try:
        return int(str(x).replace(",", "").strip())
    except Exception:
        return default

def vietqr_image_url(bank_id: str, account_no: str, template: str, amount: int, add_info: str, account_name: str) -> str:
    base = f"https://img.vietqr.io/image/{bank_id}-{account_no}-{template}.png"
    qs = {
        "amount": str(max(0, int(amount))),
        "addInfo": add_info,
        "accountName": account_name or "",
    }
    return base + "?" + urllib.parse.urlencode(qs, safe="")

def build_sieuthicode_v3_url(bank_name: str, password: str, stk: str, token: str) -> str:
    ep = SIEUTHICODE_V3_ENDPOINTS.get(bank_name)
    if not ep:
        raise ValueError(f"Bank V3 không hỗ trợ: {bank_name}")
    return f"https://api.sieuthicode.net/{ep}/{password}/{stk}/{token}"

# =========================================================
# DB
# =========================================================
def db_connect():
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db_connect()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        balance INTEGER DEFAULT 0,
        ref_by INTEGER,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        is_active INTEGER DEFAULT 1
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER,
        name TEXT NOT NULL,
        price INTEGER NOT NULL,
        stock INTEGER DEFAULT 0,
        deliver_text TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        product_id INTEGER,
        price INTEGER,
        discount INTEGER DEFAULT 0,
        final_price INTEGER,
        coupon_code TEXT,
        status TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS deposits(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount INTEGER,
        description TEXT,
        trans_id TEXT,
        status TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS coupons(
        code TEXT PRIMARY KEY,
        discount_amount INTEGER DEFAULT 0,
        discount_percent INTEGER DEFAULT 0,
        apply_product_id INTEGER DEFAULT NULL,
        min_order INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        max_uses INTEGER DEFAULT 0,
        used_count INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS affiliate(
        user_id INTEGER PRIMARY KEY,
        percent INTEGER DEFAULT 5
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS affiliate_earnings(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_user_id INTEGER,
        order_id INTEGER,
        amount INTEGER,
        created_at TEXT
    )
    """)

    # chống trùng giao dịch
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bank_seen(
        transaction_id TEXT PRIMARY KEY,
        seen_at TEXT
    )
    """)

    con.commit()
    con.close()

# ---------- user helpers ----------
def upsert_user(tg_user):
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (tg_user.id,))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO users(user_id, username, first_name, balance, ref_by, created_at) VALUES(?,?,?,?,?,?)",
            (tg_user.id, tg_user.username or "", tg_user.first_name or "", 0, None, now_str())
        )
    else:
        cur.execute(
            "UPDATE users SET username=?, first_name=? WHERE user_id=?",
            (tg_user.username or "", tg_user.first_name or "", tg_user.id)
        )
    con.commit()
    con.close()

def set_ref_if_empty(user_id: int, ref_by: int):
    if user_id == ref_by:
        return
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT ref_by FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    if r and r["ref_by"] is None:
        cur.execute("UPDATE users SET ref_by=? WHERE user_id=?", (ref_by, user_id))
    con.commit()
    con.close()

def get_user(user_id: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    con.close()
    return r

def get_balance(user_id: int) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    con.close()
    return int(r["balance"] or 0) if r else 0

def add_balance(user_id: int, amount: int):
    con = db_connect()
    cur = con.cursor()

    # nếu user chưa tồn tại thì tạo trước
    cur.execute("""
        INSERT OR IGNORE INTO users(user_id, username, first_name, balance, ref_by, created_at)
        VALUES(?, '', '', 0, NULL, ?)
    """, (int(user_id), now_str()))

    # cộng tiền
    cur.execute("""
        UPDATE users 
        SET balance = COALESCE(balance,0) + ? 
        WHERE user_id=?
    """, (int(amount), int(user_id)))

    con.commit()
    con.close()

def subtract_balance(user_id: int, amount: int) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    if not r:
        con.close()
        return False
    bal = int(r["balance"] or 0)
    if bal < amount:
        con.close()
        return False
    cur.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (int(amount), user_id))
    con.commit()
    con.close()
    return True

# ---------- categories/products ----------
def add_category(name: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT INTO categories(name, is_active) VALUES(?,1)", (name,))
    con.commit()
    cid = cur.lastrowid
    con.close()
    return cid

def edit_category(cid: int, name: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("UPDATE categories SET name=? WHERE id=?", (name, cid))
    con.commit()
    con.close()

def delete_category(cid: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("UPDATE categories SET is_active=0 WHERE id=?", (cid,))
    con.commit()
    con.close()

def list_categories():
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT id, name FROM categories WHERE is_active=1 ORDER BY id DESC")
    rows = cur.fetchall()
    con.close()
    return rows

def add_product(category_id: int, name: str, price: int, stock: int, deliver_text: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO products(category_id, name, price, stock, deliver_text, is_active) VALUES(?,?,?,?,?,1)",
        (category_id, name, int(price), int(stock), deliver_text or "")
    )
    con.commit()
    pid = cur.lastrowid
    con.close()
    return pid

def edit_product(pid: int, name: str, price: int, stock: int, deliver_text: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "UPDATE products SET name=?, price=?, stock=?, deliver_text=? WHERE id=?",
        (name, int(price), int(stock), deliver_text or "", pid)
    )
    con.commit()
    con.close()

def delete_product(pid: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("UPDATE products SET is_active=0 WHERE id=?", (pid,))
    con.commit()
    con.close()

def list_products_by_category(cid: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        SELECT id, name, price, stock FROM products
        WHERE is_active=1 AND category_id=?
        ORDER BY id DESC
    """, (cid,))
    rows = cur.fetchall()
    con.close()
    return rows

def get_product(pid: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT * FROM products WHERE id=? AND is_active=1", (pid,))
    r = cur.fetchone()
    con.close()
    return r

def decrement_stock(pid: int) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT stock FROM products WHERE id=?", (pid,))
    r = cur.fetchone()
    if not r:
        con.close()
        return False
    stock = int(r["stock"] or 0)
    if stock <= 0:
        con.close()
        return False
    cur.execute("UPDATE products SET stock = stock - 1 WHERE id=?", (pid,))
    con.commit()
    con.close()
    return True

# ---------- coupons ----------
def upsert_coupon(code: str, discount_amount: int, discount_percent: int, apply_product_id: Optional[int], min_order: int, max_uses: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO coupons(code, discount_amount, discount_percent, apply_product_id, min_order, is_active, max_uses, used_count)
        VALUES(?,?,?,?,?,1,?,0)
        ON CONFLICT(code) DO UPDATE SET
            discount_amount=excluded.discount_amount,
            discount_percent=excluded.discount_percent,
            apply_product_id=excluded.apply_product_id,
            min_order=excluded.min_order,
            max_uses=excluded.max_uses,
            is_active=1
    """, (code.upper(), int(discount_amount), int(discount_percent), apply_product_id, int(min_order), int(max_uses)))
    con.commit()
    con.close()

def disable_coupon(code: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("UPDATE coupons SET is_active=0 WHERE code=?", (code.upper(),))
    con.commit()
    con.close()

def get_coupon(code: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT * FROM coupons WHERE code=? AND is_active=1", (code.upper(),))
    r = cur.fetchone()
    con.close()
    return r

def mark_coupon_used(code: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("UPDATE coupons SET used_count = used_count + 1 WHERE code=?", (code.upper(),))
    con.commit()
    con.close()

# ---------- affiliate ----------
def get_aff_percent(user_id: int) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT percent FROM affiliate WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    if not r:
        cur.execute("INSERT OR IGNORE INTO affiliate(user_id, percent) VALUES(?,5)", (user_id,))
        con.commit()
        con.close()
        return 5
    con.close()
    return int(r["percent"] or 5)

def set_aff_percent(user_id: int, percent: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT INTO affiliate(user_id, percent) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET percent=excluded.percent", (user_id, int(percent)))
    con.commit()
    con.close()

def add_aff_earning(referrer_id: int, referred_user_id: int, order_id: int, amount: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO affiliate_earnings(referrer_id, referred_user_id, order_id, amount, created_at) VALUES(?,?,?,?,?)",
        (referrer_id, referred_user_id, order_id, int(amount), now_str())
    )
    con.commit()
    con.close()

# ---------- orders/deposits ----------
def create_order(user_id: int, product_id: int, price: int, discount: int, final_price: int, coupon_code: Optional[str], status: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO orders(user_id, product_id, price, discount, final_price, coupon_code, status, created_at)
        VALUES(?,?,?,?,?,?,?,?)
    """, (user_id, product_id, int(price), int(discount), int(final_price), coupon_code, status, now_str()))
    con.commit()
    oid = cur.lastrowid
    con.close()
    return oid

def list_orders(user_id: int, limit: int = 20):
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        SELECT o.id, o.final_price, o.status, o.created_at, p.name AS product_name
        FROM orders o
        LEFT JOIN products p ON p.id=o.product_id
        WHERE o.user_id=?
        ORDER BY o.id DESC
        LIMIT ?
    """, (user_id, int(limit)))
    rows = cur.fetchall()
    con.close()
    return rows

def create_deposit(user_id: int, amount: int, description: str, trans_id: str, status: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO deposits(user_id, amount, description, trans_id, status, created_at)
        VALUES(?,?,?,?,?,?)
    """, (user_id, int(amount), description or "", trans_id or "", status, now_str()))
    con.commit()
    con.close()

# ---------- bank seen ----------
def bank_tx_seen(transaction_id: str) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT transaction_id FROM bank_seen WHERE transaction_id=?", (transaction_id,))
    r = cur.fetchone()
    con.close()
    return r is not None

def mark_bank_tx_seen(transaction_id: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO bank_seen(transaction_id, seen_at) VALUES(?,?)", (transaction_id, now_str()))
    con.commit()
    con.close()

# =========================================================
# CONFIG DATACLASS
# =========================================================
@dataclass
class BotConfig:
    admin_id: int = 0
    bot_token: str = ""

    # /start video
    start_video_path: str = ""

    # VietQR quick link (Cách A)
    vietqr_bank_name: str = "Vietcombank"
    vietqr_bank_id: str = "vietcombank"
    vietqr_stk: str = ""
    vietqr_ctk: str = ""
    vietqr_template: str = DEFAULT_VIETQR_TEMPLATE

    # AutoBank API V3 (sieuthicode)
    bank_v3_name: str = "Vietcombank"
    bank_v3_password: str = ""
    bank_v3_stk: str = ""
    bank_v3_token: str = ""
    poll_interval: int = 15  # seconds

    # optional: raw text user asked (nội dung api auto bank)
    auto_bank_api_text: str = ""

def load_config() -> BotConfig:
    if not os.path.exists(CONFIG_FILE):
        return BotConfig()
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            j = json.load(f)
        return BotConfig(**j)
    except Exception:
        return BotConfig()

def save_config(cfg: BotConfig):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg.__dict__, f, ensure_ascii=False, indent=2)

# =========================================================
# BOT RUNNER (Telegram + Bank Poll threads)
# =========================================================
class BotRunner:
    def __init__(self, log_queue: queue.Queue):
        self.cfg: BotConfig = load_config()
        self.log_queue = log_queue

        self._app: Optional[Application] = None
        self._bot_thread: Optional[threading.Thread] = None
        self._bank_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._bot_loop: Optional[asyncio.AbstractEventLoop] = None
        self._bot_username: Optional[str] = None

        # user temp buy context
        self._buy_cache: Dict[int, Dict[str, Any]] = {}

    def log(self, msg: str):
        try:
            self.log_queue.put(f"[{now_str()}] {msg}")
        except Exception:
            pass

    # =========================
    # TELEGRAM UI
    # =========================
    def user_main_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton("📦 Chuyên mục", callback_data="u:cats")],
            [InlineKeyboardButton("💳 Nạp tiền", callback_data="u:deposit"),
             InlineKeyboardButton("🧾 Lịch sử mua", callback_data="u:history")],
            [InlineKeyboardButton("🤝 Tiếp thị liên kết", callback_data="u:aff")],
        ]
        if user_id in ADMIN_MODE:
            rows.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="a:panel")])
        return InlineKeyboardMarkup(rows)

    def admin_keyboard(self) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton("➕ Thêm chuyên mục", callback_data="a:addcat"),
             InlineKeyboardButton("✏️ Sửa chuyên mục", callback_data="a:editcat")],
            [InlineKeyboardButton("🗑 Xóa chuyên mục", callback_data="a:delcat")],
            [InlineKeyboardButton("➕ Thêm sản phẩm", callback_data="a:addprod"),
             InlineKeyboardButton("✏️ Sửa sản phẩm", callback_data="a:editprod")],
            [InlineKeyboardButton("🗑 Xóa sản phẩm", callback_data="a:delprod")],
            [InlineKeyboardButton("🏷 Coupon: Thêm/Update", callback_data="a:addcoupon"),
             InlineKeyboardButton("🚫 Coupon: Tắt", callback_data="a:delcoupon")],
            [InlineKeyboardButton("💸 % Hoa hồng (Affiliate)", callback_data="a:setcomm")],
            [InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast")],
            [InlineKeyboardButton("⬅️ Quay lại", callback_data="u:home")],
        ]
        return InlineKeyboardMarkup(rows)

    # =========================
    # TELEGRAM HANDLERS
    # =========================
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        upsert_user(user)

        # parse ref
        if context.args:
            arg0 = context.args[0].strip()
            if arg0.lower().startswith("ref_"):
                ref_id = safe_int(arg0.split("_", 1)[1], 0)
                if ref_id > 0:
                    set_ref_if_empty(user.id, ref_id)

        # send video if configured
        try:
            if self.cfg.start_video_path and os.path.exists(self.cfg.start_video_path):
                await update.message.reply_video(video=open(self.cfg.start_video_path, "rb"), caption="🎉 Chào mừng bạn!")
        except Exception as e:
            self.log(f"Send start video error: {e}")

        bal = get_balance(user.id)
        txt = (
            f"👋 Xin chào <b>{user.first_name}</b>\n"
            f"💰 Số dư: <b>{bal:,}đ</b>\n\n"
            f"Chọn chức năng bên dưới:"
        )
        await update.message.reply_text(txt, reply_markup=self.user_main_keyboard(user.id), parse_mode=ParseMode.HTML)

    async def cmd_checkadmin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        upsert_user(user)
        if user.id == int(self.cfg.admin_id or 0):
            ADMIN_MODE.add(user.id)
            await update.message.reply_text("✅ Đã bật chế độ Admin ẩn.", reply_markup=self.user_main_keyboard(user.id))
        else:
            await update.message.reply_text("❌ Bạn không phải admin.")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        txt = (
            "📌 Lệnh:\n"
            "/start\n"
            "/checkadmin (admin ẩn)\n"
            "/help\n\n"
            "Admin (sau /checkadmin):\n"
            "Dùng Admin Panel trong nút."
        )
        await update.message.reply_text(txt)

    async def on_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        user = q.from_user
        upsert_user(user)

        data = q.data or ""
        if data == "u:home":
            bal = get_balance(user.id)
            await q.edit_message_text(
                f"🏠 Menu chính\n💰 Số dư: <b>{bal:,}đ</b>",
                reply_markup=self.user_main_keyboard(user.id),
                parse_mode=ParseMode.HTML,
            )
            return

        if data == "u:cats":
            cats = list_categories()
            if not cats:
                await q.edit_message_text("❌ Chưa có chuyên mục. Admin thêm chuyên mục trước.", reply_markup=self.user_main_keyboard(user.id))
                return
            rows = []
            for c in cats:
                rows.append([InlineKeyboardButton(f"📁 {c['name']}", callback_data=f"u:cat:{c['id']}")])
            rows.append([InlineKeyboardButton("⬅️ Quay lại", callback_data="u:home")])
            await q.edit_message_text("📦 Chọn chuyên mục:", reply_markup=InlineKeyboardMarkup(rows))
            return

        if data.startswith("u:cat:"):
            cid = safe_int(data.split(":")[2], 0)
            prods = list_products_by_category(cid)
            if not prods:
                await q.edit_message_text("❌ Chưa có sản phẩm trong chuyên mục này.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Quay lại", callback_data="u:cats")]]))
                return
            rows = []
            for p in prods:
                rows.append([InlineKeyboardButton(f"🛒 {p['name']} — {p['price']:,}đ (Kho: {p['stock']})", callback_data=f"u:prod:{p['id']}")])
            rows.append([InlineKeyboardButton("⬅️ Quay lại", callback_data="u:cats")])
            await q.edit_message_text("🛍 Chọn sản phẩm:", reply_markup=InlineKeyboardMarkup(rows))
            return

        if data.startswith("u:prod:"):
            pid = safe_int(data.split(":")[2], 0)
            prod = get_product(pid)
            if not prod:
                await q.edit_message_text("❌ Sản phẩm không tồn tại.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Quay lại", callback_data="u:cats")]]))
                return
            txt = (
                f"🛒 <b>{prod['name']}</b>\n"
                f"💵 Giá: <b>{int(prod['price']):,}đ</b>\n"
                f"📦 Kho: <b>{int(prod['stock']):,}</b>\n\n"
                f"Bạn muốn mua?"
            )
            rows = [
                [InlineKeyboardButton("✅ Mua ngay", callback_data=f"u:buy:{pid}")],
                [InlineKeyboardButton("⬅️ Quay lại", callback_data="u:cats")],
            ]
            await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
            return

        if data.startswith("u:buy:"):
            pid = safe_int(data.split(":")[2], 0)
            prod = get_product(pid)
            if not prod:
                await q.edit_message_text("❌ Sản phẩm không tồn tại.", reply_markup=self.user_main_keyboard(user.id))
                return
            if int(prod["stock"] or 0) <= 0:
                await q.edit_message_text("❌ Hết hàng.", reply_markup=self.user_main_keyboard(user.id))
                return

            self._buy_cache[user.id] = {"product_id": pid}
            rows = [
                [InlineKeyboardButton("✅ Có", callback_data="u:coupon_yes"),
                 InlineKeyboardButton("❌ Không", callback_data="u:coupon_no")],
                [InlineKeyboardButton("⬅️ Huỷ", callback_data="u:home")]
            ]
            await q.edit_message_text("🏷 Bạn có muốn nhập mã giảm giá không?", reply_markup=InlineKeyboardMarkup(rows))
            return

        # coupon choice
        if data == "u:coupon_no":
            await self._finalize_purchase(user.id, coupon_code=None, message=q.message, context=context)
            return

        if data == "u:coupon_yes":
            await q.edit_message_text("📩 Hãy gửi <b>mã giảm giá</b> (ví dụ: SALE10):", parse_mode=ParseMode.HTML)
            # set state via user_data
            context.user_data["waiting_coupon"] = True
            return

        if data == "u:deposit":
            await q.edit_message_text("💳 Nhập số tiền bạn muốn nạp (VD: 50000):")
            context.user_data["waiting_deposit_amount"] = True
            return

        if data == "u:history":
            orders = list_orders(user.id, limit=15)
            if not orders:
                await q.edit_message_text("🧾 Chưa có đơn hàng.", reply_markup=self.user_main_keyboard(user.id))
                return
            lines = ["🧾 <b>Lịch sử mua</b> (15 đơn gần nhất):\n"]
            for o in orders:
                lines.append(f"#{o['id']} • {o['product_name'] or 'SP'} • {int(o['final_price']):,}đ • {o['status']} • {o['created_at']}")
            await q.edit_message_text("\n".join(lines), reply_markup=self.user_main_keyboard(user.id), parse_mode=ParseMode.HTML)
            return

        if data == "u:aff":
            # referral link needs bot username
            if not self._bot_username:
                try:
                    me = await context.bot.get_me()
                    self._bot_username = me.username
                except Exception:
                    self._bot_username = "YourBot"
            link = f"https://t.me/{self._bot_username}?start=ref_{user.id}"
            percent = get_aff_percent(user.id)
            # tổng hoa hồng
            con = db_connect()
            cur = con.cursor()
            cur.execute("SELECT COALESCE(SUM(amount),0) AS s FROM affiliate_earnings WHERE referrer_id=?", (user.id,))
            total = int(cur.fetchone()["s"] or 0)
            con.close()

            txt = (
                "🤝 <b>Tiếp thị liên kết</b>\n"
                f"🔗 Link ref: <code>{link}</code>\n"
                f"💸 % hoa hồng hiện tại: <b>{percent}%</b>\n"
                f"🏦 Tổng hoa hồng đã nhận: <b>{total:,}đ</b>\n\n"
                "Gửi link cho người khác, họ /start bằng link đó, khi họ mua bạn sẽ nhận hoa hồng."
            )
            await q.edit_message_text(txt, reply_markup=self.user_main_keyboard(user.id), parse_mode=ParseMode.HTML)
            return

        # ADMIN panel
        if data == "a:panel":
            if user.id not in ADMIN_MODE:
                await q.edit_message_text("❌ Chưa bật admin. Dùng /checkadmin", reply_markup=self.user_main_keyboard(user.id))
                return
            await q.edit_message_text("🛠 <b>Admin Panel</b>", reply_markup=self.admin_keyboard(), parse_mode=ParseMode.HTML)
            return

        # ADMIN actions (via instructions text)
        if data.startswith("a:"):
            if user.id not in ADMIN_MODE:
                await q.edit_message_text("❌ Bạn không có quyền.", reply_markup=self.user_main_keyboard(user.id))
                return

            action = data.split(":")[1]
            if action == "addcat":
                await q.edit_message_text("➕ Gửi: <code>/addcat Tên chuyên mục</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "editcat":
                await q.edit_message_text("✏️ Gửi: <code>/editcat ID Tên mới</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "delcat":
                await q.edit_message_text("🗑 Gửi: <code>/delcat ID</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "addprod":
                await q.edit_message_text(
                    "➕ Gửi:\n<code>/addprod CAT_ID | Tên SP | Giá | Kho | Nội dung giao</code>\n"
                    "Ví dụ:\n<code>/addprod 1 | Acc VIP | 99000 | 10 | Thông tin giao ở đây</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self.admin_keyboard(),
                )
            elif action == "editprod":
                await q.edit_message_text(
                    "✏️ Gửi:\n<code>/editprod PROD_ID | Tên SP | Giá | Kho | Nội dung giao</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self.admin_keyboard(),
                )
            elif action == "delprod":
                await q.edit_message_text("🗑 Gửi: <code>/delprod PROD_ID</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "addcoupon":
                await q.edit_message_text(
                    "🏷 Gửi:\n"
                    "<code>/coupon CODE | giam_tien | giam_% | apply_prod_id_or_all | min_order | max_uses</code>\n"
                    "Ví dụ giảm 10% toàn bộ:\n"
                    "<code>/coupon SALE10 | 0 | 10 | all | 0 | 0</code>\n"
                    "Ví dụ giảm 20000 cho SP id=5:\n"
                    "<code>/coupon GIAM20K | 20000 | 0 | 5 | 0 | 100</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self.admin_keyboard(),
                )
            elif action == "delcoupon":
                await q.edit_message_text("🚫 Gửi: <code>/couponoff CODE</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "setcomm":
                await q.edit_message_text("💸 Gửi: <code>/setcomm USER_ID PERCENT</code>\nVí dụ: <code>/setcomm 123456789 10</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            elif action == "broadcast":
                await q.edit_message_text("📣 Gửi: <code>/broadcast Nội dung</code>", parse_mode=ParseMode.HTML, reply_markup=self.admin_keyboard())
            else:
                await q.edit_message_text("⚠️ Không hỗ trợ.", reply_markup=self.admin_keyboard())

    async def on_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        upsert_user(user)
        text = (update.message.text or "").strip()

        # waiting deposit amount
        if context.user_data.get("waiting_deposit_amount"):
            context.user_data["waiting_deposit_amount"] = False
            amt = safe_int(text, 0)
            if amt <= 0:
                await update.message.reply_text("❌ Số tiền không hợp lệ. Bấm Nạp tiền lại.", reply_markup=self.user_main_keyboard(user.id))
                return
            if not self.cfg.vietqr_stk or not self.cfg.vietqr_ctk or not self.cfg.vietqr_bank_id:
                await update.message.reply_text("❌ Admin chưa cấu hình VietQR (STK/CTK/BANK).", reply_markup=self.user_main_keyboard(user.id))
                return

            add_info = f"Napid {user.id}"
            qr_url = vietqr_image_url(
                bank_id=self.cfg.vietqr_bank_id,
                account_no=self.cfg.vietqr_stk,
                template=self.cfg.vietqr_template or DEFAULT_VIETQR_TEMPLATE,
                amount=amt,
                add_info=add_info,
                account_name=self.cfg.vietqr_ctk,
            )
            msg = (
                "💳 <b>Nạp tiền</b>\n"
                f"✅ Nội dung chuyển khoản: <code>{add_info}</code>\n"
                f"💰 Số tiền: <b>{amt:,}đ</b>\n\n"
                "📌 Quét QR để chuyển khoản đúng nội dung.\n"
                f"🔗 QR: {qr_url}"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=self.user_main_keyboard(user.id))
            return

        # waiting coupon code
        if context.user_data.get("waiting_coupon"):
            context.user_data["waiting_coupon"] = False
            code = text.upper()
            await self._finalize_purchase(user.id, coupon_code=code, message=update.message, context=context)
            return

        # fallback
        await update.message.reply_text("Chọn chức năng trong menu nhé.", reply_markup=self.user_main_keyboard(user.id))

    async def _finalize_purchase(self, user_id: int, coupon_code: Optional[str], message, context: ContextTypes.DEFAULT_TYPE):
        cache = self._buy_cache.get(user_id)
        if not cache:
            await message.reply_text("❌ Không có giao dịch mua đang chờ.", reply_markup=self.user_main_keyboard(user_id))
            return
        pid = cache.get("product_id")
        prod = get_product(pid)
        if not prod:
            await message.reply_text("❌ Sản phẩm không tồn tại.", reply_markup=self.user_main_keyboard(user_id))
            return
        if int(prod["stock"] or 0) <= 0:
            await message.reply_text("❌ Hết hàng.", reply_markup=self.user_main_keyboard(user_id))
            return

        price = int(prod["price"])
        discount = 0
        applied_code = None

        if coupon_code:
            cp = get_coupon(coupon_code)
            if not cp:
                await message.reply_text("❌ Mã giảm giá không hợp lệ hoặc đã tắt. Tiếp tục mua không giảm.", reply_markup=self.user_main_keyboard(user_id))
            else:
                # usage limit
                max_uses = int(cp["max_uses"] or 0)
                used = int(cp["used_count"] or 0)
                if max_uses > 0 and used >= max_uses:
                    await message.reply_text("❌ Mã đã hết lượt dùng. Tiếp tục mua không giảm.", reply_markup=self.user_main_keyboard(user_id))
                else:
                    min_order = int(cp["min_order"] or 0)
                    if price < min_order:
                        await message.reply_text(f"❌ Đơn tối thiểu {min_order:,}đ mới áp mã. Tiếp tục mua không giảm.", reply_markup=self.user_main_keyboard(user_id))
                    else:
                        apply_pid = cp["apply_product_id"]
                        if apply_pid is not None and int(apply_pid) != int(pid):
                            await message.reply_text("❌ Mã này không áp dụng cho sản phẩm này. Tiếp tục mua không giảm.", reply_markup=self.user_main_keyboard(user_id))
                        else:
                            damt = int(cp["discount_amount"] or 0)
                            dpct = int(cp["discount_percent"] or 0)
                            if dpct > 0:
                                discount = (price * dpct) // 100
                            else:
                                discount = damt
                            discount = max(0, min(discount, price))
                            applied_code = coupon_code

        final_price = max(0, price - discount)
        bal = get_balance(user_id)
        if bal < final_price:
            await message.reply_text(
                f"❌ Số dư không đủ.\n💰 Số dư: {bal:,}đ\n🧾 Cần: {final_price:,}đ\n👉 Hãy nạp tiền.",
                reply_markup=self.user_main_keyboard(user_id)
            )
            return

        # deduct & stock
        if not subtract_balance(user_id, final_price):
            await message.reply_text("❌ Trừ tiền thất bại. Thử lại.", reply_markup=self.user_main_keyboard(user_id))
            return
        if not decrement_stock(pid):
            # refund if stock failed
            add_balance(user_id, final_price)
            await message.reply_text("❌ Hết hàng (vừa có người mua trước). Tiền đã hoàn lại.", reply_markup=self.user_main_keyboard(user_id))
            return

        order_id = create_order(
            user_id=user_id,
            product_id=pid,
            price=price,
            discount=discount,
            final_price=final_price,
            coupon_code=applied_code,
            status="PAID",
        )
        if applied_code:
            mark_coupon_used(applied_code)

        # affiliate payout
        u = get_user(user_id)
        ref_by = u["ref_by"] if u else None
        if ref_by:
            percent = get_aff_percent(int(ref_by))
            earn = (final_price * percent) // 100
            if earn > 0:
                add_balance(int(ref_by), earn)
                add_aff_earning(int(ref_by), user_id, order_id, earn)
                # notify referrer
                try:
                    await context.bot.send_message(chat_id=int(ref_by), text=f"🎉 Bạn nhận hoa hồng {earn:,}đ từ đơn #{order_id}")
                except Exception:
                    pass

        # deliver
        deliver_text = (prod["deliver_text"] or "").strip()
        if not deliver_text:
            deliver_text = "✅ Đơn hàng đã thanh toán. Admin sẽ giao nội dung sau."

        await message.reply_text(
            "✅ <b>Mua thành công</b>\n"
            f"🧾 Đơn: <b>#{order_id}</b>\n"
            f"🛒 Sản phẩm: <b>{prod['name']}</b>\n"
            f"💵 Giá: <b>{price:,}đ</b>\n"
            f"🏷 Giảm: <b>{discount:,}đ</b>\n"
            f"✅ Thanh toán: <b>{final_price:,}đ</b>\n\n"
            f"📦 <b>Nội dung giao:</b>\n{deliver_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=self.user_main_keyboard(user_id)
        )

        # notify admin
        if int(self.cfg.admin_id or 0) > 0:
            try:
                await context.bot.send_message(
                    chat_id=int(self.cfg.admin_id),
                    text=f"🛎 Đơn mới #{order_id}\nUser: {user_id}\nSP: {prod['name']}\nThanh toán: {final_price:,}đ\nMã: {applied_code or 'none'}"
                )
            except Exception:
                pass

        self._buy_cache.pop(user_id, None)

    # =========================
    # ADMIN COMMANDS
    # =========================
    def _is_admin(self, user_id: int) -> bool:
        return user_id == int(self.cfg.admin_id or 0)

    async def admin_guard(self, update: Update) -> bool:
        uid = update.effective_user.id
        if not self._is_admin(uid):
            await update.message.reply_text("❌ Không có quyền admin.")
            return False
        if uid not in ADMIN_MODE:
            await update.message.reply_text("⚠️ Bật admin trước bằng /checkadmin")
            return False
        return True

    async def cmd_addcat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        name = " ".join(context.args).strip()
        if not name:
            await update.message.reply_text("Dùng: /addcat Tên chuyên mục")
            return
        cid = add_category(name)
        await update.message.reply_text(f"✅ Đã thêm chuyên mục #{cid}: {name}")

    async def cmd_editcat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        if len(context.args) < 2:
            await update.message.reply_text("Dùng: /editcat ID Tên mới")
            return
        cid = safe_int(context.args[0], 0)
        name = " ".join(context.args[1:]).strip()
        if cid <= 0 or not name:
            await update.message.reply_text("❌ Sai dữ liệu.")
            return
        edit_category(cid, name)
        await update.message.reply_text(f"✅ Đã sửa chuyên mục #{cid} -> {name}")

    async def cmd_delcat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        if len(context.args) < 1:
            await update.message.reply_text("Dùng: /delcat ID")
            return
        cid = safe_int(context.args[0], 0)
        if cid <= 0:
            await update.message.reply_text("❌ ID không hợp lệ.")
            return
        delete_category(cid)
        await update.message.reply_text(f"✅ Đã xóa (ẩn) chuyên mục #{cid}")

    def _parse_pipe(self, raw: str) -> List[str]:
        return [x.strip() for x in raw.split("|")]

    async def cmd_addprod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        raw = update.message.text.replace("/addprod", "", 1).strip()
        parts = self._parse_pipe(raw)
        if len(parts) < 5:
            await update.message.reply_text("Dùng: /addprod CAT_ID | Tên SP | Giá | Kho | Nội dung giao")
            return
        cid = safe_int(parts[0], 0)
        name = parts[1]
        price = safe_int(parts[2], 0)
        stock = safe_int(parts[3], 0)
        deliver = parts[4]
        if cid <= 0 or not name or price <= 0:
            await update.message.reply_text("❌ Sai dữ liệu.")
            return
        pid = add_product(cid, name, price, stock, deliver)
        await update.message.reply_text(f"✅ Đã thêm sản phẩm #{pid}: {name}")

    async def cmd_editprod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        raw = update.message.text.replace("/editprod", "", 1).strip()
        parts = self._parse_pipe(raw)
        if len(parts) < 5:
            await update.message.reply_text("Dùng: /editprod PROD_ID | Tên SP | Giá | Kho | Nội dung giao")
            return
        pid = safe_int(parts[0], 0)
        name = parts[1]
        price = safe_int(parts[2], 0)
        stock = safe_int(parts[3], 0)
        deliver = parts[4]
        if pid <= 0 or not name or price <= 0:
            await update.message.reply_text("❌ Sai dữ liệu.")
            return
        edit_product(pid, name, price, stock, deliver)
        await update.message.reply_text(f"✅ Đã sửa sản phẩm #{pid}")

    async def cmd_delprod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        if len(context.args) < 1:
            await update.message.reply_text("Dùng: /delprod PROD_ID")
            return
        pid = safe_int(context.args[0], 0)
        if pid <= 0:
            await update.message.reply_text("❌ ID không hợp lệ.")
            return
        delete_product(pid)
        await update.message.reply_text(f"✅ Đã xóa (ẩn) sản phẩm #{pid}")

    async def cmd_coupon(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        raw = update.message.text.replace("/coupon", "", 1).strip()
        parts = self._parse_pipe(raw)
        if len(parts) < 6:
            await update.message.reply_text(
                "Dùng:\n/coupon CODE | giam_tien | giam_% | apply_prod_id_or_all | min_order | max_uses"
            )
            return
        code = parts[0].upper()
        damt = safe_int(parts[1], 0)
        dpct = safe_int(parts[2], 0)
        ap = parts[3].lower()
        min_order = safe_int(parts[4], 0)
        max_uses = safe_int(parts[5], 0)
        apply_pid = None if ap == "all" else safe_int(ap, 0)
        if not code:
            await update.message.reply_text("❌ CODE rỗng.")
            return
        if dpct < 0 or dpct > 100:
            await update.message.reply_text("❌ % giảm không hợp lệ (0-100).")
            return
        if apply_pid is not None and apply_pid <= 0:
            await update.message.reply_text("❌ apply_prod_id_or_all phải là all hoặc số ID sản phẩm.")
            return
        upsert_coupon(code, damt, dpct, apply_pid, min_order, max_uses)
        await update.message.reply_text(f"✅ Đã tạo/cập nhật coupon {code}")

    async def cmd_couponoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        if len(context.args) < 1:
            await update.message.reply_text("Dùng: /couponoff CODE")
            return
        code = context.args[0].upper()
        disable_coupon(code)
        await update.message.reply_text(f"✅ Đã tắt coupon {code}")

    async def cmd_setcomm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        if len(context.args) < 2:
            await update.message.reply_text("Dùng: /setcomm USER_ID PERCENT")
            return
        uid = safe_int(context.args[0], 0)
        pct = safe_int(context.args[1], 0)
        if uid <= 0 or pct < 0 or pct > 100:
            await update.message.reply_text("❌ Sai dữ liệu.")
            return
        set_aff_percent(uid, pct)
        await update.message.reply_text(f"✅ Đã set hoa hồng user {uid} = {pct}%")

    async def cmd_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.admin_guard(update):
            return
        msg = update.message.text.replace("/broadcast", "", 1).strip()
        if not msg:
            await update.message.reply_text("Dùng: /broadcast Nội dung")
            return
        con = db_connect()
        cur = con.cursor()
        cur.execute("SELECT user_id FROM users")
        users = [int(r["user_id"]) for r in cur.fetchall()]
        con.close()

        sent = 0
        fail = 0
        for uid in users:
            try:
                await context.bot.send_message(chat_id=uid, text=msg)
                sent += 1
            except Exception:
                fail += 1
        await update.message.reply_text(f"✅ Broadcast xong. Sent={sent}, Fail={fail}")

    # =========================
    # BANK POLL LOOP
    # =========================
    async def _notify_deposit(self, bot, user_id: int, amount: int, tid: str):
        try:
            await bot.send_message(chat_id=user_id, text=f"✅ Nạp thành công {amount:,}đ\n🧾 Mã GD: {tid}")
        except Exception:
            pass

    async def _notify_admin_deposit(self, bot, user_id: int, amount: int, tid: str, desc: str):
        if int(self.cfg.admin_id or 0) <= 0:
            return
        try:
            await bot.send_message(
                chat_id=int(self.cfg.admin_id),
                text=f"💳 Nạp mới\nUser: {user_id}\nTiền: {amount:,}đ\nMã GD: {tid}\nND: {desc[:200]}"
            )
        except Exception:
            pass

    def poll_bank_and_credit_sync(self):
        """
        chạy trong thread (sync), gọi API v3, parse giao dịch, chống trùng, cộng tiền.
        gửi notify qua bot bằng thread-safe call vào loop bot.
        """

        # thiếu config => bỏ qua
        if not self.cfg.bank_v3_password or not self.cfg.bank_v3_stk or not self.cfg.bank_v3_token:
            self.log("Bank V3 chưa cấu hình (PASSWORD/STK/TOKEN). Bỏ qua poll.")
            return

        try:
            url = build_sieuthicode_v3_url(
                self.cfg.bank_v3_name,
                self.cfg.bank_v3_password,
                self.cfg.bank_v3_stk,
                self.cfg.bank_v3_token
            )

            self.log("Polling bank...")

            r = requests.get(url, timeout=25)
            data = r.json()

            txs = data.get("transactions", []) or []
            self.log(f"Transactions found: {len(txs)}")

            for tx in txs:
                ttype = str(tx.get("type", "")).upper().strip()
                if ttype != "IN":
                    continue

                tid = str(tx.get("transactionID", "")).strip()
                amount = safe_int(tx.get("amount", 0), 0)
                desc = str(tx.get("description", "") or "")

                if not tid or amount <= 0:
                    continue

                # chống trùng giao dịch
                if bank_tx_seen(tid):
                    continue

                # mark seen sớm
                mark_bank_tx_seen(tid)

                import re
                m = re.search(r"napid\s*(\d+)", desc, re.IGNORECASE)
                if not m:
                    continue

                user_id = safe_int(m.group(1), 0)
                if user_id <= 0:
                    continue

                # cộng tiền + lưu lịch sử
                add_balance(user_id, amount)
                create_deposit(user_id, amount, desc, tid, "SUCCESS")

                self.log(f"Deposit credited: user={user_id} amount={amount} tid={tid}")

                # gửi thông báo user/admin
                if self._app and self._bot_loop:
                    bot = self._app.bot
                    asyncio.run_coroutine_threadsafe(
                        self._notify_deposit(bot, user_id, amount, tid),
                        self._bot_loop
                    )
                    asyncio.run_coroutine_threadsafe(
                        self._notify_admin_deposit(bot, user_id, amount, tid, desc),
                        self._bot_loop
                    )

        except Exception as e:
            self.log(f"Bank poll error: {e}")

    def bank_loop(self):
        self.log("Bank poll thread started.")
        while not self._stop_event.is_set():
            try:
                self.poll_bank_and_credit_sync()
            except Exception as e:
                self.log(f"Bank loop error: {e}")
            self._stop_event.wait(max(5, int(self.cfg.poll_interval or 15)))
        self.log("Bank poll thread stopped.")

    # =========================
    # START/STOP
    # =========================
    def _build_application(self) -> Application:
        app = Application.builder().token(self.cfg.bot_token).build()

        # base commands
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("help", self.cmd_help))
        app.add_handler(CommandHandler("checkadmin", self.cmd_checkadmin))

        # admin commands
        app.add_handler(CommandHandler("addcat", self.cmd_addcat))
        app.add_handler(CommandHandler("editcat", self.cmd_editcat))
        app.add_handler(CommandHandler("delcat", self.cmd_delcat))
        app.add_handler(CommandHandler("addprod", self.cmd_addprod))
        app.add_handler(CommandHandler("editprod", self.cmd_editprod))
        app.add_handler(CommandHandler("delprod", self.cmd_delprod))
        app.add_handler(CommandHandler("coupon", self.cmd_coupon))
        app.add_handler(CommandHandler("couponoff", self.cmd_couponoff))
        app.add_handler(CommandHandler("setcomm", self.cmd_setcomm))
        app.add_handler(CommandHandler("broadcast", self.cmd_broadcast))

        # callbacks & messages
        app.add_handler(CallbackQueryHandler(self.on_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_message))
        return app

    def _bot_thread_main(self):
        """
        chạy telegram bot trong 1 thread riêng, có event loop riêng
        """
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            self._bot_loop = loop

            self._app = self._build_application()
            self.log("Telegram bot starting (polling)...")

            async def runner():
                await self._app.initialize()
                await self._app.start()

                # get bot username
                try:
                    me = await self._app.bot.get_me()
                    self._bot_username = me.username
                    self.log(f"Bot username: @{self._bot_username}")
                except Exception:
                    pass

                await self._app.updater.start_polling(drop_pending_updates=True)

                # idle
                while not self._stop_event.is_set():
                    await asyncio.sleep(0.5)

                # stop
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()

            loop.run_until_complete(runner())
            self.log("Telegram bot stopped.")

        except Exception as e:
            self.log(f"Bot thread error: {e}")

    def start(self, cfg: BotConfig):
        self.cfg = cfg
        save_config(self.cfg)

        if not self.cfg.bot_token:
            raise RuntimeError("Chưa nhập BOT TOKEN")
        if int(self.cfg.admin_id or 0) <= 0:
            raise RuntimeError("Chưa nhập ADMIN ID")

        self._stop_event.clear()

        # start bot
        self._bot_thread = threading.Thread(target=self._bot_thread_main, daemon=True)
        self._bot_thread.start()

        # start bank poll
        self._bank_thread = threading.Thread(target=self.bank_loop, daemon=True)
        self._bank_thread.start()

        self.log("✅ Started all (Bot + Bank Poll).")

    def stop(self):
        self._stop_event.set()
        self.log("⛔ Stopping...")

# =========================================================
# GUI
# =========================================================
class AppGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1100x720")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        init_db()
        self.log_queue = queue.Queue()
        self.runner = BotRunner(self.log_queue)
        self.cfg = self.runner.cfg

        self._build_ui()
        self.after(200, self._poll_log_queue)

    def _build_ui(self):
        # layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(self, corner_radius=18)
        right = ctk.CTkFrame(self, corner_radius=18)
        left.grid(row=0, column=0, padx=14, pady=14, sticky="nsew")
        right.grid(row=0, column=1, padx=14, pady=14, sticky="nsew")

        left.grid_rowconfigure(99, weight=1)
        right.grid_rowconfigure(99, weight=1)

        title = ctk.CTkLabel(left, text="⚙️ Cấu hình", font=ctk.CTkFont(size=20, weight="bold"))
        title.grid(row=0, column=0, padx=14, pady=(14, 10), sticky="w")

        # --- Bot config ---
        sec1 = ctk.CTkFrame(left, corner_radius=16)
        sec1.grid(row=1, column=0, padx=14, pady=10, sticky="ew")
        sec1.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(sec1, text="👑 Admin ID").grid(row=0, column=0, padx=12, pady=10, sticky="w")
        self.ent_admin = ctk.CTkEntry(sec1, corner_radius=14)
        self.ent_admin.grid(row=0, column=1, padx=12, pady=10, sticky="ew")
        self.ent_admin.insert(0, str(self.cfg.admin_id or ""))

        ctk.CTkLabel(sec1, text="🤖 Bot Token").grid(row=1, column=0, padx=12, pady=10, sticky="w")
        self.ent_token = ctk.CTkEntry(sec1, corner_radius=14)
        self.ent_token.grid(row=1, column=1, padx=12, pady=10, sticky="ew")
        self.ent_token.insert(0, self.cfg.bot_token or "")

        ctk.CTkLabel(sec1, text="🎬 Video /start").grid(row=2, column=0, padx=12, pady=10, sticky="w")
        row = ctk.CTkFrame(sec1, corner_radius=14)
        row.grid(row=2, column=1, padx=12, pady=10, sticky="ew")
        row.grid_columnconfigure(0, weight=1)
        self.ent_video = ctk.CTkEntry(row, corner_radius=14)
        self.ent_video.grid(row=0, column=0, padx=(0, 10), pady=8, sticky="ew")
        self.ent_video.insert(0, self.cfg.start_video_path or "")
        btn_pick = ctk.CTkButton(row, text="Chọn file", corner_radius=14, command=self.pick_video)
        btn_pick.grid(row=0, column=1, padx=(0, 0), pady=8)

        # --- VietQR config ---
        sec2 = ctk.CTkFrame(left, corner_radius=16)
        sec2.grid(row=2, column=0, padx=14, pady=10, sticky="ew")
        sec2.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(sec2, text="🏦 VietQR Bank").grid(row=0, column=0, padx=12, pady=10, sticky="w")
        self.cmb_vietqr_bank = ctk.CTkOptionMenu(
            sec2, values=[x[0] for x in VIETQR_BANKS], corner_radius=14, command=self.on_vietqr_bank_changed
        )
        self.cmb_vietqr_bank.grid(row=0, column=1, padx=12, pady=10, sticky="ew")
        self.cmb_vietqr_bank.set(self.cfg.vietqr_bank_name or "Vietcombank")

        ctk.CTkLabel(sec2, text="💳 STK").grid(row=1, column=0, padx=12, pady=10, sticky="w")
        self.ent_vietqr_stk = ctk.CTkEntry(sec2, corner_radius=14)
        self.ent_vietqr_stk.grid(row=1, column=1, padx=12, pady=10, sticky="ew")
        self.ent_vietqr_stk.insert(0, self.cfg.vietqr_stk or "")

        ctk.CTkLabel(sec2, text="👤 CTK").grid(row=2, column=0, padx=12, pady=10, sticky="w")
        self.ent_vietqr_ctk = ctk.CTkEntry(sec2, corner_radius=14)
        self.ent_vietqr_ctk.grid(row=2, column=1, padx=12, pady=10, sticky="ew")
        self.ent_vietqr_ctk.insert(0, self.cfg.vietqr_ctk or "")

        ctk.CTkLabel(sec2, text="🧩 Template").grid(row=3, column=0, padx=12, pady=10, sticky="w")
        self.ent_vietqr_tpl = ctk.CTkEntry(sec2, corner_radius=14)
        self.ent_vietqr_tpl.grid(row=3, column=1, padx=12, pady=10, sticky="ew")
        self.ent_vietqr_tpl.insert(0, self.cfg.vietqr_template or DEFAULT_VIETQR_TEMPLATE)

        self.lbl_vietqr_preview = ctk.CTkLabel(
            sec2, text="🔎 QR preview link sẽ dùng nội dung: Napid <id>", text_color="#A0A0A0"
        )
        self.lbl_vietqr_preview.grid(row=4, column=0, columnspan=2, padx=12, pady=(0, 12), sticky="w")

        # --- Bank V3 config ---
        sec3 = ctk.CTkFrame(left, corner_radius=16)
        sec3.grid(row=3, column=0, padx=14, pady=10, sticky="ew")
        sec3.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(sec3, text="🏦 Bank API V3").grid(row=0, column=0, padx=12, pady=10, sticky="w")
        self.cmb_bank_v3 = ctk.CTkOptionMenu(sec3, values=list(SIEUTHICODE_V3_ENDPOINTS.keys()), corner_radius=14)
        self.cmb_bank_v3.grid(row=0, column=1, padx=12, pady=10, sticky="ew")
        self.cmb_bank_v3.set(self.cfg.bank_v3_name or "Vietcombank")

        ctk.CTkLabel(sec3, text="🔐 PASSWORD").grid(row=1, column=0, padx=12, pady=10, sticky="w")
        self.ent_bank_pw = ctk.CTkEntry(sec3, corner_radius=14, show="*")
        self.ent_bank_pw.grid(row=1, column=1, padx=12, pady=10, sticky="ew")
        self.ent_bank_pw.insert(0, self.cfg.bank_v3_password or "")

        ctk.CTkLabel(sec3, text="💳 STK (API)").grid(row=2, column=0, padx=12, pady=10, sticky="w")
        self.ent_bank_stk = ctk.CTkEntry(sec3, corner_radius=14)
        self.ent_bank_stk.grid(row=2, column=1, padx=12, pady=10, sticky="ew")
        self.ent_bank_stk.insert(0, self.cfg.bank_v3_stk or "")

        ctk.CTkLabel(sec3, text="🪙 TOKEN").grid(row=3, column=0, padx=12, pady=10, sticky="w")
        self.ent_bank_token = ctk.CTkEntry(sec3, corner_radius=14)
        self.ent_bank_token.grid(row=3, column=1, padx=12, pady=10, sticky="ew")
        self.ent_bank_token.insert(0, self.cfg.bank_v3_token or "")

        ctk.CTkLabel(sec3, text="⏱ Poll interval (s)").grid(row=4, column=0, padx=12, pady=10, sticky="w")
        self.ent_poll = ctk.CTkEntry(sec3, corner_radius=14)
        self.ent_poll.grid(row=4, column=1, padx=12, pady=10, sticky="ew")
        self.ent_poll.insert(0, str(self.cfg.poll_interval or 15))

        ctk.CTkLabel(sec3, text="🧾 Nội dung API Auto Bank (tuỳ chọn)").grid(row=5, column=0, padx=12, pady=10, sticky="w")
        self.txt_auto = ctk.CTkTextbox(sec3, corner_radius=14, height=90)
        self.txt_auto.grid(row=5, column=1, padx=12, pady=10, sticky="ew")
        if self.cfg.auto_bank_api_text:
            self.txt_auto.insert("1.0", self.cfg.auto_bank_api_text)

        # buttons
        btns = ctk.CTkFrame(left, corner_radius=16)
        btns.grid(row=4, column=0, padx=14, pady=10, sticky="ew")
        btns.grid_columnconfigure(0, weight=1)
        btns.grid_columnconfigure(1, weight=1)
        btns.grid_columnconfigure(2, weight=1)

        self.btn_save = ctk.CTkButton(btns, text="💾 Lưu cấu hình", corner_radius=16, command=self.save_cfg_ui)
        self.btn_save.grid(row=0, column=0, padx=10, pady=12, sticky="ew")

        self.btn_start = ctk.CTkButton(btns, text="▶️ Start", corner_radius=16, command=self.start_clicked)
        self.btn_start.grid(row=0, column=1, padx=10, pady=12, sticky="ew")

        self.btn_stop = ctk.CTkButton(btns, text="⛔ Stop", corner_radius=16, command=self.stop_clicked)
        self.btn_stop.grid(row=0, column=2, padx=10, pady=12, sticky="ew")

        # right logs
        t2 = ctk.CTkLabel(right, text="📟 Log hệ thống", font=ctk.CTkFont(size=20, weight="bold"))
        t2.grid(row=0, column=0, padx=14, pady=(14, 10), sticky="w")

        self.txt_log = ctk.CTkTextbox(right, corner_radius=18)
        self.txt_log.grid(row=1, column=0, padx=14, pady=10, sticky="nsew")
        right.grid_rowconfigure(1, weight=1)
        right.grid_columnconfigure(0, weight=1)

        hint = (
            "✅ PC chạy 24/7: không cần CRON.\n"
            "• User nạp: bot tạo QR VietQR với nội dung Napid <id>\n"
            "• Bank poll sẽ quét API V3 và cộng tiền tự động.\n\n"
            "Admin:\n"
            "• /checkadmin (ẩn)\n"
            "• Thêm chuyên mục: /addcat ...\n"
            "• Thêm sản phẩm: /addprod CAT_ID | Tên | Giá | Kho | Nội dung giao\n"
            "• Coupon: /coupon CODE | giam_tien | giam_% | all/PROD_ID | min_order | max_uses\n"
        )
        self.txt_log.insert("1.0", hint)

    def pick_video(self):
        path = filedialog.askopenfilename(
            title="Chọn video gửi khi /start",
            filetypes=[("Video files", "*.mp4 *.mov *.mkv *.avi"), ("All files", "*.*")]
        )
        if path:
            self.ent_video.delete(0, "end")
            self.ent_video.insert(0, path)

    def on_vietqr_bank_changed(self, bank_name: str):
        bank_id = dict(VIETQR_BANKS).get(bank_name, "vietcombank")
        self.cfg.vietqr_bank_name = bank_name
        self.cfg.vietqr_bank_id = bank_id

    def read_cfg_from_ui(self) -> BotConfig:
        cfg = BotConfig()
        cfg.admin_id = safe_int(self.ent_admin.get(), 0)
        cfg.bot_token = self.ent_token.get().strip()
        cfg.start_video_path = self.ent_video.get().strip()

        bank_name = self.cmb_vietqr_bank.get().strip()
        cfg.vietqr_bank_name = bank_name
        cfg.vietqr_bank_id = dict(VIETQR_BANKS).get(bank_name, "vietcombank")
        cfg.vietqr_stk = self.ent_vietqr_stk.get().strip()
        cfg.vietqr_ctk = self.ent_vietqr_ctk.get().strip()
        cfg.vietqr_template = self.ent_vietqr_tpl.get().strip() or DEFAULT_VIETQR_TEMPLATE

        cfg.bank_v3_name = self.cmb_bank_v3.get().strip()
        cfg.bank_v3_password = self.ent_bank_pw.get().strip()
        cfg.bank_v3_stk = self.ent_bank_stk.get().strip()
        cfg.bank_v3_token = self.ent_bank_token.get().strip()
        cfg.poll_interval = max(5, safe_int(self.ent_poll.get(), 15))

        cfg.auto_bank_api_text = self.txt_auto.get("1.0", "end").strip()
        return cfg

    def save_cfg_ui(self):
        self.cfg = self.read_cfg_from_ui()
        save_config(self.cfg)
        self.runner.cfg = self.cfg
        self._append_log("✅ Đã lưu cấu hình.")

    def start_clicked(self):
        try:
            self.cfg = self.read_cfg_from_ui()
            save_config(self.cfg)
            self.runner.start(self.cfg)
            self._append_log("▶️ START OK.")
        except Exception as e:
            messagebox.showerror("Lỗi Start", str(e))

    def stop_clicked(self):
        try:
            self.runner.stop()
            self._append_log("⛔ Đã gửi tín hiệu STOP.")
        except Exception as e:
            messagebox.showerror("Lỗi Stop", str(e))

    def _append_log(self, line: str):
        self.txt_log.insert("end", f"\n{line}")
        self.txt_log.see("end")

    def _poll_log_queue(self):
        try:
            while True:
                line = self.log_queue.get_nowait()
                self._append_log(line)
        except queue.Empty:
            pass
        self.after(200, self._poll_log_queue)

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    init_db()
    app = AppGUI()
    app.mainloop()
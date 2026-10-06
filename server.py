import os, json, math, time, asyncio, sqlite3, re, html, secrets, hashlib
from urllib.parse import urlencode
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv('DB_PATH', '/data/taixiutool_learning.db')
POLL_SECONDS = max(0.8, float(os.getenv('POLL_SECONDS', '1.2')))
MAX_HISTORY = max(2000, min(50000, int(os.getenv('MAX_HISTORY', '10000'))))
BOT_TOKEN = os.getenv('BOT_TOKEN','').strip()
PUBLIC_URL = os.getenv('PUBLIC_URL','').rstrip('/')
BOT_POLL_TIMEOUT = max(10, int(os.getenv('BOT_POLL_TIMEOUT','20')))
ADMIN_IDS = {int(x) for x in os.getenv('ADMIN_IDS','').replace(';',',').split(',') if x.strip().lstrip('-').isdigit()}
BOT_REQUIRE_ACCESS = os.getenv('BOT_REQUIRE_ACCESS','1').strip().lower() not in ('0','false','no','off')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY','').strip()
OPENAI_MODEL = os.getenv('OPENAI_MODEL','').strip()
SUPPORT_USERNAME = os.getenv('SUPPORT_USERNAME','').strip().lstrip('@')
VIETQR_BANK_ID = os.getenv('VIETQR_BANK_ID','SHB').strip() or 'SHB'
VIETQR_ACCOUNT_NO = os.getenv('VIETQR_ACCOUNT_NO','0988712947').strip() or '0988712947'
VIETQR_ACCOUNT_NAME = os.getenv('VIETQR_ACCOUNT_NAME','').strip()
TOPUP_ORDER_TTL = max(300, int(os.getenv('TOPUP_ORDER_TTL','900')))
MIN_TOPUP = max(20000, int(os.getenv('MIN_TOPUP','20000')))

BOARDS = {
    'sunwin:hu': {
        'game':'sunwin','table':'hu','kind':'tx_pair',
        'current':'https://amongst-plots-called-dining.trycloudflare.com/api/tx',
        'current_fallbacks':['https://kwinstore.com/sunwin/tx/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7'],
        'history':'https://kwinstore.com/sunwin/tx/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'sunwin:sicbo': {
        'game':'sunwin','table':'sicbo','kind':'sicbo_pair',
        'current':'https://kwinstore.com/sunwin/sicbo/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
        'history':'https://kwinstore.com/sunwin/sicbo/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'lc79:hu': {
        'game':'lc79','table':'hu','kind':'tx_pair',
        'current':'https://reported-prot-prefers-cattle.trycloudflare.com/api/tx',
        'current_fallbacks':['https://kwinstore.com/lc79/tx/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7'],
        'history':'https://kwinstore.com/lc79/tx/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'lc79:md5': {
        'game':'lc79','table':'md5','kind':'tx_pair',
        'current':'https://reported-prot-prefers-cattle.trycloudflare.com/api/txmd5',
        'current_fallbacks':['https://kwinstore.com/lc79/md5/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7'],
        'history':'https://kwinstore.com/lc79/md5/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'lc79:xocdia': {
        'game':'lc79','table':'xocdia','kind':'xocdia',
        'current':'https://reported-prot-prefers-cattle.trycloudflare.com/api/xocdia',
    },
}

_db_lock = asyncio.Lock()
_worker_task = None
_bot_task = None
_last_cycle = 0.0
_process_started_at = time.time()
_ml_cache = {}
_group_spam_state = {}


def ensure_db():
    p = Path(DB_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('''CREATE TABLE IF NOT EXISTS rounds(
            board TEXT NOT NULL, session TEXT NOT NULL, result TEXT NOT NULL,
            d1 INTEGER, d2 INTEGER, d3 INTEGER, total INTEGER, md5 TEXT,
            seen_at REAL NOT NULL, PRIMARY KEY(board,session)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS board_state(
            board TEXT PRIMARY KEY, updated_at REAL NOT NULL, source_ok INTEGER NOT NULL,
            last_error TEXT, model_json TEXT
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS shared_predictions(
            board TEXT NOT NULL, session TEXT NOT NULL, prediction TEXT NOT NULL,
            confidence INTEGER NOT NULL, score REAL NOT NULL, model_json TEXT,
            created_at REAL NOT NULL, actual TEXT, ok INTEGER, settled_at REAL,
            PRIMARY KEY(board,session)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_subscriptions(
            chat_id INTEGER NOT NULL, board TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(chat_id,board)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_chat_state(
            chat_id INTEGER PRIMARY KEY, selected_board TEXT
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_auto_messages(
            chat_id INTEGER NOT NULL, board TEXT NOT NULL, session TEXT NOT NULL,
            message_id INTEGER NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(chat_id,board)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS board_cursor(
            board TEXT PRIMARY KEY, last_completed_session TEXT, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_access(
            chat_id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1,
            granted_by INTEGER, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_permissions(
            chat_id INTEGER PRIMARY KEY,
            can_predict INTEGER NOT NULL DEFAULT 1,
            can_history INTEGER NOT NULL DEFAULT 0,
            can_ai INTEGER NOT NULL DEFAULT 0,
            can_auto INTEGER NOT NULL DEFAULT 0,
            preset TEXT NOT NULL DEFAULT 'basic',
            granted_by INTEGER,
            updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS api_overrides(
            board TEXT PRIMARY KEY, current_url TEXT, history_url TEXT, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS strategy_logs(
            board TEXT NOT NULL,
            session TEXT NOT NULL,
            strategy TEXT NOT NULL,
            prediction TEXT NOT NULL,
            actual TEXT,
            ok INTEGER,
            created_at REAL NOT NULL,
            settled_at REAL,
            PRIMARY KEY(board,session,strategy)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS champion_state(
            board TEXT PRIMARY KEY,
            champion TEXT,
            updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS timeout_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_chat_id INTEGER NOT NULL,
            started_at REAL NOT NULL,
            ends_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            snapshot_json TEXT,
            report_sent_at REAL,
            cancelled_at REAL,
            last_error TEXT
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS background_service(
            id INTEGER PRIMARY KEY CHECK(id=1),
            enabled INTEGER NOT NULL DEFAULT 1,
            activated_at REAL NOT NULL,
            activated_by INTEGER,
            updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_group_settings(
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            enabled INTEGER NOT NULL DEFAULT 0,
            auto_delete INTEGER NOT NULL DEFAULT 1,
            delete_after REAL NOT NULL DEFAULT 5,
            anti_spam INTEGER NOT NULL DEFAULT 1,
            spam_limit INTEGER NOT NULL DEFAULT 5,
            spam_window REAL NOT NULL DEFAULT 6,
            mute_seconds INTEGER NOT NULL DEFAULT 60,
            locked INTEGER NOT NULL DEFAULT 0,
            warn_limit INTEGER NOT NULL DEFAULT 3,
            enabled_by INTEGER,
            updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_group_messages(
            chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, user_id INTEGER,
            created_at REAL NOT NULL, is_command INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id,message_id)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_group_warnings(
            chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, warning_count INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL, PRIMARY KEY(chat_id,user_id)
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_users(
            chat_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT,
            balance INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_settings(
            key TEXT PRIMARY KEY, value TEXT, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_key_plans(
            code TEXT PRIMARY KEY, name TEXT NOT NULL, duration_token TEXT NOT NULL,
            price INTEGER NOT NULL, games_json TEXT NOT NULL DEFAULT '["*"]',
            note TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_keys(
            key_code TEXT PRIMARY KEY, plan_code TEXT NOT NULL, duration_token TEXT NOT NULL,
            price INTEGER NOT NULL, games_json TEXT NOT NULL, note TEXT,
            created_by INTEGER, created_at REAL NOT NULL, redeemed_by INTEGER,
            redeemed_at REAL, expires_at REAL, status TEXT NOT NULL DEFAULT 'new'
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_wallet_transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, amount INTEGER NOT NULL,
            kind TEXT NOT NULL, ref TEXT, note TEXT, created_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_topup_orders(
            order_code TEXT PRIMARY KEY, seq TEXT NOT NULL UNIQUE, chat_id INTEGER NOT NULL,
            amount INTEGER NOT NULL, transfer_content TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            created_at REAL NOT NULL, expires_at REAL NOT NULL, submitted_at REAL,
            reviewed_at REAL, reviewed_by INTEGER, admin_note TEXT
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_game_settings(
            game TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'online', reason TEXT NOT NULL DEFAULT '',
            play_url TEXT NOT NULL DEFAULT '', updated_by INTEGER, updated_at REAL NOT NULL
        )''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_user_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
            event_type TEXT NOT NULL, action TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL
        )''')
        db.execute('''CREATE INDEX IF NOT EXISTS idx_bot_user_events_chat_time
                      ON bot_user_events(chat_id,created_at DESC)''')
        db.execute('''CREATE TABLE IF NOT EXISTS bot_broadcasts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL, message TEXT NOT NULL,
            total INTEGER NOT NULL DEFAULT 0, success INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        )''')
        now=time.time()
        defaults=[
            ('1h','KEY 1 GIỜ','1h',10000,'["*"]','Dùng toàn bộ game đang hỗ trợ.',10),
            ('1d','KEY 1 NGÀY','1d',20000,'["*"]','Dùng toàn bộ game đang hỗ trợ.',20),
            ('7d','KEY 7 NGÀY','7d',67000,'["*"]','Dùng toàn bộ game đang hỗ trợ.',30),
            ('30d','KEY 30 NGÀY','30d',123000,'["*"]','Dùng toàn bộ game đang hỗ trợ.',40),
            ('forever','KEY VĨNH VIỄN','forever',236000,'["*"]','Dùng toàn bộ game đang hỗ trợ, không hết hạn.',50),
        ]
        for row in defaults:
            db.execute('''INSERT OR IGNORE INTO bot_key_plans(code,name,duration_token,price,games_json,note,enabled,sort_order,updated_at)
                          VALUES(?,?,?,?,?,?,1,?,?)''',row+(now,))
        for game in sorted({cfg.get('game') for cfg in BOARDS.values() if cfg.get('game')}):
            db.execute('''INSERT OR IGNORE INTO bot_game_settings(game,enabled,status,reason,play_url,updated_by,updated_at)
                          VALUES(?,1,'online','','',NULL,?)''',(game,now))
        db.execute('''INSERT OR IGNORE INTO background_service(id,enabled,activated_at,activated_by,updated_at)
                      VALUES(1,1,?,NULL,?)''',(now,now))
        db.execute("UPDATE timeout_runs SET status='retired_v29' WHERE status IN ('active','report_pending')")
        # Lightweight forward-compatible migration for richer game metadata.
        cols={r[1] for r in db.execute('PRAGMA table_info(rounds)').fetchall()}
        if 'meta_json' not in cols:
            db.execute('ALTER TABLE rounds ADD COLUMN meta_json TEXT')
        access_cols={r[1] for r in db.execute('PRAGMA table_info(bot_access)').fetchall()}
        if 'expires_at' not in access_cols:
            db.execute('ALTER TABLE bot_access ADD COLUMN expires_at REAL')
        if 'access_label' not in access_cols:
            db.execute("ALTER TABLE bot_access ADD COLUMN access_label TEXT DEFAULT 'manual'")
        if 'purchased_at' not in access_cols:
            db.execute('ALTER TABLE bot_access ADD COLUMN purchased_at REAL')
        group_cols={r[1] for r in db.execute('PRAGMA table_info(bot_group_settings)').fetchall()}
        group_add={
            'anti_spam':'INTEGER NOT NULL DEFAULT 1',
            'spam_limit':'INTEGER NOT NULL DEFAULT 5',
            'spam_window':'REAL NOT NULL DEFAULT 6',
            'mute_seconds':'INTEGER NOT NULL DEFAULT 60',
            'locked':'INTEGER NOT NULL DEFAULT 0',
            'warn_limit':'INTEGER NOT NULL DEFAULT 3',
        }
        for col,ddl in group_add.items():
            if col not in group_cols:
                db.execute(f'ALTER TABLE bot_group_settings ADD COLUMN {col} {ddl}')
        user_cols={r[1] for r in db.execute('PRAGMA table_info(bot_users)').fetchall()}
        user_add={
            'last_seen':'REAL',
            'last_action':"TEXT NOT NULL DEFAULT ''",
            'action_count':'INTEGER NOT NULL DEFAULT 0',
            'start_count':'INTEGER NOT NULL DEFAULT 0',
            'callback_count':'INTEGER NOT NULL DEFAULT 0',
            'last_chat_type':"TEXT NOT NULL DEFAULT 'private'",
        }
        for col,ddl in user_add.items():
            if col not in user_cols:
                db.execute(f'ALTER TABLE bot_users ADD COLUMN {col} {ddl}')
        db.execute('DELETE FROM bot_user_events WHERE id NOT IN (SELECT id FROM bot_user_events ORDER BY id DESC LIMIT 50000)')
        db.commit()


def pick(o: Any, keys):
    if not isinstance(o, dict): return None
    for k in keys:
        if k in o and o[k] is not None: return o[k]
    return None


def sid(o, fallback=None):
    return pick(o,['phien','phiên','phien_id','phienId','id','session_id','sessionId','session','gameId','game_id','stt','code','index','issue','round','roundId','round_id','sid']) or fallback



def canonical_session(v, fallback=None):
    """Normalize numeric sessions such as '#2755690' -> '2755690'."""
    if v is None:
        v=fallback
    if v is None:
        return None
    z=str(v).strip()
    m=re.search(r'(\d+)(?!.*\d)',z)
    return m.group(1) if m else z


def dice_values(o):
    arr = pick(o,['dices','dice','xuc_xac','xúc_xắc','xucXac','xucxac','dice_values','diceValues','xs','xucsac','xucxac_list'])
    if isinstance(arr,list):
        vals=[]
        for v in arr[:3]:
            if isinstance(v,dict): v=pick(v,['value','point','number','face','dice'])
            try: vals.append(int(v))
            except: pass
        if len(vals)>=3: return vals[:3]
    for ks in [('dice1','dice2','dice3'),('d1','d2','d3'),('xx1','xx2','xx3'),('xucxac1','xucxac2','xucxac3'),('xuc_xac_1','xuc_xac_2','xuc_xac_3'),('xucXac1','xucXac2','xucXac3')]:
        try:
            vals=[int(o[k]) for k in ks]
            return vals
        except: pass
    return []


def total_value(o,dice):
    v=pick(o,['point','points','Tong','tong','tổng','sum','total','score','totalPoint','total_point','tong_diem','tongDiem'])
    try: return int(v)
    except: return sum(dice) if len(dice)>=3 else None


def tx_result(o,dice=None,total=None):
    dice=dice or []
    if total is None: total=total_value(o,dice)
    v=pick(o,['ket_qua','kết_quả','ketqua','result','ketQua','resultTruyenThong','type','tai_xiu','taiXiu','taixiu','side','prediction','outcome','status_result','win','game_result','gameResult','result_text','resultText'])
    if v is not None:
        z=str(v).strip().upper()
        if 'TAI' in z or 'TÀI' in z or z in ('T','BIG','OVER','1','TRUE'): return 'TÀI'
        if 'XIU' in z or 'XỈU' in z or z in ('X','SMALL','UNDER','0','FALSE'): return 'XỈU'
    if total is not None: return 'TÀI' if total>10 else 'XỈU'
    return None


def find_rows(data, depth=0):
    if depth>4: return []
    if isinstance(data,list):
        if any(isinstance(x,dict) for x in data): return data
        return []
    if not isinstance(data,dict): return []
    for k in ['data','current','result','results','history','histories','list','items','sessions','rounds','records','rows','games','payload','response']:
        if k in data:
            hit=find_rows(data[k],depth+1)
            if hit: return hit
    if sid(data) is not None: return [data]
    for v in data.values():
        hit=find_rows(v,depth+1)
        if hit: return hit
    return []


def parse_tx(data):
    out=[]
    for i,o in enumerate(find_rows(data)):
        if not isinstance(o,dict): continue
        d=dice_values(o); t=total_value(o,d); r=tx_result(o,d,t)
        if not r: continue
        meta={
            'total_tai':pick(o,['total_tai','tong_tai','tai_total','totalTai']),
            'total_xiu':pick(o,['total_xiu','tong_xiu','xiu_total','totalXiu']),
            'jackpot':pick(o,['jackpot','hu','pot']),
            'raw_rs':pick(o,['raw_rS','raw_rs','raw']),
            'cbb':pick(o,['CBB','cbb']),
            'gbb':pick(o,['gBB','gbb']),
            'i':pick(o,['i'])
        }
        meta={k:v for k,v in meta.items() if v is not None}
        raw_sid=sid(o,i+1)
        out.append({'id':canonical_session(raw_sid,i+1),'result':r,'dice':d,'sum':t,
                    'md5':pick(o,['md5','hash','md5_hash','md5Hash','hash_md5','md5Code','md5_code','md5_result','md5_enc','md5_dec']),
                    'meta':dict(meta or {},raw_session=str(raw_sid) if raw_sid is not None else None)})
    def key(x):
        try: return (0,int(x['id']))
        except: return (1,x['id'])
    out.sort(key=key)
    return out



def parse_sicbo(data):
    """Parser riêng SUNWIN Sicbo; chuẩn hóa #phiên và kiểm tra xúc xắc 1..6."""
    out=[]
    for i,o in enumerate(find_rows(data)):
        if not isinstance(o,dict):
            continue
        d=dice_values(o)
        if len(d)<3:
            continue
        try:
            d=[int(x) for x in d[:3]]
        except:
            continue
        if any(x<1 or x>6 for x in d):
            continue
        t=total_value(o,d)
        if not isinstance(t,(int,float)) or int(t)<3 or int(t)>18:
            t=sum(d)
        else:
            t=int(t)
        r=tx_result(o,d,t)
        if r not in ('TÀI','XỈU'):
            continue
        raw_sid=sid(o,i+1)
        sess=canonical_session(raw_sid,i+1)
        if not sess:
            continue
        out.append({
            'id':sess,
            'result':r,
            'dice':d,
            'sum':t,
            'md5':pick(o,['md5_enc','md5','hash','md5_hash','md5Hash','md5_result','md5_dec','raw_rS','raw_rs']),
            'meta':{
                'source_game':pick(o,['game','name']) or 'sunwin',
                'update_at':pick(o,['update_at','updated_at','last_update','last_update_at','tick_update_at']),
                'md5_dec':pick(o,['md5_dec']),
                'raw_rs':pick(o,['raw_rS','raw_rs']),
                'jackpot':pick(o,['jackpot']),
                'raw_session':str(raw_sid) if raw_sid is not None else None
            }
        })
    out.sort(key=lambda x:(_session_num(x['id']) is None,_session_num(x['id']) or 0))
    return out


def sicbo_hash_features(h):
    """Adapted deterministic feature analyzer from sicbo(1).txt.
    The score is a heuristic signal, not a real probability.
    """
    if not h:return None
    h=str(h).strip().lower()
    m=re.search(r'([0-9a-f]{32,64})',h)
    if not m:return None
    h=m.group(1)
    try:
        p=[int(h[i:i+2],16) for i in range(0,min(len(h),64),2) if i+2<=len(h)]
        if not p:return None
        dS=sum(int(c,16) for c in h[:32]);hS=sum(p)
        bO=bin(int(h[:32],16))[2:].count('1')
        try:b1=int(h[:32],16).bit_count()/(len(h[:32])*4)
        except:b1=0.0
        h8=sum(1 for c in h[:32] if int(c,16)>=8)/max(1,len(h[:32]))
        xV=0
        for v in p[:16]:xV^=v
        lu=[2,1]
        for _ in range(2,15):lu.append(lu[-1]+lu[-2])
        lw=sum(p[i]*lu[i] for i in range(min(len(p),15)))
        mean=sum(p)/len(p)
        std=math.sqrt(sum((x-mean)**2 for x in p)/len(p))
        comp=len(set(h[:32]))
        fo=sum(abs(p[i]-p[i-1]) for i in range(1,min(len(p),16)))
        sha=''.join(hex(((ord(h[i%len(h)])*(i+1)+7)%16))[2:] for i in range(56))
        sP=[int(sha[i:i+2],16) for i in range(0,len(sha)-1,2)]
        sS=sum(sP) if sP else 0
        hl=len(h)//2
        sym=sum(1 for i in range(min(hl,16)) if i<len(h) and hl+i<len(h) and h[i]==h[hl+i])
        first=p[:10]
        geo=math.pow(math.prod(first),1/len(first)) if len(first)>=10 and all(v>0 for v in first) else 0.0
        cX=xV^(int(sha[:2],16) if sha[:2] else 0)
        def fib_mod(x,mod):
            a,b=0,1
            for _ in range(2,x+1):a,b=b,a+b
            return b%mod
        fib=fib_mod(dS,100) if dS>0 else 0
        bX=0
        for i in range(0,len(sha)-1,2):bX^=int(sha[i:i+2],16)
        wE=(p[0]*3+p[-1]*2)%100 if p else 50
        mV=[hS%x for x in (43,47,53,59,61,67)] if hS else [0]*6
        maxR=max((h[:32].count(c) for c in set(h[:32])),default=0)
        odd=sum(1 for c in h[:32] if int(c,16)%2==1)
        sI=len(p)//4;eI=(3*len(p))//4
        mid=sum(p[sI:eI]) if sI<eI else 0
        fibH=sum(1 for c in h[:32] if c in '12358')
        shaSym=sum(1 for i in range(16) if i<len(sha) and 39-i<len(sha) and sha[i]==sha[39-i])
        freq={}
        for c in h[:32]:freq[c]=freq.get(c,0)+1
        ent=0.0
        for v in freq.values():
            pc=v/max(1,len(h[:32]))
            if pc>0:ent-=pc*math.log2(pc)
        tX=xV^bX^cX;last=int(h[-1],16);wf=1.0 if len(h)>=32 else .8
        score=(dS*.05+hS*.05+bO*.05+b1*.1+h8*.1+lw*.05+std*.05+comp*.05+fo*.05+
               sS*.05+sym*.05+geo*.05+cX*.05+fib*.05+bX*.05+wE*.05+sum(mV)*.05+
               maxR*.05+odd*.05+mid*.05+fibH*.05+shaSym*.05+ent*.05+tX*.05+last*.05)*wf%100
        return {'raw_score':round(score,4),'raw_prediction':'TÀI' if score>=50 else 'XỈU',
                'tai_signal':round(score,2),'xiu_signal':round(100-score,2),
                'entropy':round(ent,4),'bit_density':round(b1,4),'byte_std':round(std,3),'hex_unique':comp}
    except Exception:
        return None

def sicbo_hash_signal(rows):
    """Calibrate hash direction causally on this board's own history."""
    hist=[r for r in rows[-260:] if r.get('result') in ('TÀI','XỈU')]
    if not hist:return {'ready':False,'usable':False,'sample':0}
    tests=[]
    for i in range(1,len(hist)):
        f=sicbo_hash_features(hist[i-1].get('md5'))
        if f:tests.append((f['raw_prediction'],hist[i]['result']))
    n=len(tests);raw_w=sum(1 for p,a in tests if p==a);inv_w=sum(1 for p,a in tests if _opp(p)==a)
    orientation='normal' if raw_w>=inv_w else 'reverse'
    best_w=max(raw_w,inv_w)
    quality=(best_w+8*.5)/max(1,n+8)
    raw_acc=raw_w/max(1,n)
    latest=sicbo_hash_features(hist[-1].get('md5'))
    if not latest:
        return {'ready':False,'usable':False,'sample':n,'orientation':orientation,
                'quality':round(quality,4),'raw_accuracy':round(raw_acc,4)}
    pred=latest['raw_prediction'] if orientation=='normal' else _opp(latest['raw_prediction'])
    usable=(n>=18 and quality>=.525)
    return {'ready':True,'usable':usable,'sample':n,'orientation':orientation,
            'prediction':pred,'quality':round(quality,4),'raw_accuracy':round(raw_acc,4),
            'raw_prediction':latest['raw_prediction'],'raw_score':latest['raw_score'],
            'tai_signal':latest['tai_signal'],'xiu_signal':latest['xiu_signal'],
            'note':'calibrated historical hash signal'}

def sicbo_dice_side(rows):
    df=dice_position_forecast(rows)
    if not df.get('ready'):
        return {'ready':False,'prediction':None,'quality':0.0,'forecast':df}
    expected=float(df.get('expected_total') or 10.5)
    pred='TÀI' if expected>=10.5 else 'XỈU'
    q=float(df.get('quality') or 0.0)
    meta_q=_clamp(.48+max(0.0,q-.18)*.20,.48,.61)
    return {'ready':True,'prediction':pred,'quality':round(meta_q,4),'forecast':df}


def parse_xocdia(data):
    rows=[]
    for i,o in enumerate(find_rows(data)):
        if not isinstance(o,dict): continue
        raw=pick(o,['ket_qua_truyen_thong','ket_qua','result','ketQua'])
        z=str(raw or '').strip().upper()
        if 'CHẴN' in z or 'CHAN' in z or 'EVEN' in z: result='TÀI'   # internal binary: TÀI == CHẴN
        elif 'LẺ' in z or z=='LE' or 'ODD' in z: result='XỈU'       # internal binary: XỈU == LẺ
        else: continue
        colors=pick(o,['xuc_xac_goc','xuc_xac','coins','colors']) or []
        if not isinstance(colors,list): colors=[]
        norm=[str(x).lower() for x in colors]
        red=sum(1 for x in norm if 'do'==x or 'đỏ' in x or x=='red')
        white=sum(1 for x in norm if 'trang'==x or 'trắng' in x or x=='white')
        raw_sid=sid(o,i+1)
        rows.append({
            'id':canonical_session(raw_sid,i+1),'result':result,'dice':[],'sum':red,
            'md5':pick(o,['md5','md5_raw','hash']),
            'meta':{'colors':norm[:4],'red_count':red,'white_count':white,
                    'detail':pick(o,['ket_qua_chi_tiet','ket_qua_chi_tiet_goc']),
                    'jackpot_result':pick(o,['jackpot_result_goc','jackpot_result'])}
        })
    def key(x):
        try:return (0,int(re.sub(r'\D','',x['id']) or 0))
        except:return (1,x['id'])
    rows.sort(key=key)
    return rows


def dice_position_forecast(rows):
    """Ensemble vị Sicbo: decay-frequency + Markov1/2 + lag-cycle + sum-state.
    `probability` là tỷ trọng model nội bộ, không phải xác suất thật của viên xúc xắc.
    """
    valid=[r for r in rows[-850:] if isinstance(r.get('dice'),list) and len(r.get('dice'))>=3]
    valid=[r for r in valid if all(isinstance(x,(int,float)) and 1<=int(x)<=6 for x in r.get('dice')[:3])]
    if len(valid)<18:
        return {'ready':False,'sample':len(valid),'faces':[],'estimated_total':None,'sum_zone':'ĐANG HỌC'}

    def fit_pos(hist,j):
        last=int(hist[-1]['dice'][j])
        prev2=tuple(int(r['dice'][j]) for r in hist[-2:])
        scores={k:2.2 for k in range(1,7)}

        # 1) recency-decayed face frequency
        for idx,r in enumerate(hist):
            v=int(r['dice'][j]); age=len(hist)-1-idx
            scores[v]+=0.78*(0.5**(age/52))

        # 2) Markov-1 transition from current face
        ev1=0.0
        for idx in range(1,len(hist)):
            if int(hist[idx-1]['dice'][j])!=last: continue
            nxt=int(hist[idx]['dice'][j]); age=len(hist)-1-idx
            w=0.5**(age/44); scores[nxt]+=1.65*w; ev1+=w

        # 3) Markov-2 on this position
        ev2=0.0
        for idx in range(2,len(hist)):
            ctx=(int(hist[idx-2]['dice'][j]),int(hist[idx-1]['dice'][j]))
            if ctx!=prev2: continue
            nxt=int(hist[idx]['dice'][j]); age=len(hist)-1-idx
            w=0.5**(age/48); scores[nxt]+=1.28*w; ev2+=w

        # 4) strongest positive/inverse lag state
        arr=[int(r['dice'][j]) for r in hist[-180:]]
        best_lag=None; best_edge=0.0; best_face=None
        for lag in (2,3,4,5,6,7,8,10,12):
            if len(arr)<lag+28: continue
            same=sum(1 for idx in range(lag,len(arr)) if arr[idx]==arr[idx-lag])
            rate=same/max(1,len(arr)-lag)
            edge=rate-(1/6)
            if edge>best_edge:
                best_edge=edge; best_lag=lag; best_face=arr[-lag]
        if best_lag and best_edge>.035:
            scores[best_face]+=min(1.6, .45+best_edge*6.0)

        # 5) previous total band -> next face at this position
        cur_sum=hist[-1].get('sum')
        if isinstance(cur_sum,(int,float)):
            band='L' if cur_sum<=8 else 'H' if cur_sum>=13 else 'M'
            for idx in range(1,len(hist)):
                ps=hist[idx-1].get('sum')
                if not isinstance(ps,(int,float)): continue
                pb='L' if ps<=8 else 'H' if ps>=13 else 'M'
                if pb!=band: continue
                nxt=int(hist[idx]['dice'][j]); age=len(hist)-1-idx
                scores[nxt]+=.58*(0.5**(age/46))

        total_score=sum(scores.values()) or 1.0
        ordered=sorted(scores.items(),key=lambda kv:kv[1],reverse=True)
        top,second=ordered[0],ordered[1]
        edge=(top[1]-second[1])/max(.001,top[1]+second[1])
        mean=sum(face*sc for face,sc in scores.items())/total_score
        return {
            'position':j+1,
            'face':top[0],
            'expected':round(mean,3),
            'strength':round(edge,3),
            'model_share':round(top[1]/total_score,3),
            'markov1_support':round(ev1,2),
            'markov2_support':round(ev2,2),
            'lag':best_lag or 0,
            'top3':[{'face':f,'share':round(sc/total_score,3)} for f,sc in ordered[:3]]
        }

    faces=[fit_pos(valid,j) for j in range(3)]
    expected_total=sum(x['expected'] for x in faces)
    est_top=sum(x['face'] for x in faces)
    zone='3–8' if expected_total<8.5 else '9–10' if expected_total<10.5 else '11–12' if expected_total<12.5 else '13–18'

    # lightweight walk-forward top-1 validation on recent known rounds
    checks=0; hits=[0,0,0]
    start=max(18,len(valid)-90)
    step=3  # keep CPU light while polling
    for idx in range(start,len(valid),step):
        hist=valid[:idx]
        if len(hist)<18: continue
        for j in range(3):
            pred=fit_pos(hist,j)['face']
            hits[j]+=int(pred==int(valid[idx]['dice'][j]))
        checks+=1
    validation=[round(h/max(1,checks),3) for h in hits]

    # reliability shrinks when top-1 validation is no better than the 1/6 baseline
    rel=sum(max(0.0,v-(1/6)) for v in validation)/3
    overall_strength=sum(x['strength'] for x in faces)/3
    quality=_clamp(.15 + rel*2.6 + overall_strength*1.8,.15,.78)

    return {
        'ready':True,'sample':len(valid),'faces':faces,
        'estimated_total_top':est_top,'expected_total':round(expected_total,2),
        'sum_zone':zone,'validation':validation,'checks':checks,
        'quality':round(quality,3)
    }


def xocdia_detail_forecast(rows, binary_prediction):
    want_even=(binary_prediction=='TÀI')
    allowed=[k for k in range(5) if (k%2==0)==want_even]
    scores={k:1.4 for k in allowed}
    valid=[]
    for r in rows[-700:]:
        meta=r.get('meta') or {};red=meta.get('red_count')
        if isinstance(red,int) and 0<=red<=4:valid.append(red)
    for idx,red in enumerate(valid):
        if red not in scores:continue
        age=len(valid)-1-idx;scores[red]+=0.5**(age/44)
    # transition conditioned on the last observed red-count
    if valid:
        last=valid[-1]
        for i in range(1,len(valid)):
            if valid[i-1]==last and valid[i] in scores:
                age=len(valid)-1-i;scores[valid[i]]+=1.25*(0.5**(age/40))
    if not scores:return None
    total=sum(scores.values());best=max(scores,key=scores.get)
    return {'red_count':best,'white_count':4-best,'label':f'{best} Đỏ · {4-best} Trắng',
            'sample':len(valid),'probability':round(scores[best]/max(.001,total),3)}


def parse_baccarat(data):
    rows = data if isinstance(data,list) else (data.get('predictions',[]) if isinstance(data,dict) else [])
    by_table={}
    for item in rows:
        if not isinstance(item,dict): continue
        table=str(item.get('table') or item.get('ban') or 'BÀN')
        rid=item.get('phien') or item.get('round') or item.get('id') or 1
        road=''.join(ch for ch in str(item.get('result') or item.get('history') or item.get('road') or '').upper() if ch in 'PBT')
        try: base=int(rid)
        except: base=None
        arr=[]
        for i,ch in enumerate(road):
            if ch=='T': continue
            sess=str(base-len(road)+1+i) if base is not None else str(i+1)
            arr.append({'id':sess,'result':'TÀI' if ch=='P' else 'XỈU','dice':[],'sum':None,'md5':None})
        if arr: by_table[table]=arr
    return by_table


def _clamp(v,a,b):
    return max(a,min(b,v))


def _entropy(seq):
    if not seq: return 1.0
    p=sum(1 for x in seq if x=='TÀI')/len(seq)
    if p<=0 or p>=1: return 0.0
    return -p*math.log2(p)-(1-p)*math.log2(1-p)


def _session_num(v):
    m=re.search(r'(\d+)(?!.*\d)',str(v)) if v is not None else None
    try: return int(m.group(1)) if m else None
    except: return None

def _stable_rows(rows):
    u={str(r.get('id')):r for r in rows if r.get('id') is not None}
    nums=sorted([r for r in u.values() if _session_num(r.get('id')) is not None],key=lambda r:_session_num(r['id']))
    if len(nums)>=4:
        groups=[]; g=[nums[0]]
        for r in nums[1:]:
            if _session_num(r['id'])-_session_num(g[-1]['id'])<=12:g.append(r)
            else:groups.append(g);g=[r]
        groups.append(g)
        best=max(groups,key=lambda x:(len(x),_session_num(x[-1]['id'])))
        if len(best)>=3: nums=best
    return nums

def _next_session(rows):
    s=_stable_rows(rows)
    return str(_session_num(s[-1]['id'])+1) if s else None



def _sigmoid(z):
    z=_clamp(float(z),-18.0,18.0)
    return 1.0/(1.0+math.exp(-z))


def _ml_feature(valid, upto):
    """Features computed only from rows before `upto`, so training never peeks at the target."""
    if upto < 12: return None
    hist=valid[:upto]
    seq=[1 if r.get('result')=='TÀI' else -1 for r in hist]
    f=[1.0]
    # last 5 binary outcomes
    for k in range(1,6):
        f.append(float(seq[-k]) if len(seq)>=k else 0.0)
    # multi-window balances
    for w in (6,12,24,48):
        q=seq[-w:]
        f.append(sum(q)/max(1,len(q)))
    # signed run length
    run=1
    for j in range(len(seq)-2,-1,-1):
        if seq[j]==seq[-1] and run<8: run+=1
        else: break
    f.append(seq[-1]*run/8.0)
    # flip rate
    q=seq[-16:]
    fr=sum(1 for j in range(1,len(q)) if q[j]!=q[j-1])/max(1,len(q)-1)
    f.append((fr-.5)*2.0)
    # transition conditioned on last state, Laplace-smoothed
    p=x=1.5
    for j in range(1,len(seq)):
        if seq[j-1]!=seq[-1]: continue
        if seq[j]>0:p+=1
        else:x+=1
    f.append((p-x)/(p+x))
    # order-2 context
    p=x=1.5
    if len(seq)>=3:
        ctx=tuple(seq[-2:])
        for j in range(2,len(seq)):
            if tuple(seq[j-2:j])!=ctx: continue
            if seq[j]>0:p+=1
            else:x+=1
    f.append((p-x)/(p+x))
    # entropy / regime
    raw=['TÀI' if v>0 else 'XỈU' for v in seq[-36:]]
    f.append((.5-_entropy(raw))*2.0)
    # dice/sum state
    last=hist[-1]
    sm=last.get('sum')
    f.append(_clamp(((float(sm)-10.5)/7.5) if isinstance(sm,(int,float)) else 0.0,-1,1))
    sums=[r.get('sum') for r in hist[-10:] if isinstance(r.get('sum'),(int,float))]
    f.append(_clamp(((sum(sums)/len(sums)-10.5)/5.0) if sums else 0.0,-1,1))
    if len(sums)>=2: f.append(_clamp((sums[-1]-sums[-2])/8.0,-1,1))
    else: f.append(0.0)
    d=last.get('dice') or []
    if len(d)>=3:
        f.append((sum(1 for v in d[:3] if int(v)>=4)-1.5)/1.5)
        f.append((sum(1 for v in d[:3] if int(v)%2==0)-1.5)/1.5)
        pair=len(set(int(v) for v in d[:3]))
        f.append(1.0 if pair==1 else .35 if pair==2 else -.35)
    else:
        f.extend([0.0,0.0,0.0])
    # optional crowd-flow feature; low influence and only when source exposes both totals
    meta=last.get('meta') or {}
    try:
        tt=float(meta.get('total_tai')); tx=float(meta.get('total_xiu'))
        f.append(_clamp((tt-tx)/max(1.0,tt+tx),-1,1))
    except:
        f.append(0.0)
    return f


def _online_ml_signal(rows, board=None):
    valid=[r for r in rows[-560:] if r.get('result') in ('TÀI','XỈU')]
    if len(valid)<70:
        return {'ready':False,'sample':max(0,len(valid)-12),'score':0.0,'p_tai':.5,'val_accuracy':.5,'brier':.25}
    last_id=str(valid[-1].get('id'))
    cache_key=(board or '',last_id,len(valid))
    if board and cache_key in _ml_cache:
        return _ml_cache[cache_key]
    samples=[]
    for i in range(12,len(valid)):
        x=_ml_feature(valid,i)
        if x is None: continue
        y=1.0 if valid[i].get('result')=='TÀI' else 0.0
        samples.append((x,y))
    if len(samples)<45:
        res={'ready':False,'sample':len(samples),'score':0.0,'p_tai':.5,'val_accuracy':.5,'brier':.25}
        if board:_ml_cache[cache_key]=res
        return res
    cut=max(30,min(len(samples)-12,int(len(samples)*.76)))
    train=samples[:cut]; val=samples[cut:]
    dim=len(train[0][0]); w=[0.0]*dim
    # SGD with L2 and recency emphasis
    for ep in range(5):
        lr=.050/(1.0+ep*.38)
        L=max(1,len(train))
        for idx,(x,y) in enumerate(train):
            p=_sigmoid(sum(a*b for a,b in zip(w,x)))
            rw=.35+.65*((idx+1)/L)
            err=(y-p)*rw
            for j in range(dim):
                reg=.0025*w[j] if j else 0.0
                w[j]+=lr*(err*x[j]-reg)
    correct=0; brier=0.0
    for x,y in val:
        p=_sigmoid(sum(a*b for a,b in zip(w,x)))
        correct += int((p>=.5)==(y>=.5))
        brier += (p-y)**2
    acc=correct/max(1,len(val)); brier/=max(1,len(val))
    # train the final model on all known samples after validation is measured
    for ep in range(2):
        lr=.025/(1+ep*.4); L=max(1,len(samples))
        for idx,(x,y) in enumerate(samples):
            p=_sigmoid(sum(a*b for a,b in zip(w,x)))
            rw=.45+.55*((idx+1)/L); err=(y-p)*rw
            for j in range(dim):
                reg=.002*w[j] if j else 0.0
                w[j]+=lr*(err*x[j]-reg)
    cur=_ml_feature(valid,len(valid))
    p=_sigmoid(sum(a*b for a,b in zip(w,cur)))
    reliability=_clamp(.08+max(0,acc-.50)*2.6+max(0,.25-brier)*1.7,.08,.72)
    # if validation is below chance, strongly shrink rather than invert/overfit.
    if acc<.49 and brier>=.25: reliability*=.45
    strength=(p-.5)*2.0*reliability
    res={'ready':True,'sample':len(samples),'validation':len(val),'p_tai':round(p,4),
         'val_accuracy':round(acc,4),'brier':round(brier,4),'reliability':round(reliability,4),
         'score':round(_clamp(strength,-.58,.58),4)}
    if board:
        # keep only the newest cache for this board
        for k in list(_ml_cache):
            if k[0]==board and k!=cache_key:_ml_cache.pop(k,None)
        _ml_cache[cache_key]=res
    return res


def model_snapshot(rows, game=None, board=None):
    seq=[r['result'] for r in rows if r.get('result') in ('TÀI','XỈU')]
    if len(seq)<6:
        return {'sample':len(seq),'score':0.0,'prediction':None,'confidence':50,'percent':50,
                'pattern':'ĐANG HỌC','alt':'Chưa đủ mẫu','entropy':round(_entropy(seq),4),
                'cycle':{'k':0,'r':0.0},'agreement':0.5,'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51',
                'skills':0,'totalSkills':60,'ml':{'ready':False},'updated_at':time.time()}
    vote=lambda x: 1 if x=='TÀI' else -1
    n=len(seq); comps=[]
    def add(name,score,weight):
        if isinstance(score,(int,float)) and math.isfinite(score):
            comps.append((name,_clamp(float(score),-.85,.85),float(weight)))

    # Decayed Markov / VOM contexts 1..5
    for order,wt in [(1,1.00),(2,1.15),(3,1.24),(4,1.18),(5,1.05)]:
        if n<order+5: continue
        ctx=tuple(seq[-order:]); p=x=1.6; ev=0.0
        for i in range(order,n):
            if tuple(seq[i-order:i])!=ctx: continue
            age=(n-1)-i; w=0.5**(age/34)
            if seq[i]=='TÀI': p+=w
            else: x+=w
            ev+=w
        if ev>=1.2: add(f'ctx{order}',(p-x)/(p+x),wt)

    # Multi-window balance + EWMA trend
    vals=[]
    for win in (6,10,18,30,48,80):
        q=seq[-win:]
        if len(q)>=min(6,win):
            bal=sum(vote(z) for z in q)/len(q); vals.append(bal)
            add(f'window{win}',bal,(.48+.30*min(1,win/48)))
    if vals:
        pos=sum(v>0 for v in vals); neg=sum(v<0 for v in vals)
        if max(pos,neg)/len(vals)>=.6:
            add('multiwin',sum(v/(1+i*.32) for i,v in enumerate(vals))/sum(1/(1+i*.32) for i in range(len(vals)))*.46,.76)
    ew=0.0; denew=0.0
    for age,z in enumerate(reversed(seq[-80:])):
        w=.5**(age/14); ew+=vote(z)*w; denew+=w
    if denew:add('ewma',ew/denew,.72)
    if n>=30:
        a=sum(vote(z) for z in seq[-10:])/10
        b=sum(vote(z) for z in seq[-30:-10])/20
        add('balance_slope',(a-b)*.55,.62)
        if abs(a-b)>=.35:add('changepoint',a*.24,.56)

    # Run pressure + flip/repeat state
    run=1
    for i in range(n-2,-1,-1):
        if seq[i]==seq[-1]: run+=1
        else: break
    if run>=5: add('runpressure',-vote(seq[-1])*(.13+.025*min(run-5,4)),.68)
    elif run==4: add('runpressure',-vote(seq[-1])*.10,.62)
    elif run==3: add('runpressure',-vote(seq[-1])*.04,.50)
    current_flip=seq[-1]!=seq[-2]
    same=flip=1.8; ev=0.0
    for i in range(2,n):
        if (seq[i-1]!=seq[i-2])!=current_flip: continue
        age=(n-1)-i; w=0.5**(age/34)
        if seq[i]==seq[i-1]: same+=w
        else: flip+=w
        ev+=w
    if ev>=2:add('fliprepeat',vote(seq[-1])*(same-flip)/(same+flip),.66)
    if n>=10:
        recent=seq[-14:]; flips=sum(1 for i in range(1,len(recent)) if recent[i]!=recent[i-1])
        rate=flips/max(1,len(recent)-1)
        if rate>.64:add('flippressure',-vote(seq[-1])*(rate-.5)*1.35,.72)
        elif rate<.36:add('repeatpressure',vote(seq[-1])*(.5-rate)*1.25,.68)

    # Recent transition + run-length conditional
    if n>=12:
        cur=seq[-1]; p=x=1.5; ev=0.0
        for i in range(1,n):
            if seq[i-1]!=cur: continue
            w=0.5**(((n-1)-i)/24)
            if seq[i]=='TÀI': p+=w
            else:x+=w
            ev+=w
        if ev>=2:add('transition',(p-x)/(p+x),.82)
    if n>=18:
        p=x=1.5; ev=0.0
        for i in range(2,n):
            rr=1; j=i-2
            while j>=0 and seq[j]==seq[i-1] and rr<8:
                rr+=1;j-=1
            if rr!=min(run,8):continue
            w=0.5**(((n-1)-i)/30)
            if seq[i]=='TÀI':p+=w
            else:x+=w
            ev+=w
        if ev>=1.5:add('runcond',(p-x)/(p+x),.76)

    # Motifs 3..6
    for L,wt in ((3,.70),(4,.78),(5,.84),(6,.78)):
        if n<L+8:continue
        motif=tuple(seq[-L:]);p=x=1.4;ev=0.0
        for i in range(L,n):
            if tuple(seq[i-L:i])!=motif:continue
            w=0.5**(((n-1)-i)/28)
            if seq[i]=='TÀI':p+=w
            else:x+=w
            ev+=w
        if ev>=1.2:add(f'motif{L}',(p-x)/(p+x),wt)

    # Markov order 2/3 with a separate recency window
    for order,wt,half in ((2,.88,34),(3,.91,40)):
        if n<order+18:continue
        ctx=tuple(seq[-order:]);p=x=1.6;ev=0.0
        for i in range(order,n):
            if tuple(seq[i-order:i])!=ctx:continue
            w=0.5**(((n-1)-i)/half);ev+=w
            if seq[i]=='TÀI':p+=w
            else:x+=w
        if ev>=2:add(f'markov{order}',(p-x)/(p+x),wt)

    # Multi-lag positive/inverse autocorrelation candidates
    arr=[vote(x) for x in seq[-120:]]; best=0.0; lag=0
    if len(arr)>=14:
        mean=sum(arr)/len(arr);den=sum((x-mean)**2 for x in arr)
        if den:
            for k in range(1,min(16,len(arr)//3)+1):
                num=sum((arr[i]-mean)*(arr[i+k]-mean) for i in range(len(arr)-k));r=num/den
                if abs(r)>abs(best):best=r;lag=k
                if abs(r)>=.16:
                    base=vote(seq[-k]);add(f'lag{k}',base*(1 if r>0 else -1)*min(.46,abs(r)*.75),.60)
    if lag:
        lagv=vote(seq[-lag]);add('autocorr',(lagv if best>=0 else -lagv)*min(.44,abs(best)*.72),.76)

    # Dice/sum contextual experts
    valid=[r for r in rows if r.get('result') in ('TÀI','XỈU')]
    if valid:
        cur_sum=valid[-1].get('sum')
        if isinstance(cur_sum,(int,float)):
            p=x=1.7;ev=0
            for i,r in enumerate(valid[:-1]):
                if r.get('sum')!=cur_sum:continue
                w=0.5**(((len(valid)-2)-i)/40)
                if valid[i+1]['result']=='TÀI':p+=w
                else:x+=w
                ev+=w
            if ev>=1.8:add('exactsum',(p-x)/(p+x),.72)
            bucket='LOW' if cur_sum<=8 else 'HIGH' if cur_sum>=13 else 'MID'
            p=x=1.6;ev=0
            for i,r in enumerate(valid[:-1]):
                sm=r.get('sum')
                if not isinstance(sm,(int,float)):continue
                b='LOW' if sm<=8 else 'HIGH' if sm>=13 else 'MID'
                if b!=bucket:continue
                w=0.5**(((len(valid)-2)-i)/36)
                if valid[i+1]['result']=='TÀI':p+=w
                else:x+=w
                ev+=w
            if ev>=1.8:add('sumband',(p-x)/(p+x),.68)
            if len(valid)>=3 and isinstance(valid[-2].get('sum'),(int,float)):
                cur_delta=1 if cur_sum>valid[-2]['sum'] else -1 if cur_sum<valid[-2]['sum'] else 0
                p=x=1.6;ev=0
                for i in range(2,len(valid)):
                    a=valid[i-2].get('sum');b=valid[i-1].get('sum')
                    if not isinstance(a,(int,float)) or not isinstance(b,(int,float)):continue
                    d=1 if b>a else -1 if b<a else 0
                    if d!=cur_delta:continue
                    w=.5**(((len(valid)-1)-i)/36)
                    if valid[i]['result']=='TÀI':p+=w
                    else:x+=w
                    ev+=w
                if ev>=1.8:add('sumdelta',(p-x)/(p+x),.64)

        cur_d=valid[-1].get('dice') or []
        if len(cur_d)>=3:
            def state_parity(d):return sum(int(v)%2==0 for v in d[:3])
            def state_high(d):return sum(int(v)>=4 for v in d[:3])
            def state_pair(d):return len(set(int(v) for v in d[:3]))
            for name,fn,wt in [('parity',state_parity,.58),('highcount',state_high,.62),('pairstate',state_pair,.57)]:
                cur=fn(cur_d);p=x=1.6;ev=0
                for i,r in enumerate(valid[:-1]):
                    d=r.get('dice') or []
                    if len(d)<3 or fn(d)!=cur:continue
                    w=.5**(((len(valid)-2)-i)/36)
                    if valid[i+1]['result']=='TÀI':p+=w
                    else:x+=w
                    ev+=w
                if ev>=1.8:add(name,(p-x)/(p+x),wt)
            # Per-position face-conditioned next-result experts
            for pos in range(3):
                face=int(cur_d[pos]);p=x=1.5;ev=0
                for i,r in enumerate(valid[:-1]):
                    d=r.get('dice') or []
                    if len(d)<3:continue
                    try:match=int(d[pos])==face
                    except:match=False
                    if not match:continue
                    w=.5**(((len(valid)-2)-i)/38)
                    if valid[i+1]['result']=='TÀI':p+=w
                    else:x+=w
                    ev+=w
                if ev>=2.0:add(f'pos{pos+1}face',(p-x)/(p+x),.54)

        # Sicbo-only position/total expectation expert.
        if board=='sunwin:sicbo' and len(valid)>=24:
            try:
                df=dice_position_forecast(valid)
                if df.get('ready'):
                    exp=float(df.get('expected_total',10.5))
                    q=float(df.get('quality',.2))
                    # Small expert only; exact dice are inherently noisy.
                    add('sicbo_possum',_clamp((exp-10.5)/5.5,-.55,.55),.36+.28*q)
            except:
                pass

        # Weak crowd-flow expert if source exposes totals; never treated as ground truth.
        meta=valid[-1].get('meta') or {}
        try:
            tt=float(meta.get('total_tai'));tx=float(meta.get('total_xiu'))
            crowd=(tt-tx)/max(1.0,tt+tx)
            if abs(crowd)>=.03:add('crowdflow',crowd*.35,.28)
        except:pass

    # Online logistic learner trained/validated only on this board's historical rows.
    ml=_online_ml_signal(rows,board)
    if ml.get('ready') and abs(ml.get('score',0))>=.012:
        # validation/reliability is already folded into score; keep it as one expert, not a dictator.
        add('online_ml',ml['score'],1.12)

    active=[c for c in comps if abs(c[1])>=.028]
    if not active:active=[('fallback',vote(seq[-1])*.02,.25)]
    num=sum(sc*wt for _,sc,wt in active);den=sum(abs(wt) for _,_,wt in active) or 1
    raw=num/den
    posw=sum(abs(wt) for _,sc,wt in active if sc>0);negw=sum(abs(wt) for _,sc,wt in active if sc<0)
    agreement=max(posw,negw)/max(.001,posw+negw)
    H=_entropy(seq[-80:])
    sample_factor=_clamp(n/42,.56,1.0)
    entropy_factor=_clamp(1.22-H*.43,.72,1.0)
    agree_factor=.62 if agreement<.55 else .82 if agreement<.67 else 1.0
    score=_clamp(raw*sample_factor*entropy_factor*agree_factor,-.82,.82)
    # explicit conflict damping (fixes the old undefined `signals` runtime bug)
    score*=1.0-min(.42,max(0.0,1.0-agreement)*.72)
    if abs(score)<.012:score=vote(seq[-1])*.012
    pred='TÀI' if score>=0 else 'XỈU'
    evidence=_clamp(abs(score)*1.92+max(0,agreement-.5)*.46,0,1)
    cap=64 if game=='baccarat' else 69
    conf=round(_clamp(50+evidence*(cap-50),51,cap))
    if run>=3:pattern=f"BỆT {seq[-1]} x{run}"
    elif current_flip:pattern='ĐẢO 1-1 / FLIP'
    else:pattern='CẦU HỖN HỢP'
    top=sorted(active,key=lambda c:abs(c[1]*c[2]),reverse=True)[:8]
    top_signals=[{'name':name,'score':round(sc,3),'weight':round(wt,3)} for name,sc,wt in top]
    ml_txt=(f" · ML val {round(ml.get('val_accuracy',.5)*100)}%" if ml.get('ready') else '')
    alt=f"{len(active)} tín hiệu · đồng thuận {round(agreement*100)}% · entropy {H:.3f}{ml_txt}"
    return {'sample':n,'score':round(score,4),'prediction':pred,'confidence':conf,'percent':conf,
            'run':run,'pattern':pattern,'alt':alt,'entropy':round(H,4),
            'cycle':{'k':lag,'r':round(best,4)},'agreement':round(agreement,4),
            'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51','skills':len(active),'totalSkills':60,
            'ml':ml,'top_signals':top_signals,'updated_at':time.time()}

# V32: V31 fusion becomes one candidate strategy instead of the only final decider.
fusion_model_snapshot = model_snapshot

STRATEGY_NAMES = (
    'FOLLOW_LAST','REVERSE_LAST','ALTERNATING_PATTERN','RUN_BREAK','RUN_FOLLOW',
    'BIAS_MEAN_REVERSION','BIAS_MOMENTUM','MARKOV_TRANSITION','ANTI_RAW',
    'HIGH_ORDER_MARKOV','MARKOV_ORDER2','RUN_HAZARD','MULTI_WINDOW',
    'DECAYED_TRANSITION','REGIME_ADAPTIVE','MOTIF_WEIGHTED',
    'FLIP_STATE_MARKOV','RUN_LENGTH_MARKOV','PERIODIC_MATCH','DUAL_HORIZON',
    'ANALOG_KNN','RUN_SURVIVAL','CONTEXT_ENTROPY',
    'BAYES_CONTEXT','HORIZON_CONSENSUS','REGIME_SWITCH','LAG_ENSEMBLE',
    'REFERENCE_PATTERN_PRIOR','JS_TREND_BLEND','JS_BREAK_CALIBRATOR','JS_ULTRA_STACK',
    'VOM_CONTEXT_6','LONG_MEMORY_BAYES','RUN_PROFILE_LONG','MULTISCALE_TRANSITION',
    'CHANGEPOINT_ADAPTIVE','WILSON_CONTEXT','KNN_RECENCY','RUN_MATRIX',
    'ENTROPY_GATE','CROSS_HORIZON_BAYES',
    'DIRICHLET_VOM','SEQUENTIAL_CHANGE','MOTIF_SURVIVAL',
    'REGIME_POSTERIOR','MULTIRESOLUTION_EDGE','BAYES_RUN_MIXTURE',
    'CTW_APPROX','HIERARCHICAL_BAYES','TRANSITION_DRIFT','RUN_CONTEXT_JOINT','SPECTRAL_LAG','ROBUST_STACK',
    'SUFFIX_CONTEXT','FUSION_CORE'
)


# V34: mỗi board tự đánh giá strategy trên lịch sử của chính board đó.
# HITCLUB giữ CHAMPION vì người dùng báo đang chạy ổn; các board còn lại dùng
# walk-forward + consensus thay vì bê nguyên champion của HITCLUB sang.
BOARD_META_PROFILES = {
    'hitclub:hu':   {'mode':'champion','top_k':1,'wf_depth':160,'reverse_min':28,'reverse_gap':.16},
    'hitclub:md5':  {'mode':'champion','top_k':1,'wf_depth':160,'reverse_min':28,'reverse_gap':.16},
    'sunwin:hu':    {'mode':'consensus','top_k':4,'wf_depth':220,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('CTW_APPROX','HIERARCHICAL_BAYES','ROBUST_STACK','MARKOV_ORDER2','CROSS_HORIZON_BAYES','SUFFIX_CONTEXT')},
    'sunwin:sicbo': {'mode':'consensus','top_k':5,'wf_depth':240,'reverse_min':40,'reverse_gap':.22,
                     'prefer':('REGIME_ADAPTIVE','DECAYED_TRANSITION','RUN_LENGTH_MARKOV','FLIP_STATE_MARKOV',
                               'MOTIF_WEIGHTED','PERIODIC_MATCH','MARKOV_ORDER2','RUN_HAZARD','SUFFIX_CONTEXT','FUSION_CORE',
                               'REFERENCE_PATTERN_PRIOR','JS_BREAK_CALIBRATOR','JS_ULTRA_STACK','CHANGEPOINT_ADAPTIVE','WILSON_CONTEXT','KNN_RECENCY','CROSS_HORIZON_BAYES')},
    'lc79:hu':      {'mode':'consensus','top_k':4,'wf_depth':220,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('CTW_APPROX','ROBUST_STACK','RUN_CONTEXT_JOINT','MARKOV_ORDER2','CROSS_HORIZON_BAYES','SUFFIX_CONTEXT')},
    'lc79:md5':     {'mode':'consensus','top_k':4,'wf_depth':240,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('HIERARCHICAL_BAYES','CTW_APPROX','ROBUST_STACK','SUFFIX_CONTEXT','HIGH_ORDER_MARKOV','MARKOV_ORDER2')},
    'lc79:xocdia':  {'mode':'consensus','top_k':3,'wf_depth':200,'reverse_min':36,'reverse_gap':.20,
                     'prefer':('ROBUST_STACK','RUN_CONTEXT_JOINT','CTW_APPROX','MARKOV_ORDER2','RUN_HAZARD','SUFFIX_CONTEXT')},
    'gb68:hu':      {'mode':'consensus','top_k':3,'wf_depth':180,'reverse_min':32,'reverse_gap':.19},
    'gb68:md5':     {'mode':'consensus','top_k':4,'wf_depth':220,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('SUFFIX_CONTEXT','MARKOV_ORDER2','HIGH_ORDER_MARKOV','FUSION_CORE')},
    'betvip:hu':    {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'betvip:md5':   {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'b52:hu':       {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'b52:md5':      {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'max789:hu':    {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'max789:md5':   {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'son789:hu':    {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
    'son789:md5':   {'mode':'consensus','top_k':3,'wf_depth':160,'reverse_min':32,'reverse_gap':.20},
}
_WF_CACHE={}

def _profile_for(board):
    if board in BOARD_META_PROFILES:return BOARD_META_PROFILES[board]
    if str(board).startswith('baccarat:'):
        return {'mode':'consensus','top_k':5,'wf_depth':260,'reverse_min':42,'reverse_gap':.22,
                'prefer':('BAYES_CONTEXT','REGIME_SWITCH','ANALOG_KNN','CONTEXT_ENTROPY','RUN_SURVIVAL',
                          'HORIZON_CONSENSUS','LAG_ENSEMBLE','REGIME_ADAPTIVE','DECAYED_TRANSITION','MOTIF_WEIGHTED','MARKOV_ORDER2','CHANGEPOINT_ADAPTIVE','WILSON_CONTEXT','KNN_RECENCY','ENTROPY_GATE','CROSS_HORIZON_BAYES','FUSION_CORE')}
    return {'mode':'consensus','top_k':3,'wf_depth':170,'reverse_min':34,'reverse_gap':.20}

def _opp(side):
    return 'XỈU' if side=='TÀI' else 'TÀI'

def _seq(rows):
    return [r.get('result') for r in rows if r.get('result') in ('TÀI','XỈU')]

def _run_len(seq):
    if not seq: return 0
    n=1
    for i in range(len(seq)-2,-1,-1):
        if seq[i]==seq[-1]: n+=1
        else: break
    return n

def _markov_prediction(seq, order=1):
    if not seq: return 'TÀI'
    order=max(1,min(4,int(order)))
    if len(seq)<=order+2: return _opp(seq[-1])
    ctx=tuple(seq[-order:]); t=x=1.0
    for i in range(order,len(seq)):
        if tuple(seq[i-order:i])!=ctx: continue
        if seq[i]=='TÀI': t+=1
        else: x+=1
    return 'TÀI' if t>=x else 'XỈU'

def _suffix_prediction(seq):
    if not seq: return 'TÀI'
    for L in (6,5,4,3,2):
        if len(seq)<=L: continue
        motif=tuple(seq[-L:]); t=x=0
        for i in range(L,len(seq)):
            if tuple(seq[i-L:i])!=motif: continue
            if seq[i]=='TÀI': t+=1
            else: x+=1
        if t+x>=2: return 'TÀI' if t>=x else 'XỈU'
    return _markov_prediction(seq,1)

def _recent_final_stats(board,limit=50):
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute(
            '''SELECT prediction,actual,ok,model_json
               FROM shared_predictions
               WHERE board=? AND actual IS NOT NULL AND ok IS NOT NULL
               ORDER BY settled_at DESC LIMIT ?''',(board,int(limit))
        ).fetchall()
    out=[]
    for pred,actual,ok,mj in rows:
        try: model=json.loads(mj) if mj else {}
        except: model={}
        out.append({'prediction':pred,'actual':actual,'ok':bool(ok),'model':model})
    return out

def _anti_phase(board):
    hist=_recent_final_stats(board,20)
    anti_tai=anti_xiu=wrong=0
    for h in hist:
        if h['ok']: continue
        wrong+=1
        raw=(h.get('model') or {}).get('raw_prediction') or h.get('prediction')
        if raw=='TÀI': anti_xiu+=1
        elif raw=='XỈU': anti_tai+=1
    return {'wrong_rate':wrong/max(1,len(hist)),'anti_tai':anti_tai,'anti_xiu':anti_xiu,'sample':len(hist)}


def _run_hazard_prediction(seq):
    if not seq: return 'TÀI'
    side=seq[-1]; cur=_run_len(seq)
    follow=brk=1.0
    for i in range(1,len(seq)):
        if seq[i-1]!=side: continue
        rl=1; j=i-2
        while j>=0 and seq[j]==side and rl<8:
            rl+=1; j-=1
        if abs(rl-cur)>1: continue
        if seq[i]==side: follow+=1
        else: brk+=1
    return side if follow>=brk else _opp(side)

def _multi_window_prediction(seq):
    if not seq: return 'TÀI'
    vote=0.0
    for w,weight in ((8,1.0),(16,1.15),(32,1.3),(64,1.1)):
        q=seq[-w:]
        if not q: continue
        p=q.count('TÀI')/len(q)
        if p>=.68: vote-=weight
        elif p>=.54: vote+=weight
        elif p<=.32: vote+=weight
        elif p<=.46: vote-=weight
    if abs(vote)<.1: return _markov_prediction(seq,2)
    return 'TÀI' if vote>0 else 'XỈU'


def _decayed_transition_prediction(seq,order=2,decay=.955):
    if not seq:return 'TÀI'
    order=max(1,min(4,int(order)))
    if len(seq)<=order+3:return _markov_prediction(seq,order)
    ctx=tuple(seq[-order:]);t=x=.7
    for i in range(order,len(seq)):
        if tuple(seq[i-order:i])!=ctx:continue
        age=(len(seq)-1)-i;w=decay**age
        if seq[i]=='TÀI':t+=w
        else:x+=w
    return 'TÀI' if t>=x else 'XỈU'

def _motif_weighted_prediction(seq):
    if not seq:return 'TÀI'
    vt=vx=0.0
    for L in (2,3,4,5,6):
        if len(seq)<=L+1:continue
        motif=tuple(seq[-L:])
        for i in range(L,len(seq)):
            if tuple(seq[i-L:i])!=motif:continue
            age=(len(seq)-1)-i;w=(1.0+.20*L)*(.965**age)
            if seq[i]=='TÀI':vt+=w
            else:vx+=w
    if vt+vx<1.2:return _markov_prediction(seq,2)
    return 'TÀI' if vt>=vx else 'XỈU'

def _regime_adaptive_prediction(seq):
    if not seq:return 'TÀI'
    q=seq[-24:];last=q[-1];run=_run_len(q)
    flips=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])/max(1,len(q)-1);p=q.count('TÀI')/len(q)
    if flips>=.70:return _opp(last)
    if run>=3:return _run_hazard_prediction(seq)
    if p>=.70:return 'XỈU'
    if p<=.30:return 'TÀI'
    if p>=.57:return 'TÀI'
    if p<=.43:return 'XỈU'
    return _decayed_transition_prediction(seq,2)


def _flip_state_markov_prediction(seq):
    if not seq:return 'TÀI'
    if len(seq)<8:return _markov_prediction(seq,1)
    state='F' if seq[-1]!=seq[-2] else 'R'
    tai=xiu=1.2
    for i in range(2,len(seq)):
        st='F' if seq[i-1]!=seq[i-2] else 'R'
        if st!=state:continue
        w=.965**((len(seq)-1)-i)
        if seq[i]=='TÀI':tai+=w
        else:xiu+=w
    return 'TÀI' if tai>=xiu else 'XỈU'

def _run_length_markov_prediction(seq):
    if not seq:return 'TÀI'
    if len(seq)<10:return _run_hazard_prediction(seq)
    side=seq[-1];target=min(_run_len(seq),6)
    follow=brk=1.25
    for i in range(2,len(seq)):
        prev=seq[i-1]
        if prev!=side:continue
        rl=1;j=i-2
        while j>=0 and seq[j]==prev and rl<6:
            rl+=1;j-=1
        if abs(rl-target)>1:continue
        w=.97**((len(seq)-1)-i)
        if seq[i]==side:follow+=w
        else:brk+=w
    return side if follow>=brk else _opp(side)

def _periodic_match_prediction(seq):
    if not seq:return 'TÀI'
    n=len(seq)
    if n<18:return _markov_prediction(seq,2)
    best_lag=None;best=-1.0
    q=seq[-min(70,n):]
    for lag in range(2,min(13,len(q)//3+1)):
        hit=tot=0
        for i in range(lag,len(q)):
            tot+=1
            if q[i]==q[i-lag]:hit+=1
        if tot<10:continue
        rate=(hit+2.5)/(tot+5)
        score=rate-(.008 if lag<=3 else 0)
        if score>best:best=score;best_lag=lag
    return seq[-best_lag] if best_lag else _markov_prediction(seq,2)

def _dual_horizon_prediction(seq):
    if not seq:return 'TÀI'
    q8=seq[-8:];q28=seq[-28:]
    p8=q8.count('TÀI')/max(1,len(q8));p28=q28.count('TÀI')/max(1,len(q28))
    short=p8-.5;long=p28-.5
    if short*long>0 and abs(short)>=.08:return 'TÀI' if short>0 else 'XỈU'
    if abs(short-long)>=.28:return 'XỈU' if short>0 else 'TÀI'
    return _decayed_transition_prediction(seq,2)


def _analog_knn_prediction(seq):
    if not seq:return 'TÀI'
    n=len(seq)
    if n<24:return _markov_prediction(seq,2)
    vt=vx=0.0
    for L in (4,5,6,7,8):
        if n<=L+2:continue
        cur=seq[-L:]
        for i in range(L,n-1):
            past=seq[i-L:i]
            sim=sum(1 for a,b in zip(cur,past) if a==b)/L
            if sim<.75:continue
            age=(n-1)-i
            w=(sim**3)*(0.97**age)*(1+.06*L)
            nxt=seq[i]
            if nxt=='TÀI':vt+=w
            else:vx+=w
    if vt+vx<1.0:return _suffix_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'

def _run_survival_prediction(seq):
    if not seq:return 'TÀI'
    side=seq[-1];cur=min(_run_len(seq),8)
    runs=[];rside=seq[0];length=1
    for x in seq[1:]:
        if x==rside:length+=1
        else:
            runs.append((rside,length));rside=x;length=1
    same=[l for sd,l in runs if sd==side and l>=cur]
    if len(same)<3:return _run_length_markov_prediction(seq)
    survive=sum(1 for l in same if l>=cur+1)
    p=(survive+2)/(len(same)+4)
    return side if p>=.52 else _opp(side)

def _context_entropy_prediction(seq):
    if not seq:return 'TÀI'
    best=None
    for order in (1,2,3,4):
        if len(seq)<order+12:continue
        ctx=tuple(seq[-order:]);t=x=1.5;sample=0
        for i in range(order,len(seq)):
            if tuple(seq[i-order:i])!=ctx:continue
            w=.97**((len(seq)-1)-i)
            if seq[i]=='TÀI':t+=w
            else:x+=w
            sample+=1
        if sample<3:continue
        p=t/(t+x)
        ent=0.0
        for z in (p,1-p):
            if z>0:ent-=z*math.log2(z)
        strength=abs(p-.5)*(1-ent*.35)*min(1,sample/10)
        cand=(strength,'TÀI' if p>=.5 else 'XỈU')
        if best is None or cand[0]>best[0]:best=cand
    return best[1] if best else _decayed_transition_prediction(seq,2)

def _bayes_context_prediction(seq):
    if not seq:return 'TÀI'
    vt=vx=0.0
    for order,ow in ((1,.7),(2,1.0),(3,1.25),(4,1.45),(5,1.6)):
        if len(seq)<order+6:continue
        ctx=tuple(seq[-order:]);t=x=2.0;sample=0
        for i in range(order,len(seq)):
            if tuple(seq[i-order:i])!=ctx:continue
            age=(len(seq)-1)-i;w=.972**age
            if seq[i]=='TÀI':t+=w
            else:x+=w
            sample+=1
        if sample<2:continue
        p=t/(t+x);strength=abs(p-.5)*min(1.0,sample/10)*ow
        if p>=.5:vt+=strength
        else:vx+=strength
    if vt+vx<.04:return _decayed_transition_prediction(seq,2)
    return 'TÀI' if vt>=vx else 'XỈU'

def _horizon_consensus_prediction(seq):
    if not seq:return 'TÀI'
    vote=0.0
    for w,wt in ((6,1.35),(10,1.25),(20,1.10),(40,.90),(80,.70)):
        q=seq[-w:]
        if len(q)<4:continue
        p=q.count('TÀI')/len(q);edge=p-.5
        if abs(edge)<.04:continue
        vote+=wt*edge*2
    if abs(vote)<.12:return _markov_prediction(seq,2)
    return 'TÀI' if vote>0 else 'XỈU'

def _regime_switch_prediction(seq):
    if not seq:return 'TÀI'
    q=seq[-32:];last=q[-1];run=_run_len(q)
    flips=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])/max(1,len(q)-1)
    p=q.count('TÀI')/len(q)
    if flips>=.68:return _opp(last)
    if flips<=.32 and run>=2:return _run_survival_prediction(seq)
    if abs(p-.5)>=.18:return _multi_window_prediction(seq)
    a=_bayes_context_prediction(seq);b=_decayed_transition_prediction(seq,3);c=_motif_weighted_prediction(seq)
    return a if a==b or a==c else b if b==c else _markov_prediction(seq,2)

def _lag_ensemble_prediction(seq):
    if not seq:return 'TÀI'
    n=len(seq)
    if n<24:return _periodic_match_prediction(seq)
    vt=vx=0.0
    for lag in range(2,min(16,n//3+1)):
        hit=tot=0
        start=max(lag,n-90)
        for i in range(start,n):
            tot+=1;hit+=1 if seq[i]==seq[i-lag] else 0
        if tot<10:continue
        rate=(hit+3)/(tot+6);edge=rate-.5
        if abs(edge)<.035:continue
        pred=seq[-lag] if edge>0 else _opp(seq[-lag])
        w=abs(edge)*(1.0/(1+.035*lag))
        if pred=='TÀI':vt+=w
        else:vx+=w
    if vt+vx<.035:return _periodic_match_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'



def _vom_context6_prediction(seq):
    if not seq:return 'TÀI'
    vt=vx=0.0;n=len(seq)
    for order,ow in ((2,.8),(3,1.0),(4,1.18),(5,1.35),(6,1.50),(7,1.60),(8,1.68)):
        if n<order+12:continue
        ctx=tuple(seq[-order:]);t=x=1.8;support=0.0
        for i in range(order,n):
            if tuple(seq[i-order:i])!=ctx:continue
            age=(n-1)-i;w=.5**(age/260.0)
            support+=w
            if seq[i]=='TÀI':t+=w
            else:x+=w
        if support<2.0:continue
        p=t/(t+x);edge=(p-.5)*2.0
        weight=ow*min(1.0,support/18.0)*min(1.0,abs(edge)*3.0+.15)
        if p>=.5:vt+=weight*max(.05,abs(edge))
        else:vx+=weight*max(.05,abs(edge))
    if vt+vx<.05:return _bayes_context_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'

def _long_memory_bayes_prediction(seq):
    if not seq:return 'TÀI'
    q=seq[-min(len(seq),MAX_HISTORY):];n=len(q);vt=vx=0.0
    for order,ow in ((1,.55),(2,.75),(3,1.0),(4,1.2),(5,1.35),(6,1.48)):
        if n<order+20:continue
        ctx=tuple(q[-order:]);t=x=3.0;support=0.0
        for i in range(order,n):
            if tuple(q[i-order:i])!=ctx:continue
            age=(n-1)-i;w=.5**(age/520.0)
            support+=w
            if q[i]=='TÀI':t+=w
            else:x+=w
        if support<3:continue
        p=t/(t+x);edge=(p-.5)*2
        rel=(1-math.exp(-support/25.0))*ow
        if p>=.5:vt+=rel*abs(edge)
        else:vx+=rel*abs(edge)
    if vt+vx<.035:return _decayed_transition_prediction(seq,3)
    return 'TÀI' if vt>=vx else 'XỈU'

def _run_profile_long_prediction(seq):
    if not seq:return 'TÀI'
    q=seq[-min(len(seq),MAX_HISTORY):];side=q[-1];cur=min(_run_len(q),12)
    same=brk=1.5;support=0
    for i in range(2,len(q)):
        prev=q[i-1];rl=1;j=i-2
        while j>=0 and q[j]==prev and rl<12:
            rl+=1;j-=1
        if prev!=side or abs(rl-cur)>1:continue
        age=(len(q)-1)-i;w=.5**(age/650.0);support+=1
        if q[i]==prev:same+=w
        else:brk+=w
    if support<6:return _run_survival_prediction(seq)
    return side if same>=brk else _opp(side)

def _multiscale_transition_prediction(seq):
    if not seq:return 'TÀI'
    votes=[]
    for w,wt in ((32,1.40),(64,1.30),(128,1.18),(256,1.04),(512,.90),(1000,.78),(2000,.66),(5000,.52),(10000,.40)):
        q=seq[-w:]
        if len(q)<min(24,w):continue
        a=_decayed_transition_prediction(q,2,.972 if w<=128 else .988)
        b=_markov_prediction(q,3)
        pred=a if a==b else _bayes_context_prediction(q)
        votes.append((pred,wt))
    if not votes:return _markov_prediction(seq,2)
    vt=sum(w for p,w in votes if p=='TÀI');vx=sum(w for p,w in votes if p=='XỈU')
    return 'TÀI' if vt>=vx else 'XỈU'


# -----------------------------------------------------------------------------
# V48 SUPER ENSEMBLE / ADAPTIVE CALIBRATION
# These remain causal: every strategy only sees rounds that existed before the
# target round during walk-forward validation.
def _regime_label(seq):
    if not seq:return 'EMPTY'
    q=list(seq[-48:])
    if len(q)<8:return 'SHORT'
    flips=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])/max(1,len(q)-1)
    p=q.count('TÀI')/len(q);run=_run_len(q)
    if run>=4:return 'RUN'
    if flips>=.68:return 'ALT'
    if flips<=.30:return 'STICKY'
    if p>=.66:return 'BIAS_T'
    if p<=.34:return 'BIAS_X'
    if flips>=.56:return 'VOLATILE'
    return 'BALANCED'


def _majority3(a,b,c):
    if a==b or a==c:return a
    if b==c:return b
    return b


def _changepoint_adaptive_prediction(seq):
    if not seq:return 'TÀI'
    if len(seq)<36:return _regime_switch_prediction(seq)
    r=seq[-16:]; old=seq[-48:-16] if len(seq)>=48 else seq[:-16]
    pr=r.count('TÀI')/len(r);po=old.count('TÀI')/max(1,len(old))
    fr=sum(1 for i in range(1,len(r)) if r[i]!=r[i-1])/max(1,len(r)-1)
    fo=sum(1 for i in range(1,len(old)) if old[i]!=old[i-1])/max(1,len(old)-1)
    shift=abs(pr-po)+.65*abs(fr-fo)
    if shift>=.28:
        q=seq[-48:]
        return _majority3(_decayed_transition_prediction(q,2,.94),
                          _bayes_context_prediction(q),
                          _regime_switch_prediction(q))
    return _majority3(_long_memory_bayes_prediction(seq),
                      _multiscale_transition_prediction(seq),
                      _bayes_context_prediction(seq))


def _wilson_lower(wins,total,z=1.2816):
    if total<=0:return .0
    p=wins/total;z2=z*z
    den=1+z2/total
    centre=p+z2/(2*total)
    spread=z*math.sqrt(max(0.0,p*(1-p)/total+z2/(4*total*total)))
    return (centre-spread)/den


def _wilson_context_prediction(seq):
    if not seq:return 'TÀI'
    n=len(seq);best=None
    for order in range(2,10):
        if n<order+12:continue
        ctx=tuple(seq[-order:]);t=x=0
        for i in range(order,n):
            if tuple(seq[i-order:i])!=ctx:continue
            if seq[i]=='TÀI':t+=1
            else:x+=1
        total=t+x
        if total<4:continue
        maj=max(t,x);side='TÀI' if t>=x else 'XỈU'
        lower=_wilson_lower(maj,total)
        edge=(maj/total)-.5
        score=(lower-.5)*min(1.0,total/28.0)*(1+.06*order)+edge*.12
        cand=(score,total,order,side)
        if best is None or cand[:3]>best[:3]:best=cand
    if not best or best[0]<=.002:return _bayes_context_prediction(seq)
    return best[3]


def _knn_recency_prediction(seq):
    if not seq:return 'TÀI'
    n=len(seq)
    if n<32:return _analog_knn_prediction(seq)
    candidates=[]
    for L in (5,6,8,10,12):
        if n<=L+3:continue
        cur=seq[-L:]
        for i in range(L,n-1):
            past=seq[i-L:i]
            sim=sum(1 for a,b in zip(cur,past) if a==b)/L
            if sim<.70:continue
            age=(n-1)-i
            w=(sim**4)*(0.5**(age/360.0))*(1+.035*L)
            candidates.append((w,seq[i]))
    if not candidates:return _analog_knn_prediction(seq)
    candidates.sort(reverse=True,key=lambda z:z[0]);candidates=candidates[:48]
    vt=sum(w for w,p in candidates if p=='TÀI');vx=sum(w for w,p in candidates if p=='XỈU')
    if vt+vx<.9:return _analog_knn_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'


def _run_matrix_prediction(seq):
    if not seq:return 'TÀI'
    side=seq[-1];cur=min(_run_len(seq),6);same=brk=2.0;support=0.0
    n=len(seq)
    for i in range(2,n):
        prev=seq[i-1];rl=1;j=i-2
        while j>=0 and seq[j]==prev and rl<6:
            rl+=1;j-=1
        if prev!=side or min(rl,6)!=cur:continue
        age=(n-1)-i;w=.5**(age/420.0);support+=w
        if seq[i]==prev:same+=w
        else:brk+=w
    if support<3.0:return _run_profile_long_prediction(seq)
    return side if same>=brk else _opp(side)


def _structure_score(seq):
    """Estimate persistent non-IID structure; used only to calibrate confidence.
    A low score does not change the side prediction, it only prevents a random
    hot streak among many strategies from being displayed as a strong signal.
    """
    q=list(seq[-min(len(seq),800):])
    n=len(q)
    if n<80:return .22
    p=q.count('TÀI')/n
    bias=abs(p-.5)*2
    # First-order transition dependence.
    tt=tx=xt=xx=1.0
    for a,b in zip(q[:-1],q[1:]):
        if a=='TÀI' and b=='TÀI':tt+=1
        elif a=='TÀI':tx+=1
        elif b=='TÀI':xt+=1
        else:xx+=1
    pt_t=tt/(tt+tx);pt_x=xt/(xt+xx)
    trans=abs(pt_t-pt_x)
    # Maximum lag correlation, shrunk for multiple lags.
    vals=[1 if x=='TÀI' else -1 for x in q]
    lag_best=0.0
    for lag in range(1,13):
        a=vals[lag:];b=vals[:-lag]
        if len(a)<50:continue
        ma=sum(a)/len(a);mb=sum(b)/len(b)
        va=sum((x-ma)**2 for x in a);vb=sum((x-mb)**2 for x in b)
        if va<=0 or vb<=0:continue
        corr=abs(sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(va*vb))
        lag_best=max(lag_best,corr)
    lag_signal=max(0.0,lag_best-0.055)*2.6
    # Order-2 conditional concentration. The 0.08 dead-zone absorbs normal sampling noise.
    ctx_edges=[]
    for ctx in (('TÀI','TÀI'),('TÀI','XỈU'),('XỈU','TÀI'),('XỈU','XỈU')):
        t=x=2.0;support=0
        for i in range(2,n):
            if tuple(q[i-2:i])!=ctx:continue
            support+=1
            if q[i]=='TÀI':t+=1
            else:x+=1
        if support>=20:ctx_edges.append(abs(t/(t+x)-.5)*2)
    ctx=max(ctx_edges,default=0.0)
    ctx_signal=max(0.0,ctx-.08)*1.45
    # Stability: true structure should appear in both halves, not only one hot patch.
    def half_features(h):
        if len(h)<40:return (0.0,0.0)
        ph=h.count('TÀI')/len(h);fl=sum(1 for i in range(1,len(h)) if h[i]!=h[i-1])/max(1,len(h)-1)
        return abs(ph-.5)*2,abs(fl-.5)*2
    h1=half_features(q[:n//2]);h2=half_features(q[n//2:])
    stable=1-min(1.0,abs(h1[0]-h2[0])+abs(h1[1]-h2[1]))
    raw=.24*bias+.28*trans+.25*lag_signal+.23*ctx_signal
    return _clamp(raw*(.72+.28*stable),0,1)


def _entropy_gate_prediction(seq):
    if not seq:return 'TÀI'
    q32=seq[-32:];q128=seq[-128:]
    h32=_entropy(q32);h128=_entropy(q128)
    flips=sum(1 for i in range(1,len(q32)) if q32[i]!=q32[i-1])/max(1,len(q32)-1)
    p32=q32.count('TÀI')/max(1,len(q32));p128=q128.count('TÀI')/max(1,len(q128))
    if h32<.78 and _run_len(q32)>=3:return _run_matrix_prediction(seq)
    if flips>=.70:return _opp(q32[-1])
    if abs(p32-.5)>=.16 and abs(p128-.5)>=.08 and (p32-.5)*(p128-.5)>0:
        return 'TÀI' if p32>.5 else 'XỈU'
    if h32-h128>=.10:return _changepoint_adaptive_prediction(seq)
    return _wilson_context_prediction(seq)


def _cross_horizon_bayes_prediction(seq):
    if not seq:return 'TÀI'
    vt=vx=0.0
    for w,wt in ((24,1.35),(48,1.22),(96,1.08),(192,.92),(384,.78),(768,.64),(1500,.50),(3000,.38)):
        q=seq[-w:]
        if len(q)<min(20,w):continue
        a=_bayes_context_prediction(q);b=_decayed_transition_prediction(q,3,.965 if w<=96 else .986)
        pred=a if a==b else _wilson_context_prediction(q)
        # stable horizons receive more influence; heavily imbalanced horizons are shrunk
        p=q.count('TÀI')/len(q);stability=1-min(.35,abs(p-.5)*.7)
        ww=wt*stability
        if pred=='TÀI':vt+=ww
        else:vx+=ww
    if vt+vx<.5:return _bayes_context_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'

# -----------------------------------------------------------------------------
# V41 REFERENCE PATTERN PRIOR
# Derived from thuattoan8.txt: 5022 observed (context -> next side) rows.
# We aggregate counts instead of treating duplicate/conflicting rows as fixed truth.
# Each tuple is (next_TAI_count, next_XIU_count), for suffix contexts length 2..8.
# This prior is always walk-forward validated on each live board before it can lead.
REFERENCE_PATTERN_COUNTS = {'TT': (678, 606),
 'TX': (597, 642),
 'XT': (610, 628),
 'XX': (637, 613),
 'TTT': (348, 328),
 'TTX': (282, 324),
 'TXT': (298, 299),
 'TXX': (322, 316),
 'XTT': (330, 277),
 'XTX': (313, 316),
 'XXT': (309, 328),
 'XXX': (313, 297),
 'TTTT': (179, 169),
 'TTTX': (154, 174),
 'TTXT': (145, 137),
 'TTXX': (170, 153),
 'TXTT': (165, 132),
 'TXTX': (156, 144),
 'TXXT': (149, 173),
 'TXXX': (156, 157),
 'XTTT': (169, 159),
 'XTTX': (127, 150),
 'XTXT': (151, 162),
 'XTXX': (151, 162),
 'XXTT': (163, 145),
 'XXTX': (157, 171),
 'XXXT': (159, 154),
 'XXXX': (157, 140),
 'TTTTT': (91, 88),
 'TTTTX': (78, 91),
 'TTTXT': (79, 75),
 'TTTXX': (86, 87),
 'TTXTT': (88, 56),
 'TTXTX': (70, 68),
 'TTXXT': (71, 99),
 'TTXXX': (67, 83),
 'TXTTT': (82, 81),
 'TXTTX': (60, 72),
 'TXTXT': (79, 77),
 'TXTXX': (66, 78),
 'TXXTT': (92, 57),
 'TXXTX': (84, 89),
 'TXXXT': (84, 72),
 'TXXXX': (78, 79),
 'XTTTT': (88, 81),
 'XTTTX': (76, 83),
 'XTTXT': (66, 61),
 'XTTXX': (84, 66),
 'XTXTT': (76, 75),
 'XTXTX': (86, 76),
 'XTXXT': (77, 74),
 'XTXXX': (88, 74),
 'XXTTT': (87, 76),
 'XXTTX': (67, 78),
 'XXTXT': (72, 85),
 'XXTXX': (84, 84),
 'XXXTT': (70, 88),
 'XXXTX': (72, 82),
 'XXXXT': (75, 82),
 'XXXXX': (79, 61),
 'TTTTTT': (46, 45),
 'TTTTTX': (41, 47),
 'TTTTXT': (36, 42),
 'TTTTXX': (39, 52),
 'TTTXTT': (42, 36),
 'TTTXTX': (38, 37),
 'TTTXXT': (37, 49),
 'TTTXXX': (42, 44),
 'TTXTTT': (41, 45),
 'TTXTTX': (27, 29),
 'TTXTXT': (36, 34),
 'TTXTXX': (32, 36),
 'TTXXTT': (48, 23),
 'TTXXTX': (51, 48),
 'TTXXXT': (35, 32),
 'TTXXXX': (36, 47),
 'TXTTTT': (43, 39),
 'TXTTTX': (36, 45),
 'TXTTXT': (30, 30),
 'TXTTXX': (39, 33),
 'TXTXTT': (33, 46),
 'TXTXTX': (36, 41),
 'TXTXXT': (32, 34),
 'TXTXXX': (38, 40),
 'TXXTTT': (51, 41),
 'TXXTTX': (22, 35),
 'TXXTXT': (38, 46),
 'TXXTXX': (37, 50),
 'TXXXTT': (36, 47),
 'TXXXTX': (35, 37),
 'TXXXXT': (34, 44),
 'TXXXXX': (45, 34),
 'XTTTTT': (45, 43),
 'XTTTTX': (37, 44),
 'XTTTXT': (43, 33),
 'XTTTXX': (47, 35),
 'XTTXTT': (46, 20),
 'XTTXTX': (32, 30),
 'XTTXXT': (34, 50),
 'XTTXXX': (25, 39),
 'XTXTTT': (41, 35),
 'XTXTTX': (32, 43),
 'XTXTXT': (43, 43),
 'XTXTXX': (34, 42),
 'XTXXTT': (43, 34),
 'XTXXTX': (33, 41),
 'XTXXXT': (48, 40),
 'XTXXXX': (42, 32),
 'XXTTTT': (45, 42),
 'XXTTTX': (40, 36),
 'XXTTXT': (36, 31),
 'XXTTXX': (45, 33),
 'XXTXTT': (43, 29),
 'XXTXTX': (50, 35),
 'XXTXXT': (44, 40),
 'XXTXXX': (50, 34),
 'XXXTTT': (35, 35),
 'XXXTTX': (45, 43),
 'XXXTXT': (34, 38),
 'XXXTXX': (47, 34),
 'XXXXTT': (34, 41),
 'XXXXTX': (37, 45),
 'XXXXXT': (41, 38),
 'XXXXXX': (34, 27),
 'TTTTTTT': (26, 20),
 'TTTTTTX': (21, 24),
 'TTTTTXT': (20, 21),
 'TTTTTXX': (20, 27),
 'TTTTXTT': (16, 20),
 'TTTTXTX': (23, 19),
 'TTTTXXT': (19, 20),
 'TTTTXXX': (24, 28),
 'TTTXTTT': (18, 23),
 'TTTXTTX': (19, 17),
 'TTTXTXT': (23, 15),
 'TTTXTXX': (16, 21),
 'TTTXXTT': (23, 14),
 'TTTXXTX': (24, 25),
 'TTTXXXT': (23, 19),
 'TTTXXXX': (19, 25),
 'TTXTTTT': (20, 21),
 'TTXTTTX': (24, 21),
 'TTXTTXT': (12, 15),
 'TTXTTXX': (15, 14),
 'TTXTXTT': (12, 24),
 'TTXTXTX': (16, 18),
 'TTXTXXT': (16, 16),
 'TTXTXXX': (11, 25),
 'TTXXTTT': (28, 20),
 'TTXXTTX': (12, 11),
 'TTXXTXT': (23, 28),
 'TTXXTXX': (19, 28),
 'TTXXXTT': (12, 22),
 'TTXXXTX': (16, 16),
 'TTXXXXT': (15, 21),
 'TTXXXXX': (26, 21),
 'TXTTTTT': (22, 21),
 'TXTTTTX': (19, 20),
 'TXTTTXT': (22, 14),
 'TXTTTXX': (24, 21),
 'TXTTXTT': (23, 7),
 'TXTTXTX': (18, 12),
 'TXTTXXT': (16, 23),
 'TXTTXXX': (13, 19),
 'TXTXTTT': (17, 16),
 'TXTXTTX': (24, 22),
 'TXTXTXT': (18, 18),
 'TXTXTXX': (18, 23),
 'TXTXXTT': (16, 16),
 'TXTXXTX': (15, 19),
 'TXTXXXT': (22, 16),
 'TXTXXXX': (25, 15),
 'TXXTTTT': (23, 28),
 'TXXTTTX': (22, 19),
 'TXXTTXT': (10, 12),
 'TXXTTXX': (21, 14),
 'TXXTXTT': (21, 17),
 'TXXTXTX': (26, 20),
 'TXXTXXT': (18, 19),
 'TXXTXXX': (29, 21),
 'TXXXTTT': (17, 19),
 'TXXXTTX': (24, 23),
 'TXXXTXT': (13, 22),
 'TXXXTXX': (19, 17),
 'TXXXXTT': (16, 18),
 'TXXXXTX': (21, 23),
 'TXXXXXT': (19, 26),
 'TXXXXXX': (18, 16),
 'XTTTTTT': (20, 25),
 'XTTTTTX': (20, 23),
 'XTTTTXT': (16, 21),
 'XTTTTXX': (19, 25),
 'XTTTXTT': (26, 16),
 'XTTTXTX': (15, 18),
 'XTTTXXT': (18, 29),
 'XTTTXXX': (18, 16),
 'XTTXTTT': (23, 22),
 'XTTXTTX': (8, 12),
 'XTTXTXT': (13, 19),
 'XTTXTXX': (16, 14),
 'XTTXXTT': (25, 9),
 'XTTXXTX': (27, 23),
 'XTTXXXT': (12, 13),
 'XTTXXXX': (17, 22),
 'XTXTTTT': (23, 18),
 'XTXTTTX': (12, 23),
 'XTXTTXT': (17, 15),
 'XTXTTXX': (24, 19),
 'XTXTXTT': (21, 22),
 'XTXTXTX': (20, 23),
 'XTXTXXT': (16, 18),
 'XTXTXXX': (27, 15),
 'XTXXTTT': (23, 20),
 'XTXXTTX': (10, 24),
 'XTXXTXT': (15, 18),
 'XTXXTXX': (18, 22),
 'XTXXXTT': (23, 25),
 'XTXXXTX': (19, 21),
 'XTXXXXT': (19, 23),
 'XTXXXXX': (19, 13),
 'XXTTTTT': (23, 22),
 'XXTTTTX': (18, 24),
 'XXTTTXT': (21, 19),
 'XXTTTXX': (22, 13),
 'XXTTXTT': (23, 13),
 'XXTTXTX': (14, 18),
 'XXTTXXT': (18, 27),
 'XXTTXXX': (12, 20),
 'XXTXTTT': (24, 19),
 'XXTXTTX': (8, 21),
 'XXTXTXT': (25, 25),
 'XXTXTXX': (16, 19),
 'XXTXXTT': (26, 18),
 'XXTXXTX': (18, 22),
 'XXTXXXT': (26, 24),
 'XXTXXXX': (17, 17),
 'XXXTTTT': (22, 13),
 'XXXTTTX': (18, 17),
 'XXXTTXT': (26, 19),
 'XXXTTXX': (24, 19),
 'XXXTXTT': (22, 12),
 'XXXTXTX': (24, 14),
 'XXXTXXT': (26, 21),
 'XXXTXXX': (21, 13),
 'XXXXTTT': (18, 16),
 'XXXXTTX': (21, 20),
 'XXXXTXT': (21, 16),
 'XXXXTXX': (28, 17),
 'XXXXXTT': (18, 23),
 'XXXXXTX': (16, 22),
 'XXXXXXT': (22, 12),
 'XXXXXXX': (16, 11),
 'TTTTTTTT': (14, 12),
 'TTTTTTTX': (8, 12),
 'TTTTTTXT': (10, 11),
 'TTTTTTXX': (10, 14),
 'TTTTTXTT': (8, 12),
 'TTTTTXTX': (11, 10),
 'TTTTTXXT': (11, 9),
 'TTTTTXXX': (12, 15),
 'TTTTXTTT': (8, 7),
 'TTTTXTTX': (9, 11),
 'TTTTXTXT': (15, 8),
 'TTTTXTXX': (12, 7),
 'TTTTXXTT': (13, 6),
 'TTTTXXTX': (8, 12),
 'TTTTXXXT': (11, 13),
 'TTTTXXXX': (13, 15),
 'TTTXTTTT': (8, 10),
 'TTTXTTTX': (11, 12),
 'TTTXTTXT': (9, 10),
 'TTTXTTXX': (8, 9),
 'TTTXTXTT': (6, 17),
 'TTTXTXTX': (8, 7),
 'TTTXTXXT': (7, 9),
 'TTTXTXXX': (7, 14),
 'TTTXXTTT': (14, 9),
 'TTTXXTTX': (7, 7),
 'TTTXXTXT': (15, 9),
 'TTTXXTXX': (10, 14),
 'TTTXXXTT': (10, 12),
 'TTTXXXTX': (7, 12),
 'TTTXXXXT': (10, 9),
 'TTTXXXXX': (12, 13),
 'TTXTTTTT': (9, 11),
 'TTXTTTTX': (10, 11),
 'TTXTTTXT': (17, 7),
 'TTXTTTXX': (11, 10),
 'TTXTTXTT': (8, 4),
 'TTXTTXTX': (10, 5),
 'TTXTTXXT': (7, 8),
 'TTXTTXXX': (8, 6),
 'TTXTXTTT': (6, 6),
 'TTXTXTTX': (13, 11),
 'TTXTXTXT': (7, 9),
 'TTXTXTXX': (8, 10),
 'TTXTXXTT': (8, 8),
 'TTXTXXTX': (9, 7),
 'TTXTXXXT': (5, 6),
 'TTXTXXXX': (17, 8),
 'TTXXTTTT': (14, 14),
 'TTXXTTTX': (8, 12),
 'TTXXTTXT': (3, 9),
 'TTXXTTXX': (8, 3),
 'TTXXTXTT': (14, 9),
 'TTXXTXTX': (15, 13),
 'TTXXTXXT': (7, 12),
 'TTXXTXXX': (17, 11),
 'TTXXXTTT': (4, 8),
 'TTXXXTTX': (13, 9),
 'TTXXXTXT': (8, 8),
 'TTXXXTXX': (7, 8),
 'TTXXXXTT': (5, 10),
 'TTXXXXTX': (9, 12),
 'TTXXXXXT': (12, 14),
 'TTXXXXXX': (11, 10),
 'TXTTTTTT': (9, 13),
 'TXTTTTTX': (9, 12),
 'TXTTTTXT': (6, 13),
 'TXTTTTXX': (8, 12),
 'TXTTTXTT': (11, 10),
 'TXTTTXTX': (6, 8),
 'TXTTTXXT': (10, 14),
 'TXTTTXXX': (8, 12),
 'TXTTXTTT': (12, 10),
 'TXTTXTTX': (3, 4),
 'TXTTXTXT': (7, 11),
 'TXTTXTXX': (6, 6),
 'TXTTXXTT': (12, 4),
 'TXTTXXTX': (13, 10),
 'TXTTXXXT': (5, 8),
 'TXTTXXXX': (10, 9),
 'TXTXTTTT': (9, 8),
 'TXTXTTTX': (8, 8),
 'TXTXTTXT': (12, 12),
 'TXTXTTXX': (12, 10),
 'TXTXTXTT': (7, 11),
 'TXTXTXTX': (8, 10),
 'TXTXTXXT': (4, 14),
 'TXTXTXXX': (13, 10),
 'TXTXXTTT': (7, 9),
 'TXTXXTTX': (5, 11),
 'TXTXXTXT': (8, 7),
 'TXTXXTXX': (10, 9),
 'TXTXXXTT': (8, 14),
 'TXTXXXTX': (11, 5),
 'TXTXXXXT': (12, 13),
 'TXTXXXXX': (7, 8),
 'TXXTTTTT': (12, 11),
 'TXXTTTTX': (12, 16),
 'TXXTTTXT': (11, 11),
 'TXXTTTXX': (12, 7),
 'TXXTTXTT': (4, 6),
 'TXXTTXTX': (8, 5),
 'TXXTTXXT': (7, 14),
 'TXXTTXXX': (6, 8),
 'TXXTXTTT': (16, 5),
 'TXXTXTTX': (6, 11),
 'TXXTXTXT': (11, 15),
 'TXXTXTXX': (10, 10),
 'TXXTXXTT': (12, 6),
 'TXXTXXTX': (9, 10),
 'TXXTXXXT': (13, 16),
 'TXXTXXXX': (9, 12),
 'TXXXTTTT': (9, 8),
 'TXXXTTTX': (8, 11),
 'TXXXTTXT': (15, 9),
 'TXXXTTXX': (14, 9),
 'TXXXTXTT': (6, 7),
 'TXXXTXTX': (12, 10),
 'TXXXTXXT': (8, 11),
 'TXXXTXXX': (10, 7),
 'TXXXXTTT': (8, 8),
 'TXXXXTTX': (8, 10),
 'TXXXXTXT': (13, 8),
 'TXXXXTXX': (18, 5),
 'TXXXXXTT': (8, 11),
 'TXXXXXTX': (10, 16),
 'TXXXXXXT': (14, 4),
 'TXXXXXXX': (12, 4),
 'XTTTTTTT': (12, 8),
 'XTTTTTTX': (13, 12),
 'XTTTTTXT': (10, 10),
 'XTTTTTXX': (10, 13),
 'XTTTTXTT': (8, 8),
 'XTTTTXTX': (12, 9),
 'XTTTTXXT': (8, 11),
 'XTTTTXXX': (12, 13),
 'XTTTXTTT': (10, 16),
 'XTTTXTTX': (10, 6),
 'XTTTXTXT': (8, 7),
 'XTTTXTXX': (4, 14),
 'XTTTXXTT': (10, 8),
 'XTTTXXTX': (16, 13),
 'XTTTXXXT': (12, 6),
 'XTTTXXXX': (6, 10),
 'XTTXTTTT': (12, 11),
 'XTTXTTTX': (13, 9),
 'XTTXTTXT': (3, 5),
 'XTTXTTXX': (7, 5),
 'XTTXTXTT': (6, 7),
 'XTTXTXTX': (8, 11),
 'XTTXTXXT': (9, 7),
 'XTTXTXXX': (4, 10),
 'XTTXXTTT': (14, 11),
 'XTTXXTTX': (5, 4),
 'XTTXXTXT': (8, 19),
 'XTTXXTXX': (9, 14),
 'XTTXXXTT': (2, 10),
 'XTTXXXTX': (9, 4),
 'XTTXXXXT': (5, 12),
 'XTTXXXXX': (14, 8),
 'XTXTTTTT': (13, 10),
 'XTXTTTTX': (9, 9),
 'XTXTTTXT': (5, 7),
 'XTXTTTXX': (13, 10),
 'XTXTTXTT': (14, 3),
 'XTXTTXTX': (8, 7),
 'XTXTTXXT': (9, 15),
 'XTXTTXXX': (5, 13),
 'XTXTXTTT': (11, 10),
 'XTXTXTTX': (11, 11),
 'XTXTXTXT': (11, 9),
 'XTXTXTXX': (10, 13),
 'XTXTXXTT': (8, 8),
 'XTXTXXTX': (6, 12),
 'XTXTXXXT': (17, 10),
 'XTXTXXXX': (8, 7),
 'XTXXTTTT': (9, 14),
 'XTXXTTTX': (13, 7),
 'XTXXTTXT': (7, 3),
 'XTXXTTXX': (13, 11),
 'XTXXTXTT': (7, 8),
 'XTXXTXTX': (11, 7),
 'XTXXTXXT': (11, 7),
 'XTXXTXXX': (12, 10),
 'XTXXXTTT': (13, 10),
 'XTXXXTTX': (11, 14),
 'XTXXXTXT': (5, 14),
 'XTXXXTXX': (12, 9),
 'XTXXXXTT': (11, 8),
 'XTXXXXTX': (12, 11),
 'XTXXXXXT': (7, 12),
 'XTXXXXXX': (7, 6),
 'XXTTTTTT': (11, 12),
 'XXTTTTTX': (11, 11),
 'XXTTTTXT': (10, 8),
 'XXTTTTXX': (11, 13),
 'XXTTTXTT': (15, 6),
 'XXTTTXTX': (9, 10),
 'XXTTTXXT': (7, 15),
 'XXTTTXXX': (9, 4),
 'XXTTXTTT': (11, 12),
 'XXTTXTTX': (5, 8),
 'XXTTXTXT': (6, 8),
 'XXTTXTXX': (10, 8),
 'XXTTXXTT': (13, 5),
 'XXTTXXTX': (14, 13),
 'XXTTXXXT': (7, 5),
 'XXTTXXXX': (7, 13),
 'XXTXTTTT': (14, 10),
 'XXTXTTTX': (4, 15),
 'XXTXTTXT': (5, 3),
 'XXTXTTXX': (12, 9),
 'XXTXTXTT': (14, 11),
 'XXTXTXTX': (12, 13),
 'XXTXTXXT': (12, 4),
 'XXTXTXXX': (14, 5),
 'XXTXXTTT': (15, 11),
 'XXTXXTTX': (5, 13),
 'XXTXXTXT': (7, 11),
 'XXTXXTXX': (8, 13),
 'XXTXXXTT': (15, 11),
 'XXTXXXTX': (8, 16),
 'XXTXXXXT': (7, 10),
 'XXTXXXXX': (12, 5),
 'XXXTTTTT': (11, 11),
 'XXXTTTTX': (6, 7),
 'XXXTTTXT': (10, 8),
 'XXXTTTXX': (10, 6),
 'XXXTTXTT': (19, 7),
 'XXXTTXTX': (6, 13),
 'XXXTTXXT': (11, 13),
 'XXXTTXXX': (6, 12),
 'XXXTXTTT': (8, 14),
 'XXXTXTTX': (2, 10),
 'XXXTXTXT': (14, 10),
 'XXXTXTXX': (6, 8),
 'XXXTXXTT': (14, 12),
 'XXXTXXTX': (9, 12),
 'XXXTXXXT': (13, 8),
 'XXXTXXXX': (8, 5),
 'XXXXTTTT': (13, 5),
 'XXXXTTTX': (10, 6),
 'XXXXTTXT': (11, 10),
 'XXXXTTXX': (10, 10),
 'XXXXTXTT': (16, 5),
 'XXXXTXTX': (12, 4),
 'XXXXTXXT': (18, 10),
 'XXXXTXXX': (11, 6),
 'XXXXXTTT': (10, 8),
 'XXXXXTTX': (13, 10),
 'XXXXXTXT': (8, 8),
 'XXXXXTXX': (10, 12),
 'XXXXXXTT': (10, 12),
 'XXXXXXTX': (6, 6),
 'XXXXXXXT': (8, 8),
 'XXXXXXXX': (4, 7)}
REFERENCE_PATTERN_SOURCE_ROWS = 5022

def _reference_allowed(board):
    # Source is Tài/Xỉu-oriented. Do not transfer it to Baccarat or Xóc Đĩa semantics.
    return bool(board) and not str(board).startswith('baccarat:') and str(board) != 'lc79:xocdia'

def _reference_pattern_signal(seq):
    if not seq:
        return {'ready':False,'prediction':'TÀI','score':0.0,'support':0,'length':0,'p_tai':.5}
    tx=''.join('T' if x=='TÀI' else 'X' for x in seq[-8:])
    num=den=0.0; best_support=0; best_len=0; best_p=.5; used=[]
    for L in range(min(8,len(tx)),1,-1):
        key=tx[-L:]; pair=REFERENCE_PATTERN_COUNTS.get(key)
        if not pair: continue
        t,x=pair; support=t+x
        min_support=4 if L>=7 else 5 if L>=5 else 7
        if support<min_support: continue
        # Beta smoothing: conflict-heavy patterns remain near 50/50.
        p=(t+3.0)/(support+6.0); edge=(p-.5)*2.0
        reliability=(1-math.exp(-support/14.0))*((L/8.0)**1.35)
        # Small edges should not dominate just because support is large.
        w=reliability*(.38+.62*min(1.0,abs(edge)*2.4))
        num+=edge*w; den+=w
        used.append((L,key,support,p,edge,w))
        if L>best_len or (L==best_len and support>best_support):
            best_len=L;best_support=support;best_p=p
    if den<=0:
        pred=_markov_prediction(seq,2)
        return {'ready':False,'prediction':pred,'score':0.0,'support':0,'length':0,'p_tai':.5}
    score=_clamp(num/den,-.72,.72)
    pred='TÀI' if score>=0 else 'XỈU'
    return {'ready':True,'prediction':pred,'score':round(score,4),'support':best_support,'length':best_len,
            'p_tai':round((score+1)/2,4),'best_p_tai':round(best_p,4),'contexts':len(used)}

def _reference_pattern_prediction(seq):
    return _reference_pattern_signal(seq).get('prediction') or _markov_prediction(seq,2)

def _js_randomness_score(seq):
    # Port of the useful part of JS model8: change ratio + balance + entropy.
    q=list(seq[-15:])
    if len(q)<10:return .5
    changes=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])
    change_ratio=changes/max(1,len(q)-1)
    t=q.count('TÀI');x=len(q)-t;distribution=abs(t-x)/len(q)
    p=t/len(q); ent=0.0
    for z in (p,1-p):
        if z>0:ent-=z*math.log2(z)
    return _clamp(change_ratio*.4+(1-distribution)*.3+ent*.3,0,1)

def _js_break_signal(seq):
    # Combines streak break rate, same-length run survival and recent break behaviour.
    if not seq:return {'prediction':'TÀI','p_break':.5,'support':0,'run':0}
    side=seq[-1];cur=max(1,min(_run_len(seq),8))
    opportunities=breaks=0
    for i in range(4,len(seq)):
        prev=seq[i-1];rl=1;j=i-2
        while j>=0 and seq[j]==prev and rl<8:
            rl+=1;j-=1
        if rl<3 or abs(rl-cur)>1:continue
        opportunities+=1
        if seq[i]!=prev:breaks+=1
    # Recent evidence gets a smaller adaptive component.
    ro=rb=0
    for i in range(max(4,len(seq)-18),len(seq)):
        prev=seq[i-1];rl=1;j=i-2
        while j>=0 and seq[j]==prev and rl<8:
            rl+=1;j-=1
        if rl<3:continue
        ro+=1;rb+=1 if seq[i]!=prev else 0
    p_global=(breaks+3)/(opportunities+6)
    p_recent=(rb+2)/(ro+4) if ro else .5
    # Longer current runs raise break pressure only mildly; history remains dominant.
    length_prior=_clamp(.42+.035*max(0,cur-2),.42,.68)
    p=_clamp(.55*p_global+.25*p_recent+.20*length_prior,.18,.82)
    pred=_opp(side) if p>=.54 else side
    return {'prediction':pred,'p_break':round(p,4),'support':opportunities,'run':cur}

def _js_break_calibrator_prediction(seq):
    return _js_break_signal(seq)['prediction']

def _js_trend_blend_prediction(seq):
    # JS model2/3/4/15 distilled into a causal, non-recursive predictor.
    if not seq:return 'TÀI'
    short=seq[-5:];long=seq[-20:];q12=seq[-12:]
    def edge(q):return (q.count('TÀI')-q.count('XỈU'))/max(1,len(q))
    es=edge(short);el=edge(long);e12=edge(q12)
    trend=('TÀI' if (es+el)>=0 else 'XỈU')
    trend_strength=.62*abs(es)+.38*abs(el)
    # Mean reversion only activates on a clearly imbalanced W12.
    meanrev=_opp('TÀI' if e12>0 else 'XỈU') if abs(e12)>=.34 else None
    br=_js_break_signal(seq)
    vt=vx=0.0
    def add(pred,w):
        nonlocal vt,vx
        if pred=='TÀI':vt+=w
        elif pred=='XỈU':vx+=w
    add(trend,.85+trend_strength)
    if meanrev:add(meanrev,.55+abs(e12)*.55)
    add(br['prediction'],.62+abs(br['p_break']-.5)*1.1)
    # Short momentum vote.
    s3=seq[-3:];add('TÀI' if s3.count('TÀI')>=2 else 'XỈU',.62)
    return 'TÀI' if vt>=vx else 'XỈU'

def _js_ultra_stack_prediction(seq, use_reference=True):
    # Performance selection is handled by BOARD_META; this is the local JS-inspired stack.
    if not seq:return 'TÀI'
    rnd=_js_randomness_score(seq)
    signals=[
        (_js_trend_blend_prediction(seq),1.00),
        (_js_break_calibrator_prediction(seq),.95),
        (_bayes_context_prediction(seq),1.10),
        (_context_entropy_prediction(seq),.92),
        (_regime_switch_prediction(seq),1.00),
    ]
    if use_reference:
        rs=_reference_pattern_signal(seq)
        if rs.get('ready'):
            rw=(.55+min(.55,abs(float(rs.get('score',0)))*.9))*(.65 if rnd>.72 else 1.0)
            signals.append((rs['prediction'],rw))
    # On bad/random runs, shrink pattern/trend concentration by giving adaptive context more say.
    if rnd>.72:
        signals += [(_multi_window_prediction(seq),.72),(_markov_prediction(seq,2),.78)]
    vt=vx=0.0
    for pred,w in signals:
        if pred=='TÀI':vt+=w
        else:vx+=w
    return 'TÀI' if vt>=vx else 'XỈU'



def _dirichlet_vom_prediction(seq):
    """Variable-order Markov 1..10 with Dirichlet smoothing + recency evidence."""
    if not seq:return 'TÀI'
    n=len(seq); vt=vx=0.0
    max_order=min(10,max(1,n//5))
    for order in range(1,max_order+1):
        if n<=order+3:continue
        ctx=tuple(seq[-order:]);t=x=1.8;ev=0.0
        half=max(28.0,72.0-order*3.0)
        for i in range(order,n):
            if tuple(seq[i-order:i])!=ctx:continue
            age=(n-1)-i;w=.5**(age/half)
            if seq[i]=='TÀI':t+=w
            else:x+=w
            ev+=w
        if ev<1.1:continue
        edge=(t-x)/(t+x)
        evidence=(1-math.exp(-ev/4.0))*min(1.35,.72+.075*order)
        if edge>=0:vt+=abs(edge)*evidence
        else:vx+=abs(edge)*evidence
    if vt+vx<.025:return _markov_prediction(seq,2)
    return 'TÀI' if vt>=vx else 'XỈU'


def _sequential_change_prediction(seq):
    """Detect distribution/transition drift and shorten memory after a change."""
    if not seq:return 'TÀI'
    n=len(seq)
    if n<36:return _decayed_transition_prediction(seq,2)
    def feats(q):
        if len(q)<3:return (0.5,0.5,0.5)
        pt=q.count('TÀI')/len(q)
        same=sum(1 for i in range(1,len(q)) if q[i]==q[i-1])/max(1,len(q)-1)
        return pt,same,1-same
    recent=seq[-24:]; older=seq[-104:-24] if n>=104 else seq[:-24]
    fr=feats(recent);fo=feats(older)
    drift=sum(abs(a-b) for a,b in zip(fr,fo))/3.0
    if drift>=.17:return _regime_switch_prediction(seq[-80:])
    if drift>=.10:
        a=_changepoint_adaptive_prediction(seq);b=_dirichlet_vom_prediction(seq[-220:])
        return a if a==b else _decayed_transition_prediction(seq,2)
    return _dirichlet_vom_prediction(seq)


def _motif_survival_prediction(seq):
    """Suffix matching 3..12 with Beta smoothing, age decay and support weighting."""
    if not seq:return 'TÀI'
    n=len(seq);vt=vx=0.0
    for L in range(3,min(12,n-4)+1):
        motif=tuple(seq[-L:]);t=x=1.4;ev=0.0
        for i in range(L,n):
            if tuple(seq[i-L:i])!=motif:continue
            age=(n-1)-i;w=.5**(age/max(36.0,92.0-L*3.0))
            if seq[i]=='TÀI':t+=w
            else:x+=w
            ev+=w
        if ev<1.0:continue
        edge=(t-x)/(t+x)
        strength=(1-math.exp(-ev/3.6))*(.66+.055*L)
        if edge>=0:vt+=abs(edge)*strength
        else:vx+=abs(edge)*strength
    if vt+vx<.02:return _suffix_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'


def _regime_posterior_prediction(seq):
    """Soft mixture over run / alternating / biased / mixed regimes."""
    if not seq:return 'TÀI'
    q=seq[-64:];run=_run_len(q)
    flips=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])/max(1,len(q)-1)
    bias=abs(q.count('TÀI')-q.count('XỈU'))/max(1,len(q));h=_entropy(q)
    prun=math.exp(2.0*min(1,run/5)+1.4*max(0,.48-flips))
    palt=math.exp(3.0*max(0,flips-.52))
    pbias=math.exp(4.0*max(0,bias-.12))
    pmix=math.exp(1.8*max(0,h-.84));z=prun+palt+pbias+pmix
    votes=[(_run_hazard_prediction(seq),prun/z),(_opp(seq[-1]),palt/z),
           (_multi_window_prediction(seq),pbias/z),(_bayes_context_prediction(seq),pmix/z)]
    vt=sum(w for p,w in votes if p=='TÀI');vx=sum(w for p,w in votes if p=='XỈU')
    return 'TÀI' if vt>=vx else 'XỈU'


def _multiresolution_edge_prediction(seq):
    """Shrinked edge over 8..1024 horizons."""
    if not seq:return 'TÀI'
    n=len(seq);vt=vx=0.0
    for k,wsize in enumerate((8,16,32,64,128,256,512,1024)):
        if n<min(8,wsize):continue
        q=seq[-min(n,wsize):];m=len(q);raw=(q.count('TÀI')-q.count('XỈU'))/m
        shr=raw*(m/(m+18.0));wt=(1.18/(1+.18*k))*(.72+.28*min(1,m/128))
        if shr>=0:vt+=abs(shr)*wt
        else:vx+=abs(shr)*wt
    if vt+vx<.02:return _multi_window_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'


def _bayes_run_mixture_prediction(seq):
    """Hierarchical continuation/break model by side and run length, blended with context."""
    if not seq:return 'TÀI'
    side=seq[-1];cur=min(_run_len(seq),12);cont=brk=2.0
    for i in range(1,len(seq)):
        prev=seq[i-1];rl=1;j=i-2
        while j>=0 and seq[j]==prev and rl<12:
            rl+=1;j-=1
        if prev!=side:continue
        d=abs(rl-cur)
        if d>2:continue
        w=(1.0,.62,.34)[d];age=(len(seq)-1)-i;w*=.5**(age/180.0)
        if seq[i]==side:cont+=w
        else:brk+=w
    pcont=cont/(cont+brk);run_pred=side if pcont>=.5 else _opp(side);ctx=_dirichlet_vom_prediction(seq)
    if abs(pcont-.5)>=.12:return run_pred
    return run_pred if run_pred==ctx else _regime_posterior_prediction(seq)


def _confidence_bucket_calibration(board, proposed):
    """Historical guardrail for displayed signal strength; not a win-probability estimate."""
    try:
        with sqlite3.connect(DB_PATH) as db:
            rows=db.execute('SELECT ok,model_json FROM shared_predictions WHERE board=? AND actual IS NOT NULL AND ok IS NOT NULL ORDER BY settled_at DESC LIMIT 320',(board,)).fetchall()
    except Exception:
        return {'sample':0,'bayes':.5,'cap':72.0,'bonus':0.0}
    target=float(proposed)
    def bucket(width):
        vals=[]
        for ok,mj in rows:
            try:m=json.loads(mj) if mj else {}
            except Exception:m={}
            c=m.get('model_confidence',m.get('confidence'))
            if isinstance(c,(int,float)) and abs(float(c)-target)<=width:vals.append(bool(ok))
        return vals
    vals=bucket(6.0)
    if len(vals)<16:vals=bucket(10.0)
    n=len(vals);wins=sum(vals);bayes=(wins+8*.5)/(n+8) if n else .5;cap=72.0;bonus=0.0
    if n>=18:
        if bayes<.45:cap=54.0
        elif bayes<.49:cap=57.0
        elif bayes<.52:cap=61.0
        elif bayes<.55:cap=66.0
        elif bayes>.59 and n>=30:bonus=1.0
    return {'sample':n,'bayes':round(bayes,4),'cap':cap,'bonus':bonus}


def _ctx_prob_weighted(seq, order, max_scan=6000):
    """Recency-weighted Beta-smoothed P(TÀI | context)."""
    n=len(seq)
    if n<=order:return .5,0.0
    ctx=tuple(seq[-order:]); t=x=0.0; support=0.0
    start=max(order,n-max_scan)
    tau=max(120.0,min(1800.0,max_scan/2.2))
    for i in range(start,n):
        if tuple(seq[i-order:i])!=ctx:continue
        age=n-i
        w=math.exp(-age/tau)
        support+=w
        if seq[i]=='TÀI':t+=w
        else:x+=w
    p=(t+2.0)/(t+x+4.0)
    return p,support

def _ctw_approx_prediction(seq):
    if len(seq)<8:return _markov_prediction(seq,1)
    logit=0.0;den=0.0
    for k in range(1,min(12,len(seq)-1)+1):
        p,sup=_ctx_prob_weighted(seq,k)
        if sup<.8:continue
        strength=abs(p-.5)*2
        w=(1.0+k*.16)*(sup/(sup+5.0))*(.25+.75*strength)
        logit+=(p-.5)*w;den+=w
    if den<=0:return _bayes_context_prediction(seq)
    return 'TÀI' if logit>=0 else 'XỈU'

def _hierarchical_bayes_prediction(seq):
    if len(seq)<10:return _markov_prediction(seq,1)
    base=(seq[-512:].count('TÀI')+6)/(len(seq[-512:])+12)
    p=base
    # Higher orders update the posterior only when they have evidence.
    for k in range(1,min(10,len(seq)-1)+1):
        pk,sup=_ctx_prob_weighted(seq,k,5000)
        shrink=sup/(sup+7.0+1.8*k)
        p=(1-shrink)*p+shrink*pk
    return 'TÀI' if p>=.5 else 'XỈU'

def _transition_drift_prediction(seq):
    if len(seq)<12:return _markov_prediction(seq,1)
    last=seq[-1]
    def p_for(window):
        q=seq[-window:];a=b=0.0
        for i in range(1,len(q)):
            if q[i-1]!=last:continue
            # mild recency emphasis
            w=.55+.45*(i/max(1,len(q)-1))
            if q[i]=='TÀI':a+=w
            else:b+=w
        return (a+2)/(a+b+4),a+b
    vals=[]
    for w in (24,64,160,512,1600):
        if len(seq)>=min(12,w//2):vals.append(p_for(w))
    if not vals:return _markov_prediction(seq,1)
    longp=vals[-1][0];score=0.0;den=0.0
    for idx,(p,sup) in enumerate(vals):
        rec=(len(vals)-idx)/len(vals)
        drift=abs(p-longp)
        wt=(sup/(sup+8))*(1.15 if idx==0 and drift>.12 else 1.0)*(.75+.5*rec)
        score+=(p-.5)*wt;den+=wt
    return 'TÀI' if score>=0 else 'XỈU'

def _run_context_joint_prediction(seq):
    if len(seq)<12:return _run_hazard_prediction(seq)
    last=seq[-1];run=min(_run_len(seq),8)
    t=x=0.0;n=len(seq)
    for i in range(4,n):
        # run length ending at i-1
        r=1
        j=i-2
        while j>=0 and seq[j]==seq[i-1] and r<8:
            r+=1;j-=1
        if seq[i-1]!=last or min(r,8)!=run:continue
        # joint state: same side/run + last two flip states where possible
        curflip=(seq[-1]!=seq[-2]); histflip=(seq[i-1]!=seq[i-2])
        if curflip!=histflip:continue
        w=math.exp(-(n-i)/900.0)
        if seq[i]=='TÀI':t+=w
        else:x+=w
    if t+x<2.0:return _bayes_run_mixture_prediction(seq)
    p=(t+2)/(t+x+4)
    return 'TÀI' if p>=.5 else 'XỈU'

def _spectral_lag_prediction(seq):
    q=seq[-768:]
    if len(q)<24:return _periodic_match_prediction(seq)
    vt=vx=0.0
    for lag in range(2,min(40,len(q)//3)+1):
        matches=sum(1 for i in range(lag,len(q)) if q[i]==q[i-lag])
        total=len(q)-lag
        if total<18:continue
        rate=(matches+4*.5)/(total+4)
        edge=abs(rate-.5)
        if edge<.025:continue
        pred=q[-lag] if rate>=.5 else _opp(q[-lag])
        wt=edge*math.sqrt(total)/(1+.035*lag)
        if pred=='TÀI':vt+=wt
        else:vx+=wt
    if vt+vx<=0:return _lag_ensemble_prediction(seq)
    return 'TÀI' if vt>=vx else 'XỈU'

def _robust_stack_prediction(seq):
    experts=[
        _ctw_approx_prediction(seq),_hierarchical_bayes_prediction(seq),
        _transition_drift_prediction(seq),_run_context_joint_prediction(seq),
        _spectral_lag_prediction(seq),_dirichlet_vom_prediction(seq),
        _cross_horizon_bayes_prediction(seq),_regime_posterior_prediction(seq),
        _entropy_gate_prediction(seq),_bayes_run_mixture_prediction(seq)
    ]
    t=experts.count('TÀI');x=len(experts)-t
    if t==x:return _ctw_approx_prediction(seq)
    return 'TÀI' if t>x else 'XỈU'

def _pure_strategy_predictions(seq, board=None):
    """Causal predictors dùng riêng để walk-forward, không đọc future/DB feedback."""
    if not seq:return {}
    last=seq[-1];run=_run_len(seq);q6=seq[-6:]
    flips=sum(1 for i in range(1,len(q6)) if q6[i]!=q6[i-1]);alt=flips/max(1,len(q6)-1)
    p20=seq[-20:].count('TÀI')/max(1,len(seq[-20:]));p12=seq[-12:].count('TÀI')/max(1,len(seq[-12:]))
    return {
      'FOLLOW_LAST':last,'REVERSE_LAST':_opp(last),
      'ALTERNATING_PATTERN':_opp(last) if alt>=.60 else _markov_prediction(seq,1),
      'RUN_BREAK':_opp(last) if run>=3 else _markov_prediction(seq,1),
      'RUN_FOLLOW':last if 2<=run<=3 else (_opp(last) if run>=5 else _markov_prediction(seq,1)),
      'BIAS_MEAN_REVERSION':'XỈU' if p20>=.60 else 'TÀI' if p20<=.40 else _opp(last),
      'BIAS_MOMENTUM':'TÀI' if p12>=.58 else 'XỈU' if p12<=.42 else last,
      'MARKOV_TRANSITION':_markov_prediction(seq,1),
      'HIGH_ORDER_MARKOV':_markov_prediction(seq,3),
      'MARKOV_ORDER2':_markov_prediction(seq,2),
      'RUN_HAZARD':_run_hazard_prediction(seq),
      'MULTI_WINDOW':_multi_window_prediction(seq),
      'DECAYED_TRANSITION':_decayed_transition_prediction(seq,2),
      'REGIME_ADAPTIVE':_regime_adaptive_prediction(seq),
      'MOTIF_WEIGHTED':_motif_weighted_prediction(seq),
      'FLIP_STATE_MARKOV':_flip_state_markov_prediction(seq),
      'RUN_LENGTH_MARKOV':_run_length_markov_prediction(seq),
      'PERIODIC_MATCH':_periodic_match_prediction(seq),
      'DUAL_HORIZON':_dual_horizon_prediction(seq),
      'ANALOG_KNN':_analog_knn_prediction(seq),
      'RUN_SURVIVAL':_run_survival_prediction(seq),
      'CONTEXT_ENTROPY':_context_entropy_prediction(seq),
      'BAYES_CONTEXT':_bayes_context_prediction(seq),
      'HORIZON_CONSENSUS':_horizon_consensus_prediction(seq),
      'REGIME_SWITCH':_regime_switch_prediction(seq),
      'LAG_ENSEMBLE':_lag_ensemble_prediction(seq),
      'REFERENCE_PATTERN_PRIOR':_reference_pattern_prediction(seq) if _reference_allowed(board) else _bayes_context_prediction(seq),
      'JS_TREND_BLEND':_js_trend_blend_prediction(seq),
      'JS_BREAK_CALIBRATOR':_js_break_calibrator_prediction(seq),
      'JS_ULTRA_STACK':_js_ultra_stack_prediction(seq,_reference_allowed(board)),
      'VOM_CONTEXT_6':_vom_context6_prediction(seq),
      'LONG_MEMORY_BAYES':_long_memory_bayes_prediction(seq),
      'RUN_PROFILE_LONG':_run_profile_long_prediction(seq),
      'MULTISCALE_TRANSITION':_multiscale_transition_prediction(seq),
      'CHANGEPOINT_ADAPTIVE':_changepoint_adaptive_prediction(seq),
      'WILSON_CONTEXT':_wilson_context_prediction(seq),
      'KNN_RECENCY':_knn_recency_prediction(seq),
      'RUN_MATRIX':_run_matrix_prediction(seq),
      'ENTROPY_GATE':_entropy_gate_prediction(seq),
      'CROSS_HORIZON_BAYES':_cross_horizon_bayes_prediction(seq),
      'DIRICHLET_VOM':_dirichlet_vom_prediction(seq),
      'SEQUENTIAL_CHANGE':_sequential_change_prediction(seq),
      'MOTIF_SURVIVAL':_motif_survival_prediction(seq),
      'REGIME_POSTERIOR':_regime_posterior_prediction(seq),
      'MULTIRESOLUTION_EDGE':_multiresolution_edge_prediction(seq),
      'BAYES_RUN_MIXTURE':_bayes_run_mixture_prediction(seq),
      'CTW_APPROX':_ctw_approx_prediction(seq),
      'HIERARCHICAL_BAYES':_hierarchical_bayes_prediction(seq),
      'TRANSITION_DRIFT':_transition_drift_prediction(seq),
      'RUN_CONTEXT_JOINT':_run_context_joint_prediction(seq),
      'SPECTRAL_LAG':_spectral_lag_prediction(seq),
      'ROBUST_STACK':_robust_stack_prediction(seq),
      'SUFFIX_CONTEXT':_suffix_prediction(seq)
    }


def _bayes_rate(wins,total,prior_n=14,prior_p=.5):
    return (wins+prior_n*prior_p)/max(1,total+prior_n)

def walk_forward_strategy_stats(board,rows):
    profile=_profile_for(board); seq_all=_seq(rows); last_id=str(rows[-1].get('id')) if rows else ''
    recent_depth=max(int(profile.get('wf_depth',170)),260)
    sparse_span=min(1100,max(recent_depth+120,int(math.sqrt(max(1,len(seq_all)))*34)))
    key=(board,last_id,len(seq_all),recent_depth,sparse_span,'v49')
    if key in _WF_CACHE:return _WF_CACHE[key]
    seq=seq_all[-min(len(seq_all),sparse_span+80):]
    names=[n for n in STRATEGY_NAMES if n not in ('ANTI_RAW','FUSION_CORE')]
    rec={n:[] for n in names};recent_start=max(24,len(seq)-recent_depth);old_start=max(24,len(seq)-sparse_span)
    indices=sorted(set(list(range(old_start,recent_start,6))+list(range(recent_start,len(seq)))))
    for i in indices:
        hist=seq[:i];actual=seq[i];reg=_regime_label(hist);preds=_pure_strategy_predictions(hist,board);is_recent=i>=recent_start
        for n in names:
            pr=preds.get(n)
            if pr in ('TÀI','XỈU'):rec[n].append((pr==actual,reg,is_recent))
    current_reg=_regime_label(seq);out={}
    for n,vals in rec.items():
        def st(items):
            q=[bool(x[0] if isinstance(x,tuple) else x) for x in items];total=len(q);wins=sum(q);raw=wins/total if total else .5
            num=den=0.0
            for age,ok in enumerate(reversed(q)):
                w=.5**(age/14.0);den+=w;num+=w*(1.0 if ok else 0.0)
            ewma=num/den if den else .5
            return {'total':total,'win':wins,'loss':total-wins,'win_rate':_bayes_rate(wins,total),'raw_win_rate':raw,'ewma':ewma,'lower':_wilson_lower(wins,total) if total else .0}
        recent_vals=[x for x in vals if len(x)<3 or x[2]]
        s20=st(recent_vals[-20:]);s50=st(recent_vals[-50:]);s100=st(recent_vals[-100:]);s200=st(vals[-200:]);s400=st(vals[-400:])
        rv=[x for x in vals if isinstance(x,tuple) and x[1]==current_reg][-80:];rs=st(rv)
        rates=[x['win_rate'] for x in (s20,s50,s100,s200) if x['total']>=10];stability=1-(max(rates)-min(rates) if len(rates)>=2 else .12)
        score=.34*s20['win_rate']+.23*s50['win_rate']+.14*s100['win_rate']+.10*s200['win_rate']+.07*s400['win_rate']+.12*s20['ewma']
        if rs['total']>=8:score+=.08*(rs['win_rate']-.5)
        score+=.05*(stability-.8)
        if s20['total']<14:score-=.025
        if s50['total']<32:score-=.020
        out[n]={'short':s20,'mid':s50,'long':s100,'xl':s200,'xxl':s400,'regime':rs,'regime_name':current_reg,'stability':round(stability,4),'score':round(score,5),'samples':len(vals),'recent_samples':len(recent_vals),'wf_span':len(seq),'wf_sparse_stride':6}
    _WF_CACHE.clear();_WF_CACHE[key]=out
    return out


def _combined_quality(board,name,live,wf):
    ls=live.get(name,{});ws=wf.get(name,{})
    l20=ls.get('short',{});l50=ls.get('mid',{});w20=ws.get('short',{});w50=ws.get('mid',{});w100=ws.get('long',{})
    ln=int(l20.get('total',0));wn=int(w20.get('total',0))
    lw=min(.52,ln/44*.52);ww=1-lw
    l20r=_bayes_rate(int(l20.get('win',0)),int(l20.get('total',0)),12,.5)
    l50r=_bayes_rate(int(l50.get('win',0)),int(l50.get('total',0)),18,.5)
    lr=.58*l20r+.42*l50r
    wxl=ws.get('xl',{});wxxl=ws.get('xxl',{})
    wr=.34*float(w20.get('win_rate',.5))+.22*float(w50.get('win_rate',.5))+.12*float(w100.get('win_rate',.5))
    wr+=.08*float(wxl.get('win_rate',.5))+.05*float(wxxl.get('win_rate',.5))
    wr+=.12*float(w20.get('ewma',.5))+.07*max(.42,float(w20.get('lower',.0)))
    q=lw*lr+ww*wr
    rs=ws.get('regime',{})
    if int(rs.get('total',0))>=8:q+=.10*(float(rs.get('win_rate',.5))-.5)
    stability=float(ws.get('stability',.88))
    if stability<.78:q-=.030
    elif stability>.92:q+=.012
    if name in _profile_for(board).get('prefer',()):q+=.012
    imported={'REFERENCE_PATTERN_PRIOR','JS_TREND_BLEND','JS_BREAK_CALIBRATOR','JS_ULTRA_STACK'}
    newv47={'CHANGEPOINT_ADAPTIVE','WILSON_CONTEXT','KNN_RECENCY','RUN_MATRIX','ENTROPY_GATE','CROSS_HORIZON_BAYES'}
    newv48={'DIRICHLET_VOM','SEQUENTIAL_CHANGE','MOTIF_SURVIVAL','REGIME_POSTERIOR','MULTIRESOLUTION_EDGE','BAYES_RUN_MIXTURE'}
    newv49={'CTW_APPROX','HIERARCHICAL_BAYES','TRANSITION_DRIFT','RUN_CONTEXT_JOINT','SPECTRAL_LAG','ROBUST_STACK'}
    if name in imported and int(ws.get('samples',0))<42:q-=.040
    if name in newv47 and int(ws.get('samples',0))<48:q-=.045
    if name in newv48 and int(ws.get('samples',0))<60:q-=.050
    if name in newv49 and int(ws.get('samples',0))<72:q-=.055
    if wn<18:q-=.022
    if int(l20.get('loss_streak',0))>=3:q-=.045
    if wn>=18 and float(w20.get('raw_win_rate',.5))<.44:q-=.040
    if wn>=18 and float(w20.get('ewma',.5))<.43:q-=.035
    return _clamp(q,.34,.705)


def _meta_consensus(board,preds,live,wf,seq=None):
    prof=_profile_for(board);c=[];seq=seq or [];rnd=_js_randomness_score(seq) if seq else .5
    for n,p in preds.items():
        if p.get('prediction') not in ('TÀI','XỈU'):continue
        q=_combined_quality(board,n,live,wf)
        if n=='ANTI_RAW' and live.get(n,{}).get('short',{}).get('total',0)<14:continue
        if q<.485 and wf.get(n,{}).get('short',{}).get('total',0)>=18:continue
        c.append({'name':n,'prediction':p['prediction'],'quality':q,'edge':q-.5,'local_confidence':p.get('local_confidence',52)})
    c.sort(key=lambda x:x['quality'],reverse=True)
    families={
      'markov':{'MARKOV_TRANSITION','MARKOV_ORDER2','HIGH_ORDER_MARKOV','DECAYED_TRANSITION','FLIP_STATE_MARKOV','RUN_LENGTH_MARKOV','CONTEXT_ENTROPY','WILSON_CONTEXT','DIRICHLET_VOM','CTW_APPROX','HIERARCHICAL_BAYES'},
      'motif':{'SUFFIX_CONTEXT','MOTIF_WEIGHTED','PERIODIC_MATCH','ANALOG_KNN','KNN_RECENCY','MOTIF_SURVIVAL'},
      'regime':{'REGIME_ADAPTIVE','REGIME_SWITCH','CHANGEPOINT_ADAPTIVE','ENTROPY_GATE','MULTI_WINDOW','DUAL_HORIZON','BIAS_MOMENTUM','BIAS_MEAN_REVERSION','SEQUENTIAL_CHANGE','REGIME_POSTERIOR','TRANSITION_DRIFT'},
      'run':{'RUN_HAZARD','RUN_BREAK','RUN_FOLLOW','RUN_SURVIVAL','JS_BREAK_CALIBRATOR','RUN_MATRIX','BAYES_RUN_MIXTURE','RUN_CONTEXT_JOINT'},
      'adaptive':{'JS_TREND_BLEND','JS_ULTRA_STACK','MULTISCALE_TRANSITION','CROSS_HORIZON_BAYES','HORIZON_CONSENSUS','LAG_ENSEMBLE','MULTIRESOLUTION_EDGE','SPECTRAL_LAG','ROBUST_STACK'},
      'longmem':{'VOM_CONTEXT_6','LONG_MEMORY_BAYES','RUN_PROFILE_LONG'},
      'reference':{'REFERENCE_PATTERN_PRIOR'},
      'other':{'FOLLOW_LAST','REVERSE_LAST','ALTERNATING_PATTERN','ANTI_RAW','FUSION_CORE'}
    }
    def fam(name):
        for k,v in families.items():
            if name in v:return k
        return 'other'
    target=max(1,int(prof.get('top_k',3)))
    if rnd>=.74:target=min(6,target+1)
    top=[];used=set()
    # First pass: one expert per family gives real diversity.
    for x in c:
        f=fam(x['name'])
        if f in used:continue
        if rnd>=.76 and f in ('reference','motif') and x['quality']<.535:continue
        top.append(x);used.add(f)
        if len(top)>=target:break
    # Second pass: fill only with genuinely strong extra experts.
    if len(top)<target:
        for x in c:
            if x in top:continue
            if x['quality']<.505 and len(top)>=2:continue
            top.append(x)
            if len(top)>=target:break
    vt=vx=0.0
    for x in top:
        n=x['name'];lc=_clamp((float(x.get('local_confidence',52))-50)/22,0,1)
        ws=wf.get(n,{});w20=ws.get('short',{});rs=ws.get('regime',{})
        lower=max(.40,float(w20.get('lower',.0)));ew=float(w20.get('ewma',.5))
        reliability=_clamp(.72+max(0,lower-.45)*1.4+max(0,ew-.5)*.7,.68,1.15)
        if int(rs.get('total',0))>=8:reliability*=_clamp(.90+(float(rs.get('win_rate',.5))-.5)*.8,.80,1.10)
        w=max(.012,x['edge']+.018)*(.88+.22*lc)*reliability
        if rnd>=.78 and fam(n) in ('reference','motif'):w*=.72
        if x['prediction']=='TÀI':vt+=w
        else:vx+=w
    if abs(vt-vx)<.012:final=top[0]['prediction'] if top else 'TÀI'
    else:final='TÀI' if vt>vx else 'XỈU'
    total=max(.001,vt+vx);agree=max(vt,vx)/total
    return final,top,agree


def _strategy_predictors(board,rows,fusion):
    seq=_seq(rows)
    if not seq: seq=['TÀI']
    last=seq[-1]; run=_run_len(seq)
    q6=seq[-6:]
    flips=sum(1 for i in range(1,len(q6)) if q6[i]!=q6[i-1])
    alt_rate=flips/max(1,len(q6)-1)
    p20=seq[-20:].count('TÀI')/max(1,len(seq[-20:]))
    p12=seq[-12:].count('TÀI')/max(1,len(seq[-12:]))
    markov=_markov_prediction(seq,1)
    high=_markov_prediction(seq,3)
    final_hist=_recent_final_stats(board,20)
    raw_bad=(sum(1 for h in final_hist if not h['ok'])/max(1,len(final_hist))) if final_hist else 0.0
    fusion_pred=fusion.get('prediction') if fusion.get('prediction') in ('TÀI','XỈU') else markov
    preds={}
    def put(name,pred,conf,reason):
        preds[name]={'name':name,'prediction':pred,'local_confidence':int(_clamp(conf,45,72)),'reason':reason}
    put('FOLLOW_LAST',last,51,'Theo kết quả gần nhất')
    put('REVERSE_LAST',_opp(last),51,'Đảo kết quả gần nhất')
    put('ALTERNATING_PATTERN',_opp(last) if alt_rate>=.60 else markov,50+abs(alt_rate-.5)*20,f'Alt rate {alt_rate:.2f}')
    put('RUN_BREAK',_opp(last) if run>=3 else markov,50+min(10,run*1.5),f'Run {run}')
    put('RUN_FOLLOW',last if 2<=run<=3 else (_opp(last) if run>=5 else markov),50+min(9,run*1.3),f'Run {run}')
    put('BIAS_MEAN_REVERSION','XỈU' if p20>=.60 else 'TÀI' if p20<=.40 else _opp(last),50+abs(p20-.5)*25,f'pT20 {p20:.2f}')
    put('BIAS_MOMENTUM','TÀI' if p12>=.58 else 'XỈU' if p12<=.42 else last,50+abs(p12-.5)*28,f'pT12 {p12:.2f}')
    put('MARKOV_TRANSITION',markov,53,'Markov chuyển trạng thái bậc 1')
    put('ANTI_RAW',_opp(fusion_pred) if raw_bad>=.55 else _opp(last),50+min(10,raw_bad*12),f'Wrong rate {raw_bad:.2f}')
    put('HIGH_ORDER_MARKOV',high,53,'Markov ngữ cảnh bậc 3')
    put('MARKOV_ORDER2',_markov_prediction(seq,2),53,'Markov ngữ cảnh bậc 2')
    put('RUN_HAZARD',_run_hazard_prediction(seq),52,'Xác suất tiếp/bẻ theo độ dài bệt')
    put('MULTI_WINDOW',_multi_window_prediction(seq),52,'Bỏ phiếu W8/W16/W32/W64')
    put('DECAYED_TRANSITION',_decayed_transition_prediction(seq,2),54,'Transition có trọng số recency')
    put('REGIME_ADAPTIVE',_regime_adaptive_prediction(seq),54,'Tự đổi logic theo alternating/run/bias regime')
    put('MOTIF_WEIGHTED',_motif_weighted_prediction(seq),53,'Motif 2–6 có trọng số độ dài + độ mới')
    put('FLIP_STATE_MARKOV',_flip_state_markov_prediction(seq),53,'Markov theo trạng thái flip/repeat')
    put('RUN_LENGTH_MARKOV',_run_length_markov_prediction(seq),54,'Markov theo side + độ dài bệt')
    put('PERIODIC_MATCH',_periodic_match_prediction(seq),52,'Chu kỳ 2–12 kiểm định trên lịch sử')
    put('DUAL_HORIZON',_dual_horizon_prediction(seq),53,'Nhịp W8 kết hợp xu hướng W28')
    put('ANALOG_KNN',_analog_knn_prediction(seq),54,'So khớp ngữ cảnh lịch sử gần giống')
    put('RUN_SURVIVAL',_run_survival_prediction(seq),53,'Tỷ lệ bệt cùng side sống tiếp/bẻ')
    put('CONTEXT_ENTROPY',_context_entropy_prediction(seq),54,'Context entropy thấp + đủ mẫu')
    put('BAYES_CONTEXT',_bayes_context_prediction(seq),55,'Bayes context 1–5 có smoothing + recency')
    put('HORIZON_CONSENSUS',_horizon_consensus_prediction(seq),54,'Đồng thuận đa cửa sổ W6/W10/W20/W40/W80')
    put('REGIME_SWITCH',_regime_switch_prediction(seq),55,'Tự chuyển logic theo bệt/đảo/bias regime')
    put('LAG_ENSEMBLE',_lag_ensemble_prediction(seq),54,'Ensemble chu kỳ lag 2–15 có kiểm định')
    ref=_reference_pattern_signal(seq) if _reference_allowed(board) else {'ready':False,'prediction':_bayes_context_prediction(seq),'score':0,'support':0,'length':0}
    ref_conf=50+min(14,abs(float(ref.get('score',0)))*20)+min(4,float(ref.get('support',0))/20)
    put('REFERENCE_PATTERN_PRIOR',ref.get('prediction'),ref_conf,f"Reference prior L{ref.get('length',0)} n={ref.get('support',0)} score={ref.get('score',0)}")
    put('JS_TREND_BLEND',_js_trend_blend_prediction(seq),54,'JS blend: trend ngắn/dài + mean-reversion + momentum')
    br=_js_break_signal(seq)
    put('JS_BREAK_CALIBRATOR',br.get('prediction'),52+abs(float(br.get('p_break',.5))-.5)*26,f"Break p={br.get('p_break',.5)} · run={br.get('run',0)} · n={br.get('support',0)}")
    rnd=_js_randomness_score(seq)
    put('JS_ULTRA_STACK',_js_ultra_stack_prediction(seq,_reference_allowed(board)),55-min(5,max(0,rnd-.65)*20),f"Adaptive stack · randomness {rnd:.2f}")
    put('VOM_CONTEXT_6',_vom_context6_prediction(seq),55,'Variable-order context 2–8 · long memory')
    put('LONG_MEMORY_BAYES',_long_memory_bayes_prediction(seq),55,'Bayes context học tối đa 5.000 phiên')
    put('RUN_PROFILE_LONG',_run_profile_long_prediction(seq),54,'Run profile dài · tiếp/bẻ theo lịch sử lớn')
    put('MULTISCALE_TRANSITION',_multiscale_transition_prediction(seq),55,'Transition đa khung W32→W10000')
    put('CHANGEPOINT_ADAPTIVE',_changepoint_adaptive_prediction(seq),55,'Phát hiện đổi regime rồi ưu tiên lịch sử gần')
    put('WILSON_CONTEXT',_wilson_context_prediction(seq),55,'Context 2–9 có Wilson lower-bound chống mẫu ảo')
    put('KNN_RECENCY',_knn_recency_prediction(seq),54,'KNN pattern 5–12 + similarity + recency')
    put('RUN_MATRIX',_run_matrix_prediction(seq),54,'Ma trận side × độ dài bệt với long-memory')
    put('ENTROPY_GATE',_entropy_gate_prediction(seq),55,'Entropy gate chọn bệt/đảo/bias/change-point')
    put('CROSS_HORIZON_BAYES',_cross_horizon_bayes_prediction(seq),56,'Bayes đa horizon W24→W3000')
    put('DIRICHLET_VOM',_dirichlet_vom_prediction(seq),56,'Dirichlet VOM bậc 1–10 + recency + evidence shrink')
    put('SEQUENTIAL_CHANGE',_sequential_change_prediction(seq),55,'Sequential drift filter tự rút ngắn memory khi đổi regime')
    put('MOTIF_SURVIVAL',_motif_survival_prediction(seq),55,'Motif 3–12 + Beta smoothing + survival/recency')
    put('REGIME_POSTERIOR',_regime_posterior_prediction(seq),56,'Posterior mixture bệt/đảo/bias/mixed')
    put('MULTIRESOLUTION_EDGE',_multiresolution_edge_prediction(seq),55,'Edge đa độ phân giải W8→W1024 có shrinkage')
    put('BAYES_RUN_MIXTURE',_bayes_run_mixture_prediction(seq),56,'Bayes run continuation/break + context mixture')
    put('CTW_APPROX',_ctw_approx_prediction(seq),57,'Context Tree Weighting gần đúng · order 1–12 · recency')
    put('HIERARCHICAL_BAYES',_hierarchical_bayes_prediction(seq),57,'Bayes phân cấp · context dài chỉ được tin khi đủ evidence')
    put('TRANSITION_DRIFT',_transition_drift_prediction(seq),56,'Transition short/long + drift detector')
    put('RUN_CONTEXT_JOINT',_run_context_joint_prediction(seq),56,'Joint state side × run × flip context')
    put('SPECTRAL_LAG',_spectral_lag_prediction(seq),56,'Lag 2–40 · shrinkage · kiểm tra chu kỳ')
    put('ROBUST_STACK',_robust_stack_prediction(seq),58,'Stack đa family · chống một họ model áp đảo')
    put('SUFFIX_CONTEXT',_suffix_prediction(seq),52,'Suffix/motif context 2–6')
    put('FUSION_CORE',fusion_pred,int(fusion.get('confidence',50)),'V31 fusion core độc lập')
    return preds

def _strategy_rows(board):
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute(
            '''SELECT session,strategy,prediction,actual,ok,settled_at
               FROM strategy_logs WHERE board=? AND actual IS NOT NULL AND ok IS NOT NULL
               ORDER BY settled_at DESC''',(board,)
        ).fetchall()
    by={}
    for session,name,pred,actual,ok,settled in rows:
        by.setdefault(name,[]).append({'session':session,'prediction':pred,'actual':actual,'ok':bool(ok),'settled_at':settled})
    return by

def _stats_for(rows,window):
    q=rows[:window]; n=len(q); wins=sum(1 for r in q if r['ok'])
    streak=0
    for r in q:
        if r['ok']: break
        streak+=1
    return {'total':n,'win':wins,'loss':n-wins,'win_rate':wins/n if n else .5,'loss_streak':streak}

def get_strategy_stats_map(board):
    by=_strategy_rows(board); out={}
    for name in STRATEGY_NAMES:
        rows=by.get(name,[])
        s20=_stats_for(rows,20); s50=_stats_for(rows,50); s100=_stats_for(rows,100)
        score=.50*s20['win_rate']+.35*s50['win_rate']+.15*s100['win_rate']
        if s20['total']<10: score-=.05
        if s50['total']<25: score-=.05
        if s20['loss_streak']>=3: score-=.07
        if s20['total']>=10 and s20['win_rate']<.45: score-=.10
        if s50['total']>=20 and s50['win_rate']<.48: score-=.05
        if s20['total']>=10 and s20['win_rate']>=.55: score+=.05
        if s50['total']>=20 and s50['win_rate']>=.53: score+=.05
        valid=((s20['total']>=10 or s50['total']>=25) and score>=.50)
        out[name]={'short':s20,'mid':s50,'long':s100,'performance_score':round(score,4),'valid':valid}
    return out

def _champion_locked(board,candidate,stats):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT champion FROM champion_state WHERE board=?',(board,)).fetchone()
    old=r[0] if r else None
    if old and old in stats and candidate in stats:
        os=stats[old]; ns=stats[candidate]; o20=os['short']
        keep=(o20['loss_streak']<3 and (o20['win_rate']>=.48 or o20['total']<10))
        if keep and ns['performance_score'] < os['performance_score']+.08:
            candidate=old
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO champion_state(board,champion,updated_at) VALUES(?,?,?)
               ON CONFLICT(board) DO UPDATE SET champion=excluded.champion,updated_at=excluded.updated_at''',
            (board,candidate,time.time())
        ); db.commit()
    return candidate

def select_champion(board,preds):
    stats=get_strategy_stats_map(board)
    valid=[n for n in STRATEGY_NAMES if n in preds and stats.get(n,{}).get('valid')]
    candidate=max(valid,key=lambda n:stats[n]['performance_score']) if valid else ('MARKOV_TRANSITION' if 'MARKOV_TRANSITION' in preds else 'REVERSE_LAST')
    candidate=_champion_locked(board,candidate,stats)
    if candidate not in preds: candidate='MARKOV_TRANSITION' if 'MARKOV_TRANSITION' in preds else next(iter(preds))
    return candidate,stats

def _final_performance(board):
    h=_recent_final_stats(board,50); q20=h[:20]; q50=h[:50]
    wr20=sum(1 for x in q20 if x['ok'])/len(q20) if q20 else .5
    wr50=sum(1 for x in q50 if x['ok'])/len(q50) if q50 else .5
    streak=0
    for x in h:
        if x['ok']: break
        streak+=1
    reverse=(sum(1 for x in q20 if not x['ok'])/len(q20)) if q20 else .5
    return {'n20':len(q20),'n50':len(q50),'wr20':wr20,'wr50':wr50,'loss_streak':streak,
            'normal_win_rate':wr20,'reverse_win_rate':reverse}

def _champion_confidence(st,recovery=False,recent=None):
    s20=st.get('short',{}); s50=st.get('mid',{})
    w20=float(s20.get('win_rate',.5)); w50=float(s50.get('win_rate',.5))
    conf=50+max(0,w20-.50)*100+max(0,w50-.50)*50
    if s20.get('total',0)<10: conf-=5
    if s20.get('loss_streak',0)>=3: conf-=8
    if s20.get('total',0)>=10 and w20<.48: conf-=8
    if s50.get('total',0)>=20 and w50<.50: conf-=5
    conf=_clamp(conf,45,75)
    if conf<55: display=min(58,conf+3)
    elif conf<65: display=min(68,conf+5)
    else: display=min(78,conf+6)
    if recent and recent.get('n20',0)>=20 and recent.get('wr20',.5)<.45: display=min(58,conf+2)
    if recovery: display=min(display,58)
    return int(round(conf)),int(round(display))

def _status_from_conf(model_conf,st):
    s20=st.get('short',{})
    if model_conf>=65 and s20.get('total',0)>=10 and s20.get('win_rate',.5)>=.55: status='MẠNH'
    elif model_conf>=58: status='TRUNG BÌNH'
    elif model_conf>=50: status='YẾU'
    else: status='NGUY HIỂM'
    if s20.get('total',0)<10 and status in ('MẠNH','TRUNG BÌNH'): status='YẾU'
    return status

def model_snapshot(rows, game=None, board=None):
    fusion=fusion_model_snapshot(rows,game,board)
    if not board:return fusion
    seq=_seq(rows)
    if len(seq)<2:
        fusion['engine']='BOARD-META 55 + ELITE ADAPTIVE V49';return fusion
    preds=_strategy_predictors(board,rows,fusion)
    live=get_strategy_stats_map(board);wf=walk_forward_strategy_stats(board,rows);prof=_profile_for(board)
    champion,_=select_champion(board,preds);recent=_final_performance(board)
    if prof.get('mode')=='champion':
        raw=preds[champion]['prediction'];decision=champion;decision_mode='CHAMPION'
        top=[{'name':champion,'prediction':raw,'quality':_combined_quality(board,champion,live,wf)}];agree=1.0
    else:
        raw,top,agree=_meta_consensus(board,preds,live,wf,seq);decision='META['+', '.join(x['name'] for x in top[:3])+']';decision_mode='BOARD_META'

    sicbo_hash=None;sicbo_dice=None
    if board=='sunwin:sicbo':
        sicbo_hash=sicbo_hash_signal(rows)
        sicbo_dice=sicbo_dice_side(rows)
        if sicbo_hash.get('usable'):
            hpred=sicbo_hash.get('prediction');hq=float(sicbo_hash.get('quality',.5))
            top.append({'name':'SICBO_HASH_CAL','prediction':hpred,'quality':hq})
            if hpred==raw:
                agree=_clamp(agree+min(.07,max(0,hq-.5)*.8),.5,.95)
            elif agree<.585 and hq>=.545:
                raw=hpred;decision='SICBO_HASH_TIEBREAK';decision_mode='SICBO_META';agree=max(.56,hq)
            else:
                agree=max(.50,agree-.025)
        if sicbo_dice.get('ready'):
            dpred=sicbo_dice.get('prediction');dq=float(sicbo_dice.get('quality',.5))
            top.append({'name':'SICBO_DICE_META','prediction':dpred,'quality':dq})
            if dpred==raw:
                agree=_clamp(agree+min(.05,max(0,dq-.5)*.7),.5,.95)
            elif agree<.555 and dq>=.535:
                raw=dpred;decision='SICBO_DICE_TIEBREAK';decision_mode='SICBO_META';agree=max(.54,dq)

    rmin=int(prof.get('reverse_min',34));gap=float(prof.get('reverse_gap',.20))
    reverse_mode=(recent['n20']>=rmin and recent['normal_win_rate']<.43 and
                  recent['reverse_win_rate']-recent['normal_win_rate']>=gap)
    recovery=((recent['n50']>=40 and recent['wr50']<.44) or
              (recent['n20']>=20 and recent['wr20']<.38) or recent['loss_streak']>=5)
    final=_opp(raw) if reverse_mode else raw
    qvals=[float(x.get('quality',.5)) for x in top[:4]] or [.5];q=sum(qvals)/len(qvals)
    conf=50+max(0,q-.5)*88+max(0,agree-.5)*15
    if recent['n20']>=12:conf+=(recent['wr20']-.5)*18
    if recovery:conf-=7
    if reverse_mode:conf-=3
    randomness=_js_randomness_score(seq)
    randomness_penalty=max(0.0,(randomness-.66)*15.0)
    conf-=randomness_penalty
    structure=_structure_score(seq)
    # Multiple-expert safeguard: on data that looks statistically close to IID,
    # never turn a lucky recent walk-forward streak into a very strong display.
    if len(seq)>=120:
        if structure<.16:conf=min(conf,54)
        elif structure<.24:conf=min(conf,58)
        elif structure<.32:conf=min(conf,63)
    top_quality=max((float(x.get('quality',.5)) for x in top),default=.5)
    # V48 calibration: a hot strategy cannot display a strong signal unless its
    # causal walk-forward lower bound and current-regime sample also support it.
    wf_lowers=[];reg_rates=[];reg_ns=[]
    for x in top[:4]:
        ws=wf.get(x.get('name'),{});w20=ws.get('short',{});rs=ws.get('regime',{})
        if int(w20.get('total',0))>=12:wf_lowers.append(float(w20.get('lower',0)))
        if int(rs.get('total',0))>=8:
            reg_rates.append(float(rs.get('win_rate',.5)));reg_ns.append(int(rs.get('total',0)))
    wf_floor=(sum(wf_lowers)/len(wf_lowers)) if wf_lowers else .44
    reg_q=(sum(reg_rates)/len(reg_rates)) if reg_rates else .5
    if wf_lowers and wf_floor<.43:conf=min(conf,55)
    elif wf_lowers and wf_floor<.47:conf=min(conf,60)
    if reg_rates and reg_q<.47:conf-=3
    elif reg_rates and reg_q>.54 and agree>=.61:conf+=1.5
    if agree<.56:conf=min(conf,54)
    if randomness>=.76 and agree<.64:conf=min(conf,55)
    if top_quality<.525:conf=min(conf,55)
    elif top_quality<.545:conf=min(conf,59)
    calibration=_confidence_bucket_calibration(board,conf)
    conf=min(conf,float(calibration.get('cap',72)))+float(calibration.get('bonus',0))
    conf=_clamp(conf,45,72);display=int(round(_clamp(conf+3,48,74)))
    anti=_anti_phase(board)
    mode='RECOVERY + REVERSE' if recovery and reverse_mode else 'RECOVERY' if recovery else 'REVERSE' if reverse_mode else decision_mode
    rank=[]
    for n in STRATEGY_NAMES:
        if n not in preds:continue
        rank.append({'name':n,'quality':round(_combined_quality(board,n,live,wf),4),
                     'wf20':round(float(wf.get(n,{}).get('short',{}).get('raw_win_rate',.5)),3),
                     'wfN':int(wf.get(n,{}).get('short',{}).get('total',0)),
                     'live20':round(float(live.get(n,{}).get('short',{}).get('win_rate',.5)),3),
                     'liveN':int(live.get(n,{}).get('short',{}).get('total',0)),
                     'prediction':preds[n]['prediction']})
    rank.sort(key=lambda x:x['quality'],reverse=True)
    if conf>=63 and agree>=.62:status='MẠNH'
    elif conf>=56:status='TRUNG BÌNH'
    elif conf>=49:status='YẾU'
    else:status='NGUY HIỂM'
    if max((x.get('wfN',0) for x in rank),default=0)<18 and status in ('MẠNH','TRUNG BÌNH'):status='YẾU'
    fusion.update({'prediction':final,'raw_prediction':raw,'confidence':display,'percent':display,
      'model_confidence':round(conf,2),'strategy_champion':decision,'champion_raw':champion,
      'strategy_predictions':{k:v['prediction'] for k,v in preds.items()},
      'strategy_reasons':{k:v['reason'] for k,v in preds.items()},'strategy_rankings':rank,
      'meta_top':top[:4],'meta_agreement':round(agree,4),'decision_mode':decision_mode,
      'board_profile':prof.get('mode','consensus'),'recovery_mode':bool(recovery),'reverse_mode':bool(reverse_mode),'mode':mode,
      'anti_tai':anti['anti_tai'],'anti_xiu':anti['anti_xiu'],'normal_win_rate':round(recent['normal_win_rate'],4),
      'reverse_win_rate':round(recent['reverse_win_rate'],4),'final_loss_streak':recent['loss_streak'],'status':status,
      'sicbo_hash':sicbo_hash if board=='sunwin:sicbo' else None,
      'sicbo_dice_meta':sicbo_dice if board=='sunwin:sicbo' else None,
      'randomness_score':round(randomness,4),'randomness_penalty':round(randomness_penalty,3),
      'structure_score':round(structure,4),'regime_label':_regime_label(seq),
      'wf_floor':round(wf_floor,4),'regime_quality':round(reg_q,4),
      'signal_calibration':calibration,
      'reference_pattern':_reference_pattern_signal(seq) if _reference_allowed(board) else None,
      'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51','totalStrategies':len(STRATEGY_NAMES),'updated_at':time.time()})
    return fusion


def log_strategy_predictions(board,session,model):
    preds=(model or {}).get('strategy_predictions') or {}
    if not preds: return
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        for name,pred in preds.items():
            if name not in STRATEGY_NAMES or pred not in ('TÀI','XỈU'): continue
            db.execute(
                '''INSERT INTO strategy_logs(board,session,strategy,prediction,created_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(board,session,strategy) DO UPDATE SET prediction=excluded.prediction''',
                (board,str(session),name,pred,now)
            )
        db.commit()

def settle_strategy_predictions(board,session,actual):
    if actual not in ('TÀI','XỈU'): return
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute('SELECT strategy,prediction FROM strategy_logs WHERE board=? AND session=?',(board,str(session))).fetchall()
        for name,pred in rows:
            db.execute(
                '''UPDATE strategy_logs SET actual=?,ok=?,settled_at=?
                   WHERE board=? AND session=? AND strategy=?''',
                (actual,1 if pred==actual else 0,now,board,str(session),name)
            )
        db.commit()



async def store_rows(board, rows):
    if not rows: return
    async with _db_lock:
        with sqlite3.connect(DB_PATH) as db:
            now=time.time()
            for r in rows:
                d=(r.get('dice') or [])+[None,None,None]
                db.execute('''INSERT INTO rounds(board,session,result,d1,d2,d3,total,md5,seen_at,meta_json)
                              VALUES(?,?,?,?,?,?,?,?,?,?)
                              ON CONFLICT(board,session) DO UPDATE SET
                              result=excluded.result,d1=COALESCE(excluded.d1,rounds.d1),
                              d2=COALESCE(excluded.d2,rounds.d2),d3=COALESCE(excluded.d3,rounds.d3),
                              total=COALESCE(excluded.total,rounds.total),md5=COALESCE(excluded.md5,rounds.md5),
                              meta_json=COALESCE(excluded.meta_json,rounds.meta_json),seen_at=excluded.seen_at''',
                           (board,str(r['id']),r['result'],d[0],d[1],d[2],r.get('sum'),r.get('md5'),now,
                            json.dumps(r.get('meta'),ensure_ascii=False) if r.get('meta') else None))
            # bound rows per board
            db.execute('''DELETE FROM rounds WHERE board=? AND rowid NOT IN
                          (SELECT rowid FROM rounds WHERE board=? ORDER BY seen_at DESC LIMIT ?)''',(board,board,MAX_HISTORY))
            db.commit()


def load_rows(board, limit=500):
    with sqlite3.connect(DB_PATH) as db:
        rows=[]
        for s,r,d1,d2,d3,total,md5,seen,mj in db.execute('SELECT session,result,d1,d2,d3,total,md5,seen_at,meta_json FROM rounds WHERE board=?',(board,)):
            try: meta=json.loads(mj) if mj else {}
            except: meta={}
            rid=canonical_session(s,s)
            rows.append({'id':rid,'result':r,'dice':[x for x in (d1,d2,d3) if x is not None],
                         'sum':total,'md5':md5,'seen_at':seen,'meta':meta})
        return _stable_rows(rows)[-limit:]


def get_prediction_history(board, limit=20):
    with sqlite3.connect(DB_PATH) as db:
        cur=db.execute('''SELECT p.session,p.prediction,p.confidence,p.score,p.created_at,p.actual,p.ok,p.settled_at,p.model_json,
                                 r.d1,r.d2,r.d3,r.total,r.md5
                          FROM shared_predictions p
                          LEFT JOIN rounds r ON r.board=p.board AND r.session=p.session
                          WHERE p.board=? ORDER BY p.created_at DESC LIMIT ?''',(board,limit))
        out=[]
        for session,pred,conf,score,created,actual,ok,settled,mj,d1,d2,d3,total,md5 in cur.fetchall():
            try: model=json.loads(mj) if mj else {}
            except: model={}
            dice=[x for x in (d1,d2,d3) if x is not None]
            out.append({'session':session,'prediction':pred,'confidence':conf,'score':score,'created_at':created,
                        'actual':actual,'ok':None if ok is None else bool(ok),'settled_at':settled,'model':model,
                        'dice':dice,'sum':total,'md5':md5})
        return out


def get_shared_prediction(board):
    hist=get_prediction_history(board,5)
    for x in hist:
        if x['actual'] is None: return x
    return hist[0] if hist else None


def _settle_predictions(board, rows):
    actual={str(r['id']):r['result'] for r in rows if r.get('result') in ('TÀI','XỈU')}
    if not actual: return []
    settled=[]
    with sqlite3.connect(DB_PATH) as db:
        pend=db.execute('SELECT session,prediction FROM shared_predictions WHERE board=? AND actual IS NULL',(board,)).fetchall()
        now=time.time()
        for session,pred in pend:
            if str(session) not in actual: continue
            act=actual[str(session)]
            ok=1 if pred==act else 0
            db.execute('UPDATE shared_predictions SET actual=?,ok=?,settled_at=? WHERE board=? AND session=?',
                       (act,ok,now,board,str(session)))
            settled.append({'session':str(session),'prediction':pred,'actual':act,'ok':bool(ok)})
        db.commit()
    for item in settled:
        settle_strategy_predictions(board,item['session'],item['actual'])
    return settled



def _current_anchor(current_rows):
    nums=[_session_num(r.get('id')) for r in (current_rows or [])
          if r.get('result') in ('TÀI','XỈU') and _session_num(r.get('id')) is not None]
    return str(max(nums)) if nums else None

def _drop_stale_pending(board,target_session):
    # V33: keep unresolved predictions. History APIs can lag behind current APIs;
    # deleting them caused SUNWIN history to vanish instead of settling later.
    return


def _create_shared_prediction(board, rows, current_session=None):
    if not rows: return None,False
    game=board.split(':',1)[0]
    model=model_snapshot(rows,game,board)
    if board=='sunwin:sicbo':
        model['dice_forecast']=dice_position_forecast(rows)
    if board=='lc79:xocdia' and model.get('prediction'):
        model['xocdia_forecast']=xocdia_detail_forecast(rows,model['prediction'])
    if not model.get('prediction'): return None,False
    latest=str(current_session) if current_session is not None else None
    session=str(_session_num(latest)+1) if latest is not None and _session_num(latest) is not None else _next_session(rows)
    if not session: return None,False
    if latest is None: latest=str(int(session)-1)
    _drop_stale_pending(board,session)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO board_cursor(board,last_completed_session,updated_at) VALUES(?,?,?)
                      ON CONFLICT(board) DO UPDATE SET last_completed_session=excluded.last_completed_session,
                      updated_at=excluded.updated_at''',(board,latest,time.time())); db.commit()
    with sqlite3.connect(DB_PATH) as db:
        old=db.execute('SELECT prediction,confidence,score,model_json,created_at,actual,ok,settled_at FROM shared_predictions WHERE board=? AND session=?',(board,session)).fetchone()
        if old:
            try: mj=json.loads(old[3]) if old[3] else {}
            except: mj={}
            return {'session':session,'prediction':old[0],'confidence':old[1],'score':old[2],'model':mj,'created_at':old[4],
                    'actual':old[5],'ok':None if old[6] is None else bool(old[6]),'settled_at':old[7]},False
        now=time.time()
        db.execute('''INSERT INTO shared_predictions(board,session,prediction,confidence,score,model_json,created_at)
                      VALUES(?,?,?,?,?,?,?)''',(board,session,model['prediction'],model['confidence'],model['score'],json.dumps(model,ensure_ascii=False),now))
        db.commit()
    log_strategy_predictions(board,session,model)
    return {'session':session,'prediction':model['prediction'],'confidence':model['confidence'],'score':model['score'],
            'model':model,'created_at':now,'actual':None,'ok':None,'settled_at':None},True


async def refresh_shared_prediction(board,current_session=None):
    rows=load_rows(board,MAX_HISTORY)
    settled=_settle_predictions(board,rows)
    if settled and BOT_TOKEN:
        for item in settled:
            asyncio.create_task(bot_clear_settled_prediction(board,item['session']))
    pred,created=_create_shared_prediction(board,rows,current_session)
    if created and BOT_TOKEN:
        asyncio.create_task(bot_notify_prediction(board,pred))
    return pred


async def set_state(board, ok, error=None):
    rows=load_rows(board,500)
    model=model_snapshot(rows, board.split(':',1)[0], board)
    async with _db_lock:
        with sqlite3.connect(DB_PATH) as db:
            db.execute('''INSERT INTO board_state(board,updated_at,source_ok,last_error,model_json)
                          VALUES(?,?,?,?,?) ON CONFLICT(board) DO UPDATE SET updated_at=excluded.updated_at,
                          source_ok=excluded.source_ok,last_error=excluded.last_error,model_json=excluded.model_json''',
                       (board,time.time(),1 if ok else 0,error,json.dumps(model,ensure_ascii=False)))
            db.commit()


def effective_cfg(board, cfg):
    out=dict(cfg)
    try:
        with sqlite3.connect(DB_PATH) as db:
            r=db.execute('SELECT current_url,history_url FROM api_overrides WHERE board=?',(board,)).fetchone()
        if r:
            if r[0]: out['current']=r[0]
            if r[1]: out['history']=r[1]
    except Exception:
        pass
    return out


def set_api_override(board,current_url=None,history_url=None):
    if board not in BOARDS: return False
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO api_overrides(board,current_url,history_url,updated_at) VALUES(?,?,?,?)
                      ON CONFLICT(board) DO UPDATE SET current_url=excluded.current_url,
                      history_url=excluded.history_url,updated_at=excluded.updated_at''',
                   (board,current_url or None,history_url or None,time.time()))
        db.commit()
    return True


def reset_api_override(board):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('DELETE FROM api_overrides WHERE board=?',(board,));db.commit()


async def fetch_first(client, urls):
    last=None
    for url in [u for u in urls if u]:
        try:
            return await fetch_json(client,url),url
        except Exception as e:
            last=e
    if last: raise last
    raise RuntimeError('không có URL API')

async def fetch_json(client,url):
    r=await client.get(url,headers={'Accept':'application/json','Cache-Control':'no-cache'},timeout=2.0)
    r.raise_for_status(); return r.json()


async def poll_board(client, board, cfg):
    cfg=effective_cfg(board,cfg)
    try:
        if cfg['kind']=='baccarat':
            data,_=await fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
            tables=parse_baccarat(data)
            for table,rows in tables.items():
                b=f'baccarat:{table}'
                await store_rows(b,rows)
                _settle_predictions(b,rows)
                anchor=_current_anchor(rows)
                await set_state(b,True)
                await refresh_shared_prediction(b,anchor)
            await set_state(board,True)
            return

        if cfg['kind']=='sicbo_pair':
            current_task=fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
            history_task=fetch_json(client,cfg['history']) if cfg.get('history') else asyncio.sleep(0,result=None)
            current_pack,history_raw=await asyncio.gather(current_task,history_task,return_exceptions=True)
            current_raw=current_pack[0] if not isinstance(current_pack,Exception) else current_pack
            current_rows=[] if isinstance(current_raw,Exception) else parse_sicbo(current_raw)
            history_rows=[] if isinstance(history_raw,Exception) or history_raw is None else parse_sicbo(history_raw)
            anchor=_current_anchor(current_rows)
            if anchor is None:
                raise current_pack if isinstance(current_pack,Exception) else RuntimeError('Sicbo current API không có phiên hợp lệ')
            live_rows=history_rows+current_rows
            await store_rows(board,live_rows)
            _settle_predictions(board,live_rows)
            await set_state(board,True)
            await refresh_shared_prediction(board,anchor)
            return

        if cfg['kind']=='tx_pair':
            current_task=fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
            history_task=fetch_json(client,cfg['history']) if cfg.get('history') else asyncio.sleep(0,result=None)
            current_pack,history_raw=await asyncio.gather(current_task,history_task,return_exceptions=True)
            current_raw=current_pack[0] if not isinstance(current_pack,Exception) else current_pack
            current_rows=[] if isinstance(current_raw,Exception) else parse_tx(current_raw)
            history_rows=[] if isinstance(history_raw,Exception) or history_raw is None else parse_tx(history_raw)
            anchor=_current_anchor(current_rows)
            if anchor is None:
                raise current_pack if isinstance(current_pack,Exception) else RuntimeError('current API không có phiên hợp lệ')
            # History learns; current locks the live session. Never let stale history set the clock.
            live_rows=history_rows+current_rows
            await store_rows(board,live_rows)
            _settle_predictions(board,live_rows)
            await set_state(board,True)
            await refresh_shared_prediction(board,anchor)
            return

        data,_=await fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
        rows=(parse_xocdia(data) if cfg['kind']=='xocdia'
              else parse_sicbo(data) if cfg['kind']=='sicbo'
              else parse_tx(data))
        anchor=_current_anchor(rows)
        if not rows or anchor is None: raise RuntimeError('current API không có phiên hợp lệ')
        await store_rows(board,rows)
        _settle_predictions(board,rows)
        await set_state(board,True)
        await refresh_shared_prediction(board,anchor)
    except Exception as e:
        await set_state(board,False,str(e)[:240])


async def worker_loop():
    global _last_cycle
    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            start=time.time()
            targets=[(b,c) for b,c in BOARDS.items() if game_operational(c.get('game'))]
            await asyncio.gather(*(poll_board(client,b,c) for b,c in targets), return_exceptions=True)
            _last_cycle=time.time()
            await asyncio.sleep(max(0.2,POLL_SECONDS-(time.time()-start)))




def board_label(board):
    labels={
      'sunwin:hu':'SUNWIN HŨ','sunwin:sicbo':'SUNWIN SICBO',
      'lc79:hu':'LC79 HŨ','lc79:md5':'LC79 MD5','lc79:xocdia':'LC79 XÓC ĐĨA',
      'betvip:hu':'BETVIP HŨ','betvip:md5':'BETVIP MD5',
      'gb68:hu':'68GB HŨ','gb68:md5':'68GB MD5',
      'b52:hu':'B52 HŨ','b52:md5':'B52 MD5',
      'max789:hu':'MAX789 HŨ','max789:md5':'MAX789 MD5',
      'son789:hu':'SON789 HŨ','son789:md5':'SON789 MD5',
      'hitclub:hu':'HITCLUB HŨ','hitclub:md5':'HITCLUB MD5'
    }
    if board.startswith('baccarat:') and board!='baccarat:main': return 'BACCARAT · '+board.split(':',1)[1]
    return labels.get(board,board.upper())


def display_pred(board,p):
    if not p: return '---'
    if board.startswith('baccarat:'):
        return 'PLAYER' if p=='TÀI' else 'BANKER' if p=='XỈU' else p
    if board=='lc79:xocdia':
        return 'CHẴN' if p=='TÀI' else 'LẺ' if p=='XỈU' else p
    return p


def available_bot_boards():
    return list(BOARDS.keys())


def is_group_chat_id(chat_id):
    try:return int(chat_id)<0
    except:return False

def get_group_settings(chat_id):
    defaults={'enabled':False,'auto_delete':True,'delete_after':5.0,'anti_spam':True,
              'spam_limit':5,'spam_window':6.0,'mute_seconds':60,'locked':False,
              'warn_limit':3,'title':None,'enabled_by':None,'updated_at':None}
    if not is_group_chat_id(chat_id):
        return {**defaults,'auto_delete':False,'anti_spam':False}
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute(
            '''SELECT title,enabled,auto_delete,delete_after,anti_spam,spam_limit,spam_window,
                      mute_seconds,locked,warn_limit,enabled_by,updated_at
               FROM bot_group_settings WHERE chat_id=?''',(int(chat_id),)
        ).fetchone()
    if not r:return defaults
    return {'title':r[0],'enabled':bool(r[1]),'auto_delete':bool(r[2]),'delete_after':float(r[3] or 5),
            'anti_spam':bool(r[4]),'spam_limit':int(r[5] or 5),'spam_window':float(r[6] or 6),
            'mute_seconds':int(r[7] or 60),'locked':bool(r[8]),'warn_limit':int(r[9] or 3),
            'enabled_by':r[10],'updated_at':r[11]}

def group_enabled(chat_id):
    return bool(get_group_settings(chat_id).get('enabled'))

def _ensure_group_row(chat_id,admin_id=None,title=None):
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO bot_group_settings(chat_id,title,enabled,enabled_by,updated_at)
               VALUES(?,?,0,?,?) ON CONFLICT(chat_id) DO UPDATE SET
               title=COALESCE(excluded.title,bot_group_settings.title),
               enabled_by=COALESCE(excluded.enabled_by,bot_group_settings.enabled_by),
               updated_at=excluded.updated_at''',
            (int(chat_id),title,int(admin_id) if admin_id is not None else None,now))
        db.commit()

def set_group_enabled(chat_id,enabled,admin_id=None,title=None):
    if not is_group_chat_id(chat_id):raise ValueError('group only')
    _ensure_group_row(chat_id,admin_id,title);now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('UPDATE bot_group_settings SET enabled=?,enabled_by=COALESCE(?,enabled_by),updated_at=? WHERE chat_id=?',
                   (1 if enabled else 0,int(admin_id) if admin_id is not None else None,now,int(chat_id)))
        if not enabled:db.execute('UPDATE bot_subscriptions SET enabled=0 WHERE chat_id=?',(int(chat_id),))
        db.commit()

def set_group_delete(chat_id,enabled,delay=5.0,admin_id=None,title=None):
    if not is_group_chat_id(chat_id):raise ValueError('group only')
    _ensure_group_row(chat_id,admin_id,title);delay=max(1,min(3600,float(delay)));now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('UPDATE bot_group_settings SET auto_delete=?,delete_after=?,updated_at=? WHERE chat_id=?',
                   (1 if enabled else 0,delay,now,int(chat_id)));db.commit()

def set_group_antispam(chat_id,enabled,admin_id=None,title=None):
    _ensure_group_row(chat_id,admin_id,title)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('UPDATE bot_group_settings SET anti_spam=?,updated_at=? WHERE chat_id=?',
                   (1 if enabled else 0,time.time(),int(chat_id)));db.commit()

def set_group_spam_limits(chat_id,limit,window,mute_seconds,admin_id=None,title=None):
    _ensure_group_row(chat_id,admin_id,title)
    limit=max(3,min(20,int(limit)));window=max(2,min(60,float(window)));mute_seconds=max(30,min(86400,int(mute_seconds)))
    with sqlite3.connect(DB_PATH) as db:
        db.execute('UPDATE bot_group_settings SET spam_limit=?,spam_window=?,mute_seconds=?,updated_at=? WHERE chat_id=?',
                   (limit,window,mute_seconds,time.time(),int(chat_id)));db.commit()

def set_group_locked(chat_id,locked,admin_id=None,title=None):
    _ensure_group_row(chat_id,admin_id,title)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('UPDATE bot_group_settings SET locked=?,updated_at=? WHERE chat_id=?',
                   (1 if locked else 0,time.time(),int(chat_id)));db.commit()

def record_group_message(msg):
    chat_id=(msg.get('chat') or {}).get('id');mid=msg.get('message_id');uid=(msg.get('from') or {}).get('id')
    if not is_group_chat_id(chat_id) or not mid:return
    text=(msg.get('text') or '')
    with sqlite3.connect(DB_PATH) as db:
        db.execute('INSERT OR REPLACE INTO bot_group_messages(chat_id,message_id,user_id,created_at,is_command) VALUES(?,?,?,?,?)',
                   (int(chat_id),int(mid),int(uid) if uid is not None else None,time.time(),1 if text.startswith('/') else 0))
        db.execute('DELETE FROM bot_group_messages WHERE chat_id=? AND message_id NOT IN (SELECT message_id FROM bot_group_messages WHERE chat_id=? ORDER BY created_at DESC LIMIT 1200)',
                   (int(chat_id),int(chat_id)))
        db.commit()

def recent_group_message_ids(chat_id,limit=100,user_id=None):
    limit=max(1,min(500,int(limit)))
    with sqlite3.connect(DB_PATH) as db:
        if user_id is None:
            rows=db.execute('SELECT message_id FROM bot_group_messages WHERE chat_id=? ORDER BY created_at DESC LIMIT ?',
                            (int(chat_id),limit)).fetchall()
        else:
            rows=db.execute('SELECT message_id FROM bot_group_messages WHERE chat_id=? AND user_id=? ORDER BY created_at DESC LIMIT ?',
                            (int(chat_id),int(user_id),limit)).fetchall()
    return [int(r[0]) for r in rows]

def add_group_warning(chat_id,user_id):
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_group_warnings(chat_id,user_id,warning_count,updated_at) VALUES(?,?,1,?)
                      ON CONFLICT(chat_id,user_id) DO UPDATE SET warning_count=warning_count+1,updated_at=excluded.updated_at''',
                   (int(chat_id),int(user_id),now))
        r=db.execute('SELECT warning_count FROM bot_group_warnings WHERE chat_id=? AND user_id=?',(int(chat_id),int(user_id))).fetchone()
        db.commit()
    return int(r[0]) if r else 1

def reset_group_warning(chat_id,user_id):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('DELETE FROM bot_group_warnings WHERE chat_id=? AND user_id=?',(int(chat_id),int(user_id)));db.commit()

def group_status_text(chat_id):
    g=get_group_settings(chat_id);title=html.escape(str(g.get('title') or 'Telegram Group'))
    return (f"<b>🛡 QUẢN TRỊ NHÓM</b>\n"
            f"<i>{title}</i>\n{BOT_DIV}\n"
            f"🤖 Bot  <b>{'🟢 BẬT' if g.get('enabled') else '🔴 TẮT'}</b>\n"
            f"🛡 Chống spam  <b>{'ON' if g.get('anti_spam') else 'OFF'}</b>  •  {g.get('spam_limit',5)} tin/{int(g.get('spam_window',6))}s\n"
            f"🔇 Auto mute  <b>{int(g.get('mute_seconds',60))}s</b>\n"
            f"🧹 Auto-delete  <b>{'ON' if g.get('auto_delete') else 'OFF'}</b>  •  {int(g.get('delete_after',5))}s\n"
            f"🔒 Khóa chat  <b>{'ON' if g.get('locked') else 'OFF'}</b>\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>ID nhóm: {chat_id} • /lenhnhom để xem lệnh.</i>")

def group_help_text():
    return (f"<b>🛡 {BOT_NAME} · QUẢN TRỊ NHÓM</b>\n{BOT_DIV}\n"
            "<b>Thiết lập</b>\n"
            "/batnhom · bật bot trong nhóm\n/tatnhom · tắt bot\n/trangthainhom · xem trạng thái\n/lenhnhom · xem lệnh\n\n"
            "<b>Chống spam & dọn chat</b>\n"
            "/chongspam on|off\n/gioihanspam 5 6 60\n"
            "/tuxoatin 5 · tự xóa sau 5 giây\n/tuxoatin off\n"
            "/dontin 100 · dọn tin đã ghi nhận\n/donall · tối đa 500 tin\n\n"
            "<b>Khóa & thành viên</b>\n"
            "/khoanhom · chỉ admin được gửi\n/monhom · mở chat\n"
            "/canhbao · reply user\n/tatnhan 10 · reply, khóa 10 phút\n/monhan · reply\n"
            "/duoi · reply\n/cam · reply\n/bocam USER_ID\n\n"
            "<i>Bot cần quyền Admin: Xóa tin + Hạn chế/Cấm thành viên. Lệnh cũ vẫn được giữ làm alias ẩn.</i>")

def list_groups_text():
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute('SELECT chat_id,title,enabled,auto_delete,delete_after,anti_spam,locked,updated_at FROM bot_group_settings ORDER BY updated_at DESC LIMIT 100').fetchall()
    lines=[f'<b>🛡 {BOT_NAME} • DANH SÁCH NHÓM</b>',BOT_DIV]
    for cid,title,en,ad,delay,asp,locked,updated in rows:
        lines += [f"{'🟢' if en else '🔴'} <b>{html.escape(str(title or 'Không tên'))}</b>",
                  f"<i>ID {cid} • Spam {'ON' if asp else 'OFF'} • Xóa {'ON' if ad else 'OFF'} {int(delay or 5)}s • Khóa {'ON' if locked else 'OFF'}</i>",'']
    if len(lines)==2:lines.append('<i>Chưa có nhóm nào được cấu hình.</i>')
    return '\n'.join(lines).strip()[:4000]

def set_selected_board(chat_id,board):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('INSERT INTO bot_chat_state(chat_id,selected_board) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET selected_board=excluded.selected_board',(chat_id,board));db.commit()


def get_selected_board(chat_id):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT selected_board FROM bot_chat_state WHERE chat_id=?',(chat_id,)).fetchone()
    return r[0] if r else None


def set_sub(chat_id,board,enabled):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('INSERT INTO bot_subscriptions(chat_id,board,enabled) VALUES(?,?,?) ON CONFLICT(chat_id,board) DO UPDATE SET enabled=excluded.enabled',(chat_id,board,1 if enabled else 0));db.commit()


def sub_enabled(chat_id,board):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT enabled FROM bot_subscriptions WHERE chat_id=? AND board=?',(chat_id,board)).fetchone()
    return bool(r and r[0])


def all_subscribers(board):
    with sqlite3.connect(DB_PATH) as db:
        ids=[r[0] for r in db.execute('SELECT chat_id FROM bot_subscriptions WHERE board=? AND enabled=1',(board,))]
    return [x for x in ids if has_access(x) and feature_allowed(x,'auto')]



def _money(v):
    try:return f"{int(v):,}".replace(',','.')+'đ'
    except:return '0đ'

def register_bot_user(msg):
    if not isinstance(msg,dict):return False
    chat=msg.get('chat') or {}; u=msg.get('from') or {}
    chat_id=chat.get('id')
    if not chat_id or is_group_chat_id(chat_id):return False
    now=time.time();text=str(msg.get('text') or '').strip();action=(text.split(maxsplit=1)[0] if text else 'message')[:80]
    is_start=1 if action.split('@',1)[0].lower()=='/start' else 0
    with sqlite3.connect(DB_PATH) as db:
        existed=bool(db.execute('SELECT 1 FROM bot_users WHERE chat_id=?',(int(chat_id),)).fetchone())
        db.execute('''INSERT INTO bot_users(chat_id,username,first_name,last_name,balance,created_at,updated_at,last_seen,last_action,action_count,start_count,callback_count,last_chat_type)
                      VALUES(?,?,?,?,0,?,?,?,?,1,?,0,?)
                      ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,
                      first_name=excluded.first_name,last_name=excluded.last_name,updated_at=excluded.updated_at,
                      last_seen=excluded.last_seen,last_action=excluded.last_action,
                      action_count=bot_users.action_count+1,start_count=bot_users.start_count+excluded.start_count,
                      last_chat_type=excluded.last_chat_type''',
                   (int(chat_id),u.get('username') or '',u.get('first_name') or '',u.get('last_name') or '',now,now,now,action,is_start,chat.get('type') or 'private'))
        db.execute('INSERT INTO bot_user_events(chat_id,event_type,action,created_at) VALUES(?,?,?,?)',
                   (int(chat_id),'message',action,now))
        db.commit()
    return not existed

def register_callback_user(q):
    if not isinstance(q,dict):return
    msg=q.get('message') or {};chat=msg.get('chat') or {};u=q.get('from') or {};chat_id=chat.get('id')
    if not chat_id or is_group_chat_id(chat_id):return
    now=time.time();action=('BTN:'+str(q.get('data') or ''))[:80]
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_users(chat_id,username,first_name,last_name,balance,created_at,updated_at,last_seen,last_action,action_count,start_count,callback_count,last_chat_type)
                      VALUES(?,?,?,?,0,?,?,?,?,1,0,1,?)
                      ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,
                      first_name=excluded.first_name,last_name=excluded.last_name,updated_at=excluded.updated_at,
                      last_seen=excluded.last_seen,last_action=excluded.last_action,
                      action_count=bot_users.action_count+1,callback_count=bot_users.callback_count+1,
                      last_chat_type=excluded.last_chat_type''',
                   (int(chat_id),u.get('username') or '',u.get('first_name') or '',u.get('last_name') or '',now,now,now,action,chat.get('type') or 'private'))
        db.execute('INSERT INTO bot_user_events(chat_id,event_type,action,created_at) VALUES(?,?,?,?)',
                   (int(chat_id),'callback',action,now));db.commit()

def bot_user_row(chat_id):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('''SELECT username,first_name,last_name,balance,created_at,updated_at,last_seen,last_action,
                               action_count,start_count,callback_count,last_chat_type
                        FROM bot_users WHERE chat_id=?''',(int(chat_id),)).fetchone()
    if not r:return {'username':'','first_name':'','last_name':'','balance':0,'action_count':0,'start_count':0,'callback_count':0}
    keys=('username','first_name','last_name','balance','created_at','updated_at','last_seen','last_action','action_count','start_count','callback_count','last_chat_type')
    out=dict(zip(keys,r));out['balance']=int(out.get('balance') or 0);return out

def wallet_balance(chat_id):
    return int(bot_user_row(chat_id).get('balance',0) or 0)

def wallet_adjust(chat_id,amount,kind='admin',ref=None,note=None):
    amount=int(amount);now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('INSERT OR IGNORE INTO bot_users(chat_id,username,first_name,last_name,balance,created_at,updated_at) VALUES(?,?,?,?,0,?,?)',
                   (int(chat_id),'','','',now,now))
        bal=int(db.execute('SELECT balance FROM bot_users WHERE chat_id=?',(int(chat_id),)).fetchone()[0] or 0)
        new=bal+amount
        if new<0:raise ValueError('Số dư không đủ')
        db.execute('UPDATE bot_users SET balance=?,updated_at=? WHERE chat_id=?',(new,now,int(chat_id)))
        db.execute('INSERT INTO bot_wallet_transactions(chat_id,amount,kind,ref,note,created_at) VALUES(?,?,?,?,?,?)',
                   (int(chat_id),amount,str(kind),str(ref) if ref else None,str(note) if note else None,now))
        db.commit()
    return new

def _setting(key,default=''):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT value FROM bot_settings WHERE key=?',(str(key),)).fetchone()
    return (r[0] if r else default) or default

def _set_setting(key,value):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_settings(key,value,updated_at) VALUES(?,?,?)
                      ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at''',
                   (str(key),str(value),time.time()));db.commit()

def support_username():
    return _setting('support_username',SUPPORT_USERNAME).strip().lstrip('@')

def _known_games():
    base={c.get('game') for c in BOARDS.values() if c.get('game')}
    return [g for g in GAME_ORDER if g in base] + [g for g in sorted(base) if g not in GAME_ORDER]

def game_setting(game):
    game=str(game or '').lower().strip()
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT enabled,status,reason,play_url,updated_by,updated_at FROM bot_game_settings WHERE game=?',(game,)).fetchone()
    if not r:return {'game':game,'enabled':True,'status':'online','reason':'','play_url':'','updated_by':None,'updated_at':None}
    return {'game':game,'enabled':bool(r[0]),'status':r[1] or 'online','reason':r[2] or '',
            'play_url':r[3] or '','updated_by':r[4],'updated_at':r[5]}

def game_operational(game):
    g=game_setting(game);return bool(g.get('enabled')) and g.get('status')=='online'

def game_state_icon(game):
    st=game_setting(game)
    return '🟢' if game_operational(game) else ('🛠' if st.get('status')=='maintenance' else '🔴')

def game_state_text(game):
    st=game_setting(game);name=GAME_TITLES.get(game,str(game).upper())
    state='ĐANG HOẠT ĐỘNG' if game_operational(game) else ('BẢO TRÌ' if st.get('status')=='maintenance' else 'TẠM TẮT DO LỖI' if st.get('status')=='error' else 'ĐANG TẮT')
    reason=st.get('reason') or ('Hệ thống hoạt động bình thường.' if game_operational(game) else 'Admin chưa ghi lý do.')
    link=st.get('play_url') or 'CHƯA CÀI'
    return (f"<b>{game_state_icon(game)} {html.escape(name)}</b>\n"
            f"<i>TRẠNG THÁI GAME</i>\n{BOT_DIV}\n"
            f"📌 Trạng thái  <b>{state}</b>\n"
            f"📝 Lý do  {html.escape(reason)}\n"
            f"🌐 Link chơi  <i>{html.escape(link)}</i>")

def set_game_mode(game,mode,reason='',admin_id=None):
    game=str(game or '').lower().strip();mode=str(mode or '').lower().strip()
    if game not in {c.get('game') for c in BOARDS.values()}:raise ValueError('Game không tồn tại')
    if mode not in ('online','maintenance','error','off'):raise ValueError('Trạng thái không hợp lệ')
    enabled=1 if mode=='online' else 0;now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_game_settings(game,enabled,status,reason,play_url,updated_by,updated_at)
                      VALUES(?,?,?,?,COALESCE((SELECT play_url FROM bot_game_settings WHERE game=?),''),?,?)
                      ON CONFLICT(game) DO UPDATE SET enabled=excluded.enabled,status=excluded.status,
                      reason=excluded.reason,updated_by=excluded.updated_by,updated_at=excluded.updated_at''',
                   (game,enabled,mode,str(reason or ''),game,int(admin_id) if admin_id is not None else None,now));db.commit()
    return game_setting(game)

def set_game_link(game,url,admin_id=None):
    game=str(game or '').lower().strip();url=str(url or '').strip()
    if game not in {c.get('game') for c in BOARDS.values()}:raise ValueError('Game không tồn tại')
    if url and not url.startswith(('http://','https://')):raise ValueError('Link phải bắt đầu bằng http:// hoặc https://')
    now=time.time();st=game_setting(game)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_game_settings(game,enabled,status,reason,play_url,updated_by,updated_at)
                      VALUES(?,?,?,?,?,?,?) ON CONFLICT(game) DO UPDATE SET play_url=excluded.play_url,
                      updated_by=excluded.updated_by,updated_at=excluded.updated_at''',
                   (game,1 if st.get('enabled') else 0,st.get('status') or 'online',st.get('reason') or '',url,
                    int(admin_id) if admin_id is not None else None,now));db.commit()
    return game_setting(game)

def admin_games_text():
    lines=[f"<b>🎮 QUẢN LÝ GAME</b>",f"<i>{BOT_NAME} • LIVE OPERATIONS</i>",BOT_DIV]
    for game in _known_games():
        st=game_setting(game);name=GAME_TITLES.get(game,game.upper())
        state='ONLINE' if game_operational(game) else ('BẢO TRÌ' if st.get('status')=='maintenance' else 'LỖI' if st.get('status')=='error' else 'OFF')
        lines += ['',f"{game_state_icon(game)} <b>{html.escape(name)}</b>  •  <b>{state}</b>"]
        if st.get('reason'):lines.append(f"<i>↳ {html.escape(st['reason'][:90])}</i>")
        lines.append(f"<i>🌐 {'Đã có link chơi' if st.get('play_url') else 'Chưa cài link chơi'}</i>")
    lines += ['',BOT_DIV_SOFT,'<i>Bấm game bên dưới để bật/tắt, bảo trì, xem link và API.</i>']
    return '\n'.join(lines)[:4000]

def admin_games_keyboard():
    rows=[]
    games=_known_games()
    for i in range(0,len(games),2):
        row=[]
        for game in games[i:i+2]:
            st=game_setting(game);name=GAME_TITLES.get(game,game.upper()).replace('🎲 ','').replace('🃏 ','')
            row.append({'text':f"{game_state_icon(game)} {name[:18]}",'callback_data':'admg|'+game})
        rows.append(row)
    rows.append([{'text':'↩ QUẢN TRỊ','callback_data':'adminhome'},{'text':'⌂ TRANG CHỦ','callback_data':'home'}])
    return {'inline_keyboard':rows}

def admin_game_detail_text(game):
    game=str(game or '').lower().strip();st=game_setting(game);name=GAME_TITLES.get(game,game.upper())
    state='HOẠT ĐỘNG' if game_operational(game) else ('BẢO TRÌ' if st.get('status')=='maintenance' else 'LỖI' if st.get('status')=='error' else 'ĐÃ TẮT')
    lines=[f"<b>🎮 {html.escape(name)}</b>","<i>QUẢN LÝ GAME</i>",BOT_DIV,
           f"📌 Trạng thái  {game_state_icon(game)} <b>{state}</b>",
           f"📝 Lý do  <i>{html.escape(st.get('reason') or '---')}</i>",
           f"🌐 Link chơi  <b>{'ĐÃ CÀI' if st.get('play_url') else 'CHƯA CÀI'}</b>"]
    if st.get('play_url'):lines.append(f"<code>{html.escape(st['play_url'][:180])}</code>")
    lines += ['',BOT_DIV_SOFT,'<b>📡 API ĐANG DÙNG</b>']
    for b,c in BOARDS.items():
        if c.get('game')!=game:continue
        ec=effective_cfg(b,c);url=str(ec.get('current') or '-')
        lines.append(f"• <b>{html.escape(b)}</b>\n<code>{html.escape(url[:180])}</code>")
    lines += ['', '<i>Đổi link/API bằng lệnh admin tiếng Việt trong /quantri.</i>']
    return '\n'.join(lines)[:4000]

def admin_game_detail_keyboard(game):
    return {'inline_keyboard':[
      [{'text':'✅ BẬT','callback_data':f'admgmode|{game}|online'},{'text':'🛠 BẢO TRÌ','callback_data':f'admgmode|{game}|maintenance'}],
      [{'text':'🔴 BÁO LỖI','callback_data':f'admgmode|{game}|error'},{'text':'⛔ TẮT','callback_data':f'admgmode|{game}|off'}],
      [{'text':'↩ DANH SÁCH GAME','callback_data':'adm|gamesys'},{'text':'⌂ QUẢN TRỊ','callback_data':'adminhome'}]
    ]}

def _fmt_dt(ts):
    if not ts:return '---'
    try:return time.strftime('%d/%m/%Y %H:%M:%S',time.localtime(float(ts)))
    except:return '---'

def admin_users_text(limit=30):
    now=time.time();limit=max(1,min(80,int(limit)))
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute('''SELECT u.chat_id,u.username,u.first_name,u.balance,u.created_at,u.last_seen,u.last_action,
                                  COALESCE(u.action_count,0),COALESCE(u.start_count,0),a.enabled,a.expires_at,a.access_label
                           FROM bot_users u LEFT JOIN bot_access a ON a.chat_id=u.chat_id
                           ORDER BY COALESCE(u.last_seen,u.updated_at) DESC LIMIT ?''',(limit,)).fetchall()
    lines=[f"<b>👥 {BOT_NAME} · NGƯỜI DÙNG</b>",BOT_DIV]
    if not rows:return '\n'.join(lines+['Chưa có người dùng.'])
    for uid,user,first,bal,created,last_seen,last_action,actions,starts,en,exp,label in rows:
        active=bool(en) and (exp is None or float(exp)>now);uname='@'+user if user else (first or '---')
        remain='∞' if active and exp is None else (_fmt_remaining(float(exp)-now) if active and exp else '---')
        lines.append(f"{'🟢' if active else '⚪'} <code>{uid}</code> · <b>{html.escape(uname)}</b> · {_money(bal)}")
        lines.append(f"<i>{label or 'NO KEY'} · còn {remain} · dùng {actions} lần · {_fmt_dt(last_seen)}</i>")
    lines += ['', '<i>Xem đầy đủ: /thongtinuser USER_ID</i>']
    return '\n'.join(lines)[:4000]

def admin_user_detail_text(uid):
    uid=int(uid);u=bot_user_row(uid);info=access_info(uid);k=active_key_record(uid)
    with sqlite3.connect(DB_PATH) as db:
        paid=db.execute("SELECT COALESCE(SUM(amount),0),COUNT(*) FROM bot_topup_orders WHERE chat_id=? AND status='approved'",(uid,)).fetchone()
        pend=db.execute("SELECT COUNT(*) FROM bot_topup_orders WHERE chat_id=? AND status IN ('pending','submitted')",(uid,)).fetchone()[0]
        txs=db.execute('SELECT amount,kind,note,created_at FROM bot_wallet_transactions WHERE chat_id=? ORDER BY id DESC LIMIT 5',(uid,)).fetchall()
    username='@'+u.get('username','') if u.get('username') else (u.get('first_name') or '---')
    if is_admin(uid):exp='VĨNH VIỄN · ADMIN'
    elif info.get('active') and info.get('expires_at') is None:exp='VĨNH VIỄN'
    elif info.get('expires_at'):exp=_fmt_dt(info['expires_at'])+' · '+_fmt_remaining(info.get('remaining'))
    else:exp='KHÔNG CÓ / HẾT KEY'
    plan=(get_key_plan(k.get('plan_code')) or {}).get('name') if k else info.get('label','-')
    games=_games_text(k.get('games')) if k else ('TẤT CẢ GAME' if info.get('active') else '---')
    lines=[f"<b>👤 USER {uid}</b>",BOT_DIV,
           f"<b>Tài khoản:</b> {html.escape(username)}",
           f"<b>Họ tên:</b> {html.escape(((u.get('first_name') or '')+' '+(u.get('last_name') or '')).strip() or '---')}",
           f"<b>Số dư:</b> {_money(u.get('balance',0))}",
           f"<b>Key:</b> {html.escape(str(plan or '---'))}",f"<b>Mã key:</b> <code>{html.escape(str((k or {}).get('key_code') or '---'))}</code>",
           f"<b>Hạn:</b> {exp}",f"<b>Game:</b> {html.escape(games)}",
           f"<b>Loại chat:</b> {html.escape(str(u.get('last_chat_type') or 'private'))}",
           f"<b>Tạo user:</b> {_fmt_dt(u.get('created_at'))}",f"<b>Lần cuối:</b> {_fmt_dt(u.get('last_seen'))}",
           f"<b>Hành động cuối:</b> <code>{html.escape(str(u.get('last_action') or '---'))}</code>",
           f"<b>Lượt dùng:</b> {u.get('action_count',0)} · /start {u.get('start_count',0)} · nút {u.get('callback_count',0)}",
           f"<b>Nạp đã duyệt:</b> {_money(paid[0])} · {paid[1]} đơn · đang chờ {pend}"]
    if txs:
        lines += ['', '<b>5 biến động ví gần nhất</b>']
        for amt,kind,note,ts in txs:lines.append(f"{'+' if int(amt)>=0 else ''}{_money(amt)} · {html.escape(str(kind))} · {_fmt_dt(ts)}")
    return '\n'.join(lines)[:4000]

def admin_dashboard_stats_text():
    now=time.time();day=now-86400
    with sqlite3.connect(DB_PATH) as db:
        total=db.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0]
        active24=db.execute('SELECT COUNT(*) FROM bot_users WHERE last_seen>=?',(day,)).fetchone()[0]
        starts=db.execute('SELECT COALESCE(SUM(start_count),0) FROM bot_users').fetchone()[0]
        actions=db.execute('SELECT COALESCE(SUM(action_count),0) FROM bot_users').fetchone()[0]
        wallet=db.execute('SELECT COALESCE(SUM(balance),0) FROM bot_users').fetchone()[0]
        access=db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?)',(now,)).fetchone()[0]
        pending=db.execute("SELECT COUNT(*) FROM bot_topup_orders WHERE status IN ('pending','submitted')").fetchone()[0]
        revenue=db.execute("SELECT COALESCE(SUM(amount),0) FROM bot_topup_orders WHERE status='approved'").fetchone()[0]
        groups=db.execute('SELECT COUNT(*) FROM bot_group_settings WHERE enabled=1').fetchone()[0]
        broadcasts=db.execute('SELECT COUNT(*) FROM bot_broadcasts').fetchone()[0]
        rounds=db.execute('SELECT COUNT(*) FROM rounds').fetchone()[0]
        settled=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE actual IS NOT NULL').fetchone()[0]
        wins=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE ok=1').fetchone()[0]
        recent=db.execute('''SELECT e.chat_id,u.username,u.first_name,e.action,e.created_at
                             FROM bot_user_events e LEFT JOIN bot_users u ON u.chat_id=e.chat_id
                             ORDER BY e.id DESC LIMIT 6''').fetchall()
    games=_known_games();on=sum(1 for g in games if game_operational(g));rate=round(wins/settled*100,1) if settled else 0
    text=(f"<b>📊 {BOT_NAME} · THỐNG KÊ</b>\n{BOT_DIV}\n"
          f"<b>👥 Tổng user</b> {total} · 24h {active24}\n<b>🎟 Key đang dùng</b> {access}\n"
          f"<b>🧭 Lượt sử dụng</b> {actions} · /start {starts}\n<b>💰 Tổng số dư ví</b> {_money(wallet)}\n"
          f"<b>💳 Nạp đã duyệt</b> {_money(revenue)} · chờ {pending}\n<b>📣 Thông báo</b> {broadcasts}\n"
          f"<b>🎮 Game</b> {on}/{len(games)} hoạt động · <b>🛡 Nhóm</b> {groups}\n"
          f"<b>🧠 Dữ liệu</b> {rounds} phiên · chốt {settled} · đúng {rate}%")
    if recent:
        text+='\n\n<b>🕘 Hoạt động gần nhất</b>'
        for uid,user,first,action,ts in recent:
            who='@'+str(user) if user else (str(first or uid))
            text+=f"\n• <code>{uid}</code> · {html.escape(who[:24])} · <code>{html.escape(str(action or '-')[:32])}</code> · {_fmt_dt(ts)}"
    text+=f"\n\n<i>DB: {html.escape(DB_PATH)} · thay file code không mất dữ liệu nếu Railway Volume /data vẫn được giữ.</i>"
    return text[:4000]

def create_db_backup(tag='manual',retain=10):
    src_path=Path(DB_PATH);backup_dir=src_path.parent/'backups';backup_dir.mkdir(parents=True,exist_ok=True)
    stamp=time.strftime('%Y%m%d_%H%M%S');dst=backup_dir/f'ONGCHUNHACAI_{tag}_{stamp}.db'
    src=sqlite3.connect(DB_PATH);out=sqlite3.connect(str(dst))
    try:src.backup(out)
    finally:out.close();src.close()
    old=sorted(backup_dir.glob('ONGCHUNHACAI_*.db'),key=lambda x:x.stat().st_mtime,reverse=True)
    for f in old[max(1,int(retain)):]:
        try:f.unlink()
        except:pass
    return dst

async def send_db_backup(client,chat_id):
    path=create_db_backup('admin',10);url=f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    with open(path,'rb') as f:
        r=await client.post(url,data={'chat_id':str(chat_id),'caption':f'💾 Backup {BOT_NAME} · {time.strftime("%d/%m/%Y %H:%M:%S")}'},
                            files={'document':(path.name,f,'application/octet-stream')},timeout=90);r.raise_for_status()
    return path

async def broadcast_all_users(client,admin_id,message):
    message=str(message or '').strip()
    if not message:raise ValueError('Nội dung thông báo trống')
    with sqlite3.connect(DB_PATH) as db:ids=[int(r[0]) for r in db.execute('SELECT chat_id FROM bot_users WHERE chat_id>0 ORDER BY chat_id').fetchall()]
    ok=fail=0;body=f"<b>📣 THÔNG BÁO · {BOT_NAME}</b>\n{BOT_DIV}\n{html.escape(message)}"
    for uid in ids:
        try:
            res=await tg_call(client,'sendMessage',{'chat_id':uid,'text':body,'parse_mode':'HTML','disable_web_page_preview':True});ok+=1 if res else 0;fail+=0 if res else 1
        except Exception:fail+=1
        await asyncio.sleep(.04)
    with sqlite3.connect(DB_PATH) as db:
        db.execute('INSERT INTO bot_broadcasts(admin_id,message,total,success,failed,created_at) VALUES(?,?,?,?,?,?)',(int(admin_id),message,len(ids),ok,fail,time.time()));db.commit()
    return {'total':len(ids),'success':ok,'failed':fail}

def list_key_plans(enabled_only=True):
    sql='SELECT code,name,duration_token,price,games_json,note,enabled,sort_order FROM bot_key_plans'
    if enabled_only:sql+=' WHERE enabled=1'
    sql+=' ORDER BY sort_order,price,code'
    with sqlite3.connect(DB_PATH) as db:rows=db.execute(sql).fetchall()
    out=[]
    for r in rows:
        try:games=json.loads(r[4] or '["*"]')
        except:games=['*']
        out.append({'code':r[0],'name':r[1],'duration':r[2],'price':int(r[3]),'games':games,'note':r[5] or '', 'enabled':bool(r[6]),'sort':r[7]})
    return out

def get_key_plan(code):
    code=str(code or '').lower().strip()
    return next((x for x in list_key_plans(False) if x['code']==code),None)

def _games_text(games):
    games=list(games or ['*'])
    if '*' in games:return 'TẤT CẢ GAME'
    return ', '.join(GAME_TITLES.get(g,g.upper()) for g in games)

def active_key_record(chat_id):
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('''SELECT key_code,plan_code,duration_token,price,games_json,note,redeemed_at,expires_at,status
                        FROM bot_keys WHERE redeemed_by=? AND status='active'
                        ORDER BY redeemed_at DESC LIMIT 1''',(int(chat_id),)).fetchone()
    if not r:return None
    if r[7] is not None and float(r[7])<=now:
        with sqlite3.connect(DB_PATH) as db:
            db.execute("UPDATE bot_keys SET status='expired' WHERE key_code=?",(r[0],));db.commit()
        return None
    try:games=json.loads(r[4] or '["*"]')
    except:games=['*']
    return {'key_code':r[0],'plan_code':r[1],'duration':r[2],'price':r[3],'games':games,'note':r[5] or '',
            'redeemed_at':r[6],'expires_at':r[7],'status':r[8]}

def game_allowed_for_user(chat_id,game):
    if is_admin(chat_id) or is_group_chat_id(chat_id):return True
    if not has_access(chat_id):return False
    k=active_key_record(chat_id)
    if not k:
        # Legacy/manual admin grants keep full access for backward compatibility.
        return True
    games=k.get('games') or ['*']
    return '*' in games or str(game) in games

def _new_key_code():
    alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    for _ in range(30):
        raw=''.join(secrets.choice(alphabet) for _ in range(12))
        code=f'ONG-{raw[:4]}-{raw[4:8]}-{raw[8:]}'
        with sqlite3.connect(DB_PATH) as db:
            if not db.execute('SELECT 1 FROM bot_keys WHERE key_code=?',(code,)).fetchone():return code
    raise RuntimeError('Không tạo được key')

def purchase_plan(chat_id,plan_code):
    plan=get_key_plan(plan_code)
    if not plan or not plan.get('enabled'):raise ValueError('Gói key không tồn tại hoặc đang tắt')
    price=int(plan['price']);now=time.time();seconds=_duration_seconds(plan['duration'])
    key_code=_new_key_code()
    cur=access_info(chat_id)
    if seconds is None:expires_at=None
    else:
        base=now
        if cur.get('active') and cur.get('expires_at') and float(cur['expires_at'])>now:base=float(cur['expires_at'])
        expires_at=base+seconds
    with sqlite3.connect(DB_PATH) as db:
        db.execute('BEGIN IMMEDIATE')
        row=db.execute('SELECT balance FROM bot_users WHERE chat_id=?',(int(chat_id),)).fetchone()
        bal=int(row[0] or 0) if row else 0
        if bal<price:
            db.rollback();raise ValueError(f'Số dư thiếu {_money(price-bal)}')
        db.execute('UPDATE bot_users SET balance=balance-?,updated_at=? WHERE chat_id=?',(price,now,int(chat_id)))
        db.execute('INSERT INTO bot_wallet_transactions(chat_id,amount,kind,ref,note,created_at) VALUES(?,?,?,?,?,?)',
                   (int(chat_id),-price,'buy_key',key_code,plan['code'],now))
        db.execute('''INSERT INTO bot_keys(key_code,plan_code,duration_token,price,games_json,note,created_at,redeemed_by,redeemed_at,expires_at,status)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                   (key_code,plan['code'],plan['duration'],price,json.dumps(plan['games'],ensure_ascii=False),plan['note'],now,int(chat_id),now,expires_at,'active'))
        db.commit()
    grant_access(chat_id,None,True,'vip',expires_at,'KEY:'+plan['code'].upper())
    return key_code,expires_at,wallet_balance(chat_id),plan

def redeem_key(chat_id,key_code):
    key_code=str(key_code or '').strip().upper();now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT plan_code,duration_token,price,games_json,note,status FROM bot_keys WHERE key_code=?',(key_code,)).fetchone()
        if not r:raise ValueError('Key không tồn tại')
        if r[5]!='new':raise ValueError('Key đã dùng hoặc không còn hiệu lực')
        seconds=_duration_seconds(r[1]);cur=access_info(chat_id)
        expires_at=None if seconds is None else (max(now,float(cur.get('expires_at') or 0)) if cur.get('active') else now)+seconds
        db.execute("UPDATE bot_keys SET redeemed_by=?,redeemed_at=?,expires_at=?,status='active' WHERE key_code=?",
                   (int(chat_id),now,expires_at,key_code));db.commit()
    grant_access(chat_id,None,True,'vip',expires_at,'KEY:'+str(r[0]).upper())
    return access_info(chat_id)

def guest_home_text(chat_id):
    u=bot_user_row(chat_id);info=access_info(chat_id)
    username='@'+u['username'] if u.get('username') else (u.get('first_name') or '---')
    if info.get('active'):
        if info.get('expires_at') is None: expiry='VĨNH VIỄN'
        else: expiry=time.strftime('%d/%m/%Y %H:%M',time.localtime(float(info['expires_at'])))
    else: expiry='CHƯA CÓ KEY'
    return (f"<b>💯 {BOT_NAME}</b>\n"
            f"<i>BOT TOOL • KEY ACCESS</i>\n{BOT_DIV}\n"
            f"<b>👤 TÀI KHOẢN</b>\n"
            f"├ 🆔 ID  <code>{chat_id}</code>\n"
            f"├ 👤 User  {html.escape(str(username))}\n"
            f"├ 🎟 Hạn key  <b>{expiry}</b>\n"
            f"└ 💰 Số dư  <b>{_money(u.get('balance',0))}</b>\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>Chọn chức năng bên dưới để tiếp tục.</i>")

def guest_keyboard():
    return {'inline_keyboard':[
        [{'text':'🔑 MUA KEY','callback_data':'keyshop'},
         {'text':'💳 NẠP TIỀN','callback_data':'topup'}],
        [{'text':'💬 HỖ TRỢ','callback_data':'support'}]
    ]}

def keyshop_text(chat_id):
    plans=list_key_plans(True)
    lines=[f"<b>🔑 CỬA HÀNG KEY</b>",f"<i>{BOT_NAME} • PREMIUM ACCESS</i>",BOT_DIV,
           f"💰 Số dư hiện tại  <b>{_money(wallet_balance(chat_id))}</b>",
           "<i>Chọn gói phù hợp với thời hạn và game bạn cần.</i>"]
    for p in plans:
        lines += ['',f"🔐 <b>{html.escape(str(p['name']))}</b>  •  <b>{_money(p['price'])}</b>",
                  f"🎮 <i>{html.escape(_games_text(p['games']))}</i>",
                  f"📝 <i>{html.escape(p['note'] or 'Không có ghi chú.')}</i>"]
    return '\n'.join(lines)[:4000]

def keyshop_keyboard():
    plans=list_key_plans(True);rows=[]
    for p in plans:
        rows.append([{'text':f"🔑 {p['name']}  •  {_money(p['price'])}",'callback_data':'plan|'+p['code']}])
    rows.append([{'text':'‹ QUAY LẠI','callback_data':'guesthome'}])
    return {'inline_keyboard':rows}

def plan_detail_text(chat_id,code):
    p=get_key_plan(code)
    if not p:return '❌ Gói key không tồn tại.'
    return (f"<b>🔑 XÁC NHẬN GÓI KEY</b>\n"
            f"<i>{BOT_NAME}</i>\n{BOT_DIV}\n"
            f"<b>{html.escape(str(p['name']))}</b>\n\n"
            f"💵 Giá  <b>{_money(p['price'])}</b>\n"
            f"⏳ Thời hạn  <b>{html.escape(str(p['duration']).upper())}</b>\n"
            f"🎮 Game  <b>{html.escape(_games_text(p['games']))}</b>\n"
            f"📝 <i>{html.escape(p['note'] or 'Không có ghi chú.')}</i>\n"
            f"{BOT_DIV_SOFT}\n"
            f"💰 Số dư của bạn  <b>{_money(wallet_balance(chat_id))}</b>")

def plan_detail_keyboard(code):
    return {'inline_keyboard':[
        [{'text':'✅ MUA GÓI NÀY','callback_data':'buy|'+str(code)}],
        [{'text':'‹ DANH SÁCH KEY','callback_data':'keyshop'}]
    ]}

def topup_menu_text(chat_id):
    return (f"<b>💳 NẠP TIỀN VÀO VÍ</b>\n"
            f"<i>{BOT_NAME} • VietQR tự động</i>\n{BOT_DIV}\n"
            f"💰 Số dư  <b>{_money(wallet_balance(chat_id))}</b>\n"
            f"📌 Nạp tối thiểu  <b>{_money(MIN_TOPUP)}</b>\n\n"
            f"<i>Chọn nhanh số tiền muốn nạp:</i>")

def topup_menu_keyboard():
    vals=(20000,50000,100000,200000,500000,1000000)
    buttons=[{'text':_money(v),'callback_data':f'topupamt|{v}'} for v in vals]
    rows=[buttons[i:i+2] for i in range(0,len(buttons),2)]
    rows.append([{'text':'‹ QUAY LẠI','callback_data':'guesthome'}])
    return {'inline_keyboard':rows}

def _new_topup_seq():
    for _ in range(40):
        seq=str(100000+secrets.randbelow(900000))
        with sqlite3.connect(DB_PATH) as db:
            if not db.execute('SELECT 1 FROM bot_topup_orders WHERE seq=?',(seq,)).fetchone():return seq
    raise RuntimeError('Không tạo được mã nạp')

def create_topup_order(chat_id,amount):
    amount=int(amount)
    if amount<MIN_TOPUP:raise ValueError(f'Nạp tối thiểu {_money(MIN_TOPUP)}')
    now=time.time();seq=_new_topup_seq();code='UP'+seq;content=f'upbottoolhtungvip+#{seq}'
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_topup_orders(order_code,seq,chat_id,amount,transfer_content,status,created_at,expires_at)
                      VALUES(?,?,?,?,?,'pending',?,?)''',(code,seq,int(chat_id),amount,content,now,now+TOPUP_ORDER_TTL));db.commit()
    params={'amount':amount,'addInfo':content}
    if VIETQR_ACCOUNT_NAME:params['accountName']=VIETQR_ACCOUNT_NAME
    qr=f"https://img.vietqr.io/image/{VIETQR_BANK_ID}-{VIETQR_ACCOUNT_NO}-compact2.png?"+urlencode(params)
    return {'code':code,'seq':seq,'amount':amount,'content':content,'qr':qr,'expires_at':now+TOPUP_ORDER_TTL}

def get_topup_order(code):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT order_code,seq,chat_id,amount,transfer_content,status,created_at,expires_at,submitted_at,reviewed_at,reviewed_by FROM bot_topup_orders WHERE order_code=?',(str(code),)).fetchone()
    if not r:return None
    keys=('code','seq','chat_id','amount','content','status','created_at','expires_at','submitted_at','reviewed_at','reviewed_by')
    return dict(zip(keys,r))

def topup_caption(o):
    return (f"<b>💳 QUÉT QR NẠP TIỀN</b>\n"
            f"<i>{BOT_NAME}</i>\n{BOT_DIV}\n"
            f"💵 Số tiền  <b>{_money(o['amount'])}</b>\n"
            f"🏦 Ngân hàng  <b>SHB</b>\n"
            f"💳 Số tài khoản  <code>{VIETQR_ACCOUNT_NO}</code>\n"
            f"📝 Nội dung  <code>{html.escape(o['content'])}</code>\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>Chuyển đúng số tiền và nội dung. Sau khi chuyển, bấm “TÔI ĐÃ CK”. Quá 5 phút chưa cộng tiền thì liên hệ Hỗ trợ.</i>")

def topup_pay_keyboard(code):
    return {'inline_keyboard':[
        [{'text':'✅ TÔI ĐÃ CK','callback_data':'paid|'+str(code)},
         {'text':'✖ HỦY','callback_data':'cancelpay|'+str(code)}]
    ]}

def admin_pending_payments_text(limit=15):
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute("SELECT order_code,chat_id,amount,transfer_content,created_at,status FROM bot_topup_orders WHERE status IN ('pending','submitted') ORDER BY created_at DESC LIMIT ?",(int(limit),)).fetchall()
    lines=[f"<b>💳 {BOT_NAME} · NẠP TIỀN</b>",BOT_DIV]
    if not rows:return '\n'.join(lines+['Không có đơn chờ.'])
    for code,uid,amt,content,created,status in rows:
        lines.append(f"<b>{code}</b> · {uid} · {_money(amt)} · {status.upper()}")
        lines.append(f"<i>{html.escape(content)} · {time.strftime('%H:%M:%S',time.localtime(created))}</i>")
    return '\n'.join(lines)[:4000]

def admin_plans_text():
    lines=[f"<b>🔑 {BOT_NAME} · GÓI KEY</b>",BOT_DIV]
    for p in list_key_plans(False):
        lines += [f"<b>{p['code']} · {p['name']} · {_money(p['price'])} · {'BẬT' if p['enabled'] else 'TẮT'}</b>",
                  f"<i>{p['duration'].upper()} · {_games_text(p['games'])}</i>",f"<i>{html.escape(p['note'] or '-')}</i>"]
    lines += ['',"<b>Lệnh chỉnh nhanh</b>",
              '<code>/giakey 7d 67000</code>',
              '<code>/gamekey 7d sunwin,lc79,max789</code>',
              '<code>/ghichukey 7d Nội dung ghi chú</code>',
              '<code>/hankey 7d 7d</code>',
              '<code>/batgoikey 7d</code> · <code>/tatgoikey 7d</code>',
              '<code>/congtien ID 50000</code> · <code>/trutien ID 10000</code>',
              '<code>/hotro username</code>']
    return '\n'.join(lines)[:4000]

def admin_pay_keyboard():
    return {'inline_keyboard':[[{'text':'↻ LÀM MỚI','callback_data':'adm|payments'},{'text':'‹ ADMIN','callback_data':'adminhome'}]]}

def _topup_admin_keyboard(code):
    return {'inline_keyboard':[[{'text':'✅ CỘNG TIỀN','callback_data':'payok|'+str(code)},
                                {'text':'❌ TỪ CHỐI','callback_data':'payno|'+str(code)}]]}

async def _edit_caption(client,q,caption,reply_markup=None):
    msg=(q or {}).get('message') or {};chat_id=(msg.get('chat') or {}).get('id');message_id=msg.get('message_id')
    payload={'chat_id':chat_id,'message_id':message_id,'caption':caption,'parse_mode':'HTML'}
    if reply_markup is not None:payload['reply_markup']=reply_markup
    try:
        return await tg_call(client,'editMessageCaption',payload)
    except Exception:
        return None

def is_admin(chat_id):
    try:return int(chat_id) in ADMIN_IDS
    except:return False


def _access_row(chat_id):
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            'SELECT enabled,granted_by,updated_at,expires_at,access_label,purchased_at FROM bot_access WHERE chat_id=?',
            (int(chat_id),)
        ).fetchone()

def _duration_seconds(token):
    t=str(token or '').strip().lower().replace(' ','')
    if t in ('forever','life','lifetime','vv','vinhvien','vĩnhviễn','0'): return None
    aliases={'1day':'1d','7day':'7d','30day':'30d','1ngay':'1d','7ngay':'7d','30ngay':'30d'}
    t=aliases.get(t,t); m=re.fullmatch(r'(\d+)(m|h|d|w)',t)
    if not m: raise ValueError('duration')
    n=int(m.group(1)); unit=m.group(2)
    if n<=0: raise ValueError('duration')
    return n*{'m':60,'h':3600,'d':86400,'w':604800}[unit]

def _fmt_remaining(seconds):
    if seconds is None:return 'VĨNH VIỄN'
    sec=max(0,int(seconds)); d,sec=divmod(sec,86400); h,sec=divmod(sec,3600); m,_=divmod(sec,60)
    if d:return f'{d}d {h}h {m}m'
    if h:return f'{h}h {m}m'
    return f'{m}m'

def access_info(chat_id):
    if is_group_chat_id(chat_id):
        g=get_group_settings(chat_id)
        return {'active':bool(g.get('enabled')),'expires_at':None,'remaining':None,
                'label':'GROUP','enabled':bool(g.get('enabled'))}
    if is_admin(chat_id): return {'active':True,'expires_at':None,'remaining':None,'label':'ADMIN','enabled':True}
    if not BOT_REQUIRE_ACCESS: return {'active':True,'expires_at':None,'remaining':None,'label':'OPEN','enabled':True}
    r=_access_row(chat_id)
    if not r:return {'active':False,'expires_at':None,'remaining':0,'label':'NONE','enabled':False}
    enabled,granted_by,updated_at,expires_at,label,purchased_at=r; now=time.time()
    expired=expires_at is not None and float(expires_at)<=now
    if enabled and expired:
        with sqlite3.connect(DB_PATH) as db:
            db.execute('UPDATE bot_access SET enabled=0,updated_at=? WHERE chat_id=?',(now,int(chat_id)))
            db.execute('UPDATE bot_subscriptions SET enabled=0 WHERE chat_id=?',(int(chat_id),));db.commit()
        enabled=0
    remaining=None if expires_at is None else max(0,float(expires_at)-now)
    return {'active':bool(enabled) and not expired,'expires_at':expires_at,'remaining':remaining,
            'label':label or 'manual','enabled':bool(enabled),'granted_by':granted_by,'updated_at':updated_at,'purchased_at':purchased_at}

def has_access(chat_id):
    if is_group_chat_id(chat_id): return group_enabled(chat_id)
    if is_admin(chat_id): return True
    if not BOT_REQUIRE_ACCESS: return True
    return bool(access_info(chat_id).get('active'))


FEATURES=('predict','history','ai','auto')
PRESETS={
    'basic': {'predict':1,'history':0,'ai':0,'auto':0},
    'pro':   {'predict':1,'history':1,'ai':0,'auto':1},
    'vip':   {'predict':1,'history':1,'ai':1,'auto':1},
}


def _perm_row(chat_id):
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            'SELECT can_predict,can_history,can_ai,can_auto,preset,granted_by,updated_at FROM bot_permissions WHERE chat_id=?',
            (int(chat_id),)
        ).fetchone()


def get_permissions(chat_id):
    if is_group_chat_id(chat_id):
        if group_enabled(chat_id):
            return {'predict':True,'history':True,'ai':True,'auto':True,'preset':'group'}
        return {'predict':False,'history':False,'ai':False,'auto':False,'preset':'group_locked'}
    if is_admin(chat_id):
        return {'predict':True,'history':True,'ai':True,'auto':True,'preset':'admin'}
    if not has_access(chat_id):
        return {'predict':False,'history':False,'ai':False,'auto':False,'preset':'locked'}
    r=_perm_row(chat_id)
    if not r:
        # Backward compatibility for users granted by older versions.
        return {'predict':True,'history':True,'ai':True,'auto':True,'preset':'legacy'}
    return {'predict':bool(r[0]),'history':bool(r[1]),'ai':bool(r[2]),'auto':bool(r[3]),'preset':r[4] or 'custom'}


def feature_allowed(chat_id,feature):
    return bool(get_permissions(chat_id).get(feature)) if feature in FEATURES else False


def set_preset(chat_id,preset,admin_id=None):
    preset=(preset or '').lower().strip()
    if preset not in PRESETS: raise ValueError('preset')
    p=PRESETS[preset]
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO bot_permissions(chat_id,can_predict,can_history,can_ai,can_auto,preset,granted_by,updated_at)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 can_predict=excluded.can_predict,can_history=excluded.can_history,
                 can_ai=excluded.can_ai,can_auto=excluded.can_auto,preset=excluded.preset,
                 granted_by=excluded.granted_by,updated_at=excluded.updated_at''',
            (int(chat_id),p['predict'],p['history'],p['ai'],p['auto'],preset,
             int(admin_id) if admin_id is not None else None,time.time())
        )
        db.commit()


def set_feature(chat_id,feature,enabled,admin_id=None):
    feature=(feature or '').lower().strip()
    if feature not in FEATURES: raise ValueError('feature')
    cur=get_permissions(chat_id)
    vals={k:int(bool(cur.get(k))) for k in FEATURES}
    vals[feature]=1 if enabled else 0
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO bot_permissions(chat_id,can_predict,can_history,can_ai,can_auto,preset,granted_by,updated_at)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 can_predict=excluded.can_predict,can_history=excluded.can_history,
                 can_ai=excluded.can_ai,can_auto=excluded.can_auto,preset='custom',
                 granted_by=excluded.granted_by,updated_at=excluded.updated_at''',
            (int(chat_id),vals['predict'],vals['history'],vals['ai'],vals['auto'],'custom',
             int(admin_id) if admin_id is not None else None,time.time())
        )
        db.commit()


def grant_access(chat_id,admin_id=None,enabled=True,preset=None,expires_at=None,access_label=None):
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            """INSERT INTO bot_access(chat_id,enabled,granted_by,updated_at,expires_at,access_label,purchased_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET enabled=excluded.enabled,
               granted_by=excluded.granted_by,updated_at=excluded.updated_at,
               expires_at=excluded.expires_at,access_label=excluded.access_label,
               purchased_at=COALESCE(bot_access.purchased_at,excluded.purchased_at)""",
            (int(chat_id),1 if enabled else 0,int(admin_id) if admin_id is not None else None,now,
             expires_at,access_label or ('manual' if enabled else 'locked'),now if enabled else None)
        )
        if not enabled: db.execute('UPDATE bot_subscriptions SET enabled=0 WHERE chat_id=?',(int(chat_id),))
        db.commit()
    if enabled and preset:set_preset(chat_id,preset,admin_id)

def grant_timed_access(chat_id,duration='1d',admin_id=None,extend=False):
    seconds=_duration_seconds(duration); now=time.time(); cur=access_info(chat_id)
    if seconds is None:
        expires_at=None; label='LIFETIME'
    else:
        base=now
        if extend and cur.get('expires_at') and float(cur['expires_at'])>now: base=float(cur['expires_at'])
        expires_at=base+seconds; label=str(duration).upper()
    grant_access(chat_id,admin_id,True,'vip',expires_at,label)
    return access_info(chat_id)

def admin_user_keyboard(uid):
    uid=int(uid)
    return {'inline_keyboard':[
        [{'text':'＋ 1 NGÀY','callback_data':f'uadd|{uid}|1d'},{'text':'＋ 7 NGÀY','callback_data':f'uadd|{uid}|7d'}],
        [{'text':'＋ 30 NGÀY','callback_data':f'uadd|{uid}|30d'},{'text':'♾ VĨNH VIỄN','callback_data':f'ulife|{uid}'}],
        [{'text':'🔒 KHÓA USER','callback_data':f'ulock|{uid}'},{'text':'↻ XEM LẠI','callback_data':f'uinfo|{uid}'}],
        [{'text':'‹ ADMIN','callback_data':'adminhome'}]
    ]}


def permission_text(chat_id):
    p=get_permissions(chat_id);info=access_info(chat_id)
    status='ADMIN' if is_admin(chat_id) else ('ĐANG DÙNG' if info.get('active') else 'HẾT HẠN / KHÓA');ico=lambda v:'✅' if v else '🔒'
    if is_admin(chat_id):exp='VĨNH VIỄN'
    elif info.get('expires_at') is None and info.get('active'):exp='VĨNH VIỄN'
    elif info.get('expires_at'):exp=time.strftime('%d/%m/%Y %H:%M',time.localtime(float(info['expires_at'])))+' • còn '+_fmt_remaining(info.get('remaining'))
    else:exp='---'
    return (f"<b>👤 USER • {chat_id}</b>\n{BOT_DIV}\n"
            f"📌 Trạng thái  <b>{status}</b>\n"
            f"🎟 Gói  <b>{str(p.get('preset','-')).upper()}</b> • {html.escape(str(info.get('label','-')))}\n"
            f"⏳ Hạn dùng  <b>{html.escape(str(exp))}</b>\n\n"
            f"{ico(p['predict'])} Dự đoán   {ico(p['history'])} Lịch sử\n"
            f"{ico(p['ai'])} Phân tích   {ico(p['auto'])} AUTO\n"
            f"🛡 Admin  <b>{'CÓ' if is_admin(chat_id) else 'KHÔNG'}</b>")

def locked_text(chat_id,feature):
    names={'predict':'Dự đoán','history':'Lịch sử','ai':'GPT Deep Analysis','auto':'AUTO'}; info=access_info(chat_id)
    if not info.get('active'):
        return (f"⛔ QUYỀN SỬ DỤNG ĐÃ HẾT\n{BOT_DIV}\nUser ID: {chat_id}\n"
                "Liên hệ admin để gia hạn 1D / 7D / 30D / Vĩnh viễn.")
    return (f"🔒 {names.get(feature,feature)} chưa được mở.\nUser ID: {chat_id}\nAdmin dùng: /moquyen {chat_id} {feature}")


def admin_help():
    return (f"<b>◆ {BOT_NAME} · LỆNH ADMIN</b>\n{BOT_DIV}\n"
            "<b>Người dùng / ví</b>\n"
            "/nguoidung · danh sách user\n/thongtinuser ID · đầy đủ thông tin\n"
            "/capuser ID 1d|7d|30d|forever\n/khoauser ID\n/congtien ID 50000\n/trutien ID 10000\n\n"
            "<b>Key</b>\n"
            "/goikey · danh sách gói\n/giakey GOI GIA\n/gamekey GOI all|sunwin,lc79\n"
            "/ghichukey GOI NOI_DUNG\n/hankey GOI 7d\n/batgoikey GOI · /tatgoikey GOI\n/taokey GOI SO_LUONG\n\n"
            "<b>Nạp / thông báo</b>\n"
            "/donnap · đơn chờ\n/hotro USERNAME\n/thongbao NOI_DUNG · gửi toàn bộ user\n\n"
            "<b>Game / API</b>\n"
            "/trangthaigame\n/batgame GAME\n/tatgame GAME LY_DO\n/loigame GAME LY_DO\n"
            "/linkgame GAME https://...\n/xoalinkgame GAME\n"
            "/kiemtraapi · /danhsachapi\n/thuapi BOARD\n/doapi BOARD CURRENT [HISTORY]\n/khoiphucapi BOARD\n\n"
            "<b>Hệ thống</b>\n"
            "/thongke · /nhom · /hethong · /saoluu\n"
            "<i>Tên lệnh Telegram không hỗ trợ dấu tiếng Việt nên lệnh dùng chữ không dấu; phần mô tả đều là tiếng Việt.</i>")

def user_help(chat_id):
    p=get_permissions(chat_id)
    lines=[f"💯 {BOT_NAME} · LỆNH",BOT_DIV,
           "/start · mở bảng điều khiển","/game · danh sách game","/dudoan · dự đoán bàn đang chọn",
           "/taikhoan · tài khoản","/id · User/Group ID","/baccarat · lọc bàn Baccarat"]
    if p.get('history'):lines += ["/lichsu · húp/gãy","/ketqua · kết quả game","/lichsuall · toàn bộ game"]
    if p.get('ai'):lines.append("/phantich · GPT phân tích")
    if p.get('auto'):lines.append("/tuadong · bật/tắt AUTO")
    return '\n'.join(lines)

def admin_health_text():
    with sqlite3.connect(DB_PATH) as db:
        state={b:(u,ok,err) for b,u,ok,err in db.execute('SELECT board,updated_at,source_ok,last_error FROM board_state')}
    now=time.time();lines=[f'<b>📡 {BOT_NAME} • API STATUS</b>',BOT_DIV]
    for b in BOARDS:
        u,ok,err=state.get(b,(0,0,'chưa có dữ liệu'));age=max(0,int(now-u)) if u else -1
        icon='🟢' if ok and age<=max(15,int(POLL_SECONDS*8)) else '🟡' if ok else '🔴';suffix=f' • {age}s' if age>=0 else ''
        lines.append(f"{icon} <b>{html.escape(board_label(b))}</b>{suffix}")
        if not ok and err:lines.append(f"<i>↳ {html.escape(str(err)[:90])}</i>")
    return '\n'.join(lines)[:4000]

def admin_stats_text():
    return admin_dashboard_stats_text()


def engine_diag_text(board):
    if board not in available_bot_boards():return 'Board không hợp lệ.'
    rows=load_rows(board,MAX_HISTORY)
    m=model_snapshot(rows,board.split(':',1)[0],board)
    ml=m.get('ml') or {}
    top=m.get('top_signals') or []
    lines=[f"🧠 ENGINE · {board_label(board)}",
           f"{m.get('engine')} · sample {m.get('sample',0)}",
           f"Prediction: {display_pred(board,m.get('prediction'))} · {m.get('confidence',50)}%",
           f"Đồng thuận: {round(m.get('agreement',0)*100)}% · entropy {m.get('entropy',1):.3f}",
           f"Skills: {m.get('skills',0)}/{m.get('totalSkills',48)}"]
    if ml.get('ready'):
        lines.append(f"Online ML: P(TÀI) {round(ml.get('p_tai',.5)*100)}% · val {round(ml.get('val_accuracy',.5)*100)}% · Brier {ml.get('brier',.25)}")
    else:lines.append('Online ML: đang tích mẫu')
    if m.get('strategy_champion'):
        lines.append(f"Decision: {m.get('strategy_champion')} · {m.get('mode','BOARD_META')} · {m.get('status','---')}")
        lines.append(f"Meta agree: {round(float(m.get('meta_agreement',.5))*100,1)}% · Normal W20 {round(float(m.get('normal_win_rate',.5))*100,1)}%")
        if m.get('meta_top'): lines.append('Board top: '+', '.join(f"{x.get('name')} {round(float(x.get('quality',.5))*100,1)}%" for x in m.get('meta_top',[])[:4]))
        lines.append(f"Randomness: {round(float(m.get('randomness_score',.5))*100,1)}% · penalty {m.get('randomness_penalty',0)}")
        rp=m.get('reference_pattern') or {}
        if rp.get('ready'):lines.append(f"Reference prior: L{rp.get('length')} · n={rp.get('support')} · score {rp.get('score'):+.3f}")
    if board=='sunwin:sicbo':
        hs=m.get('sicbo_hash') or {};ds=m.get('sicbo_dice_meta') or {}
        if hs:
            lines.append(f"Hash meta: {'ON' if hs.get('usable') else 'HỌC'} · sample {hs.get('sample',0)} · quality {round(float(hs.get('quality',.5))*100,1)}% · {display_pred(board,hs.get('prediction'))}")
        if ds.get('ready'):
            lines.append(f"Dice meta: {display_pred(board,ds.get('prediction'))} · quality {round(float(ds.get('quality',.5))*100,1)}%")
    if top:
        lines.append('Top signal: '+', '.join(f"{x['name']}({x['score']:+.2f})" for x in top[:6]))
    return '\n'.join(lines)[:4000]


async def admin_test_api(client,board):
    if board not in BOARDS:return 'Board không hợp lệ.'
    cfg=effective_cfg(board,BOARDS[board]);t0=time.perf_counter()
    try:
        data,url=await fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
        if cfg.get('kind')=='baccarat':
            tables=parse_baccarat(data);count=sum(len(v) for v in tables.values())
            latest=max((_session_num(r.get('id')) or 0 for arr in tables.values() for r in arr),default=0)
        else:
            kind=cfg.get('kind')
            rows=(parse_xocdia(data) if kind=='xocdia'
                  else parse_sicbo(data) if kind in ('sicbo','sicbo_pair')
                  else parse_tx(data))
            count=len(rows);latest=_current_anchor(rows) or (rows[-1]['id'] if rows else '---')
        ms=round((time.perf_counter()-t0)*1000)
        return f"✅ TEST API {board_label(board)}\n{ms} ms · rows {count} · phiên {latest}\n{url}"
    except Exception as e:
        return f"❌ TEST API {board_label(board)}\n{str(e)[:300]}"


GAME_TITLES={'sunwin':'☀️ SUNWIN','lc79':'🎲 LC79'}
GAME_ORDER=('sunwin','lc79')


def boards_for_game(game):
    return [b for b in available_bot_boards() if b.split(':',1)[0]==game]


def cau_token(board,result):
    if board=='lc79:xocdia':
        return 'C' if result=='TÀI' else 'L' if result=='XỈU' else '?'
    if board.startswith('baccarat:'):
        return 'P' if result=='TÀI' else 'B' if result=='XỈU' else '?'
    return 'T' if result=='TÀI' else 'X' if result=='XỈU' else '?'

def cau_icon(board,result):
    if board=='lc79:xocdia':
        return '⚪' if result=='TÀI' else '🟣'
    if board.startswith('baccarat:'):
        return '🔵' if result=='TÀI' else '🔴'
    return '🔴' if result=='TÀI' else '🔵'

def current_cau(board,limit=12):
    rows=load_rows(board,max(20,limit))[-limit:]
    vals=[r.get('result') for r in rows if r.get('result') in ('TÀI','XỈU')]
    if not vals:return {'text':'---','icons':'','count':0}
    return {'text':' '.join(cau_token(board,x) for x in vals),
            'icons':' '.join(cau_icon(board,x) for x in vals),
            'count':len(vals)}

def game_cau_overview(game):
    lines=[f"{GAME_TITLES.get(game,game)} · CẦU HIỆN TẠI"]
    for b in boards_for_game(game):
        c=current_cau(b,10)
        label=board_label(b)
        if game!='baccarat':
            game_name=GAME_TITLES.get(game,game).split(' ',1)[-1]
            label=label.replace(game_name+' ','')
        else:
            label=label.replace('BACCARAT · ','')
        lines.append(f"• {label}: {c['text']}")
        if c['icons']: lines.append(f"  {c['icons']}")
    return '\n'.join(lines)[:3500]


BOT_UI_VERSION='V50'
BOT_NAME='ONGCHUNHACAI💯'
BOT_DIV='━━━━━━━━━━━━━━━━'
BOT_DIV_SOFT='┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄'

def _board_source(board):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT updated_at,source_ok,last_error FROM board_state WHERE board=?',(board,)).fetchone()
    if not r:return {'icon':'⚪','ok':False,'age':None,'error':'chưa có dữ liệu'}
    updated,ok,err=r
    age=max(0,int(time.time()-updated)) if updated else None
    fresh=bool(ok) and age is not None and age<=max(18,int(POLL_SECONDS*10))
    return {'icon':'🟢' if fresh else ('🟡' if ok else '🔴'),'ok':bool(ok),'age':age,'error':err}

def _short_board_label(board):
    game=board.split(':',1)[0]; label=board_label(board)
    if game!='baccarat':
        game_name=GAME_TITLES.get(game,game).split(' ',1)[-1]
        label=label.replace(game_name+' ','')
    else: label=label.replace('BACCARAT · ','')
    return label

def _compact_cau(board,limit=7):
    return current_cau(board,limit)['text'].replace(' ','')

def _selected_summary(chat_id):
    board=get_selected_board(chat_id)
    if not board or board not in available_bot_boards():return 'Chưa chọn bàn'
    pred=get_shared_prediction(board)
    if not pred:return f"{board_label(board)} · đang đồng bộ"
    return f"{board_label(board)} · #{pred.get('session','---')} → {display_pred(board,pred.get('prediction'))} {pred.get('confidence',50)}%"

def bot_home_text(chat_id):
    if not has_access(chat_id) and not is_admin(chat_id):
        return guest_home_text(chat_id)
    p=get_permissions(chat_id);info=access_info(chat_id);u=bot_user_row(chat_id);k=active_key_record(chat_id)
    if is_admin(chat_id):expiry='VĨNH VIỄN • ADMIN'
    elif info.get('expires_at') is None and info.get('active'):expiry='VĨNH VIỄN'
    elif info.get('expires_at'):expiry=time.strftime('%d/%m %H:%M',time.localtime(float(info['expires_at'])))+f" • {_fmt_remaining(info.get('remaining'))}"
    else:expiry='HẾT HẠN'
    keyname=(get_key_plan(k['plan_code']) or {}).get('name') if k else str(p.get('preset','VIP')).upper()
    lines=[f"<b>💯 {BOT_NAME}</b>","<i>PREMIUM CONTROL CENTER</i>",BOT_DIV,
           f"🎟 Gói  <b>{html.escape(str(keyname or 'VIP'))}</b>",
           f"⏳ Hạn  <b>{html.escape(str(expiry))}</b>",
           f"💰 Số dư  <b>{_money(u.get('balance',0))}</b>"]
    board=get_selected_board(chat_id)
    if board:
        pred=get_shared_prediction(board);c=current_cau(board,6);src=_board_source(board)
        lines += ['',BOT_DIV_SOFT,f"<b>🎯 BÀN ĐANG THEO DÕI</b>",f"{src['icon']} <b>{html.escape(_short_board_label(board))}</b>"]
        if pred:
            lines += [f"➜ <b>#{pred.get('session')}  •  {display_pred(board,pred.get('prediction'))}  •  {pred.get('confidence')}%</b>"]
        else: lines += ["<i>⏳ Đang đồng bộ dữ liệu…</i>"]
        lines += [f"<i>〽️ {c['text']}</i>"]
    else:
        lines += ['',BOT_DIV_SOFT,'<i>🎮 Chọn game bên dưới để mở bàn dự đoán.</i>']
    return '\n'.join(lines)[:4000]

def bot_game_text(game,chat_id):
    if game=='baccarat':return baccarat_top_text(6)
    title=GAME_TITLES.get(game,game)
    lines=[f"<b>🎮 {html.escape(str(title))}</b>","<i>CHỌN BÀN • LIVE DATA</i>",BOT_DIV]
    for board in boards_for_game(game):
        src=_board_source(board);rows=load_rows(board,1);latest=rows[-1] if rows else None
        pred=get_shared_prediction(board);c=current_cau(board,6)
        kq=f"#{latest.get('id')} • {display_pred(board,latest.get('result'))}" if latest else '---'
        lines += ['',f"{src['icon']} <b>{html.escape(_short_board_label(board))}</b>",f"<i>KQ gần nhất  {kq}</i>"]
        if pred: lines += [f"➜ <b>#{pred.get('session')} • {display_pred(board,pred.get('prediction'))} • {pred.get('confidence')}%</b>"]
        else: lines += ['<i>➜ Đang học dữ liệu…</i>']
        lines += [f"<i>〽️ {c['text']}</i>"]
    return '\n'.join(lines)[:4000]

def bot_cau_text(board,limit=24):
    c=current_cau(board,limit); rows=load_rows(board,max(30,limit))[-limit:]
    lines=[f"<b>〽️ CẦU • {html.escape(board_label(board))}</b>","<i>24 PHIÊN GẦN NHẤT</i>",BOT_DIV]
    if not rows:return '\n'.join(lines+['<i>Chưa có dữ liệu.</i>'])
    lines += [f"<b>{c['text']}</b>"]
    if c['icons']:lines.append(c['icons'])
    vals=[r.get('result') for r in rows if r.get('result') in ('TÀI','XỈU')]
    rs=_run_stats(vals)
    lines += ['',BOT_DIV_SOFT]
    if rs.get('current'):lines.append(f"⚡ Nhịp hiện tại  <b>{display_pred(board,rs.get('current_side'))} ×{rs.get('current')}</b>")
    lines.append(f"📚 Mẫu hiển thị  <b>{len(vals)} phiên</b>")
    return '\n'.join(lines)[:4000]

def bot_account_text(chat_id):
    info=access_info(chat_id);u=bot_user_row(chat_id);k=active_key_record(chat_id)
    if is_admin(chat_id):expiry='VĨNH VIỄN • ADMIN'
    elif info.get('expires_at') is None and info.get('active'):expiry='VĨNH VIỄN'
    elif info.get('expires_at'):expiry=time.strftime('%d/%m/%Y %H:%M',time.localtime(float(info['expires_at'])))+f" • còn {_fmt_remaining(info.get('remaining'))}"
    else:expiry='CHƯA CÓ / HẾT KEY'
    username='@'+u['username'] if u.get('username') else (u.get('first_name') or '---')
    games=_games_text(k.get('games')) if k else ('TẤT CẢ GAME' if info.get('active') else '---')
    plan=(get_key_plan(k.get('plan_code')) or {}).get('name') if k else ('ADMIN' if is_admin(chat_id) else '---')
    return (f"<b>👤 TÀI KHOẢN CỦA BẠN</b>\n"
            f"<i>{BOT_NAME}</i>\n{BOT_DIV}\n"
            f"🆔 ID  <code>{chat_id}</code>\n"
            f"👤 User  <b>{html.escape(str(username))}</b>\n"
            f"🔑 Gói key  <b>{html.escape(str(plan or '---'))}</b>\n"
            f"⏳ Hạn dùng  <b>{html.escape(str(expiry))}</b>\n"
            f"💰 Số dư  <b>{_money(u.get('balance',0))}</b>\n"
            f"🎮 Game  <b>{html.escape(games)}</b>")

def bot_admin_home_text():
    now=time.time();bg=background_status_payload()
    with sqlite3.connect(DB_PATH) as db:
        total=db.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0]
        active24=db.execute('SELECT COUNT(*) FROM bot_users WHERE last_seen>=?',(now-86400,)).fetchone()[0]
        active=db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?)',(now,)).fetchone()[0]
        pending=db.execute("SELECT COUNT(*) FROM bot_topup_orders WHERE status IN ('pending','submitted')").fetchone()[0]
        revenue=db.execute("SELECT COALESCE(SUM(amount),0) FROM bot_topup_orders WHERE status='approved'").fetchone()[0]
        groups=db.execute('SELECT COUNT(*) FROM bot_group_settings WHERE enabled=1').fetchone()[0]
    games=_known_games();online=sum(1 for g in games if game_operational(g));boards=available_bot_boards();live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    return (f"<b>◆ {BOT_NAME} • QUẢN TRỊ</b>\n"
            f"<i>ADMIN SYSTEM PRO</i>\n{BOT_DIV}\n"
            f"🖥 <b>HỆ THỐNG</b>\n"
            f"├ Worker  <b>{'🟢 LIVE' if bg.get('active') else '🟡 CHỜ'}</b>\n"
            f"└ API live  <b>{live}/{len(boards)}</b>\n\n"
            f"👥 <b>NGƯỜI DÙNG</b>\n"
            f"├ Tổng  <b>{total}</b>  •  24h  <b>{active24}</b>\n"
            f"└ Key active  <b>{active}</b>\n\n"
            f"💳 <b>TÀI CHÍNH</b>\n"
            f"├ Đơn chờ  <b>{pending}</b>\n"
            f"└ Đã duyệt  <b>{_money(revenue)}</b>\n\n"
            f"🎮 <b>VẬN HÀNH</b>  Game <b>{online}/{len(games)}</b>  •  Nhóm <b>{groups}</b>\n"
            f"{BOT_DIV_SOFT}\n"
            f"💾 <i>DB: {html.escape(DB_PATH)}</i>")

async def tg_panel(client,q,text,reply_markup=None):
    msg=(q or {}).get('message') or {};chat_id=(msg.get('chat') or {}).get('id');message_id=msg.get('message_id')
    if not chat_id:return None
    payload={'chat_id':chat_id,'message_id':message_id,'text':text,'disable_web_page_preview':True}
    if isinstance(text,str) and ('<b>' in text or '<i>' in text):payload['parse_mode']='HTML'
    if reply_markup is not None:payload['reply_markup']=reply_markup
    try:
        url=f'https://api.telegram.org/bot{BOT_TOKEN}/editMessageText'
        r=await client.post(url,json=payload,timeout=30);data=r.json()
        if data.get('ok'):return data.get('result')
        if 'message is not modified' in str(data.get('description','')).lower():return msg
    except Exception:pass
    send={'chat_id':chat_id,'text':text,'disable_web_page_preview':True}
    if isinstance(text,str) and ('<b>' in text or '<i>' in text):send['parse_mode']='HTML'
    if reply_markup is not None:send['reply_markup']=reply_markup
    return await tg_call(client,'sendMessage',send)

def _road_structure_score(seq):
    q=seq[-60:]
    if len(q)<10:return .5
    flip=sum(1 for i in range(1,len(q)) if q[i]!=q[i-1])/max(1,len(q)-1)
    flip_structure=min(1.0,abs(flip-.5)*2.0)
    best_period=.0
    for lag in range(1,min(9,len(q)//3+1)):
        matches=sum(1 for i in range(lag,len(q)) if q[i]==q[i-lag])
        rate=matches/max(1,len(q)-lag)
        best_period=max(best_period,abs(rate-.5)*2.0)
    cond=[]
    for side in ('TÀI','XỈU'):
        nxt=[q[i] for i in range(1,len(q)) if q[i-1]==side]
        if nxt:
            p=nxt.count('TÀI')/len(nxt)
            cond.append(abs(p-.5)*2)
    trans=sum(cond)/len(cond) if cond else 0
    return _clamp(.5+.18*(.35*flip_structure+.40*best_period+.25*trans),.5,.68)

def baccarat_table_metrics(board):
    rows=load_rows(board,120);seq=_seq(rows);st=history_stats(board,50)
    settled=st.get('settled',0);wins=st.get('wins',0)
    acc=(wins+8)/(settled+16)
    pred=get_shared_prediction(board) or {};model=pred.get('model') or {}
    agree=float(model.get('meta_agreement',.5))
    top=model.get('meta_top') or []
    topq=max([float(x.get('quality',.5)) for x in top] or [.5])
    road=_road_structure_score(seq);sample_factor=min(1.0,len(seq)/36)
    score=.46*acc+.22*topq+.16*(.5+(agree-.5)*.75)+.16*road
    score=.5+(score-.5)*(.55+.45*sample_factor)
    score=_clamp(score,.42,.72)
    raw_acc=(wins/settled) if settled else .5
    if score>=.60 and settled>=10 and raw_acc>=.54:grade='🔥 ĐẸP'
    elif score>=.555 and len(seq)>=18:grade='✨ ỔN'
    else:grade='⚪ THEO DÕI'
    return {'board':board,'score':round(score*100,1),'grade':grade,
            'accuracy':round(raw_acc*100,1) if settled else None,'settled':settled,
            'road':round(road*100,1),'agreement':round(agree*100,1),
            'top_quality':round(topq*100,1),'sample':len(seq),'prediction':pred}

def baccarat_rank_tables(limit=6):
    boards=[b for b in available_bot_boards() if b.startswith('baccarat:') and b!='baccarat:main']
    ranked=[baccarat_table_metrics(b) for b in boards]
    ranked.sort(key=lambda x:(x['score'],x['settled'],x['sample']),reverse=True)
    return ranked[:max(1,int(limit))]

def baccarat_top_text(limit=6):
    ranked=baccarat_rank_tables(limit)
    lines=['🃏 BACCARAT · LỌC BÀN ĐẸP',BOT_DIV,
           'Score = LS đúng/sai + meta + chất lượng strategy + độ rõ của cầu.']
    if not ranked:
        lines.append('Chưa có đủ bàn Baccarat.')
        return '\\n'.join(lines)
    for i,x in enumerate(ranked,1):
        board=x['board'];acc='--' if x['accuracy'] is None else f"{x['accuracy']}%"
        pred=x.get('prediction') or {};nxt=display_pred(board,pred.get('prediction')) if pred else '---'
        lines.append(f"{i}. {x['grade']} · {_short_board_label(board)} · {x['score']}")
        lines.append(f"   W {acc}/{x['settled']} · cầu {x['road']} · meta {x['agreement']} · NEXT {nxt}")
    lines.append('※ Score là chỉ số lọc thống kê, không phải xác suất chắc thắng.')
    return '\\n'.join(lines)[:4000]


def bot_games_keyboard(chat_id):
    if not has_access(chat_id) and not is_admin(chat_id): return guest_keyboard()
    available={b.split(':',1)[0] for b in available_bot_boards()};buttons=[]
    for g in _known_games():
        if g not in available or not game_allowed_for_user(chat_id,g):continue
        name=GAME_TITLES.get(g,g.upper()).replace('🎲 ','').replace('🃏 ','')
        buttons.append({'text':f"{game_state_icon(g)} {name}",'callback_data':('game|'+g if game_operational(g) else 'gameoff|'+g)})
    rows=[buttons[i:i+2] for i in range(0,len(buttons),2)]
    quick=[]
    if get_permissions(chat_id).get('history'):quick.append({'text':'📜 LỊCH SỬ','callback_data':'histall'})
    quick.append({'text':'👤 TÀI KHOẢN','callback_data':'account'})
    if quick:rows.append(quick)
    if feature_allowed(chat_id,'auto'):
        rows.append([{'text':'🔔 AUTO ALL','callback_data':'allon'},{'text':'🔕 TẮT AUTO','callback_data':'alloff'}])
    if is_admin(chat_id):rows.append([{'text':'◆ MỞ QUẢN TRỊ','callback_data':'adminhome'}])
    return {'inline_keyboard':rows}

def bot_game_keyboard(game,chat_id):
    rows=[]
    if game=='baccarat':
        ranked=baccarat_rank_tables(6);boards=[x['board'] for x in ranked];metric={x['board']:x for x in ranked}
    else:
        boards=boards_for_game(game);metric={}
    for board in boards:
        src=_board_source(board);auto='🔔' if sub_enabled(chat_id,board) else '▫️'
        if game=='baccarat':
            x=metric[board];acc='--' if x['accuracy'] is None else f"{x['accuracy']:.0f}%";txt=f"{src['icon']} {_short_board_label(board)} • {x['score']:.0f} • W{acc}"
        else: txt=f"{src['icon']} {auto} {_short_board_label(board)}  •  {_compact_cau(board,5)}"
        rows.append([{'text':txt,'callback_data':'sel|'+board}])
    if game=='baccarat':rows.append([{'text':'↻ LỌC LẠI TOP BÀN','callback_data':'game|baccarat'}])
    link=game_setting(game).get('play_url')
    if link:rows.append([{'text':'🌐 CHƠI TRỰC TIẾP','url':link}])
    rows.append([{'text':'‹ DANH SÁCH GAME','callback_data':'games'},{'text':'⌂ TRANG CHỦ','callback_data':'home'}])
    return {'inline_keyboard':rows}

def bot_board_keyboard(board,chat_id=None):
    p=get_permissions(chat_id) if chat_id is not None else {'predict':True,'history':True,'ai':True,'auto':True}
    on=sub_enabled(chat_id,board) if chat_id is not None and p.get('auto') else False;game=board.split(':',1)[0]
    rows=[
      [{'text':'↻ CẬP NHẬT' if p.get('predict') else '🔒 DỰ ĐOÁN','callback_data':(('now|' if p.get('predict') else 'locked|predict|')+board)},
       {'text':('🔔 AUTO ✓' if on else '🔔 AUTO') if p.get('auto') else '🔒 AUTO','callback_data':(('auto|' if p.get('auto') else 'locked|auto|')+board)}],
      [{'text':'📜 LỊCH SỬ' if p.get('history') else '🔒 LỊCH SỬ','callback_data':(('hist|' if p.get('history') else 'locked|history|')+board)},
       {'text':'🧾 KẾT QUẢ' if p.get('history') else '🔒 KẾT QUẢ','callback_data':(('rounds|' if p.get('history') else 'locked|history|')+board)}],
      [{'text':'〽️ XEM CẦU','callback_data':'cau|'+board},
       {'text':'🧠 PHÂN TÍCH' if p.get('ai') else '🔒 PHÂN TÍCH','callback_data':(('ai|' if p.get('ai') else 'locked|ai|')+board)}]
    ]
    link=game_setting(game).get('play_url')
    if link:rows.append([{'text':'🌐 CHƠI TRỰC TIẾP','url':link}])
    rows.append([{'text':'‹ BÀN GAME','callback_data':'game|'+game},{'text':'⌂ TRANG CHỦ','callback_data':'home'}])
    if is_admin(chat_id):rows.append([{'text':'◆ QUẢN TRỊ','callback_data':'adminhome'}])
    return {'inline_keyboard':rows}

def bot_admin_keyboard():
    return {'inline_keyboard':[
      [{'text':'👥 NGƯỜI DÙNG','callback_data':'adm|users'},{'text':'📊 THỐNG KÊ','callback_data':'adm|stats'}],
      [{'text':'🔑 KEY & GIÁ','callback_data':'adm|keys'},{'text':'💳 ĐƠN NẠP','callback_data':'adm|payments'}],
      [{'text':'🎮 GAME & LINK','callback_data':'adm|gamesys'},{'text':'📡 API','callback_data':'adm|health'}],
      [{'text':'🛡 NHÓM','callback_data':'adm|groups'},{'text':'📣 THÔNG BÁO','callback_data':'adm|broadcast'}],
      [{'text':'💾 SAO LƯU','callback_data':'adm|backup'},{'text':'♾ HỆ THỐNG','callback_data':'adm|background'}],
      [{'text':'⌂ TRANG CHỦ','callback_data':'home'}]
    ]}

def history_stats(board,limit=100):
    h=get_prediction_history(board,limit)
    settled=[x for x in h if x.get('actual') is not None and x.get('ok') is not None]
    wins=sum(1 for x in settled if x.get('ok'))
    losses=len(settled)-wins
    pending=sum(1 for x in h if x.get('actual') is None)
    # result streak over settled items newest -> older
    streak=0; streak_ok=None
    for x in settled:
        v=bool(x.get('ok'))
        if streak_ok is None: streak_ok=v;streak=1
        elif v==streak_ok: streak+=1
        else: break
    return {
        'wins':wins,'losses':losses,'settled':len(settled),'pending':pending,
        'accuracy':round(wins/len(settled)*100,1) if settled else None,
        'streak':streak,'streak_ok':streak_ok
    }


def _round_line(board,r):
    rid=str(r.get('id','---'))
    side=display_pred(board,r.get('result'))
    d=r.get('dice') or []
    sm=r.get('sum')
    extra=''
    if len(d)>=3:
        extra=f" · 🎲{d[0]}-{d[1]}-{d[2]}={sm if sm is not None else sum(d[:3])}"
    elif board=='lc79:xocdia':
        m=r.get('meta') or {}
        red=m.get('red_count');white=m.get('white_count')
        if isinstance(red,int):
            extra=f" · 🔴{red}/⚪{white if isinstance(white,int) else 4-red}"
    elif sm is not None:
        extra=f" · tổng {sm}"
    return f"#{rid} · {side}{extra}"


def format_round_history(board,limit=12):
    rows=load_rows(board,max(20,limit))[-limit:]
    if not rows:return f"🧾 {board_label(board)} · chưa có lịch sử kết quả."
    lines=[f"<b>🧾 KẾT QUẢ • {html.escape(board_label(board))}</b>",f"<i>{len(rows)} phiên gần nhất</i>",BOT_DIV]
    for r in reversed(rows):lines.append('• '+_round_line(board,r))
    return '\n'.join(lines)[:4000]

def format_all_history(limit_per_board=40):
    lines=[f"<b>📚 {BOT_NAME} • TỔNG LỊCH SỬ</b>",BOT_DIV];count=0
    for b in available_bot_boards():
        st=history_stats(b,limit_per_board);rows=load_rows(b,1);last=rows[-1] if rows else None
        if st['settled']==0 and not last:continue
        acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--';streak=''
        if st['streak']:streak=f" • {'🔥' if st['streak_ok'] else '👾'}x{st['streak']}"
        lines.append(f"<b>{html.escape(board_label(b))}</b>")
        lines.append(f"<i>🔥 {st['wins']} • 👾 {st['losses']} • {acc}{streak}</i>")
        if last:lines.append(f"KQ #{last.get('id')}  •  {display_pred(b,last.get('result'))}")
        lines.append('')
        count+=1
        if len('\n'.join(lines))>3600:break
    if count==0:lines.append('<i>Chưa có lịch sử.</i>')
    return '\n'.join(lines).strip()[:4000]

def _timeout_row_to_dict(r):
    if not r:return None
    return {'id':r[0],'admin_chat_id':r[1],'started_at':r[2],'ends_at':r[3],
            'status':r[4],'snapshot_json':r[5],'report_sent_at':r[6],
            'cancelled_at':r[7],'last_error':r[8]}

def get_timeout_run(active_only=True):
    ensure_db()
    with sqlite3.connect(DB_PATH) as db:
        if active_only:
            r=db.execute('''SELECT id,admin_chat_id,started_at,ends_at,status,snapshot_json,
                                   report_sent_at,cancelled_at,last_error
                            FROM timeout_runs
                            WHERE status IN ('active','report_pending')
                            ORDER BY id DESC LIMIT 1''').fetchone()
        else:
            r=db.execute('''SELECT id,admin_chat_id,started_at,ends_at,status,snapshot_json,
                                   report_sent_at,cancelled_at,last_error
                            FROM timeout_runs ORDER BY id DESC LIMIT 1''').fetchone()
    return _timeout_row_to_dict(r)

def timeout_training_active():
    r=get_timeout_run(True)
    return bool(r and r['status']=='active' and time.time()<float(r['ends_at']))

def timeout_status_payload():
    r=get_timeout_run(False)
    if not r:return {'active':False,'status':'idle','remaining_seconds':0}
    now=time.time()
    active=r['status']=='active' and now<r['ends_at']
    return {'active':active,'id':r['id'],'admin_chat_id':r['admin_chat_id'],
            'started_at':r['started_at'],'ends_at':r['ends_at'],
            'remaining_seconds':max(0,int(r['ends_at']-now)) if active else 0,
            'status':r['status'],'report_sent_at':r['report_sent_at'],
            'cancelled_at':r['cancelled_at'],'last_error':r['last_error']}

def timeout_status_text():
    p=timeout_status_payload()
    if p['status']=='idle':
        return '⏱ AUTO-TRAIN: chưa chạy.\nAdmin chỉ cần /start, bot sẽ tự học ngầm đúng 2 giờ.'
    if p.get('active'):
        rem=p['remaining_seconds'];hh=rem//3600;mm=(rem%3600)//60;ss=rem%60
        return (f"⏱ AUTO-TRAIN #{p['id']} · ĐANG HỌC NGẦM\n"
                f"Còn {hh:02d}:{mm:02d}:{ss:02d}\n"
                "ALL GAME vẫn poll + tạo prediction + chốt đúng/sai.\n"
                "AUTO Telegram tạm im lặng; web vẫn đồng bộ realtime.")
    if p['status']=='report_pending':return f"⏱ AUTO-TRAIN #{p['id']} · đủ 2 giờ · đang tạo/gửi file."
    if p['status']=='finished':return f"✅ TIMEOUT #{p['id']} · hoàn tất · file đã gửi admin."
    if p['status']=='cancelled':return f"⛔ TIMEOUT #{p['id']} · đã hủy."
    return f"⏱ AUTO-TRAIN #{p['id']} · {p['status']}"

def _snapshot_sessions():
    snap={}
    with sqlite3.connect(DB_PATH) as db:
        for board,session in db.execute('SELECT board,session FROM rounds'):
            snap.setdefault(board,[]).append(str(session))
    return snap

def start_timeout_run(admin_chat_id):
    current=get_timeout_run(True)
    if current:return False,current
    now=time.time();snap=_snapshot_sessions()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO timeout_runs(admin_chat_id,started_at,ends_at,status,snapshot_json)
                      VALUES(?,?,?,?,?)''',
                   (int(admin_chat_id),now,now+TIMEOUT_SECONDS,'active',json.dumps(snap,ensure_ascii=False)))
        db.commit()
    return True,get_timeout_run(True)

def cancel_timeout_run(admin_chat_id):
    r=get_timeout_run(True)
    if not r:return False,None
    with sqlite3.connect(DB_PATH) as db:
        db.execute("UPDATE timeout_runs SET status='cancelled',cancelled_at=?,last_error=NULL WHERE id=?",
                   (time.time(),r['id']))
        db.commit()
    return True,get_timeout_run(False)

def _timeout_prediction_rows(board,start,end):
    with sqlite3.connect(DB_PATH) as db:
        q=db.execute('''SELECT p.session,p.prediction,p.confidence,p.score,p.created_at,p.actual,p.ok,p.settled_at,
                               r.d1,r.d2,r.d3,r.total,r.md5
                        FROM shared_predictions p
                        LEFT JOIN rounds r ON r.board=p.board AND r.session=p.session
                        WHERE p.board=? AND p.created_at>=? AND p.created_at<=?
                        ORDER BY p.created_at ASC''',(board,start,end))
        out=[]
        for session,pred,conf,score,created,actual,ok,settled,d1,d2,d3,total,md5 in q.fetchall():
            out.append({'session':session,'prediction':display_pred(board,pred),'prediction_internal':pred,
                        'confidence':conf,'score':score,'created_at':created,
                        'actual':display_pred(board,actual) if actual else None,'actual_internal':actual,
                        'ok':None if ok is None else bool(ok),'settled_at':settled,
                        'dice':[x for x in (d1,d2,d3) if x is not None],'sum':total,'md5':md5})
    return out

def _timeout_source_state(board):
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT updated_at,source_ok,last_error FROM board_state WHERE board=?',(board,)).fetchone()
    return {'updated_at':r[0],'source_ok':bool(r[1]),'last_error':r[2]} if r else None

def build_timeout_report(run):
    try:snapshot=json.loads(run.get('snapshot_json') or '{}')
    except:snapshot={}
    start=float(run['started_at']);end=float(run['ends_at'])
    report={'type':'ONGCHUNHACAI_AUTO_TRAIN_2H_REPORT','version':'V28','run_id':run['id'],
            'admin_chat_id':run['admin_chat_id'],'started_at':start,'ends_at':end,
            'duration_seconds':int(end-start),'generated_at':time.time(),
            'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51','boards':{},
            'totals':{'boards':0,'new_rounds':0,'predictions':0,'settled':0,'wins':0,'losses':0,'pending':0}}
    for board in available_bot_boards():
        rows=load_rows(board,MAX_HISTORY);before=set(str(x) for x in snapshot.get(board,[]))
        new_rows=[]
        for r in rows:
            if str(r.get('id')) in before:continue
            new_rows.append({'session':r.get('id'),'result':display_pred(board,r.get('result')),
                             'result_internal':r.get('result'),'dice':r.get('dice') or [],
                             'sum':r.get('sum'),'md5':r.get('md5'),'seen_at':r.get('seen_at'),
                             'meta':r.get('meta') or {}})
        preds=_timeout_prediction_rows(board,start,end)
        settled=[x for x in preds if x['ok'] is not None]
        wins=sum(1 for x in settled if x['ok']);losses=sum(1 for x in settled if not x['ok'])
        pending=sum(1 for x in preds if x['ok'] is None)
        cfg=effective_cfg(board,BOARDS.get(board,BOARDS.get('baccarat:main',{})))
        model=model_snapshot(rows,board.split(':',1)[0],board) if rows else {}
        report['boards'][board]={
            'label':board_label(board),
            'api':{'current':cfg.get('current'),'history':cfg.get('history'),
                   'fallbacks':cfg.get('current_fallbacks',[])},
            'source_state':_timeout_source_state(board),'new_rounds':new_rows,'predictions':preds,
            'summary':{'new_rounds':len(new_rows),'predictions':len(preds),'settled':len(settled),
                       'wins':wins,'losses':losses,'pending':pending,
                       'accuracy':round(wins/len(settled)*100,2) if settled else None},
            'model_end':{'sample':model.get('sample'),'pattern':model.get('pattern'),
                         'prediction':display_pred(board,model.get('prediction')),
                         'confidence':model.get('confidence'),'agreement':model.get('agreement'),
                         'entropy':model.get('entropy'),'engine':model.get('engine'),'ml':model.get('ml',{})}}
        report['totals']['boards']+=1;report['totals']['new_rounds']+=len(new_rows)
        report['totals']['predictions']+=len(preds);report['totals']['settled']+=len(settled)
        report['totals']['wins']+=wins;report['totals']['losses']+=losses;report['totals']['pending']+=pending
    st=report['totals']['settled']
    report['totals']['accuracy']=round(report['totals']['wins']/st*100,2) if st else None
    return report

def timeout_report_caption(report):
    t=report['totals'];acc=f"{t.get('accuracy')}%" if t.get('accuracy') is not None else '--'
    return (f"✅ AUTO-TRAIN 2H #{report['run_id']} HOÀN TẤT\n"
            f"Board {t['boards']} · phiên mới {t['new_rounds']}\n"
            f"Prediction {t['predictions']} · chốt {t['settled']}\n"
            f"✅ {t['wins']} · ❌ {t['losses']} · {acc}\n"
            "File JSON: API + lịch sử + đúng/sai + model cuối.")

async def tg_send_document(client,chat_id,filename,content,caption=''):
    if not BOT_TOKEN:return False
    url=f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    r=await client.post(url,data={'chat_id':str(chat_id),'caption':caption[:900]},
                        files={'document':(filename,content.encode('utf-8'),'application/json')},timeout=90)
    r.raise_for_status();data=r.json()
    return bool(data.get('ok'))

async def timeout_monitor_loop():
    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            try:
                r=get_timeout_run(True)
                if r:
                    now=time.time()
                    if r['status']=='active' and now>=r['ends_at']:
                        with sqlite3.connect(DB_PATH) as db:
                            db.execute("UPDATE timeout_runs SET status='report_pending' WHERE id=?",(r['id'],));db.commit()
                        r=get_timeout_run(True)
                    if r and r['status']=='report_pending':
                        report=build_timeout_report(r)
                        stamp=time.strftime('%Y%m%d_%H%M%S',time.localtime())
                        filename=f"ONGCHUNHACAI_AUTO_TRAIN_2H_{r['id']}_{stamp}.json"
                        raw=json.dumps(report,ensure_ascii=False,indent=2)
                        ok=await tg_send_document(client,r['admin_chat_id'],filename,raw,timeout_report_caption(report))
                        if ok:
                            with sqlite3.connect(DB_PATH) as db:
                                db.execute("UPDATE timeout_runs SET status='finished',report_sent_at=?,last_error=NULL WHERE id=?",
                                           (time.time(),r['id']));db.commit()
                await asyncio.sleep(10)
            except asyncio.CancelledError:raise
            except Exception as e:
                try:
                    r=get_timeout_run(True)
                    if r:
                        with sqlite3.connect(DB_PATH) as db:
                            db.execute("UPDATE timeout_runs SET last_error=? WHERE id=?",(str(e)[:300],r['id']));db.commit()
                except:pass
                await asyncio.sleep(20)

def mark_background_started(admin_id=None):
    ensure_db()
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        row=db.execute('SELECT activated_at FROM background_service WHERE id=1').fetchone()
        activated_at=row[0] if row and row[0] else now
        db.execute('''INSERT INTO background_service(id,enabled,activated_at,activated_by,updated_at)
                      VALUES(1,1,?,?,?)
                      ON CONFLICT(id) DO UPDATE SET enabled=1,
                        activated_by=COALESCE(background_service.activated_by,excluded.activated_by),
                        updated_at=excluded.updated_at''',
                   (activated_at,int(admin_id) if admin_id is not None else None,now))
        db.commit()


def background_status_payload():
    ensure_db()
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT enabled,activated_at,activated_by,updated_at FROM background_service WHERE id=1').fetchone()
        round_count=db.execute('SELECT COUNT(*) FROM rounds').fetchone()[0]
        pred_count=db.execute('SELECT COUNT(*) FROM shared_predictions').fetchone()[0]
        settled=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE actual IS NOT NULL').fetchone()[0]
    now=time.time()
    return {
        'active':True,
        'mode':'forever',
        'enabled':bool(r[0]) if r else True,
        'activated_at':r[1] if r else _process_started_at,
        'activated_by':r[2] if r else None,
        'process_started_at':_process_started_at,
        'last_cycle':_last_cycle,
        'last_cycle_age':max(0,round(now-_last_cycle,2)) if _last_cycle else None,
        'rounds_saved':round_count,
        'predictions_saved':pred_count,
        'settled_saved':settled,
        'history_limit_per_board':MAX_HISTORY,
    }


def background_status_text():
    p=background_status_payload();age=p.get('last_cycle_age');age_txt=f"{age}s" if age is not None else 'đang khởi động'
    return (f"<b>♾ HỆ THỐNG NỀN 24/7</b>\n{BOT_DIV}\n"
            f"🟢 Worker  <b>{age_txt}</b>\n"
            f"📚 Phiên đã lưu  <b>{p['rounds_saved']}</b>\n"
            f"🎯 Prediction  <b>{p['predictions_saved']}</b>  •  Đã chốt <b>{p['settled_saved']}</b>\n"
            f"🧠 Lịch sử/board  <b>{p['history_limit_per_board']}</b>\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>Tự poll toàn bộ game và cập nhật engine liên tục. Redeploy xong worker tự chạy lại; dữ liệu SQLite /data vẫn được giữ.</i>")

def prediction_result_detail(board,item):
    """Use joined history details first, then exact-session fallback from rounds."""
    if not item:return {'dice':[],'sum':None,'meta':{}}
    d=list(item.get('dice') or [])[:3]
    total=item.get('sum')
    if len(d)>=3 or total is not None:
        return {'dice':d,'sum':total,'meta':{}}
    sess=canonical_session(item.get('session'),item.get('session'))
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT d1,d2,d3,total,meta_json FROM rounds WHERE board=? AND session=?',
                     (board,str(sess))).fetchone()
    if not r:return {'dice':[],'sum':None,'meta':{}}
    d=[x for x in r[:3] if x is not None]
    try:meta=json.loads(r[4]) if r[4] else {}
    except:meta={}
    return {'dice':d[:3],'sum':r[3],'meta':meta}


def format_prediction(board,pred):
    src=_board_source(board);title=board_label(board)
    if not pred:
        return (f"<b>🎯 {html.escape(title)}</b>  {src['icon']}\n"
                f"<i>⏳ Đang đồng bộ dữ liệu…</i>")
    model=pred.get('model') or {};st=history_stats(board,20)
    acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--'
    recent=get_prediction_history(board,12);last=next((x for x in recent if x.get('actual') is not None),None)
    cau=current_cau(board,8);agree=round(float(model.get('meta_agreement',.5))*100)
    conf=int(pred.get('confidence',50) or 50);next_pred=display_pred(board,pred.get('prediction'))
    if conf>=68:strength='MẠNH';strength_icon='🔥'
    elif conf>=60:strength='KHÁ';strength_icon='⚡'
    else:strength='THẬN TRỌNG';strength_icon='🟡'
    lines=[f"<b>🎯 {html.escape(title)}</b>  {src['icon']}",f"<code>{cau['text']}</code>",BOT_DIV]
    if last:
        verdict='🔥 HÚP' if last.get('ok') else '👾 GÃY';detail=prediction_result_detail(board,last)
        prev=display_pred(board,last.get('prediction'));actual=display_pred(board,last.get('actual'))
        lines += [f"↩ <b>#{last.get('session')} · {verdict}</b>  {prev} → <b>{actual}</b>"]
        if board!='lc79:xocdia' and not board.startswith('baccarat:'):
            d=detail.get('dice') or [];total=detail.get('sum')
            if len(d)>=3:
                if total is None:total=sum(int(v) for v in d[:3])
                lines.append(f"<i>🎲 {d[0]} • {d[1]} • {d[2]}  =  {total}</i>")
        lines += [BOT_DIV_SOFT]
    lines += [f"⚡ <b>#{pred.get('session','---')} · {next_pred}</b>",
              f"{strength_icon} <b>{conf}% · {strength}</b>",
              f"<i>W20 {st['wins']}-{st['losses']} · {acc}  •  Đồng thuận {agree}%</i>"]
    return '\n'.join(lines)[:4000]

def format_history(board,limit=12):
    h=get_prediction_history(board,limit)
    if not h:return f"📜 {board_label(board)} · chưa có lịch sử."
    st=history_stats(board,max(20,limit));acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--'
    lines=[f"<b>📜 LỊCH SỬ • {html.escape(board_label(board))}</b>",f"<i>W20  🔥 {st['wins']}  •  👾 {st['losses']}  •  {acc}</i>",BOT_DIV]
    for x in h:
        pred=display_pred(board,x.get('prediction'))
        if x.get('actual') is None:
            lines.append(f"⏳ <b>#{x.get('session')}</b>  •  {pred}  •  {x.get('confidence')}%")
            continue
        mark='🔥' if x.get('ok') else '👾';label='HÚP' if x.get('ok') else 'GÃY';actual=display_pred(board,x.get('actual'))
        detail=prediction_result_detail(board,x);suffix=''
        if not board.startswith('baccarat:') and board!='lc79:xocdia':
            d=detail.get('dice') or []
            if len(d)>=3:
                total=detail.get('sum');total=sum(int(v) for v in d[:3]) if total is None else total
                suffix=f"  •  🎲 {d[0]}-{d[1]}-{d[2]}={total}"
        lines.append(f"{mark} <b>#{x.get('session')} • {label}</b>  {pred} → {actual}{suffix}")
    return '\n'.join(lines)[:4000]

async def tg_call(client,method,payload=None):
    if not BOT_TOKEN: return None
    payload=dict(payload or {})
    txt=payload.get('text')
    if method in ('sendMessage','editMessageText') and isinstance(txt,str) and ('<b>' in txt or '<i>' in txt):
        payload.setdefault('parse_mode','HTML')
    url=f'https://api.telegram.org/bot{BOT_TOKEN}/{method}'
    r=await client.post(url,json=payload,timeout=30)
    r.raise_for_status(); data=r.json()
    return data.get('result') if data.get('ok') else None


async def _delete_group_message_after(client,chat_id,message_id,delay=5.0):
    try:
        await asyncio.sleep(max(.1,float(delay)))
        await tg_call(client,'deleteMessage',{'chat_id':int(chat_id),'message_id':int(message_id)})
    except Exception:
        pass

def schedule_group_user_delete(client,msg):
    chat=(msg.get('chat') or {});chat_id=chat.get('id');message_id=msg.get('message_id')
    if not chat_id or not message_id or not is_group_chat_id(chat_id):return
    g=get_group_settings(chat_id)
    if not g.get('enabled') or not g.get('auto_delete'):return
    try:asyncio.create_task(_delete_group_message_after(client,chat_id,message_id,g.get('delete_after',5)))
    except Exception:pass

async def is_group_admin_actor(client,chat_id,user_id):
    if user_id is None:return False
    if is_admin(user_id):return True
    try:
        m=await tg_call(client,'getChatMember',{'chat_id':int(chat_id),'user_id':int(user_id)}) or {}
        return str(m.get('status','')).lower() in ('creator','administrator')
    except Exception:return False

async def _delete_group_ids(client,chat_id,ids):
    ids=[] if not ids else list(dict.fromkeys(int(x) for x in ids if x))
    deleted=0
    for i in range(0,len(ids),100):
        batch=ids[i:i+100]
        if not batch:continue
        try:
            ok=await tg_call(client,'deleteMessages',{'chat_id':int(chat_id),'message_ids':batch})
            if ok:deleted+=len(batch);continue
        except Exception:pass
        for mid in batch:
            try:
                ok=await tg_call(client,'deleteMessage',{'chat_id':int(chat_id),'message_id':int(mid)})
                if ok is not None:deleted+=1
            except Exception:pass
    return deleted

def _group_spam_push(chat_id,user_id,message_id,limit,window):
    now=time.time();key=(int(chat_id),int(user_id));arr=_group_spam_state.get(key,[])
    arr=[x for x in arr if now-x[0]<=float(window)]
    arr.append((now,int(message_id)))
    _group_spam_state[key]=arr[-max(30,int(limit)*3):]
    if len(arr)>=int(limit):
        ids=[mid for _,mid in arr];_group_spam_state[key]=[];return ids
    return []

def _full_send_permissions(value=True):
    v=bool(value)
    return {'can_send_messages':v,'can_send_audios':v,'can_send_documents':v,'can_send_photos':v,
            'can_send_videos':v,'can_send_video_notes':v,'can_send_voice_notes':v,'can_send_polls':v,
            'can_send_other_messages':v,'can_add_web_page_previews':v,'can_invite_users':v}

async def _restrict_user(client,chat_id,user_id,seconds=None,unmute=False):
    payload={'chat_id':int(chat_id),'user_id':int(user_id),'permissions':_full_send_permissions(True if unmute else False),
             'use_independent_chat_permissions':True}
    if not unmute and seconds:
        payload['until_date']=int(time.time()+max(30,int(seconds)))
    try:return bool(await tg_call(client,'restrictChatMember',payload))
    except Exception:return False

async def _set_group_chat_lock(client,chat_id,locked):
    try:return bool(await tg_call(client,'setChatPermissions',{
        'chat_id':int(chat_id),'permissions':_full_send_permissions(not locked),
        'use_independent_chat_permissions':True}))
    except Exception:return False

def _reply_target(msg):
    r=msg.get('reply_to_message') or {};u=r.get('from') or {}
    uid=u.get('id');name=u.get('first_name') or u.get('username') or str(uid or '')
    return (int(uid),str(name)) if uid is not None else (None,None)


async def tg_send(chat_id,text,reply_markup=None):
    if not BOT_TOKEN: return
    try:
        async with httpx.AsyncClient() as c:
            payload={'chat_id':chat_id,'text':text,'disable_web_page_preview':True}
            if reply_markup: payload['reply_markup']=reply_markup
            await tg_call(c,'sendMessage',payload)
    except Exception:
        pass


async def bot_notify_prediction(board,pred):
    if not pred: return
    chats=all_subscribers(board)
    if not chats: return
    text='🔔 AUTO · '+format_prediction(board,pred)
    async with httpx.AsyncClient() as c:
        for chat in chats:
            try:
                old=None
                with sqlite3.connect(DB_PATH) as db:
                    old=db.execute('SELECT message_id FROM bot_auto_messages WHERE chat_id=? AND board=?',(chat,board)).fetchone()
                if old:
                    try: await tg_call(c,'deleteMessage',{'chat_id':chat,'message_id':old[0]})
                    except: pass
                msg=await tg_call(c,'sendMessage',{'chat_id':chat,'text':text,'disable_web_page_preview':True,'reply_markup':bot_board_keyboard(board,chat)})
                if msg and msg.get('message_id'):
                    with sqlite3.connect(DB_PATH) as db:
                        db.execute('''INSERT INTO bot_auto_messages(chat_id,board,session,message_id,created_at)
                                      VALUES(?,?,?,?,?) ON CONFLICT(chat_id,board) DO UPDATE SET
                                      session=excluded.session,message_id=excluded.message_id,created_at=excluded.created_at''',
                                   (chat,board,str(pred.get('session','')),int(msg['message_id']),time.time()))
                        db.commit()
            except Exception:
                pass


async def bot_clear_settled_prediction(board,session):
    async with httpx.AsyncClient() as c:
        with sqlite3.connect(DB_PATH) as db:
            rows=db.execute('SELECT chat_id,message_id FROM bot_auto_messages WHERE board=? AND session=?',(board,str(session))).fetchall()
        for chat,message_id in rows:
            try: await tg_call(c,'deleteMessage',{'chat_id':chat,'message_id':message_id})
            except: pass
        if rows:
            with sqlite3.connect(DB_PATH) as db:
                db.execute('DELETE FROM bot_auto_messages WHERE board=? AND session=?',(board,str(session)));db.commit()



def _safe_mean(vals):
    vals=[float(v) for v in vals if isinstance(v,(int,float))]
    return sum(vals)/len(vals) if vals else None

def _safe_std(vals):
    vals=[float(v) for v in vals if isinstance(v,(int,float))]
    if len(vals)<2:return 0.0
    m=sum(vals)/len(vals)
    return math.sqrt(sum((x-m)**2 for x in vals)/len(vals))

def _run_stats(seq):
    if not seq:return {'current':0,'current_side':None,'max_tai':0,'max_xiu':0,'runs':[]}
    runs=[];side=seq[0];n=1
    for x in seq[1:]:
        if x==side:n+=1
        else:runs.append((side,n));side=x;n=1
    runs.append((side,n))
    return {'current':runs[-1][1],'current_side':runs[-1][0],
            'max_tai':max([n for side,n in runs if side=='TÀI'] or [0]),
            'max_xiu':max([n for side,n in runs if side=='XỈU'] or [0]),
            'runs':runs[-20:]}

def _window_balance(seq,w):
    q=seq[-w:]
    if not q:return {'n':0,'tai':0,'xiu':0,'tai_pct':50.0,'balance':0.0}
    t=q.count('TÀI');x=q.count('XỈU')
    return {'n':len(q),'tai':t,'xiu':x,'tai_pct':round(t/len(q)*100,1),'balance':round((t-x)/len(q),4)}

def _transition_digest(seq):
    out={}
    for prev in ('TÀI','XỈU'):
        t=x=1
        for i in range(1,len(seq)):
            if seq[i-1]!=prev:continue
            if seq[i]=='TÀI':t+=1
            else:x+=1
        out[prev]={'next_tai_pct':round(t/(t+x)*100,1),'next_xiu_pct':round(x/(t+x)*100,1),'sample':max(0,t+x-2)}
    return out

def _context_digest(seq,order):
    if len(seq)<=order:return None
    ctx=tuple(seq[-order:]);t=x=1;sample=0
    for i in range(order,len(seq)):
        if tuple(seq[i-order:i])!=ctx:continue
        sample+=1
        if seq[i]=='TÀI':t+=1
        else:x+=1
    return {'order':order,'context':['T' if z=='TÀI' else 'X' for z in ctx],
            'sample':sample,'next_tai_pct':round(t/(t+x)*100,1),'next_xiu_pct':round(x/(t+x)*100,1)}

def _motif_digest(seq):
    out=[]
    for k in (3,4,5,6):
        if len(seq)<=k:continue
        motif=tuple(seq[-k:]);t=x=0
        for i in range(k,len(seq)):
            if tuple(seq[i-k:i])!=motif:continue
            if seq[i]=='TÀI':t+=1
            elif seq[i]=='XỈU':x+=1
        n=t+x
        if n:out.append({'k':k,'motif':' '.join('T' if z=='TÀI' else 'X' for z in motif),
                         'sample':n,'next_tai_pct':round(t/n*100,1),'next_xiu_pct':round(x/n*100,1)})
    return out

def _lag_digest(seq):
    out=[]
    for lag in (1,2,3,4,5,6,8,10,12):
        if len(seq)<lag+16:continue
        q=seq[-min(240,len(seq)):]
        same=sum(1 for i in range(lag,len(q)) if q[i]==q[i-lag])
        n=max(1,len(q)-lag);pct=same/n
        out.append({'lag':lag,'same_pct':round(pct*100,1),'corr':round((pct-.5)*2,4),'sample':n})
    return out

def _prediction_perf(board):
    h=get_prediction_history(board,100)
    settled=[x for x in h if x.get('actual') is not None and x.get('ok') is not None]
    def one(n):
        q=settled[:n];wins=sum(1 for x in q if x.get('ok'))
        return {'n':len(q),'win':wins,'accuracy':round(wins/len(q)*100,1) if q else None}
    return {'w20':one(20),'w50':one(50),'w100':one(100)}

def _dice_digest(rows):
    good=[r for r in rows if isinstance(r.get('dice'),list) and len(r.get('dice'))>=3]
    if not good:return None
    recent=good[-240:];pos=[]
    for j in range(3):
        cnt={str(face):0 for face in range(1,7)}
        for r in recent:
            try:cnt[str(int(r['dice'][j]))]+=1
            except:pass
        total=sum(cnt.values()) or 1
        hot=sorted(cnt.items(),key=lambda kv:(kv[1],kv[0]),reverse=True)[:3]
        pos.append({'position':j+1,'sample':total,'freq_pct':{k:round(v/total*100,1) for k,v in cnt.items()},
                    'hot_faces':[int(k) for k,v in hot]})
    sums=[r.get('sum') for r in recent if isinstance(r.get('sum'),(int,float))]
    return {'sample':len(recent),'positions':pos,'sum_mean':round(_safe_mean(sums),3) if sums else None,
            'sum_std':round(_safe_std(sums),3) if sums else None,
            'sum_min':min(sums) if sums else None,'sum_max':max(sums) if sums else None}

def _xocdia_digest(rows):
    vals=[]
    for r in rows[-300:]:
        m=r.get('meta') or {}
        if isinstance(m.get('red_count'),(int,float)):vals.append(int(m['red_count']))
    if not vals:return None
    n=len(vals);freq={str(i):vals.count(i) for i in range(5)}
    return {'sample':n,'red_count_pct':{k:round(v/n*100,1) for k,v in freq.items()},
            'recent_red_counts':vals[-30:]}

def build_gpt_analysis_payload(board,rows,pred):
    seq=[r.get('result') for r in rows if r.get('result') in ('TÀI','XỈU')]
    sums=[r.get('sum') for r in rows if isinstance(r.get('sum'),(int,float))]
    compact=[]
    for r in rows[-90:]:
        item={'id':r.get('id'),'r':'T' if r.get('result')=='TÀI' else 'X' if r.get('result')=='XỈU' else str(r.get('result'))}
        if r.get('dice'):item['d']=r.get('dice')
        if isinstance(r.get('sum'),(int,float)):item['s']=r.get('sum')
        m=r.get('meta') or {}
        if board=='lc79:xocdia' and 'red_count' in m:item['red']=m.get('red_count')
        compact.append(item)
    model=model_snapshot(rows,board.split(':',1)[0],board)
    return {
        'board':board,'sample_total':len(rows),'binary_sample':len(seq),
        'current_prediction':{'session':pred.get('session') if pred else None,
                              'side':display_pred(board,pred.get('prediction')) if pred else None,
                              'confidence':pred.get('confidence') if pred else None,
                              'score':pred.get('score') if pred else None},
        'engine':{'pattern':model.get('pattern'),'alt':model.get('alt'),'score':model.get('score'),
                  'confidence':model.get('confidence'),'entropy':model.get('entropy'),
                  'agreement':model.get('agreement'),'cycle':model.get('cycle'),
                  'top_signals':model.get('top_signals',[])[:12],'ml':model.get('ml',{})},
        'windows':{str(w):_window_balance(seq,w) for w in (10,20,50,100,200,500,1000,2000,5000) if seq},
        'run':_run_stats(seq),'transition':_transition_digest(seq),
        'contexts':[x for x in (_context_digest(seq,1),_context_digest(seq,2),_context_digest(seq,3),_context_digest(seq,4)) if x],
        'motifs':_motif_digest(seq),'lags':_lag_digest(seq),
        'sum':{'mean':round(_safe_mean(sums),3) if sums else None,'std':round(_safe_std(sums),3) if sums else None,
               'recent20_mean':round(_safe_mean(sums[-20:]),3) if sums else None,
               'recent50_mean':round(_safe_mean(sums[-50:]),3) if sums else None},
        'prediction_performance':_prediction_perf(board),
        'dice':_dice_digest(rows),
        'xocdia':_xocdia_digest(rows) if board=='lc79:xocdia' else None,
        'recent90':compact
    }

def deep_local_text(board):
    if not board:return 'Cú pháp: /deep <board>'
    rows=load_rows(board,1000)
    if len(rows)<20:return f'🧮 {board_label(board)} · chưa đủ dữ liệu ({len(rows)} phiên).'
    pred=get_shared_prediction(board);p=build_gpt_analysis_payload(board,rows,pred or {})
    w=p.get('windows',{});run=p.get('run',{});ctx=p.get('contexts',[]);motifs=p.get('motifs',[])
    lags=sorted(p.get('lags',[]),key=lambda x:abs(x.get('corr',0)),reverse=True)[:4]
    perf=p.get('prediction_performance',{});lines=[f"🧮 DEEP LOCAL · {board_label(board)} · {len(rows)} phiên"]
    if pred:lines.append(f"#{pred.get('session')} · {display_pred(board,pred.get('prediction'))} · tín hiệu {pred.get('confidence')}%")
    if '20' in w and '100' in w:lines.append(f"W20 T {w['20']['tai_pct']}% · W100 T {w['100']['tai_pct']}%")
    lines.append(f"Run: {display_pred(board,run.get('current_side'))} x{run.get('current')} · max T {run.get('max_tai')} / X {run.get('max_xiu')}")
    if ctx:
        c=ctx[-1];lines.append(f"Markov-{c['order']} {''.join(c['context'])}: T {c['next_tai_pct']}% / X {c['next_xiu_pct']}% · n={c['sample']}")
    if motifs:
        mm=max(motifs,key=lambda x:x.get('sample',0));lines.append(f"Motif {mm['motif']}: T {mm['next_tai_pct']}% / X {mm['next_xiu_pct']}% · n={mm['sample']}")
    if lags:lines.append("Lag: "+", ".join(f"L{x['lag']} {x['corr']:+.2f}" for x in lags))
    ml=p.get('engine',{}).get('ml') or {}
    if ml:lines.append(f"Online ML: pT={ml.get('p_tai',0.5)} · val={round(float(ml.get('val_accuracy',.5))*100,1)}% · brier={ml.get('brier')}")
    if perf.get('w50',{}).get('accuracy') is not None:lines.append(f"Prediction W50: {perf['w50']['accuracy']}% ({perf['w50']['win']}/{perf['w50']['n']})")
    if p.get('dice'):lines.append(f"Dice: mean tổng {p['dice'].get('sum_mean')} · std {p['dice'].get('sum_std')}")
    if p.get('xocdia'):lines.append("Xóc Đĩa red-count %: "+json.dumps(p['xocdia']['red_count_pct'],ensure_ascii=False))
    lines.append("Tín hiệu thống kê lịch sử, không bảo đảm phiên kế.")
    return '\n'.join(lines)[:4000]


async def ai_explain_board(client, board):
    if not OPENAI_API_KEY or not OPENAI_MODEL:
        return '🧠 GPT Deep Analysis chưa bật. Admin cần cấu hình OPENAI_API_KEY và OPENAI_MODEL trên Railway.'
    rows=load_rows(board,1000);pred=get_shared_prediction(board)
    if len(rows)<20 or not pred:return '🧠 Chưa đủ dữ liệu để GPT phân tích sâu.'
    payload=build_gpt_analysis_payload(board,rows,pred)
    prompt=(
        "Bạn là module phân tích thống kê chuỗi cho ONGCHUNHACAI💯. Chỉ phân tích dữ liệu đã cung cấp; "
        "không tuyên bố biết trước kết quả, không nói chắc thắng, không giải MD5/hash thành kết quả tương lai. "
        "Phân tích: cầu bệt/đảo/kẹp, run-length, cửa sổ 10/20/50/100/200/500, Markov bậc 1-4, "
        "motif 3-6, autocorrelation/lag, entropy/regime, tổng điểm, xúc xắc theo vị trí nếu có, "
        "Xóc Đĩa đỏ-trắng nếu có, hiệu năng prediction gần đây, top_signals và Online ML. "
        "Nêu rõ tín hiệu mạnh/yếu và xung đột. Confidence là độ mạnh tín hiệu thống kê, không phải xác suất thắng thật. "
        "Trả lời tiếng Việt tối đa 18 dòng theo mục: CẦU HIỆN TẠI / NHỊP & MARKOV / MOTIF & LAG / "
        "DICE-SUM hoặc XÓC ĐĨA / ENGINE & ML / HIỆU NĂNG / KẾT LUẬN TÍN HIỆU.\n\nDỮ LIỆU:\n"
        +json.dumps(payload,ensure_ascii=False,separators=(',',':'))
    )
    try:
        r=await client.post('https://api.openai.com/v1/responses',
            headers={'Authorization':'Bearer '+OPENAI_API_KEY,'Content-Type':'application/json'},
            json={'model':OPENAI_MODEL,'input':prompt,'max_output_tokens':900},timeout=45)
        r.raise_for_status();data=r.json();parts=[]
        if isinstance(data.get('output_text'),str) and data.get('output_text').strip():parts.append(data['output_text'].strip())
        for item in data.get('output',[]) or []:
            if not isinstance(item,dict):continue
            for c in item.get('content',[]) or []:
                if isinstance(c,dict) and c.get('type')=='output_text' and c.get('text'):parts.append(c['text'])
        text='\n'.join(dict.fromkeys(x for x in parts if x)).strip()
        return ('🧠 GPT DEEP ANALYSIS · '+board_label(board)+'\n'+text)[:4000] if text else '🧠 GPT không trả nội dung.'
    except Exception as e:
        return '🧠 GPT lỗi: '+str(e)[:220]


''
# ============================================================================
# V49 TWO-GAME UI / ACCESS-ONLY BOT LAYER
# Key/wallet/topup/group code from older DB versions is intentionally not used.
# Existing legacy tables are left untouched so upgrading never destroys data.
# ============================================================================

def _v49_user_name(chat_id):
    u=bot_user_row(chat_id)
    if u.get('username'):return '@'+u['username'].lstrip('@')
    name=((u.get('first_name') or '')+' '+(u.get('last_name') or '')).strip()
    return name or '---'

def v49_access_card(chat_id):
    active=has_access(chat_id)
    return (f"<b>💯 {BOT_NAME}</b>\n"
            f"<i>SUNWIN • LC79 · ELITE ENGINE</i>\n{BOT_DIV}\n"
            f"👤 <b>{html.escape(_v49_user_name(chat_id))}</b>\n"
            f"🆔 <code>{chat_id}</code>\n"
            f"🔐 Quyền  <b>{'ĐÃ MỞ' if active else 'CHƯA CẤP'}</b>\n"
            f"{BOT_DIV_SOFT}\n"
            + ("<i>Chọn game bên dưới để bắt đầu.</i>" if active else "<i>Gửi ID trên cho admin để được cấp quyền sử dụng.</i>"))

def v49_guest_keyboard():
    return {'inline_keyboard':[[{'text':'↻ KIỂM TRA QUYỀN','callback_data':'checkaccess'}]]}

def bot_home_text(chat_id):
    boards=available_bot_boards();live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    selected=get_selected_board(chat_id)
    lastline='Chưa chọn bàn'
    if selected in boards:
        p=get_shared_prediction(selected) or {}
        lastline=f"{_short_board_label(selected)} · {display_pred(selected,p.get('prediction'))} · {p.get('confidence','--')}%"
    return (f"<b>💯 {BOT_NAME}</b>\n"
            f"<i>2 GAME • LIVE PREDICTION</i>\n{BOT_DIV}\n"
            f"☀️ <b>SUNWIN</b>    🎲 <b>LC79</b>\n"
            f"📡 API  <b>{live}/{len(boards)}</b>  •  🧠 <b>55 MODEL</b>\n"
            f"👤 {html.escape(_v49_user_name(chat_id))}\n"
            f"🔐 <i>Gửi MD5/SHA256 trực tiếp để phân tích.</i>\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>🎯 {html.escape(lastline)}</i>")

def bot_games_keyboard(chat_id):
    if not has_access(chat_id) and not is_admin(chat_id):return v49_guest_keyboard()
    rows=[
      [{'text':f"{game_state_icon('sunwin')} ☀️ SUNWIN",'callback_data':'game|sunwin'},
       {'text':f"{game_state_icon('lc79')} 🎲 LC79",'callback_data':'game|lc79'}],
      [{'text':'📜 LỊCH SỬ','callback_data':'histall'}]
    ]
    if is_admin(chat_id):rows.append([{'text':'◆ QUẢN TRỊ','callback_data':'adminhome'}])
    return {'inline_keyboard':rows}

def bot_game_keyboard(game,chat_id):
    rows=[]
    for board in boards_for_game(game):
        src=_board_source(board);p=get_shared_prediction(board) or {};pred=display_pred(board,p.get('prediction')) if p else '---'
        conf=p.get('confidence','--') if p else '--';cau=_compact_cau(board,6)
        rows.append([{'text':f"{src['icon']} {_short_board_label(board)}  •  {pred} {conf}%  •  {cau}",'callback_data':'sel|'+board}])
    link=game_setting(game).get('play_url')
    if link:rows.append([{'text':'↗ MỞ GAME','url':link}])
    rows.append([{'text':'‹ 2 GAME','callback_data':'games'},{'text':'⌂ HOME','callback_data':'home'}])
    return {'inline_keyboard':rows}

def bot_board_keyboard(board,chat_id=None):
    game=board.split(':',1)[0]
    rows=[
      [{'text':'📜 LỊCH SỬ','callback_data':'hist|'+board},{'text':'〽️ CẦU','callback_data':'cau|'+board}]
    ]
    link=game_setting(game).get('play_url')
    if link:rows.append([{'text':'↗ CHƠI TRỰC TIẾP','url':link}])
    rows.append([{'text':'‹ BÀN GAME','callback_data':'game|'+game},{'text':'⌂ HOME','callback_data':'home'}])
    return {'inline_keyboard':rows}

def _v50_stop_auto(chat_id):
    for _board in BOARDS:
        if _board.split(':',1)[0] in ('sunwin','lc79'):
            try:set_sub(chat_id,_board,False)
            except Exception:pass

def _v50_start_auto(chat_id,board):
    _v50_stop_auto(chat_id)
    try:set_sub(chat_id,board,True)
    except Exception:pass

def permission_text(chat_id):
    active=has_access(chat_id)
    u=bot_user_row(chat_id)
    return (f"<b>👤 THÔNG TIN USER</b>\n{BOT_DIV}\n"
            f"🆔 <code>{chat_id}</code>\n"
            f"👤 {html.escape(_v49_user_name(chat_id))}\n"
            f"🔐 Quyền  <b>{'✅ ĐANG MỞ' if active else '⛔ ĐANG KHÓA'}</b>\n"
            f"📲 Lượt dùng  <b>{int(u.get('action_count') or 0)}</b>  •  Nút <b>{int(u.get('callback_count') or 0)}</b>\n"
            f"🕘 Gần nhất  <b>{html.escape(str(u.get('last_action') or '---'))}</b>")

def bot_admin_keyboard():
    return {'inline_keyboard':[
      [{'text':'👥 NGƯỜI DÙNG','callback_data':'adm|users'},{'text':'📊 THỐNG KÊ','callback_data':'adm|stats'}],
      [{'text':'🔐 CẤP / THU QUYỀN','callback_data':'adm|access'},{'text':'📡 API','callback_data':'adm|health'}],
      [{'text':'⌂ TRANG CHỦ','callback_data':'home'}]
    ]}

def v49_admin_stats_text():
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        total=int(db.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0] or 0)
        active24=int(db.execute('SELECT COUNT(*) FROM bot_users WHERE COALESCE(last_seen,updated_at)>=?',(now-86400,)).fetchone()[0] or 0)
        actions=int(db.execute('SELECT COALESCE(SUM(action_count),0) FROM bot_users').fetchone()[0] or 0)
        starts=int(db.execute('SELECT COALESCE(SUM(start_count),0) FROM bot_users').fetchone()[0] or 0)
        callbacks=int(db.execute('SELECT COALESCE(SUM(callback_count),0) FROM bot_users').fetchone()[0] or 0)
        access=int(db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?)',(now,)).fetchone()[0] or 0)
        recent=db.execute('SELECT chat_id,username,last_action,COALESCE(last_seen,updated_at),action_count FROM bot_users ORDER BY COALESCE(last_seen,updated_at) DESC LIMIT 6').fetchall()
    boards=available_bot_boards();live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    lines=[f"<b>📊 {BOT_NAME} · THỐNG KÊ</b>",BOT_DIV,
           f"👥 Tổng user  <b>{total}</b>  •  24h <b>{active24}</b>",
           f"🔐 Đang có quyền  <b>{access}</b>",
           f"📲 Hành động  <b>{actions}</b>  •  /start <b>{starts}</b>  •  nút <b>{callbacks}</b>",
           f"📡 API live  <b>{live}/{len(boards)}</b>",BOT_DIV_SOFT,"<b>HOẠT ĐỘNG GẦN NHẤT</b>"]
    for uid,uname,act,seen,cnt in recent:
        who='@'+uname if uname else str(uid)
        lines.append(f"• {html.escape(who)} · {html.escape(str(act or '---'))} · {int(cnt or 0)} lượt")
    return '\n'.join(lines)[:4000]

def v49_users_text(limit=40):
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute('''SELECT u.chat_id,u.username,u.first_name,u.last_name,u.last_action,
                                  COALESCE(u.last_seen,u.updated_at),u.action_count,
                                  COALESCE(a.enabled,0),a.expires_at
                           FROM bot_users u LEFT JOIN bot_access a ON a.chat_id=u.chat_id
                           ORDER BY COALESCE(u.last_seen,u.updated_at) DESC LIMIT ?''',(int(limit),)).fetchall()
    lines=[f"<b>👥 NGƯỜI DÙNG · {len(rows)}</b>",BOT_DIV]
    for uid,un,fn,ln,act,seen,cnt,en,exp in rows:
        ok=bool(en) and (exp is None or float(exp)>now)
        name='@'+un if un else ((fn or '')+' '+(ln or '')).strip() or str(uid)
        lines.append(f"{'🟢' if ok else '⚪'} <code>{uid}</code> · {html.escape(name)}")
        lines.append(f"   {int(cnt or 0)} lượt · {html.escape(str(act or '---'))}")
    if not rows:lines.append('<i>Chưa có user.</i>')
    lines += [BOT_DIV_SOFT,"<i>/thongtin ID · /capquyen ID · /thuquyen ID</i>"]
    return '\n'.join(lines)[:4000]

def v49_user_detail(uid):
    u=bot_user_row(uid);info=access_info(uid)
    created=u.get('created_at');seen=u.get('last_seen') or u.get('updated_at')
    return (f"<b>👤 USER DETAIL</b>\n{BOT_DIV}\n"
            f"🆔 <code>{uid}</code>\n"
            f"👤 {html.escape(_v49_user_name(uid))}\n"
            f"🔐 <b>{'ĐÃ CẤP QUYỀN' if info.get('active') else 'CHƯA CÓ QUYỀN'}</b>\n"
            f"📲 Tổng thao tác  <b>{int(u.get('action_count') or 0)}</b>\n"
            f"▶️ /start  <b>{int(u.get('start_count') or 0)}</b>  •  Nút <b>{int(u.get('callback_count') or 0)}</b>\n"
            f"🧭 Cuối  <b>{html.escape(str(u.get('last_action') or '---'))}</b>\n"
            f"📅 Tạo  {(_fmt_dt(created) if created else '---')}\n"
            f"🕘 Online  {(_fmt_dt(seen) if seen else '---')}")

def v49_admin_user_keyboard(uid):
    return {'inline_keyboard':[
      [{'text':'✅ CẤP QUYỀN','callback_data':f'v49grant|{int(uid)}'},{'text':'⛔ THU QUYỀN','callback_data':f'v49revoke|{int(uid)}'}],
      [{'text':'↻ XEM LẠI','callback_data':f'v49user|{int(uid)}'},{'text':'‹ ADMIN','callback_data':'adminhome'}]
    ]}

def bot_admin_home_text():
    return (f"<b>◆ {BOT_NAME} · ADMIN</b>\n"
            f"<i>ACCESS • USERS • API</i>\n{BOT_DIV}\n"
            f"👥 Quản lý user và quyền sử dụng\n"
            f"📊 Thống kê hoạt động realtime\n"
            f"📡 Theo dõi API SUNWIN / LC79\n"
            f"{BOT_DIV_SOFT}\n"
            f"<i>Không key • không ví • không nạp tiền • không quản lý nhóm.</i>")

def admin_help():
    return (f"<b>◆ LỆNH ADMIN · {BOT_NAME}</b>\n{BOT_DIV}\n"
            "/quantri · bảng quản trị\n"
            "/thongke · thống kê user\n"
            "/nguoidung · danh sách user\n"
            "/thongtin ID · chi tiết user\n"
            "/capquyen ID · mở quyền full\n"
            "/thuquyen ID · khóa quyền\n"
            "/kiemtraapi · trạng thái API\n"
            "/doapi BOARD URL [HISTORY_URL]\n"
            "/linkgame GAME URL · link chơi trực tiếp")

def _v49_send_payload(chat_id,text,reply_markup=None):
    p={'chat_id':chat_id,'text':text,'disable_web_page_preview':True}
    if '<b>' in text or '<i>' in text or '<code>' in text:p['parse_mode']='HTML'
    if reply_markup:p['reply_markup']=reply_markup
    return p


# ============================================================================
# V51 HASH ULTRA AUTO-DETECT LAYER
# ============================================================================
def ensure_hash_tables():
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS bot_hash_analyses(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,user_id INTEGER,chat_type TEXT,
            hash_type TEXT NOT NULL,hash_value TEXT NOT NULL,prediction TEXT NOT NULL,
            tai_pct REAL NOT NULL,xiu_pct REAL NOT NULL,agreement REAL NOT NULL,created_at REAL NOT NULL)''')
        db.execute('CREATE INDEX IF NOT EXISTS idx_hash_analyses_created ON bot_hash_analyses(created_at)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_hash_analyses_user ON bot_hash_analyses(user_id,created_at)')
        db.commit()

def _hash_kind(text):
    z=(text or '').strip().lower()
    if re.fullmatch(r'[0-9a-f]{32}',z): return 'MD5',z
    if re.fullmatch(r'[0-9a-f]{64}',z): return 'SHA256',z
    return None,None

def _looks_like_hash_attempt(text):
    z=(text or '').strip()
    if not z:return False
    if len(z) in (32,64):return True
    return bool(re.fullmatch(r'[0-9a-fA-F]{16,80}',z))

def _hex_entropy(z):
    if not z:return 0.0
    cnt={}
    for c in z:cnt[c]=cnt.get(c,0)+1
    n=len(z);e=0.0
    for v in cnt.values():
        p=v/n
        if p>0:e-=p*math.log2(p)
    return e

def _clip(v,lo,hi):return max(lo,min(hi,float(v)))

def _prob_from_total(total):return _clip(0.5+(float(total)-10.5)/15.0*0.34,0.34,0.66)

def hash_ultra_predict(hash_hex):
    z=hash_hex.lower();L=len(z);kind='MD5' if L==32 else 'SHA256'
    n=int(z,16);nibs=[int(c,16) for c in z];bs=bytes.fromhex(z);models=[]
    def add(name,p,w):models.append((name,_clip(p,0.18,0.82),float(w)))
    d1=((n>>0)%6)+1;d2=((n>>8)%6)+1;d3=((n>>16)%6)+1
    add('DICE_BYTE',_prob_from_total(d1+d2+d3),1.15)
    last12=int(z[-12:],16);q1=(last12%6)+1;q2=((last12//6)%6)+1;q3=((last12//36)%6)+1
    add('DICE_LAST12',_prob_from_total(q1+q2+q3),1.00)
    thirds=[z[:L//3],z[L//3:2*L//3],z[2*L//3:]];sd=[(int(x,16)%6)+1 for x in thirds if x]
    add('DICE_SEGMENT',_prob_from_total(sum(sd[:3])),1.05)
    dec=str(n);odd=sum((ord(c)-48)%2 for c in dec);even=len(dec)-odd
    add('DECIMAL_PARITY',0.5+((odd-even)/max(1,len(dec)))*0.22,0.75)
    digit_vals=[int(c,16) for c in z if c.isdigit()];letter_vals=[ord(c) for c in z if c.isalpha()]
    if digit_vals and letter_vals:
        ev=sum(1 for x in digit_vals if x%2==0);od=len(digit_vals)-ev
        raw=(sum(digit_vals)+sum(letter_vals)+ev*5-od*3)%100
        add('COMPLEX_SCORE',0.5+(raw-50)/100*0.38,0.85)
    votes=[1 if int(z[-2:],16)%2==0 else 0,1 if sum(nibs)%2==0 else 0]
    digits=sum(c.isdigit() for c in z);letters=L-digits;votes.append(1 if digits>=letters else 0)
    add('BIT_VOTING',0.38+(sum(votes)/3)*0.24,0.95)
    raw_xiu=n%100;raw_tai=100-raw_xiu;add('MOD100',0.5+(raw_tai-50)/100*0.24,0.62)
    char_sum=sum(ord(c) for c in z);add('CHARCODE_PARITY',0.57 if char_sum%2==0 else 0.43,0.55)
    mean=sum(nibs)/L;add('HEX_MEAN',0.5+(mean-7.5)/15*0.36,0.92)
    one_bits=sum(b.bit_count() for b in bs);ratio=one_bits/(8*len(bs));add('BIT_DENSITY',0.5+(ratio-0.5)*0.52,0.88)
    k=8 if L==64 else 4;seg_len=max(1,L//k);sv=[]
    for i in range(k):
        seg=nibs[i*seg_len:(i+1)*seg_len] if i<k-1 else nibs[i*seg_len:]
        if seg:sv.append(1 if sum(seg)/len(seg)>=7.5 else 0)
    add('SEGMENT_VOTE',0.38+(sum(sv)/max(1,len(sv)))*0.24,1.00)
    half=L//2;mirror_delta=sum(nibs[i]-nibs[-1-i] for i in range(half));add('MIRROR_FLOW',0.5+math.tanh(mirror_delta/max(8,half*5))*0.13,0.70)
    ups=sum(bs[i]>bs[i-1] for i in range(1,len(bs)));downs=sum(bs[i]<bs[i-1] for i in range(1,len(bs)));add('BYTE_TREND',0.5+((ups-downs)/max(1,len(bs)-1))*0.16,0.78)
    pos=sum((i+1)*v for i,v in enumerate(nibs))%101;add('POSITIONAL',0.5+(pos-50)/100*0.28,0.76)
    xorv=0
    for b in bs:xorv^=b
    add('XOR_FOLD',0.5+(xorv-127.5)/255*0.22,0.72)
    derived=hashlib.sha256(z.encode()).hexdigest();dn=[int(c,16) for c in derived];dmean=sum(dn)/len(dn);dedge=(int(derived[:8],16)^int(derived[-8:],16))&0xffffffff
    add('SHA_DEEP',0.5+(dmean-7.5)/15*0.20+((dedge/0xffffffff)-0.5)*0.10,1.05)
    ent=_hex_entropy(z);ent_norm=_clip(ent/4.0,0,1);unique=len(set(z))/16.0
    repeated=sum(1 for c in set(z) if z.count(c)>max(2,L//12))/max(1,len(set(z)))
    quality=_clip(0.50+0.34*ent_norm+0.16*_clip(unique,0,1)-0.12*repeated,0.45,1.0)
    tw=sum(w for _,_,w in models);rawp=sum(p*w for _,p,w in models)/tw
    sign_tai=sum(w for _,p,w in models if p>=0.5)/tw
    # Blend magnitude-vote with direction-vote so one extreme heuristic cannot dominate the verdict.
    fused=0.68*rawp+0.32*sign_tai;direction=1 if fused>=0.5 else 0
    agree=sign_tai if direction else (1-sign_tai)
    # Convert ensemble agreement into a calibrated signal percentage, not a claimed win probability.
    strength=50.0 + max(0.0,agree-0.5)*30.0 + abs(fused-0.5)*115.0
    strength += max(-2.0,min(2.0,(quality-0.75)*8.0))
    if agree<0.56:strength=min(strength,56.5)
    strength=_clip(strength,51.2,70.0)
    tai=strength if direction else 100.0-strength;tai=round(tai,2);xiu=round(100-tai,2);pred='TÀI' if direction else 'XỈU';strength=max(tai,xiu)
    level='MẠNH' if strength>=66 else 'KHÁ' if strength>=59 else 'NHẸ'
    return {'type':kind,'prediction':pred,'tai_pct':tai,'xiu_pct':xiu,'agreement':round(agree*100,1),'entropy':round(ent,3),'level':level,'models':len(models),'dice':(d1,d2,d3),'dice_total':d1+d2+d3}

def format_hash_prediction(hash_hex):
    r=hash_ultra_predict(hash_hex)
    return (f"<b>🔐 {r['type']}</b>\nTÀI <b>{r['tai_pct']:.2f}%</b>  •  XỈU <b>{r['xiu_pct']:.2f}%</b>\n🎯 <b>{r['prediction']} · {r['level']}</b>\n<i>{r['models']} tín hiệu · đồng thuận {r['agreement']:.1f}%</i>")

def record_hash_analysis(chat_id,user_id,chat_type,hash_hex,result):
    try:
        with sqlite3.connect(DB_PATH) as db:
            db.execute('''INSERT INTO bot_hash_analyses(chat_id,user_id,chat_type,hash_type,hash_value,prediction,tai_pct,xiu_pct,agreement,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)''',(int(chat_id),int(user_id) if user_id else None,str(chat_type or ''),result['type'],hash_hex,result['prediction'],float(result['tai_pct']),float(result['xiu_pct']),float(result['agreement']),time.time()));db.commit()
    except Exception:pass

def register_group_actor(msg,action='hash'):
    u=msg.get('from') or {};uid=u.get('id')
    if not uid:return
    now=time.time();username=(u.get('username') or '').strip();first=(u.get('first_name') or '').strip();last=(u.get('last_name') or '').strip()
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_users(chat_id,username,first_name,last_name,balance,created_at,updated_at,last_seen,last_action,action_count,start_count,callback_count,last_chat_type) VALUES(?,?,?,?,0,?,?,?,?,1,0,0,'group') ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,last_name=excluded.last_name,updated_at=excluded.updated_at,last_seen=excluded.last_seen,last_action=excluded.last_action,action_count=bot_users.action_count+1,last_chat_type='group' ''',(int(uid),username,first,last,now,now,now,action));db.commit()

def hash_admin_stats():
    try:
        with sqlite3.connect(DB_PATH) as db:
            total=db.execute('SELECT COUNT(*) FROM bot_hash_analyses').fetchone()[0] or 0;md5=db.execute("SELECT COUNT(*) FROM bot_hash_analyses WHERE hash_type='MD5'").fetchone()[0] or 0;sha=db.execute("SELECT COUNT(*) FROM bot_hash_analyses WHERE hash_type='SHA256'").fetchone()[0] or 0;today=db.execute('SELECT COUNT(*) FROM bot_hash_analyses WHERE created_at>=?',(time.time()-86400,)).fetchone()[0] or 0;groups=db.execute('SELECT COUNT(*) FROM bot_group_settings WHERE enabled=1').fetchone()[0] or 0
        return int(total),int(md5),int(sha),int(today),int(groups)
    except Exception:return 0,0,0,0,0

def v49_admin_stats_text():
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        total=int(db.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0] or 0);active24=int(db.execute('SELECT COUNT(*) FROM bot_users WHERE COALESCE(last_seen,updated_at)>=?',(now-86400,)).fetchone()[0] or 0);actions=int(db.execute('SELECT COALESCE(SUM(action_count),0) FROM bot_users').fetchone()[0] or 0);starts=int(db.execute('SELECT COALESCE(SUM(start_count),0) FROM bot_users').fetchone()[0] or 0);callbacks=int(db.execute('SELECT COALESCE(SUM(callback_count),0) FROM bot_users').fetchone()[0] or 0);access=int(db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1 AND (expires_at IS NULL OR expires_at>?)',(now,)).fetchone()[0] or 0);recent=db.execute('SELECT chat_id,username,last_action,COALESCE(last_seen,updated_at),action_count FROM bot_users ORDER BY COALESCE(last_seen,updated_at) DESC LIMIT 6').fetchall()
    htotal,hmd5,hsha,h24,groups=hash_admin_stats();boards=available_bot_boards();live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    lines=[f"<b>📊 {BOT_NAME} · THỐNG KÊ</b>",BOT_DIV,f"👥 User <b>{total}</b>  •  24h <b>{active24}</b>  •  quyền <b>{access}</b>",f"🔐 Hash <b>{htotal}</b>  •  MD5 <b>{hmd5}</b>  •  SHA256 <b>{hsha}</b>  •  24h <b>{h24}</b>",f"👥 Nhóm tự cấp <b>{groups}</b>  •  📡 API <b>{live}/{len(boards)}</b>",f"📲 Hành động <b>{actions}</b>  •  /start <b>{starts}</b>  •  nút <b>{callbacks}</b>",BOT_DIV_SOFT,"<b>HOẠT ĐỘNG GẦN NHẤT</b>"]
    for uid,uname,act,seen,cnt in recent:
        who='@'+uname if uname else str(uid);lines.append(f"• {html.escape(who)} · {html.escape(str(act or '---'))} · {int(cnt or 0)} lượt")
    return '\n'.join(lines)[:4000]

async def bot_handle_my_chat_member(client,upd):
    chat=upd.get('chat') or {};chat_id=chat.get('id');ctype=chat.get('type') or ''
    if not chat_id or ctype not in ('group','supergroup'):return
    old=((upd.get('old_chat_member') or {}).get('status') or '').lower();new=((upd.get('new_chat_member') or {}).get('status') or '').lower();active={'member','administrator','creator'};actor=upd.get('from') or {};actor_id=actor.get('id');title=chat.get('title') or 'Telegram Group'
    if new in active and old not in active:
        try:set_group_enabled(chat_id,True,actor_id,title)
        except Exception:
            try:_ensure_group_row(chat_id,actor_id,title);set_group_enabled(chat_id,True,actor_id,title)
            except Exception:pass
        try:grant_access(chat_id,actor_id,True,'group',None,'AUTO_GROUP')
        except Exception:pass
        who='@'+actor.get('username') if actor.get('username') else (actor.get('first_name') or str(actor_id or '---'))
        try:count=int(await tg_call(client,'getChatMemberCount',{'chat_id':chat_id}) or 0)
        except Exception:count=0
        notice=(f"<b>👥 BOT ĐƯỢC THÊM VÀO NHÓM</b>\n{BOT_DIV}\n🏷 <b>{html.escape(str(title))}</b>\n🆔 <code>{chat_id}</code>\n👤 Thêm bởi {html.escape(str(who))} · <code>{actor_id or '---'}</code>\n👥 Thành viên <b>{count}</b>\n🔐 Quyền nhóm <b>ĐÃ TỰ CẤP</b>")
        for aid in ADMIN_IDS:
            try:await tg_call(client,'sendMessage',_v49_send_payload(aid,notice))
            except Exception:pass
    elif old in active and new not in active:
        try:set_group_enabled(chat_id,False,actor_id,title)
        except Exception:pass

async def bot_handle_message(client,msg):
    chat=msg.get('chat') or {};chat_id=chat.get('id');ctype=chat.get('type') or 'private';text=(msg.get('text') or '').strip();actor=(msg.get('from') or {}).get('id')
    if not chat_id:return
    cmd=text.split(maxsplit=1)[0].split('@',1)[0].lower() if text.startswith('/') else '';hkind,hval=_hash_kind(text)
    if is_group_chat_id(chat_id):
        if not group_enabled(chat_id):
            try:set_group_enabled(chat_id,True,actor,chat.get('title') or 'Telegram Group')
            except Exception:pass
        if cmd=='/start':
            register_group_actor(msg,'/start group');await tg_call(client,'sendMessage',_v49_send_payload(chat_id,f"<b>💯 {BOT_NAME}</b>\n<i>Đã hoạt động trong nhóm.</i>\n{BOT_DIV}\nGửi trực tiếp <b>MD5 32 HEX</b> hoặc <b>SHA256 64 HEX</b>."));return
        if hkind:
            register_group_actor(msg,'hash '+hkind);result=hash_ultra_predict(hval);record_hash_analysis(chat_id,actor,ctype,hval,result);await tg_call(client,'sendMessage',_v49_send_payload(chat_id,format_hash_prediction(hval)));return
        return
    new=register_bot_user(msg)
    if new:
        u=msg.get('from') or {};who='@'+u.get('username') if u.get('username') else (u.get('first_name') or '---');notice=(f"<b>👤 USER MỚI</b>\n{BOT_DIV}\nID <code>{chat_id}</code>\nUser {html.escape(who)}\nTên {html.escape(((u.get('first_name') or '')+' '+(u.get('last_name') or '')).strip())}\nLúc {_fmt_dt(time.time())}")
        for aid in ADMIN_IDS:
            try:await tg_call(client,'sendMessage',_v49_send_payload(aid,notice))
            except:pass
    if hkind:
        if not has_access(chat_id) and not is_admin(actor):return
        result=hash_ultra_predict(hval);record_hash_analysis(chat_id,actor,ctype,hval,result);await tg_call(client,'sendMessage',_v49_send_payload(chat_id,format_hash_prediction(hval)));return
    if _looks_like_hash_attempt(text):return
    if is_admin(actor) and cmd in ('/quantri','/admin'):
        await tg_call(client,'sendMessage',_v49_send_payload(chat_id,bot_admin_home_text(),bot_admin_keyboard()));return
    if is_admin(actor) and cmd=='/thongke':await tg_call(client,'sendMessage',_v49_send_payload(chat_id,v49_admin_stats_text(),bot_admin_keyboard()));return
    if is_admin(actor) and cmd=='/nguoidung':await tg_call(client,'sendMessage',_v49_send_payload(chat_id,v49_users_text(),bot_admin_keyboard()));return
    if is_admin(actor) and cmd=='/thongtin':
        try:uid=int(text.split()[1])
        except:await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Cú pháp: /thongtin USER_ID'});return
        await tg_call(client,'sendMessage',_v49_send_payload(chat_id,v49_user_detail(uid),v49_admin_user_keyboard(uid)));return
    if is_admin(actor) and cmd in ('/capquyen','/thuquyen'):
        try:uid=int(text.split()[1])
        except:await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'Cú pháp: {cmd} USER_ID'});return
        grant_access(uid,actor,cmd=='/capquyen','vip' if cmd=='/capquyen' else None,None,'ACCESS' if cmd=='/capquyen' else None);await tg_call(client,'sendMessage',_v49_send_payload(chat_id,v49_user_detail(uid),v49_admin_user_keyboard(uid)));return
    if is_admin(actor) and cmd=='/kiemtraapi':await tg_call(client,'sendMessage',_v49_send_payload(chat_id,admin_health_text(),bot_admin_keyboard()));return
    if is_admin(actor) and cmd=='/doapi':
        parts=text.split(maxsplit=3)
        if len(parts)<3 or parts[1] not in BOARDS:out='Cú pháp: /doapi BOARD CURRENT_URL [HISTORY_URL]'
        elif not parts[2].startswith(('http://','https://')):out='❌ URL không hợp lệ.'
        else:set_api_override(parts[1],parts[2],parts[3] if len(parts)>3 else None);out=f'✅ Đã đổi API {parts[1]}'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if is_admin(actor) and cmd=='/linkgame':
        parts=text.split(maxsplit=2)
        if len(parts)<3 or parts[1].lower() not in ('sunwin','lc79') or not parts[2].startswith(('http://','https://')):out='Cú pháp: /linkgame sunwin|lc79 https://...'
        else:
            st=game_setting(parts[1].lower())
            with sqlite3.connect(DB_PATH) as db:db.execute('''INSERT INTO bot_game_settings(game,enabled,status,reason,play_url,updated_by,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(game) DO UPDATE SET play_url=excluded.play_url,updated_by=excluded.updated_by,updated_at=excluded.updated_at''',(parts[1].lower(),1,st.get('status','online'),st.get('reason',''),parts[2],actor,time.time()));db.commit()
            out='✅ Đã cập nhật link '+parts[1].upper()
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if cmd in ('/start','/menu','/game','/games'):
        _v50_stop_auto(chat_id)
        if not has_access(chat_id) and not is_admin(chat_id):await tg_call(client,'sendMessage',_v49_send_payload(chat_id,v49_access_card(chat_id),v49_guest_keyboard()));return
        await tg_call(client,'sendMessage',_v49_send_payload(chat_id,bot_home_text(chat_id),bot_games_keyboard(chat_id)));return
    if cmd in ('/quyen','/me','/taikhoan'):
        await tg_call(client,'sendMessage',_v49_send_payload(chat_id,permission_text(chat_id),bot_games_keyboard(chat_id) if has_access(chat_id) else v49_guest_keyboard()));return
    if not has_access(chat_id) and not is_admin(chat_id):return
    board=get_selected_board(chat_id)
    if cmd in ('/dudoan','/status'):
        txt=format_prediction(board,get_shared_prediction(board)) if board else 'Chưa chọn bàn. Bấm /game.';await tg_call(client,'sendMessage',_v49_send_payload(chat_id,txt,bot_board_keyboard(board,chat_id) if board else bot_games_keyboard(chat_id)));return
    if cmd in ('/lichsu','/history'):
        txt=format_history(board) if board else 'Chưa chọn bàn. Bấm /game.';await tg_call(client,'sendMessage',_v49_send_payload(chat_id,txt,bot_board_keyboard(board,chat_id) if board else bot_games_keyboard(chat_id)));return
    return

async def bot_handle_callback(client,q):
    msg=q.get('message') or {};chat_id=(msg.get('chat') or {}).get('id');actor=(q.get('from') or {}).get('id');data=q.get('data') or ''
    if not chat_id or is_group_chat_id(chat_id):return
    register_callback_user(q)
    try:await tg_call(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
    except:pass
    if data=='checkaccess':
        if has_access(chat_id) or is_admin(chat_id):await tg_panel(client,q,bot_home_text(chat_id),bot_games_keyboard(chat_id))
        else:await tg_panel(client,q,v49_access_card(chat_id),v49_guest_keyboard())
        return
    if data=='adminhome':
        if is_admin(actor):await tg_panel(client,q,bot_admin_home_text(),bot_admin_keyboard())
        return
    if data.startswith(('v49grant|','v49revoke|','v49user|')):
        if not is_admin(actor):return
        act,uidtxt=data.split('|',1)
        try:uid=int(uidtxt)
        except:return
        if act=='v49grant':grant_access(uid,actor,True,'vip',None,'ACCESS')
        elif act=='v49revoke':grant_access(uid,actor,False)
        await tg_panel(client,q,v49_user_detail(uid),v49_admin_user_keyboard(uid));return
    if data.startswith('adm|'):
        if not is_admin(actor):return
        act=data.split('|',1)[1]
        if act=='users':txt=v49_users_text()
        elif act=='stats':txt=v49_admin_stats_text()
        elif act=='health':txt=admin_health_text()
        elif act=='access':txt=(f"<b>🔐 CẤP / THU QUYỀN</b>\n{BOT_DIV}\n"
                                "/capquyen USER_ID\n/thuquyen USER_ID\n/thongtin USER_ID\n\n<i>Hoặc mở danh sách Người dùng rồi chọn user.</i>")
        else:txt=bot_admin_home_text()
        await tg_panel(client,q,txt,bot_admin_keyboard());return
    if data=='home':
        _v50_stop_auto(chat_id)
        await tg_panel(client,q,bot_home_text(chat_id) if has_access(chat_id) or is_admin(chat_id) else v49_access_card(chat_id),bot_games_keyboard(chat_id) if has_access(chat_id) or is_admin(chat_id) else v49_guest_keyboard());return
    if data=='games':
        _v50_stop_auto(chat_id)
        await tg_panel(client,q,bot_home_text(chat_id),bot_games_keyboard(chat_id));return
    if not has_access(chat_id) and not is_admin(chat_id):
        await tg_panel(client,q,v49_access_card(chat_id),v49_guest_keyboard());return
    if data.startswith('game|'):
        _v50_stop_auto(chat_id)
        game=data.split('|',1)[1]
        if game not in ('sunwin','lc79'):return
        if not game_operational(game):await tg_panel(client,q,game_state_text(game),bot_games_keyboard(chat_id));return
        await tg_panel(client,q,game_cau_overview(game),bot_game_keyboard(game,chat_id));return
    if data=='histall':
        await tg_panel(client,q,format_all_history(),bot_games_keyboard(chat_id));return
    if '|' not in data:return
    action,board=data.split('|',1)
    if board not in BOARDS:return
    if board.split(':',1)[0] not in ('sunwin','lc79'):return
    set_selected_board(chat_id,board)
    if action=='sel':
        _v50_start_auto(chat_id,board)
        txt=format_prediction(board,get_shared_prediction(board))
    elif action=='now':txt=format_prediction(board,get_shared_prediction(board))
    elif action=='auto':
        _v50_start_auto(chat_id,board);txt=format_prediction(board,get_shared_prediction(board))
    elif action=='hist':txt=format_history(board)
    elif action=='rounds':txt=format_round_history(board,15)
    elif action=='cau':txt=bot_cau_text(board,24)
    elif action=='ai':txt=await ai_explain_board(client,board)
    else:return
    await tg_panel(client,q,txt,bot_board_keyboard(board,chat_id))

async def telegram_loop():
    offset=0
    async with httpx.AsyncClient() as client:
        try:
            await tg_call(client,'setMyName',{'name':BOT_NAME})
            await tg_call(client,'setMyShortDescription',{'short_description':'💯 SUNWIN • LC79 • MD5/SHA256 tự nhận diện'})
            await tg_call(client,'setMyDescription',{'description':f'{BOT_NAME} · SUNWIN & LC79 · tự nhận diện MD5/SHA256 và phân tích Tài/Xỉu.'})
        except Exception:pass
        try:
            user_commands=[
                {'command':'start','description':'💯 Mở bot'},
                {'command':'game','description':'🎮 Chọn SUNWIN / LC79'},
                {'command':'dudoan','description':'🎯 Dự đoán bàn đang chọn'},
                {'command':'lichsu','description':'📜 Lịch sử húp/gãy'}
            ]
            await tg_call(client,'setMyCommands',{'commands':user_commands})
            admin_commands=user_commands+[
                {'command':'quantri','description':'◆ Bảng quản trị'},
                {'command':'thongke','description':'📊 Thống kê user'},
                {'command':'nguoidung','description':'👥 Danh sách user'},
                {'command':'thongtin','description':'👤 Chi tiết user'},
                {'command':'capquyen','description':'✅ Cấp quyền user'},
                {'command':'thuquyen','description':'⛔ Thu quyền user'},
                {'command':'kiemtraapi','description':'📡 Trạng thái API'},
                {'command':'doapi','description':'🔧 Đổi API bàn'},
                {'command':'linkgame','description':'↗ Đặt link game'}
            ]
            for aid in ADMIN_IDS:
                try:await tg_call(client,'setMyCommands',{'commands':admin_commands,'scope':{'type':'chat','chat_id':int(aid)}})
                except:pass
        except Exception:pass
        try:await tg_call(client,'deleteWebhook',{'drop_pending_updates':False})
        except:pass
        while True:
            try:
                result=await tg_call(client,'getUpdates',{'offset':offset,'timeout':BOT_POLL_TIMEOUT,'allowed_updates':['message','callback_query','my_chat_member']}) or []
                for upd in result:
                    offset=max(offset,int(upd.get('update_id',0))+1)
                    if upd.get('message'):await bot_handle_message(client,upd['message'])
                    elif upd.get('callback_query'):await bot_handle_callback(client,upd['callback_query'])
                    elif upd.get('my_chat_member'):await bot_handle_my_chat_member(client,upd['my_chat_member'])
            except asyncio.CancelledError:raise
            except Exception:await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_task, _bot_task
    ensure_db()
    ensure_hash_tables()
    try:create_db_backup('startup',10)
    except Exception:pass
    mark_background_started(None)
    _worker_task=asyncio.create_task(worker_loop())
    if BOT_TOKEN:
        _bot_task=asyncio.create_task(telegram_loop())
    yield
    for task in (_worker_task,_bot_task):
        if task:
            task.cancel()
            try: await task
            except BaseException: pass

app=FastAPI(title='ONGCHUNHACAI V51 Hash Ultra Auto Group API', lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['GET'],allow_headers=['*'])

@app.get('/api/health')
def health():
    games=_known_games()
    return {'ok':True,'worker_last_cycle':_last_cycle,'poll_seconds':POLL_SECONDS,'db':DB_PATH,
            'persistent_volume':str(DB_PATH).startswith('/data/'),'bot_enabled':bool(BOT_TOKEN),'admin_count':len(ADMIN_IDS),
            'max_history':MAX_HISTORY,'games_online':sum(1 for g in games if game_operational(g)),
            'games_total':len(games),'chatgpt_enabled':bool(OPENAI_API_KEY and OPENAI_MODEL),
            'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51','background':background_status_payload(),'sync_version':'V51_HASH_ULTRA_AUTO_GROUP'}

@app.get('/api/learn/{game}/{table}')
def learn(game:str, table:str, limit:int=Query(1000,ge=20,le=1000), sub:str|None=None):
    if not game_operational(game):
        return JSONResponse({'error':'game unavailable','game':game,'status':game_setting(game)},status_code=503)
    if game=='baccarat':
        if sub:
            board=f'baccarat:{sub}'
            rows=load_rows(board,limit)
            return {'game':game,'table':table,'sub':sub,'rows':rows,'model':model_snapshot(rows,game,board),
                    'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,40)}
        with sqlite3.connect(DB_PATH) as db:
            names=[r[0].split(':',1)[1] for r in db.execute("SELECT DISTINCT board FROM rounds WHERE board LIKE 'baccarat:%' AND board<>'baccarat:main'")]
        tables={}
        for name in names:
            board=f'baccarat:{name}'; rows=load_rows(board,limit)
            tables[name]={'rows':rows,'model':model_snapshot(rows,game,board),'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,40)}
        return {'game':game,'table':table,'tables':tables}
    board=f'{game}:{table}'
    if board not in BOARDS:
        return JSONResponse({'error':'unknown board'},status_code=404)
    rows=load_rows(board,limit)
    state=None
    with sqlite3.connect(DB_PATH) as db:
        row=db.execute('SELECT updated_at,source_ok,last_error,model_json FROM board_state WHERE board=?',(board,)).fetchone()
        if row:
            state={'updated_at':row[0],'source_ok':bool(row[1]),'last_error':row[2], 'model':json.loads(row[3]) if row[3] else None}
    return {'game':game,'table':table,'rows':rows,'model':model_snapshot(rows,game,board),'state':state,
            'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,40)}

@app.get('/api/background/status')
def api_background_status():
    return background_status_payload()

@app.get('/api/timeout/status')
def api_timeout_status_legacy():
    return {'deprecated':True,'message':'V29 dùng nền 24/7','background':background_status_payload()}

@app.get('/api/sync/all')
def api_sync_all(limit:int=Query(80,ge=20,le=240)):
    boards={}
    for board in available_bot_boards():
        rows=load_rows(board,limit)
        with sqlite3.connect(DB_PATH) as db:
            st=db.execute('SELECT updated_at,source_ok,last_error,model_json FROM board_state WHERE board=?',(board,)).fetchone()
        state=None;model=None
        if st:
            try:model=json.loads(st[3]) if st[3] else None
            except:model=None
            state={'updated_at':st[0],'source_ok':bool(st[1]),'last_error':st[2]}
        boards[board]={'rows':rows,'model':model,'state':state,
                       'shared_prediction':get_shared_prediction(board),
                       'prediction_history':get_prediction_history(board,40)}
    return {'ok':True,'server_time':time.time(),'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51',
            'poll_seconds':POLL_SECONDS,'source_of_truth':'railway',
            'sync_id':int(_last_cycle*1000) if _last_cycle else 0,
            'boards':boards,'background':background_status_payload()}

@app.get('/api/history/all')
def api_history_all():
    data={}
    for b in available_bot_boards():
        data[b]={
            'label':board_label(b),
            'stats':history_stats(b,100),
            'prediction_history':get_prediction_history(b,40),
            'rounds':load_rows(b,20)
        }
    return {'ok':True,'source':'background-24x7','background':background_status_payload(),'boards':data}


@app.get('/api/predict/{game}/{table}')
def predict_api(game:str,table:str,sub:str|None=None):
    if not game_operational(game):
        return JSONResponse({'error':'game unavailable','game':game,'status':game_setting(game)},status_code=503)
    board=f'baccarat:{sub}' if game=='baccarat' and sub else f'{game}:{table}'
    p=get_shared_prediction(board)
    if not p: return JSONResponse({'error':'prediction unavailable'},status_code=404)
    return {'board':board,'prediction':p,'history':get_prediction_history(board,20)}

@app.get('/api/games')
def api_games():
    out=[]
    with sqlite3.connect(DB_PATH) as db:
        states={r[0]:(r[1],bool(r[2]),r[3]) for r in db.execute('SELECT board,updated_at,source_ok,last_error FROM board_state')}
    for b,c in BOARDS.items():
        if b=='baccarat:main': continue
        st=states.get(b);gs=game_setting(c['game'])
        out.append({'board':b,'game':c['game'],'table':c['table'],'kind':c['kind'],
                    'enabled':game_operational(c['game']),'game_status':gs.get('status'),'maintenance_reason':gs.get('reason'),
                    'play_url':gs.get('play_url'),'source_ok':st[1] if st else False,
                    'updated_at':st[0] if st else None,'error':st[2] if st else None})
    return {'ok':True,'boards':out,'max_history':MAX_HISTORY,'min_topup':MIN_TOPUP}


@app.get('/api/current/{game}/{table}')
def current_api(game:str,table:str):
    if not game_operational(game):
        return JSONResponse({'error':'game unavailable','game':game,'status':game_setting(game)},status_code=503)
    board=f'{game}:{table}'
    if board not in BOARDS: return JSONResponse({'error':'unknown board'},status_code=404)
    rows=load_rows(board,1)
    if not rows: return JSONResponse({'error':'no data'},status_code=404)
    r=rows[-1]
    result=display_pred(board,r.get('result'))
    return {'ok':True,'board':board,'session':r.get('id'),'result':result,'dice':r.get('dice',[]),
            'total':r.get('sum'),'md5':r.get('md5'),'meta':r.get('meta',{}),
            'prediction':get_shared_prediction(board)}


@app.get('/api/history/{game}/{table}')
def history_api(game:str,table:str,limit:int=Query(100,ge=1,le=1000)):
    if not game_operational(game):
        return JSONResponse({'error':'game unavailable','game':game,'status':game_setting(game)},status_code=503)
    board=f'{game}:{table}'
    if board not in BOARDS: return JSONResponse({'error':'unknown board'},status_code=404)
    rows=load_rows(board,limit)
    if board=='lc79:xocdia':
        rows=[dict(r,result=display_pred(board,r.get('result'))) for r in rows]
    return {'ok':True,'board':board,'count':len(rows),'rows':rows}

@app.get('/')
def index():
    return {'ok':True,'service':'ONGCHUNHACAI V43 COMPACT PRO · FUSION MAX GROUP BACCARAT','mode':'telegram-bot-only',
            'worker':'24/7','bot_enabled':bool(BOT_TOKEN),'boards':len(available_bot_boards()),
            'engine':'BOARD-META 55 + HASH-16 ENSEMBLE V51'}

@app.get('/{path:path}')
def no_web_fallback(path:str):
    return JSONResponse({'ok':False,'mode':'telegram-bot-only',
                         'message':'Web UI đã tắt. Dùng Telegram bot hoặc endpoint /api/*.'},status_code=404)

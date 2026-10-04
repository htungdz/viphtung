import os, json, math, time, asyncio, sqlite3, re
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
MAX_HISTORY = max(300, min(1000, int(os.getenv('MAX_HISTORY', '1000'))))
BOT_TOKEN = os.getenv('BOT_TOKEN','').strip()
PUBLIC_URL = os.getenv('PUBLIC_URL','').rstrip('/')
BOT_POLL_TIMEOUT = max(10, int(os.getenv('BOT_POLL_TIMEOUT','20')))
ADMIN_IDS = {int(x) for x in os.getenv('ADMIN_IDS','').replace(';',',').split(',') if x.strip().lstrip('-').isdigit()}
BOT_REQUIRE_ACCESS = os.getenv('BOT_REQUIRE_ACCESS','1').strip().lower() not in ('0','false','no','off')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY','').strip()
OPENAI_MODEL = os.getenv('OPENAI_MODEL','').strip()

BOARDS = {
    # HYBRID: current live endpoint decides the clock; KWIN history only backfills learning.
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
    'betvip:hu': {'game':'betvip','table':'hu','kind':'tx_single','current':'https://paying-hon-bullet-sms.trycloudflare.com/api/tx'},
    'betvip:md5': {'game':'betvip','table':'md5','kind':'tx_single','current':'https://paying-hon-bullet-sms.trycloudflare.com/api/txmd5'},
    'gb68:hu': {'game':'gb68','table':'hu','kind':'tx_single','current':'https://winds-fonts-seq-jaguar.trycloudflare.com/api/68/thuong'},
    'gb68:md5': {
        'game':'gb68','table':'md5','kind':'tx_pair',
        'current':'https://objectives-scanning-list-reliance.trycloudflare.com/api/68/md5',
        'current_fallbacks':['https://kwinstore.com/68gamebip/md5/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7'],
        'history':'https://kwinstore.com/68gamebip/md5/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'b52:hu': {'game':'b52','table':'hu','kind':'tx_single','current':'https://volunteers-executives-granted-liz.trycloudflare.com/taixiu'},
    'b52:md5': {'game':'b52','table':'md5','kind':'tx_single','current':'https://volunteers-executives-granted-liz.trycloudflare.com/txmd5'},
    'max789:hu': {'game':'max789','table':'hu','kind':'tx_single','current':'https://person-talent-mission-opening.trycloudflare.com/api/tx'},
    'max789:md5': {'game':'max789','table':'md5','kind':'tx_single','current':'https://person-talent-mission-opening.trycloudflare.com/api/txmd5'},
    'son789:hu': {'game':'son789','table':'hu','kind':'tx_single','current':'https://pregnancy-blake-debut-hybrid.trycloudflare.com/api/tx'},
    'son789:md5': {'game':'son789','table':'md5','kind':'tx_single','current':'https://pregnancy-blake-debut-hybrid.trycloudflare.com/api/txmd5'},
    'hitclub:hu': {'game':'hitclub','table':'hu','kind':'tx_single','current':'https://apihitclubhihi.onrender.com/api/hitclub/hu'},
    'hitclub:md5': {'game':'hitclub','table':'md5','kind':'tx_single','current':'https://apihitclubhihi.onrender.com/api/hitclub/md5'},
    'baccarat:main': {'game':'baccarat','table':'main','kind':'baccarat','current':'https://jjjjbcrsexxy-1.onrender.com/api/bcrvh11'},
}

_db_lock = asyncio.Lock()
_worker_task = None
_bot_task = None
_last_cycle = 0.0
_process_started_at = time.time()
_ml_cache = {}


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
            enabled_by INTEGER,
            updated_at REAL NOT NULL
        )''')
        now=time.time()
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
                'cycle':{'k':0,'r':0.0},'agreement':0.5,'engine':'BOARD-META 25 + BCR FILTER V39',
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
            'engine':'BOARD-META 25 + BCR FILTER V39','skills':len(active),'totalSkills':60,
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
    'SUFFIX_CONTEXT','FUSION_CORE'
)


# V34: mỗi board tự đánh giá strategy trên lịch sử của chính board đó.
# HITCLUB giữ CHAMPION vì người dùng báo đang chạy ổn; các board còn lại dùng
# walk-forward + consensus thay vì bê nguyên champion của HITCLUB sang.
BOARD_META_PROFILES = {
    'hitclub:hu':   {'mode':'champion','top_k':1,'wf_depth':160,'reverse_min':28,'reverse_gap':.16},
    'hitclub:md5':  {'mode':'champion','top_k':1,'wf_depth':160,'reverse_min':28,'reverse_gap':.16},
    'sunwin:hu':    {'mode':'consensus','top_k':4,'wf_depth':220,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('MARKOV_ORDER2','SUFFIX_CONTEXT','MULTI_WINDOW','HIGH_ORDER_MARKOV','FUSION_CORE')},
    'sunwin:sicbo': {'mode':'consensus','top_k':5,'wf_depth':240,'reverse_min':40,'reverse_gap':.22,
                     'prefer':('REGIME_ADAPTIVE','DECAYED_TRANSITION','RUN_LENGTH_MARKOV','FLIP_STATE_MARKOV',
                               'MOTIF_WEIGHTED','PERIODIC_MATCH','MARKOV_ORDER2','RUN_HAZARD','SUFFIX_CONTEXT','FUSION_CORE')},
    'lc79:hu':      {'mode':'consensus','top_k':4,'wf_depth':220,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('MARKOV_ORDER2','SUFFIX_CONTEXT','RUN_HAZARD','MULTI_WINDOW','FUSION_CORE')},
    'lc79:md5':     {'mode':'consensus','top_k':4,'wf_depth':240,'reverse_min':34,'reverse_gap':.18,
                     'prefer':('SUFFIX_CONTEXT','HIGH_ORDER_MARKOV','MARKOV_ORDER2','MULTI_WINDOW','FUSION_CORE')},
    'lc79:xocdia':  {'mode':'consensus','top_k':3,'wf_depth':200,'reverse_min':36,'reverse_gap':.20,
                     'prefer':('MARKOV_ORDER2','RUN_HAZARD','SUFFIX_CONTEXT','MULTI_WINDOW')},
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
                'prefer':('ANALOG_KNN','CONTEXT_ENTROPY','RUN_SURVIVAL','REGIME_ADAPTIVE',
                          'DECAYED_TRANSITION','MOTIF_WEIGHTED','MARKOV_ORDER2','FUSION_CORE')}
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

def _pure_strategy_predictions(seq):
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
      'SUFFIX_CONTEXT':_suffix_prediction(seq)
    }


def _bayes_rate(wins,total,prior_n=14,prior_p=.5):
    return (wins+prior_n*prior_p)/max(1,total+prior_n)

def walk_forward_strategy_stats(board,rows):
    profile=_profile_for(board); seq=_seq(rows); last_id=str(rows[-1].get('id')) if rows else ''
    key=(board,last_id,len(seq),profile.get('wf_depth',170))
    if key in _WF_CACHE:return _WF_CACHE[key]
    depth=int(profile.get('wf_depth',170));seq=seq[-max(depth+40,70):]
    names=[n for n in STRATEGY_NAMES if n not in ('ANTI_RAW','FUSION_CORE')]
    rec={n:[] for n in names};start=max(18,len(seq)-depth)
    for i in range(start,len(seq)):
        hist=seq[:i];actual=seq[i];preds=_pure_strategy_predictions(hist)
        for n in names:
            p=preds.get(n)
            if p in ('TÀI','XỈU'):rec[n].append(p==actual)
    out={}
    for n,vals in rec.items():
        def st(q):
            total=len(q);wins=sum(1 for x in q if x)
            return {'total':total,'win':wins,'loss':total-wins,
                    'win_rate':_bayes_rate(wins,total),'raw_win_rate':wins/total if total else .5}
        s20=st(vals[-20:]);s50=st(vals[-50:]);s100=st(vals[-100:])
        score=.54*s20['win_rate']+.31*s50['win_rate']+.15*s100['win_rate']
        if s20['total']<14:score-=.025
        if s50['total']<32:score-=.020
        out[n]={'short':s20,'mid':s50,'long':s100,'score':round(score,5),'samples':len(vals)}
    _WF_CACHE.clear();_WF_CACHE[key]=out
    return out

def _combined_quality(board,name,live,wf):
    ls=live.get(name,{});ws=wf.get(name,{})
    l20=ls.get('short',{});l50=ls.get('mid',{});w20=ws.get('short',{});w50=ws.get('mid',{})
    ln=int(l20.get('total',0));wn=int(w20.get('total',0))
    lw=min(.62,ln/38*.62);ww=1-lw
    lr=.62*float(l20.get('win_rate',.5))+.38*float(l50.get('win_rate',.5))
    wr=.64*float(w20.get('win_rate',.5))+.36*float(w50.get('win_rate',.5))
    q=lw*lr+ww*wr
    if name in _profile_for(board).get('prefer',()):q+=.012
    if wn<18:q-=.020
    if int(l20.get('loss_streak',0))>=3:q-=.045
    # Strategies worse than chance on both recent windows should not lead.
    if wn>=18 and float(w20.get('raw_win_rate',.5))<.44:q-=.035
    return _clamp(q,.34,.69)

def _meta_consensus(board,preds,live,wf):
    prof=_profile_for(board);c=[]
    for n,p in preds.items():
        if p.get('prediction') not in ('TÀI','XỈU'):continue
        q=_combined_quality(board,n,live,wf)
        if n=='ANTI_RAW' and live.get(n,{}).get('short',{}).get('total',0)<14:continue
        if q<.485 and wf.get(n,{}).get('short',{}).get('total',0)>=18:continue
        c.append({'name':n,'prediction':p['prediction'],'quality':q,'edge':q-.5})
    c.sort(key=lambda x:x['quality'],reverse=True)
    families={
      'markov':{'MARKOV_TRANSITION','MARKOV_ORDER2','HIGH_ORDER_MARKOV','DECAYED_TRANSITION','FLIP_STATE_MARKOV','RUN_LENGTH_MARKOV','CONTEXT_ENTROPY'},
      'motif':{'SUFFIX_CONTEXT','MOTIF_WEIGHTED','PERIODIC_MATCH','ANALOG_KNN'},
      'regime':{'REGIME_ADAPTIVE','MULTI_WINDOW','DUAL_HORIZON','BIAS_MOMENTUM','BIAS_MEAN_REVERSION'},
      'run':{'RUN_HAZARD','RUN_BREAK','RUN_FOLLOW','RUN_SURVIVAL'},
      'other':{'FOLLOW_LAST','REVERSE_LAST','ALTERNATING_PATTERN','ANTI_RAW','FUSION_CORE'}
    }
    def fam(name):
        for k,v in families.items():
            if name in v:return k
        return 'other'
    top=[];counts={};target=max(1,int(prof.get('top_k',3)))
    for x in c:
        f=fam(x['name'])
        if counts.get(f,0)>=2:continue
        top.append(x);counts[f]=counts.get(f,0)+1
        if len(top)>=target:break
    if len(top)<target:
        for x in c:
            if x not in top:
                top.append(x)
                if len(top)>=target:break
    vt=vx=0.0
    for x in top:
        w=max(.015,x['edge']+.02)
        if x['prediction']=='TÀI':vt+=w
        else:vx+=w
    if abs(vt-vx)<.015:final=top[0]['prediction'] if top else 'TÀI'
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
        fusion['engine']='BOARD-META 25 + BCR FILTER V39';return fusion
    preds=_strategy_predictors(board,rows,fusion)
    live=get_strategy_stats_map(board);wf=walk_forward_strategy_stats(board,rows);prof=_profile_for(board)
    champion,_=select_champion(board,preds);recent=_final_performance(board)
    if prof.get('mode')=='champion':
        raw=preds[champion]['prediction'];decision=champion;decision_mode='CHAMPION'
        top=[{'name':champion,'prediction':raw,'quality':_combined_quality(board,champion,live,wf)}];agree=1.0
    else:
        raw,top,agree=_meta_consensus(board,preds,live,wf);decision='META['+', '.join(x['name'] for x in top[:3])+']';decision_mode='BOARD_META'

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
    top_quality=max((float(x.get('quality',.5)) for x in top),default=.5)
    if agree<.56:conf=min(conf,54)
    if top_quality<.525:conf=min(conf,55)
    elif top_quality<.545:conf=min(conf,59)
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
      'engine':'BOARD-META 25 + BCR FILTER V39','totalStrategies':len(STRATEGY_NAMES),'updated_at':time.time()})
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
            await asyncio.gather(*(poll_board(client,b,c) for b,c in BOARDS.items()), return_exceptions=True)
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
    base=[b for b in BOARDS if b!='baccarat:main']
    with sqlite3.connect(DB_PATH) as db:
        bcr=[r[0] for r in db.execute("SELECT DISTINCT board FROM rounds WHERE board LIKE 'baccarat:%' AND board<>'baccarat:main' ORDER BY board")]
    return base+bcr


def is_group_chat_id(chat_id):
    try:return int(chat_id)<0
    except:return False

def get_group_settings(chat_id):
    if not is_group_chat_id(chat_id):
        return {'enabled':False,'auto_delete':False,'delete_after':5.0,'title':None}
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute(
            'SELECT title,enabled,auto_delete,delete_after,enabled_by,updated_at FROM bot_group_settings WHERE chat_id=?',
            (int(chat_id),)
        ).fetchone()
    if not r:
        return {'enabled':False,'auto_delete':True,'delete_after':5.0,'title':None,'enabled_by':None}
    return {'title':r[0],'enabled':bool(r[1]),'auto_delete':bool(r[2]),
            'delete_after':float(r[3] or 5),'enabled_by':r[4],'updated_at':r[5]}

def group_enabled(chat_id):
    return bool(get_group_settings(chat_id).get('enabled'))

def set_group_enabled(chat_id,enabled,admin_id=None,title=None):
    if not is_group_chat_id(chat_id):raise ValueError('group only')
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO bot_group_settings(chat_id,title,enabled,auto_delete,delete_after,enabled_by,updated_at)
               VALUES(?,?,?,1,5,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 title=COALESCE(excluded.title,bot_group_settings.title),
                 enabled=excluded.enabled,enabled_by=excluded.enabled_by,updated_at=excluded.updated_at''',
            (int(chat_id),title,1 if enabled else 0,int(admin_id) if admin_id is not None else None,now)
        )
        if not enabled:
            db.execute('UPDATE bot_subscriptions SET enabled=0 WHERE chat_id=?',(int(chat_id),))
        db.commit()

def set_group_delete(chat_id,enabled,delay=5.0,admin_id=None,title=None):
    if not is_group_chat_id(chat_id):raise ValueError('group only')
    now=time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            '''INSERT INTO bot_group_settings(chat_id,title,enabled,auto_delete,delete_after,enabled_by,updated_at)
               VALUES(?,?,0,?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 title=COALESCE(excluded.title,bot_group_settings.title),
                 auto_delete=excluded.auto_delete,delete_after=excluded.delete_after,
                 enabled_by=COALESCE(excluded.enabled_by,bot_group_settings.enabled_by),
                 updated_at=excluded.updated_at''',
            (int(chat_id),title,1 if enabled else 0,float(delay),
             int(admin_id) if admin_id is not None else None,now)
        )
        db.commit()

def group_status_text(chat_id):
    g=get_group_settings(chat_id)
    return (f"👥 GROUP CONTROL · {BOT_UI_VERSION}\\n{BOT_DIV}\\n"
            f"Group ID: {chat_id}\\n"
            f"Bot lệnh: {'🟢 BẬT' if g.get('enabled') else '🔴 TẮT'}\\n"
            f"Xóa tin user: {'🟢 ON' if g.get('auto_delete') else '🔴 OFF'}"
            f" · {int(g.get('delete_after',5))}s\\n"
            "Admin bot dùng /groupon để mở lệnh cho nhóm.")

def list_groups_text():
    with sqlite3.connect(DB_PATH) as db:
        rows=db.execute(
            'SELECT chat_id,title,enabled,auto_delete,delete_after,updated_at FROM bot_group_settings ORDER BY updated_at DESC LIMIT 100'
        ).fetchall()
    lines=['👥 GROUPS · V39',BOT_DIV]
    for cid,title,en,ad,delay,updated in rows:
        lines.append(f"{'🟢' if en else '🔴'} {cid} · {title or 'Không tên'} · DEL {'ON' if ad else 'OFF'} {int(delay or 5)}s")
    if len(lines)==2:lines.append('Chưa có nhóm nào được cấu hình.')
    return '\\n'.join(lines)[:4000]


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
        [{'text':'＋ 1 DAY','callback_data':f'uadd|{uid}|1d'},{'text':'＋ 7 DAYS','callback_data':f'uadd|{uid}|7d'}],
        [{'text':'＋ 30 DAYS','callback_data':f'uadd|{uid}|30d'},{'text':'♾ VĨNH VIỄN','callback_data':f'ulife|{uid}'}],
        [{'text':'🔒 KHÓA USER','callback_data':f'ulock|{uid}'},{'text':'↻ XEM LẠI','callback_data':f'uinfo|{uid}'}],
        [{'text':'‹ ADMIN','callback_data':'adminhome'}]
    ]}


def permission_text(chat_id):
    p=get_permissions(chat_id); info=access_info(chat_id)
    status='ADMIN' if is_admin(chat_id) else ('ĐANG DÙNG' if info.get('active') else 'HẾT HẠN / KHÓA')
    ico=lambda v:'✅' if v else '🔒'
    if is_admin(chat_id): exp='VĨNH VIỄN'
    elif info.get('expires_at') is None and info.get('active'): exp='VĨNH VIỄN'
    elif info.get('expires_at'): exp=time.strftime('%d/%m/%Y %H:%M',time.localtime(float(info['expires_at'])))+' · còn '+_fmt_remaining(info.get('remaining'))
    else: exp='---'
    return (f"👤 USER {chat_id}\n{BOT_DIV}\nTrạng thái: {status}\n"
            f"Gói: {str(p.get('preset','-')).upper()} · {info.get('label','-')}\nHạn dùng: {exp}\n"
            f"{ico(p['predict'])} Dự đoán  {ico(p['history'])} Lịch sử\n{ico(p['ai'])} GPT  {ico(p['auto'])} AUTO\n"
            f"🛡 Admin: {'CÓ' if is_admin(chat_id) else 'KHÔNG'}")


def locked_text(chat_id,feature):
    names={'predict':'Dự đoán','history':'Lịch sử','ai':'GPT Deep Analysis','auto':'AUTO'}; info=access_info(chat_id)
    if not info.get('active'):
        return (f"⛔ QUYỀN SỬ DỤNG ĐÃ HẾT\n{BOT_DIV}\nUser ID: {chat_id}\n"
                "Liên hệ admin để gia hạn 1D / 7D / 30D / Vĩnh viễn.")
    return (f"🔒 {names.get(feature,feature)} chưa được mở.\nUser ID: {chat_id}\nAdmin dùng: /permit {chat_id} {feature}")


def admin_help():
    return (f"◆ QUẢN LÝ USER · {BOT_UI_VERSION}\n{BOT_DIV}\n"
            "🎟 CẤP FULL USER (không có quyền admin)\n/adduser ID 1d\n/adduser ID 7d\n/adduser ID 30d\n"
            "/lifetime ID\n/extend ID 7d\n/userinfo ID\n/users\n/lock ID\n\n"
            "⏱ Thời gian: 30m · 12h · 1d · 7d · 30d · 4w · forever\n\n"
            "🔧 QUYỀN TÙY CHỈNH\n/permit ID predict|history|ai|auto\n/deny ID predict|history|ai|auto\n/perms ID\n\n"
            "⚙️ HỆ THỐNG\n/background · /health · /stats\n/engine [board] · /deep [board]\n"
            "/testapi <board> · /apis\n/setapi <board> <current_url> [history_url]\n/resetapi <board>")


def user_help(chat_id):
    p=get_permissions(chat_id)
    lines=[f"✦ HƯỚNG DẪN · {BOT_UI_VERSION}",BOT_DIV,
           "/start · bảng điều khiển","/games · danh sách game","/status · dự đoán bàn đang chọn",
           "/me · quyền tài khoản","/id · User/Group ID","/bcrtop · lọc bàn Baccarat đẹp"]
    if p.get('history'):lines += ["/history · đúng/sai","/results · KQ game","/historyall · toàn bộ game"]
    if p.get('ai'):lines.append("/ai · GPT phân tích")
    if p.get('auto'):lines.append("/auto · bật/tắt AUTO")
    return '\n'.join(lines)

def admin_health_text():
    with sqlite3.connect(DB_PATH) as db:
        state={b:(u,ok,err) for b,u,ok,err in db.execute('SELECT board,updated_at,source_ok,last_error FROM board_state')}
    now=time.time();lines=['🩺 API HEALTH · V39']
    for b in BOARDS:
        u,ok,err=state.get(b,(0,0,'chưa có dữ liệu'))
        age=max(0,int(now-u)) if u else -1
        icon='🟢' if ok and age<=max(15,int(POLL_SECONDS*8)) else '🟡' if ok else '🔴'
        suffix=f' · {age}s' if age>=0 else ''
        lines.append(f"{icon} {board_label(b)}{suffix}"+(f" · {str(err)[:55]}" if not ok and err else ''))
    return '\n'.join(lines)[:4000]


def admin_stats_text():
    with sqlite3.connect(DB_PATH) as db:
        users=db.execute('SELECT COUNT(*) FROM bot_access').fetchone()[0]
        active=db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1').fetchone()[0]
        ai_users=db.execute('SELECT COUNT(*) FROM bot_permissions WHERE can_ai=1').fetchone()[0]
        hist_users=db.execute('SELECT COUNT(*) FROM bot_permissions WHERE can_history=1').fetchone()[0]
        auto_users=db.execute('SELECT COUNT(*) FROM bot_permissions WHERE can_auto=1').fetchone()[0]
        rounds=db.execute('SELECT COUNT(*) FROM rounds').fetchone()[0]
        preds=db.execute('SELECT COUNT(*) FROM shared_predictions').fetchone()[0]
        settled=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE actual IS NOT NULL').fetchone()[0]
        wins=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE ok=1').fetchone()[0]
        boards=db.execute('SELECT COUNT(DISTINCT board) FROM rounds').fetchone()[0]
        groups=db.execute('SELECT COUNT(*) FROM bot_group_settings WHERE enabled=1').fetchone()[0]
    hit=(wins/settled*100) if settled else 0.0
    return (f"📊 STATS V39\nUser mở: {active}/{users} · AI {ai_users} · LS {hist_users} · AUTO {auto_users}\n"
            f"Board có dữ liệu: {boards} · Group ON: {groups}\nRound đang lưu: {rounds}\n"
            f"Prediction: {preds} · đã chốt {settled}\n"
            f"Đúng lịch sử: {wins}/{settled} ({hit:.1f}%)\n"
            "Tỷ lệ lịch sử chỉ để theo dõi, không phải bảo đảm cho phiên kế.")


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


GAME_TITLES={
    'sunwin':'☀️ SUNWIN','lc79':'🎲 LC79','betvip':'💎 BETVIP','gb68':'🎰 68GB',
    'b52':'🅱️ B52','max789':'👑 MAX789','son789':'✨ SON789','hitclub':'🔥 HITCLUB','baccarat':'🃏 BACCARAT'
}
GAME_ORDER=('sunwin','lc79','hitclub','max789','son789','gb68','betvip','b52','baccarat')


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


BOT_UI_VERSION='V39'
BOT_DIV='━━━━━━━━━━━━━━━━━━'

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
    p=get_permissions(chat_id)
    preset='ADMIN' if is_admin(chat_id) else str(p.get('preset','BASIC')).upper()
    bg=background_status_payload();boards=available_bot_boards()
    live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    selected=get_selected_board(chat_id);info=access_info(chat_id)
    remain='ADMIN' if is_admin(chat_id) else _fmt_remaining(info.get('remaining'))
    lines=[f"✦ TAIXIUTOOL {BOT_UI_VERSION} · {preset}",
           f"♾ {'LIVE' if bg.get('active') else 'CHỜ'} · 📡 {live}/{len(boards)} · 🎟 {remain}"]
    if selected and selected in boards:
        pred=get_shared_prediction(selected);c=current_cau(selected,8)
        if pred:lines.append(f"🎮 {board_label(selected)} · #{pred.get('session')} → {display_pred(selected,pred.get('prediction'))} {pred.get('confidence')}%")
        else:lines.append(f"🎮 {board_label(selected)} · đang học")
        if c['count']:lines.append(f"〽️ {c['text']}")
    else:lines.append("🎮 Chọn game để bắt đầu")
    return '\n'.join(lines)[:4000]


def bot_game_text(game,chat_id):
    if game=='baccarat':return baccarat_top_text(6)
    lines=[f"{GAME_TITLES.get(game,game)} · GAME CENTER",BOT_DIV]
    for board in boards_for_game(game):
        src=_board_source(board);rows=load_rows(board,1);latest=rows[-1] if rows else None
        pred=get_shared_prediction(board);c=current_cau(board,6)
        kq=f"#{latest.get('id')} {display_pred(board,latest.get('result'))}" if latest else '---'
        nx=f"#{pred.get('session')} {display_pred(board,pred.get('prediction'))} {pred.get('confidence')}%" if pred else 'đang học'
        lines.append(f"{src['icon']} {_short_board_label(board)} · KQ {kq}")
        lines.append(f"   ➜ {nx} · 〽️ {c['text']}")
    return '\\n'.join(lines)[:4000]


def bot_cau_text(board,limit=24):
    c=current_cau(board,limit); rows=load_rows(board,max(30,limit))[-limit:]
    lines=[f"〽️ CẦU · {board_label(board)}",BOT_DIV]
    if not rows:return '\n'.join(lines+['Chưa có dữ liệu.'])
    lines.append(c['text'])
    if c['icons']:lines.append(c['icons'])
    vals=[r.get('result') for r in rows if r.get('result') in ('TÀI','XỈU')]
    rs=_run_stats(vals)
    if rs.get('current'):lines.append(f"Nhịp hiện tại: {display_pred(board,rs.get('current_side'))} ×{rs.get('current')}")
    lines.append(f"Mẫu: {len(vals)} phiên gần nhất")
    return '\n'.join(lines)[:4000]

def bot_account_text(chat_id):
    p=get_permissions(chat_id);board=get_selected_board(chat_id);info=access_info(chat_id)
    if is_admin(chat_id):expiry='VĨNH VIỄN · ADMIN'
    elif info.get('expires_at') is None and info.get('active'):expiry='VĨNH VIỄN'
    elif info.get('expires_at'):expiry=time.strftime('%d/%m %H:%M',time.localtime(float(info['expires_at'])))+f" · còn {_fmt_remaining(info.get('remaining'))}"
    else:expiry='HẾT HẠN'
    return (f"👤 TÀI KHOẢN\n{BOT_DIV}\nID: {chat_id}\n"
            f"Gói: {'ADMIN' if is_admin(chat_id) else str(p.get('preset','-')).upper()}\n🎟 Hạn: {expiry}\n"
            f"{'✅' if p.get('predict') else '🔒'} Dự đoán  {'✅' if p.get('history') else '🔒'} Lịch sử\n"
            f"{'✅' if p.get('ai') else '🔒'} GPT  {'✅' if p.get('auto') else '🔒'} AUTO\n"
            f"🎮 Bàn: {board_label(board) if board else 'chưa chọn'}")


def bot_admin_home_text():
    bg=background_status_payload()
    with sqlite3.connect(DB_PATH) as db:
        users=db.execute('SELECT COUNT(*) FROM bot_access WHERE enabled=1').fetchone()[0]
        settled=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE actual IS NOT NULL').fetchone()[0]
        wins=db.execute('SELECT COUNT(*) FROM shared_predictions WHERE ok=1').fetchone()[0]
    rate=round(wins/settled*100,1) if settled else 0
    boards=available_bot_boards();live=sum(1 for b in boards if _board_source(b)['icon']=='🟢')
    return (f"◆ ADMIN CONTROL · {BOT_UI_VERSION}\n{BOT_DIV}\n"
            f"♾ Worker 24/7: {'LIVE' if bg.get('active') else 'CHỜ'}\n"
            f"📡 API: {live}/{len(boards)} live\n"
            f"👥 User active: {users}\n"
            f"🎟 /adduser ID 1d|7d|30d\n"
            f"📊 Lịch sử chốt: {settled} · ✅ {rate}%\n"
            f"🧠 Engine: BOARD-META 25\n\nChọn bảng điều khiển:")

async def tg_panel(client,q,text,reply_markup=None):
    msg=(q or {}).get('message') or {};chat_id=(msg.get('chat') or {}).get('id');message_id=msg.get('message_id')
    if not chat_id:return None
    payload={'chat_id':chat_id,'message_id':message_id,'text':text,'disable_web_page_preview':True}
    if reply_markup is not None:payload['reply_markup']=reply_markup
    try:
        url=f'https://api.telegram.org/bot{BOT_TOKEN}/editMessageText'
        r=await client.post(url,json=payload,timeout=30);data=r.json()
        if data.get('ok'):return data.get('result')
        if 'message is not modified' in str(data.get('description','')).lower():return msg
    except Exception:pass
    send={'chat_id':chat_id,'text':text,'disable_web_page_preview':True}
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
    available={b.split(':',1)[0] for b in available_bot_boards()}
    buttons=[{'text':GAME_TITLES[g],'callback_data':'game|'+g} for g in GAME_ORDER if g in available]
    rows=[buttons[i:i+2] for i in range(0,len(buttons),2)]
    quick=[]
    if get_permissions(chat_id).get('history'):quick.append({'text':'📚 LS ALL','callback_data':'histall'})
    quick.append({'text':'👤 TÀI KHOẢN','callback_data':'account'})
    rows.append(quick)
    if feature_allowed(chat_id,'auto'):
        rows.append([{'text':'🔔 AUTO ALL','callback_data':'allon'},{'text':'🔕 TẮT ALL','callback_data':'alloff'}])
    if is_admin(chat_id):rows.append([{'text':'◆ ADMIN','callback_data':'adminhome'}])
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
            x=metric[board];acc='--' if x['accuracy'] is None else f"{x['accuracy']:.0f}%"
            txt=f"{src['icon']} {x['grade']} {_short_board_label(board)} · {x['score']:.1f} · W{acc}"
        else:
            txt=f"{src['icon']} {auto} {_short_board_label(board)} · {_compact_cau(board,6)}"
        rows.append([{'text':txt,'callback_data':'sel|'+board}])
    if game=='baccarat':
        rows.append([{'text':'↻ LỌC LẠI TOP BÀN','callback_data':'game|baccarat'}])
    rows.append([{'text':'⌂ HOME','callback_data':'home'},{'text':'🎮 TẤT CẢ GAME','callback_data':'games'}])
    return {'inline_keyboard':rows}


def bot_board_keyboard(board,chat_id=None):
    p=get_permissions(chat_id) if chat_id is not None else {'predict':True,'history':True,'ai':True,'auto':True}
    on=sub_enabled(chat_id,board) if chat_id is not None and p.get('auto') else False
    game=board.split(':',1)[0]
    rows=[
      [{'text':'↻ MỚI' if p.get('predict') else '🔒 DỰ ĐOÁN',
        'callback_data':(('now|' if p.get('predict') else 'locked|predict|')+board)},
       {'text':('🔕 AUTO ON' if on else '🔔 AUTO OFF') if p.get('auto') else '🔒 AUTO',
        'callback_data':(('auto|' if p.get('auto') else 'locked|auto|')+board)}],
      [{'text':'📜 HÚP/GÃY' if p.get('history') else '🔒 LỊCH SỬ',
        'callback_data':(('hist|' if p.get('history') else 'locked|history|')+board)},
       {'text':'🧾 KQ' if p.get('history') else '🔒 KẾT QUẢ',
        'callback_data':(('rounds|' if p.get('history') else 'locked|history|')+board)}],
      [{'text':'〽️ CẦU','callback_data':'cau|'+board},
       {'text':'🧠 GPT' if p.get('ai') else '🔒 GPT',
        'callback_data':(('ai|' if p.get('ai') else 'locked|ai|')+board)}]
    ]
    if is_admin(chat_id):rows.append([{'text':'⚙️ ENGINE','callback_data':'diag|'+board}])
    rows.append([{'text':'‹ '+GAME_TITLES.get(game,game),'callback_data':'game|'+game},
                 {'text':'⌂ HOME','callback_data':'home'}])
    return {'inline_keyboard':rows}

def bot_admin_keyboard():
    return {'inline_keyboard':[
      [{'text':'🎟 QUẢN LÝ USER','callback_data':'adm|permhelp'},{'text':'📊 THỐNG KÊ','callback_data':'adm|stats'}],
      [{'text':'📡 API HEALTH','callback_data':'adm|health'},{'text':'♾ NỀN 24/7','callback_data':'adm|background'}],
      [{'text':'🧠 ENGINE','callback_data':'adm|engine'},{'text':'🧮 DEEP','callback_data':'adm|deep'}],
      [{'text':'🔗 API LINKS','callback_data':'adm|apis'},{'text':'🎲 SICBO API','callback_data':'adm|sicboapi'}],
      [{'text':'🎮 GAME','callback_data':'games'}],
      [{'text':'⌂ HOME','callback_data':'home'}]
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
    if not rows:
        return f"🧾 {board_label(board)} · chưa có lịch sử kết quả."
    lines=[f"🧾 KẾT QUẢ {board_label(board)} · {len(rows)} phiên"]
    for r in reversed(rows):
        lines.append(_round_line(board,r))
    return '\n'.join(lines)[:4000]


def format_all_history(limit_per_board=40):
    lines=['📚 LỊCH SỬ ALL GAME · HÚP/GÃY']
    count=0
    for b in available_bot_boards():
        st=history_stats(b,limit_per_board)
        rows=load_rows(b,1)
        last=rows[-1] if rows else None
        if st['settled']==0 and not last:
            continue
        acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--'
        streak=''
        if st['streak']:
            streak=f" · {'🔥' if st['streak_ok'] else '👾'}x{st['streak']}"
        last_txt=(f" · KQ #{last.get('id')} {display_pred(b,last.get('result'))}" if last else '')
        lines.append(f"{board_label(b)} · 🔥{st['wins']} 👾{st['losses']} · {acc}{streak}{last_txt}")
        count+=1
        if len('\n'.join(lines))>3600:
            break
    if count==0:
        lines.append('Chưa có lịch sử.')
    return '\n'.join(lines)[:4000]


TIMEOUT_SECONDS=2*60*60

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
    report={'type':'TAIXIUTOOL_AUTO_TRAIN_2H_REPORT','version':'V28','run_id':run['id'],
            'admin_chat_id':run['admin_chat_id'],'started_at':start,'ends_at':end,
            'duration_seconds':int(end-start),'generated_at':time.time(),
            'engine':'BOARD-META 25 + BCR FILTER V39','boards':{},
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
                        filename=f"TAIXIUTOOL_AUTO_TRAIN_2H_{r['id']}_{stamp}.json"
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
    p=background_status_payload()
    age=p.get('last_cycle_age')
    age_txt=f"{age}s" if age is not None else 'đang khởi động'
    return (f"♾ NỀN 24/7 · ĐANG CHẠY\n"
            f"Worker: {age_txt} · lưu {p['rounds_saved']} phiên\n"
            f"Prediction đã lưu: {p['predictions_saved']} · đã chốt {p['settled_saved']}\n"
            "Tự poll ALL GAME, lưu lịch sử và cập nhật engine kể cả khi không mở web/Telegram.\n"
            "Railway restart/redeploy xong worker tự chạy lại; SQLite /data giữ lịch sử.")


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
    src=_board_source(board)
    if not pred:return f"◆ {board_label(board)} · {src['icon']}\n⏳ Đang đồng bộ prediction…"
    model=pred.get('model') or {};st=history_stats(board,20)
    acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--'
    recent=get_prediction_history(board,12)
    last=next((x for x in recent if x.get('actual') is not None),None)
    cau=current_cau(board,10)
    lines=[f"◆ {board_label(board)} · {src['icon']}",f"〽️ {cau['text']}",BOT_DIV]
    if last:
        verdict='🔥 HÚP!!' if last.get('ok') else '👾 GÃY CMNR!!'
        detail=prediction_result_detail(board,last)
        prev=display_pred(board,last.get('prediction'));actual=display_pred(board,last.get('actual'))
        lines.append(f"↩ #{last.get('session')} · {verdict}")
        if board=='lc79:xocdia':
            lines.append(f"   {prev}→{actual}")
        elif board.startswith('baccarat:'):
            lines.append(f"   {prev}→{actual}")
        else:
            d=detail.get('dice') or [];total=detail.get('sum')
            if len(d)>=3:
                if total is None:total=sum(int(v) for v in d[:3])
                lines.append(f"   {prev}→{actual} · 🎲 {d[0]}-{d[1]}-{d[2]}={total}")
            else:
                lines.append(f"   {prev}→{actual} · 🎲 chưa có dữ liệu")
    else:
        lines.append("↩ Chưa có phiên dự đoán đã chốt")
    agree=round(float(model.get('meta_agreement',.5))*100)
    lines += [
        f"➜ #{pred.get('session','---')} · {display_pred(board,pred.get('prediction'))} · {pred.get('confidence',50)}%",
        f"📊 W20 🔥{st['wins']} 👾{st['losses']} · {acc} · META {agree}%"
    ]
    mode=model.get('decision_mode') or model.get('mode','BOARD_META')
    top=model.get('meta_top') or []
    if top:
        top_txt=' / '.join(str(x.get('name','?')).replace('_',' ') for x in top[:2])
        lines.append(f"🧠 {mode} · {model.get('status','---')} · {top_txt}")
    elif model.get('strategy_champion'):
        lines.append(f"🧠 {mode} · {model.get('status','---')} · {model.get('strategy_champion')}")
    df=model.get('dice_forecast') or {}
    if board=='sunwin:sicbo':
        if df.get('ready'):
            fv='-'.join(str(x.get('face','?')) for x in df.get('faces',[])[:3])
            lines.append(f"🎲 Vị kế {fv} · tổng {df.get('sum_zone','---')}")
        hs=model.get('sicbo_hash') or {}
        if hs.get('ready'):
            lines.append(f"🔐 HASH {'ON' if hs.get('usable') else 'HỌC'} · {round(float(hs.get('quality',.5))*100,1)}% · n={hs.get('sample',0)}")
    xf=model.get('xocdia_forecast') or {}
    if board=='lc79:xocdia' and xf:lines.append(f"⚪🟣 {xf.get('label','---')}")
    return '\n'.join(lines)[:4000]


def format_history(board,limit=12):
    h=get_prediction_history(board,limit)
    if not h:return f"📜 {board_label(board)} · chưa có lịch sử."
    st=history_stats(board,max(20,limit))
    acc=f"{st['accuracy']}%" if st['accuracy'] is not None else '--'
    lines=[f"📜 {board_label(board)} · 🔥{st['wins']} 👾{st['losses']} · {acc}",BOT_DIV]
    for x in h:
        pred=display_pred(board,x.get('prediction'))
        if x.get('actual') is None:
            lines.append(f"⏳ #{x.get('session')} · {pred} · {x.get('confidence')}%")
            continue
        mark='🔥 HÚP!!' if x.get('ok') else '👾 GÃY CMNR!!'
        actual=display_pred(board,x.get('actual'))
        detail=prediction_result_detail(board,x)
        suffix=''
        if not board.startswith('baccarat:') and board!='lc79:xocdia':
            d=detail.get('dice') or []
            if len(d)>=3:
                total=detail.get('sum')
                if total is None:total=sum(int(v) for v in d[:3])
                suffix=f" · 🎲{d[0]}-{d[1]}-{d[2]}={total}"
        lines.append(f"{mark} #{x.get('session')} · {pred}→{actual}{suffix}")
    return '\n'.join(lines)[:4000]


async def tg_call(client,method,payload=None):
    if not BOT_TOKEN: return None
    url=f'https://api.telegram.org/bot{BOT_TOKEN}/{method}'
    r=await client.post(url,json=payload or {},timeout=30)
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
        'windows':{str(w):_window_balance(seq,w) for w in (10,20,50,100,200,500) if seq},
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
        "Bạn là module phân tích thống kê chuỗi cho TAIXIUTOOL. Chỉ phân tích dữ liệu đã cung cấp; "
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


async def bot_handle_message(client,msg):
    chat=msg.get('chat') or {}
    chat_id=chat.get('id'); text=(msg.get('text') or '').strip()
    actor_id=(msg.get('from') or {}).get('id')
    is_group=is_group_chat_id(chat_id)
    admin_actor=is_admin(actor_id)
    if not chat_id:return

    if is_group:
        title=chat.get('title') or chat.get('username') or 'Telegram Group'
        if admin_actor and text.startswith('/groupon'):
            set_group_enabled(chat_id,True,actor_id,title)
            set_group_delete(chat_id,True,5,actor_id,title)
            schedule_group_user_delete(client,msg)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,
                'text':'🟢 BOT GROUP ĐÃ BẬT\n'+BOT_DIV+'\nLệnh đã mở cho nhóm. Tin user bot nhận được tự xóa sau 5s.\n'+group_status_text(chat_id)})
            return
        if admin_actor and text.startswith('/groupoff'):
            schedule_group_user_delete(client,msg)
            set_group_enabled(chat_id,False,actor_id,title)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'🔴 BOT GROUP ĐÃ TẮT. Lệnh trong nhóm bị khóa.'})
            return
        if admin_actor and text.startswith('/groupdelete'):
            parts=text.split()
            if len(parts)<2 or parts[1].lower() not in ('on','off'):
                out='Cú pháp: /groupdelete on hoặc /groupdelete off'
            else:
                on=parts[1].lower()=='on';set_group_delete(chat_id,on,5,actor_id,title)
                out=f"🧹 Auto-delete user: {'ON · 5s' if on else 'OFF'}"
            if group_enabled(chat_id):schedule_group_user_delete(client,msg)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
        if admin_actor and text.startswith('/groupstatus'):
            if group_enabled(chat_id):schedule_group_user_delete(client,msg)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':group_status_text(chat_id)});return

        if not group_enabled(chat_id):
            return

        schedule_group_user_delete(client,msg)
        if not text.startswith('/'):
            return

        if text.startswith('/admin'):
            if admin_actor:
                await tg_call(client,'sendMessage',{'chat_id':chat_id,
                    'text':bot_admin_home_text()+'\n\nℹ️ Quản lý user nhạy cảm nên dùng chat riêng với bot.',
                    'reply_markup':bot_admin_keyboard()})
            else:
                await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'⛔ Chỉ admin bot mở bảng quản trị.'})
            return

    if text.startswith('/admin'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,
            'text':bot_admin_home_text() if is_admin(chat_id) else '⛔ Không có quyền admin.',
            'reply_markup':bot_admin_keyboard() if is_admin(chat_id) else None}); return

    if is_admin(chat_id) and text.startswith('/adduser '):
        parts=text.split()
        try:
            if len(parts)!=3:raise ValueError()
            uid=int(parts[1]);dur=parts[2];info=grant_timed_access(uid,dur,chat_id,False)
            out=(f"✅ ĐÃ CẤP FULL USER\n{BOT_DIV}\nID: {uid}\nGói: VIP USER · tất cả chức năng thường\n"
                 f"Hạn: {_fmt_remaining(info.get('remaining')) if info.get('expires_at') is not None else 'VĨNH VIỄN'}\nAdmin: KHÔNG")
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out,'reply_markup':admin_user_keyboard(uid)});return
        except:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Cú pháp: /adduser <ID> <1d|7d|30d|4w|forever>'});return
    if is_admin(chat_id) and text.startswith('/extend '):
        parts=text.split()
        try:
            if len(parts)!=3:raise ValueError()
            uid=int(parts[1]);dur=parts[2];grant_timed_access(uid,dur,chat_id,True)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'⏱ ĐÃ GIA HẠN {uid} · +{dur.upper()}\n'+permission_text(uid),'reply_markup':admin_user_keyboard(uid)});return
        except:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Cú pháp: /extend <ID> <1d|7d|30d|4w>'});return
    if is_admin(chat_id) and text.startswith('/lifetime '):
        try:
            uid=int(text.split(maxsplit=1)[1]);grant_timed_access(uid,'forever',chat_id,False)
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'♾ ĐÃ CẤP VĨNH VIỄN\n'+permission_text(uid),'reply_markup':admin_user_keyboard(uid)});return
        except:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Cú pháp: /lifetime <ID>'});return
    if is_admin(chat_id) and text.startswith('/userinfo '):
        try:
            uid=int(text.split(maxsplit=1)[1]);await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':permission_text(uid),'reply_markup':admin_user_keyboard(uid)});return
        except:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Cú pháp: /userinfo <ID>'});return

    if is_admin(chat_id) and text.startswith('/grantvip '):
        try: uid=int(text.split(maxsplit=1)[1]); grant_access(uid,chat_id,True,'vip'); out=f'💎 VIP {uid}: mở full + GPT'
        except: out='Cú pháp: /grantvip <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/grantpro '):
        try: uid=int(text.split(maxsplit=1)[1]); grant_access(uid,chat_id,True,'pro'); out=f'⭐ PRO {uid}: dự đoán + lịch sử + AUTO'
        except: out='Cú pháp: /grantpro <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and (text.startswith('/grantall ') or text.startswith('/grant ')):
        cmd=text.split()[0]
        try:
            uid=int(text.split(maxsplit=1)[1]); preset='vip' if cmd=='/grantall' else 'basic'
            grant_access(uid,chat_id,True,preset)
            out=f"✅ {uid} · {'VIP FULL' if preset=='vip' else 'BASIC: chỉ dự đoán'}"
        except: out=f'Cú pháp: {cmd} <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/permit '):
        parts=text.split()
        if len(parts)!=3: out='Cú pháp: /permit <user_id> <predict|history|ai|auto>'
        else:
            try:
                uid=int(parts[1]); feat=parts[2].lower()
                if feat not in FEATURES: raise ValueError()
                grant_access(uid,chat_id,True)
                set_feature(uid,feat,True,chat_id); out=f'✅ Mở {feat.upper()} cho {uid}\n'+permission_text(uid)
            except: out='Cú pháp: /permit <user_id> <predict|history|ai|auto>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/deny '):
        parts=text.split()
        if len(parts)!=3: out='Cú pháp: /deny <user_id> <predict|history|ai|auto>'
        else:
            try:
                uid=int(parts[1]); feat=parts[2].lower()
                if feat not in FEATURES: raise ValueError()
                set_feature(uid,feat,False,chat_id); out=f'🔒 Khóa {feat.upper()} của {uid}\n'+permission_text(uid)
            except: out='Cú pháp: /deny <user_id> <predict|history|ai|auto>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/perms'):
        parts=text.split(maxsplit=1)
        uid=int(parts[1]) if len(parts)>1 and parts[1].lstrip('-').isdigit() else chat_id
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':permission_text(uid)}); return
    if is_admin(chat_id) and text.startswith('/lock '):
        try:
            uid=int(text.split(maxsplit=1)[1]); grant_access(uid,chat_id,False)
            out=f'⛔ USER {uid} đã bị TẮT toàn bộ quyền + AUTO.'
        except: out='Cú pháp: /lock <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/unlock '):
        parts=text.split()
        try:
            uid=int(parts[1]); preset=(parts[2].lower() if len(parts)>2 else 'basic')
            if preset not in PRESETS: raise ValueError()
            grant_access(uid,chat_id,True,preset)
            out=f'✅ USER {uid} đã mở lại · {preset.upper()}\n'+permission_text(uid)
        except: out='Cú pháp: /unlock <user_id> [basic|pro|vip]'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/revoke '):
        try: uid=int(text.split(maxsplit=1)[1]); grant_access(uid,chat_id,False); out=f'⛔ Đã khóa toàn bộ quyền của {uid}'
        except: out='Cú pháp: /revoke <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/users'):
        with sqlite3.connect(DB_PATH) as db:
            rows=db.execute("""SELECT a.chat_id,a.enabled,a.expires_at,a.access_label,COALESCE(p.preset,'legacy')
                               FROM bot_access a LEFT JOIN bot_permissions p ON p.chat_id=a.chat_id
                               ORDER BY a.updated_at DESC LIMIT 100""").fetchall()
        lines=['👥 USERS · PREMIUM ACCESS',BOT_DIV];now=time.time()
        for uid,en,exp,label,preset in rows:
            active=bool(en) and (exp is None or float(exp)>now)
            remain='∞' if exp is None and active else (_fmt_remaining(float(exp)-now) if exp else '---')
            lines.append(f"{'🟢' if active else '🔴'} {uid} · {str(preset).upper()} · {label or '-'} · {remain}")
        if len(lines)==2:lines.append('Chưa có user.')
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'\n'.join(lines)[:4000]});return


    if is_admin(chat_id) and text.startswith('/groups'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':list_groups_text()}); return

    if is_admin(chat_id) and text.startswith('/background'):
        mark_background_started(chat_id)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':background_status_text(),
                                            'reply_markup':bot_admin_keyboard()}); return
    if is_admin(chat_id) and text.startswith('/timeout'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,
            'text':'♾ V29 đã bỏ chế độ 2 giờ. Nền 24/7 chạy liên tục.\n\n'+background_status_text(),
            'reply_markup':bot_admin_keyboard()}); return
    if is_admin(chat_id) and text.startswith('/health'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':admin_health_text(),'reply_markup':bot_admin_keyboard()}); return
    if is_admin(chat_id) and text.startswith('/stats'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':admin_stats_text(),'reply_markup':bot_admin_keyboard()}); return
    if is_admin(chat_id) and text.startswith('/engine'):
        parts=text.split(maxsplit=1); b=parts[1].strip() if len(parts)>1 else get_selected_board(chat_id)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':engine_diag_text(b) if b else 'Cú pháp: /engine <board>'}); return
    if is_admin(chat_id) and text.startswith('/deep'):
        parts=text.split(maxsplit=1); b=parts[1].strip() if len(parts)>1 else get_selected_board(chat_id)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':deep_local_text(b) if b else 'Cú pháp: /deep <board>'}); return
    if is_admin(chat_id) and text.startswith('/testapi '):
        b=text.split(maxsplit=1)[1].strip()
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':await admin_test_api(client,b)}); return
    if is_admin(chat_id) and text.startswith('/apis'):
        lines=['🔗 API V39']
        for b,c in BOARDS.items():
            ec=effective_cfg(b,c); lines.append(f"{b}\n→ {ec.get('current','-')}"+(f"\nH {ec.get('history')}" if ec.get('history') else ''))
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'\n'.join(lines)[:4000],'reply_markup':bot_admin_keyboard()}); return
    if is_admin(chat_id) and text.startswith('/setsicboapi '):
        parts=text.split(maxsplit=2)
        if len(parts)!=3:
            out='Cú pháp: /setsicboapi <current_url> <history_url>'
        elif not parts[1].startswith(('http://','https://')) or not parts[2].startswith(('http://','https://')):
            out='❌ Cả current_url và history_url phải bắt đầu bằng http:// hoặc https://'
        else:
            set_api_override('sunwin:sicbo',parts[1],parts[2])
            out='✅ Đã đổi CURRENT + HISTORY cho SUNWIN SICBO'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out,'reply_markup':bot_admin_keyboard()}); return

    if is_admin(chat_id) and text.startswith('/resetsicboapi'):
        reset_api_override('sunwin:sicbo')
        ec=effective_cfg('sunwin:sicbo',BOARDS['sunwin:sicbo'])
        out='↩️ Đã trả SUNWIN SICBO về KWIN mặc định\\n'+ec.get('current','')+'\\n'+ec.get('history','')
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out,'reply_markup':bot_admin_keyboard()}); return

    if is_admin(chat_id) and text.startswith('/setapi '):
        parts=text.split(maxsplit=3)
        if len(parts)<3 or parts[1] not in BOARDS: out='Cú pháp: /setapi <board> <current_url> [history_url]'
        elif not parts[2].startswith(('http://','https://')): out='❌ current_url phải bắt đầu bằng http:// hoặc https://'
        elif len(parts)>3 and parts[3] and not parts[3].startswith(('http://','https://')): out='❌ history_url phải bắt đầu bằng http:// hoặc https://'
        else: set_api_override(parts[1],parts[2],parts[3] if len(parts)>3 else None); out=f'✅ Đã đổi API {parts[1]}'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if is_admin(chat_id) and text.startswith('/resetapi '):
        b=text.split(maxsplit=1)[1].strip() if ' ' in text else ''
        if b in BOARDS: reset_api_override(b); out=f'↩️ Đã trả API mặc định {b}'
        else: out='Cú pháp: /resetapi <board>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return

    if text.startswith('/id'):
        out=(f'🆔 Group ID: {chat_id}\n👤 User ID: {actor_id}' if is_group else f'🆔 User ID: {chat_id}')
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out}); return
    if text.startswith('/me'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':permission_text(chat_id)}); return
    if text.startswith('/help'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':user_help(chat_id)}); return
    if text.startswith('/bcrtop'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':baccarat_top_text(6),
                                            'reply_markup':bot_game_keyboard('baccarat',chat_id)}); return

    if text.startswith('/start') or text.startswith('/games') or text.startswith('/menu'):
        if not has_access(chat_id):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,
                'text':f'🔒 CHƯA ĐƯỢC CẤP QUYỀN\n{BOT_DIV}\nID: {chat_id}\nGửi ID này cho admin để mở quyền.'}); return
        if text.startswith('/start') and is_admin(chat_id):mark_background_started(chat_id)
        txt=('🎮 CHỌN GAME\n'+BOT_DIV+'\nChọn game để xem cầu, kết quả và prediction.') if text.startswith('/games') else bot_home_text(chat_id)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':txt,'reply_markup':bot_games_keyboard(chat_id)}); return


    if not has_access(chat_id):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'🔒 Chưa được cấp quyền. User ID: {chat_id}'}); return

    board=get_selected_board(chat_id)
    if text.startswith('/status'):
        if not feature_allowed(chat_id,'predict'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'predict')}); return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,
            'text':format_prediction(board,get_shared_prediction(board)) if board else 'Chưa chọn bàn. Dùng /games.',
            'reply_markup':bot_board_keyboard(board,chat_id) if board else bot_games_keyboard(chat_id)}); return
    if text.startswith('/historyall'):
        if not feature_allowed(chat_id,'history'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'history')}); return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_all_history(),
                                            'reply_markup':bot_games_keyboard(chat_id)}); return
    if text.startswith('/results'):
        if not feature_allowed(chat_id,'history'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'history')}); return
        if not board:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Chưa chọn bàn. Dùng /games.'}); return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_round_history(board,15),
                                            'reply_markup':bot_board_keyboard(board,chat_id)}); return
    if text.startswith('/history'):
        if not feature_allowed(chat_id,'history'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'history')}); return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,
            'text':format_history(board) if board else 'Chưa chọn bàn. Dùng /games.',
            'reply_markup':bot_board_keyboard(board,chat_id) if board else bot_games_keyboard(chat_id)}); return
    if text.startswith('/ai'):
        if not feature_allowed(chat_id,'ai'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'ai')}); return
        if not board:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Chưa chọn bàn. Dùng /games.'}); return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':await ai_explain_board(client,board),
                                            'reply_markup':bot_board_keyboard(board,chat_id)}); return
    if text.startswith('/auto'):
        if not feature_allowed(chat_id,'auto'):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':locked_text(chat_id,'auto')}); return
        if not board:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Chưa chọn bàn. Dùng /games.'}); return
        new=not sub_enabled(chat_id,board); set_sub(chat_id,board,new)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':('🔔 AUTO ON · ' if new else '🔕 AUTO OFF · ')+board_label(board),
                                            'reply_markup':bot_board_keyboard(board,chat_id)}); return

    await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':user_help(chat_id),'reply_markup':bot_games_keyboard(chat_id)})


async def bot_handle_callback(client,q):
    qid=q.get('id');data=q.get('data') or '';msg=q.get('message') or {};chat_id=(msg.get('chat') or {}).get('id')
    actor_id=(q.get('from') or {}).get('id')
    if not chat_id:return
    if is_group_chat_id(chat_id) and not group_enabled(chat_id):
        try:await tg_call(client,'answerCallbackQuery',{'callback_query_id':qid,'text':'Bot group đang tắt. Admin dùng /groupon.'})
        except:pass
        return
    try:await tg_call(client,'answerCallbackQuery',{'callback_query_id':qid})
    except:pass

    if data=='home':
        if not has_access(chat_id):await tg_panel(client,q,f'🔒 Chưa được cấp quyền · ID {chat_id}');return
        await tg_panel(client,q,bot_home_text(chat_id),bot_games_keyboard(chat_id));return
    if data=='adminhome':
        if not is_admin(actor_id):await tg_panel(client,q,'⛔ Không có quyền admin.');return
        await tg_panel(client,q,bot_admin_home_text(),bot_admin_keyboard());return
    if data.startswith(('uinfo|','uadd|','ulife|','ulock|')):
        if not is_admin(actor_id):await tg_panel(client,q,'⛔ Không có quyền admin.');return
        parts=data.split('|');act=parts[0]
        try:uid=int(parts[1])
        except:
            await tg_panel(client,q,'❌ User ID không hợp lệ.',bot_admin_keyboard());return
        if act=='uadd':
            dur=parts[2] if len(parts)>2 else '1d';grant_timed_access(uid,dur,chat_id,True);txt=f"✅ ĐÃ GIA HẠN +{dur.upper()}\n"+permission_text(uid)
        elif act=='ulife':grant_timed_access(uid,'forever',chat_id,False);txt='♾ ĐÃ CẤP VĨNH VIỄN\n'+permission_text(uid)
        elif act=='ulock':grant_access(uid,chat_id,False);txt='🔒 ĐÃ KHÓA USER\n'+permission_text(uid)
        else:txt=permission_text(uid)
        await tg_panel(client,q,txt,admin_user_keyboard(uid));return


    if data.startswith('adm|'):
        if not is_admin(actor_id):await tg_panel(client,q,'⛔ Không có quyền admin.');return
        act=data.split('|',1)[1]
        if act=='health':txt=admin_health_text()
        elif act=='stats':txt=admin_stats_text()
        elif act=='background':txt=background_status_text()
        elif act=='apis':txt='🔗 API LINKS\n'+BOT_DIV+'\nDùng /apis để xem URL hoặc /testapi <board>.'
        elif act=='sicboapi':
            ec=effective_cfg('sunwin:sicbo',BOARDS['sunwin:sicbo'])
            txt=('🎲 SUNWIN SICBO · API MANAGER\n'+BOT_DIV+
                 f"\nCURRENT:\n{ec.get('current','-')}\n\nHISTORY:\n{ec.get('history','-')}"+
                 '\n\nĐổi nhanh:\n/setsicboapi <current_url> <history_url>\n/resetsicboapi')
        elif act=='engine':
            b=get_selected_board(chat_id);txt=engine_diag_text(b) if b else '🧠 Chọn bàn trước rồi mở ENGINE.'
        elif act=='deep':
            b=get_selected_board(chat_id);txt=deep_local_text(b) if b else '🧮 Chọn bàn trước rồi mở DEEP.'
        else:txt=admin_help()
        await tg_panel(client,q,txt,bot_admin_keyboard());return

    if data=='account':
        await tg_panel(client,q,bot_account_text(chat_id),bot_games_keyboard(chat_id));return
    if data=='perm':
        await tg_panel(client,q,permission_text(chat_id),bot_games_keyboard(chat_id) if has_access(chat_id) else None);return
    if data=='help':
        await tg_panel(client,q,user_help(chat_id),bot_games_keyboard(chat_id) if has_access(chat_id) else None);return

    if not has_access(chat_id):
        await tg_panel(client,q,f'🔒 Chưa được cấp quyền · ID {chat_id}');return
    if data=='games':
        await tg_panel(client,q,'🎮 CHỌN GAME\n'+BOT_DIV+'\nChọn game để mở dashboard.',bot_games_keyboard(chat_id));return
    if data.startswith('game|'):
        game=data.split('|',1)[1]
        if not boards_for_game(game):await tg_panel(client,q,'Game chưa có bàn dữ liệu.',bot_games_keyboard(chat_id));return
        await tg_panel(client,q,bot_game_text(game,chat_id),bot_game_keyboard(game,chat_id));return
    if data=='histall':
        if not feature_allowed(chat_id,'history'):await tg_panel(client,q,locked_text(chat_id,'history'),bot_games_keyboard(chat_id));return
        await tg_panel(client,q,format_all_history(),bot_games_keyboard(chat_id));return
    if data in ('allon','alloff'):
        if not feature_allowed(chat_id,'auto'):await tg_panel(client,q,locked_text(chat_id,'auto'),bot_games_keyboard(chat_id));return
        on=data=='allon'
        for b in available_bot_boards():set_sub(chat_id,b,on)
        await tg_panel(client,q,('🔔 AUTO ALL · ĐÃ BẬT\n' if on else '🔕 AUTO ALL · ĐÃ TẮT\n')+BOT_DIV+'\n'+bot_home_text(chat_id),bot_games_keyboard(chat_id));return
    if data.startswith('locked|'):
        parts=data.split('|',2);feat=parts[1] if len(parts)>1 else 'predict';board=parts[2] if len(parts)>2 else get_selected_board(chat_id)
        await tg_panel(client,q,locked_text(chat_id,feat),bot_board_keyboard(board,chat_id) if board else bot_games_keyboard(chat_id));return

    if '|' not in data:return
    action,board=data.split('|',1)
    if board not in available_bot_boards():return
    set_selected_board(chat_id,board)
    feat={'now':'predict','hist':'history','rounds':'history','ai':'ai','auto':'auto','on':'auto','off':'auto'}.get(action)
    if feat and not feature_allowed(chat_id,feat):
        await tg_panel(client,q,locked_text(chat_id,feat),bot_board_keyboard(board,chat_id));return

    if action in ('sel','now'):txt=format_prediction(board,get_shared_prediction(board))
    elif action=='auto':
        on=not sub_enabled(chat_id,board);set_sub(chat_id,board,on)
        txt=('🔔 AUTO · ĐÃ BẬT\n' if on else '🔕 AUTO · ĐÃ TẮT\n')+BOT_DIV+'\n'+format_prediction(board,get_shared_prediction(board))
    elif action=='on':set_sub(chat_id,board,True);txt='🔔 AUTO · ĐÃ BẬT\n'+BOT_DIV+'\n'+format_prediction(board,get_shared_prediction(board))
    elif action=='off':set_sub(chat_id,board,False);txt='🔕 AUTO · ĐÃ TẮT\n'+BOT_DIV+'\n'+format_prediction(board,get_shared_prediction(board))
    elif action=='hist':txt=format_history(board)
    elif action=='rounds':txt=format_round_history(board,15)
    elif action=='cau':txt=bot_cau_text(board,24)
    elif action=='ai':txt=await ai_explain_board(client,board)
    elif action=='diag':txt=engine_diag_text(board) if is_admin(chat_id) else '⛔ Chỉ admin xem ENGINE DIAG.'
    else:return
    await tg_panel(client,q,txt,bot_board_keyboard(board,chat_id))


async def telegram_loop():
    offset=0
    async with httpx.AsyncClient() as client:
        try:
            await tg_call(client,'setMyCommands',{'commands':[
                {'command':'start','description':'Mở bảng điều khiển'},
                {'command':'games','description':'Chọn game / bàn'},
                {'command':'status','description':'Dự đoán hiện tại'},
                {'command':'history','description':'Lịch sử đúng / sai'},
                {'command':'results','description':'Kết quả game'},
                {'command':'historyall','description':'Lịch sử tất cả game'},
                {'command':'auto','description':'Bật / tắt AUTO'},
                {'command':'ai','description':'GPT phân tích'},
                {'command':'me','description':'Quyền tài khoản'},
                {'command':'bcrtop','description':'Lọc top bàn Baccarat'},
                {'command':'groupon','description':'Admin bật bot trong nhóm'},
                {'command':'groupstatus','description':'Trạng thái bot nhóm'}
            ]})
        except:pass
        try: await tg_call(client,'deleteWebhook',{'drop_pending_updates':False})
        except: pass
        while True:
            try:
                result=await tg_call(client,'getUpdates',{'offset':offset,'timeout':BOT_POLL_TIMEOUT,'allowed_updates':['message','callback_query']}) or []
                for upd in result:
                    offset=max(offset,int(upd.get('update_id',0))+1)
                    if upd.get('message'): await bot_handle_message(client,upd['message'])
                    elif upd.get('callback_query'): await bot_handle_callback(client,upd['callback_query'])
            except asyncio.CancelledError: raise
            except Exception:
                await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_task, _bot_task
    ensure_db()
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

app=FastAPI(title='TAIXIUTOOL V39 Group Baccarat Pro API', lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['GET'],allow_headers=['*'])

@app.get('/api/health')
def health():
    return {'ok':True,'worker_last_cycle':_last_cycle,'poll_seconds':POLL_SECONDS,'db':DB_PATH,'bot_enabled':bool(BOT_TOKEN),'admin_count':len(ADMIN_IDS),'max_history':MAX_HISTORY,'chatgpt_enabled':bool(OPENAI_API_KEY and OPENAI_MODEL),'engine':'BOARD-META 25 + BCR FILTER V39','background':background_status_payload(),'sync_version':'V39_GROUP_BCR'}

@app.get('/api/learn/{game}/{table}')
def learn(game:str, table:str, limit:int=Query(1000,ge=20,le=1000), sub:str|None=None):
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
    return {'ok':True,'server_time':time.time(),'engine':'BOARD-META 25 + BCR FILTER V39',
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
        st=states.get(b)
        out.append({'board':b,'game':c['game'],'table':c['table'],'kind':c['kind'],
                    'source_ok':st[1] if st else False,'updated_at':st[0] if st else None,'error':st[2] if st else None})
    return {'ok':True,'boards':out,'max_history':MAX_HISTORY}


@app.get('/api/current/{game}/{table}')
def current_api(game:str,table:str):
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
    board=f'{game}:{table}'
    if board not in BOARDS: return JSONResponse({'error':'unknown board'},status_code=404)
    rows=load_rows(board,limit)
    if board=='lc79:xocdia':
        rows=[dict(r,result=display_pred(board,r.get('result'))) for r in rows]
    return {'ok':True,'board':board,'count':len(rows),'rows':rows}

@app.get('/')
def index():
    return {'ok':True,'service':'TAIXIUTOOL V39 GROUP BACCARAT PRO','mode':'telegram-bot-only',
            'worker':'24/7','bot_enabled':bool(BOT_TOKEN),'boards':len(available_bot_boards()),
            'engine':'BOARD-META 25 + BCR FILTER V39'}

@app.get('/{path:path}')
def no_web_fallback(path:str):
    return JSONResponse({'ok':False,'mode':'telegram-bot-only',
                         'message':'Web UI đã tắt. Dùng Telegram bot hoặc endpoint /api/*.'},status_code=404)

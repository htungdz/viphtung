import os, json, math, time, asyncio, sqlite3, re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
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
        'game':'sunwin','table':'sicbo','kind':'sicbo',
        'current':'https://ent-glenn-terrain-project.trycloudflare.com/sicbo/sunwin',
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
    'baccarat:main': {'game':'baccarat','table':'main','kind':'baccarat','current':'https://jjjjbcrsexxy-1.onrender.com/api/bcrvh11'},
}

_db_lock = asyncio.Lock()
_worker_task = None
_bot_task = None
_last_cycle = 0.0


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
        db.execute('''CREATE TABLE IF NOT EXISTS api_overrides(
            board TEXT PRIMARY KEY, current_url TEXT, history_url TEXT, updated_at REAL NOT NULL
        )''')
        # Lightweight forward-compatible migration for richer game metadata.
        cols={r[1] for r in db.execute('PRAGMA table_info(rounds)').fetchall()}
        if 'meta_json' not in cols:
            db.execute('ALTER TABLE rounds ADD COLUMN meta_json TEXT')
        db.commit()


def pick(o: Any, keys):
    if not isinstance(o, dict): return None
    for k in keys:
        if k in o and o[k] is not None: return o[k]
    return None


def sid(o, fallback=None):
    return pick(o,['phien','phiên','phien_id','phienId','id','session_id','sessionId','session','gameId','game_id','stt','code','index','issue','round','roundId','round_id','sid']) or fallback


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
        out.append({'id':str(sid(o,i+1)),'result':r,'dice':d,'sum':t,'md5':pick(o,['md5','hash','md5_hash','md5Hash','hash_md5','md5Code','md5_code','md5_result','md5_enc','md5_dec'])})
    def key(x):
        try: return (0,int(x['id']))
        except: return (1,x['id'])
    out.sort(key=key)
    return out


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
        rows.append({
            'id':str(sid(o,i+1)),'result':result,'dice':[],'sum':red,
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
    valid=[r for r in rows[-500:] if isinstance(r.get('dice'),list) and len(r.get('dice'))>=3]
    if len(valid)<8: return {'ready':False,'sample':len(valid),'faces':[]}
    faces=[]
    for j in range(3):
        last=int(valid[-1]['dice'][j]); scores={k:1.2 for k in range(1,7)}
        # recency-weighted face frequency
        for idx,r in enumerate(valid):
            try:v=int(r['dice'][j])
            except:continue
            age=len(valid)-1-idx; scores[v]+=0.5**(age/44)
        # transition conditioned on the last face
        for idx in range(1,len(valid)):
            try:prev=int(valid[idx-1]['dice'][j]); nxt=int(valid[idx]['dice'][j])
            except:continue
            if prev==last:
                age=len(valid)-1-idx; scores[nxt]+=1.45*(0.5**(age/36))
        ordered=sorted(scores.items(),key=lambda kv:kv[1],reverse=True)
        top,second=ordered[0],ordered[1]
        strength=(top[1]-second[1])/max(.001,top[1]+second[1])
        faces.append({'position':j+1,'face':top[0],'strength':round(strength,3)})
    total=sum(x['face'] for x in faces)
    zone='3–8' if total<=8 else '9–10' if total<=10 else '11–12' if total<=12 else '13–18'
    return {'ready':True,'sample':len(valid),'faces':faces,'estimated_total':total,'sum_zone':zone}


def xocdia_detail_forecast(rows, binary_prediction):
    want_even=(binary_prediction=='TÀI')
    scores={k:1.0 for k in range(5) if (k%2==0)==want_even}
    sample=0
    for idx,r in enumerate(rows[-500:]):
        meta=r.get('meta') or {}
        red=meta.get('red_count')
        if not isinstance(red,int) or red not in scores: continue
        age=min(120,len(rows)-1-idx); scores[red]+=0.5**(age/42); sample+=1
    if not scores:return None
    best=max(scores,key=scores.get)
    return {'red_count':best,'white_count':4-best,'label':f'{best} Đỏ · {4-best} Trắng','sample':sample}

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


def model_snapshot(rows, game=None):
    seq=[r['result'] for r in rows if r.get('result') in ('TÀI','XỈU')]
    if len(seq)<6:
        return {'sample':len(seq),'score':0.0,'prediction':None,'confidence':50,'percent':50,
                'pattern':'ĐANG HỌC','alt':'Chưa đủ mẫu','entropy':round(_entropy(seq),4),
                'cycle':{'k':0,'r':0.0},'agreement':0.5,'engine':'HYBRID MAX-COMPUTE V22',
                'skills':0,'totalSkills':37,'updated_at':time.time()}
    vote=lambda x: 1 if x=='TÀI' else -1
    n=len(seq)
    comps=[]
    def add(name,score,weight):
        if isinstance(score,(int,float)) and math.isfinite(score):
            comps.append((name,_clamp(float(score),-.85,.85),float(weight)))

    # Decayed contexts 1..5
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

    # Multi-window balance with agreement guard
    vals=[]
    for w in (6,10,18,30,48):
        q=seq[-w:]
        if len(q)>=min(6,w): vals.append(sum(vote(z) for z in q)/len(q))
    if vals:
        pos=sum(v>0 for v in vals); neg=sum(v<0 for v in vals)
        if max(pos,neg)/len(vals)>=.6:
            add('multiwin',sum(v/(1+i*.35) for i,v in enumerate(vals))/sum(1/(1+i*.35) for i in range(len(vals)))*.44,.72)

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
    if ev>=2: add('fliprepeat',vote(seq[-1])*(same-flip)/(same+flip),.66)

    # Recent transition
    if n>=12:
        cur=seq[-1]; p=x=1.5; ev=0.0
        for i in range(1,n):
            if seq[i-1]!=cur: continue
            w=0.5**(((n-1)-i)/24)
            if seq[i]=='TÀI': p+=w
            else: x+=w
            ev+=w
        if ev>=2: add('transition',(p-x)/(p+x),.82)

    # Run-length conditional
    if n>=18:
        p=x=1.5; ev=0.0
        for i in range(2,n):
            rr=1; j=i-2
            while j>=0 and seq[j]==seq[i-1] and rr<8:
                rr+=1; j-=1
            if rr!=min(run,8): continue
            w=0.5**(((n-1)-i)/30)
            if seq[i]=='TÀI': p+=w
            else: x+=w
            ev+=w
        if ev>=1.5: add('runcond',(p-x)/(p+x),.76)

    # Motif 4/5
    for L,wt in ((4,.78),(5,.84)):
        if n<L+8: continue
        motif=tuple(seq[-L:]); p=x=1.4; ev=0.0
        for i in range(L,n):
            if tuple(seq[i-L:i])!=motif: continue
            w=0.5**(((n-1)-i)/28)
            if seq[i]=='TÀI': p+=w
            else: x+=w
            ev+=w
        if ev>=1.2: add(f'motif{L}',(p-x)/(p+x),wt)

    # Multi-window momentum
    for win,wt in ((6,.54),(10,.62),(18,.70),(30,.74),(48,.78)):
        if n>=win:
            q=seq[-win:]; add(f'window{win}',(q.count('TÀI')-q.count('XỈU'))/win,wt)

    # Flip/repeat pressure
    if n>=10:
        recent=seq[-14:]; flips=sum(1 for i in range(1,len(recent)) if recent[i]!=recent[i-1])
        rate=flips/max(1,len(recent)-1)
        if rate>.64: add('flippressure',-1 if seq[-1]=='TÀI' else 1,min(.88,.42+rate*.55))
        elif rate<.36: add('repeatpressure',1 if seq[-1]=='TÀI' else -1,min(.84,.48+(1-rate)*.42))

    # Markov order 2
    if n>=20:
        ctx=tuple(seq[-2:]); p=x=1.5; ev=0.0
        for i in range(2,n):
            if tuple(seq[i-2:i])!=ctx: continue
            w=0.5**(((n-1)-i)/34); ev+=w
            if seq[i]=='TÀI': p+=w
            else: x+=w
        if ev>=2: add('markov2',(p-x)/(p+x),.88)

    # Markov order 3
    if n>=28:
        ctx=tuple(seq[-3:]); p=x=1.8; ev=0.0
        for i in range(3,n):
            if tuple(seq[i-3:i])!=ctx: continue
            w=0.5**(((n-1)-i)/40); ev+=w
            if seq[i]=='TÀI': p+=w
            else: x+=w
        if ev>=2.2: add('markov3',(p-x)/(p+x),.91)

    # Multi-lag correlation votes
    for lag in (2,3,4,5,6,8,10,12):
        if n>=lag+18:
            a=seq[-min(n,80):]; same=tot=0.0
            for i in range(lag,len(a)):
                w=0.5**(((len(a)-1)-i)/36); tot+=w
                if a[i]==a[i-lag]: same+=w
            if tot:
                corr=(same/tot-.5)*2
                if abs(corr)>=.16:
                    base=1 if seq[-lag]=='TÀI' else -1
                    add(f'lag{lag}',base*(1 if corr>0 else -1),min(.82,.42+abs(corr)*.7))

    # Change point
    if n>=32:
        a=sum(vote(z) for z in seq[-10:])/10
        b=sum(vote(z) for z in seq[-30:-10])/20
        if abs(a-b)>=.35: add('changepoint',a*.24,.56)

    # Autocorrelation 1..12 incl inversion
    arr=[vote(x) for x in seq[-100:]]; best=0.0; lag=0
    if len(arr)>=14:
        mean=sum(arr)/len(arr); den=sum((x-mean)**2 for x in arr)
        if den:
            for k in range(1,min(12,len(arr)//3)+1):
                num=sum((arr[i]-mean)*(arr[i+k]-mean) for i in range(len(arr)-k)); r=num/den
                if abs(r)>abs(best): best=r; lag=k
    if lag:
        lagv=vote(seq[-lag]); add('autocorr',(lagv if best>=0 else -lagv)*min(.44,abs(best)*.72),.76)

    # Dice/sum contexts aligned with rows
    valid=[r for r in rows if r.get('result') in ('TÀI','XỈU')]
    if valid:
        cur_sum=valid[-1].get('sum')
        if isinstance(cur_sum,(int,float)):
            p=x=1.7; ev=0
            for i,r in enumerate(valid[:-1]):
                if r.get('sum')!=cur_sum: continue
                w=0.5**(((len(valid)-2)-i)/40)
                if valid[i+1]['result']=='TÀI': p+=w
                else: x+=w
                ev+=w
            if ev>=1.8: add('exactsum',(p-x)/(p+x),.72)
            bucket='LOW' if cur_sum<=8 else 'HIGH' if cur_sum>=13 else 'MID'
            p=x=1.6; ev=0
            for i,r in enumerate(valid[:-1]):
                sm=r.get('sum')
                if not isinstance(sm,(int,float)): continue
                b='LOW' if sm<=8 else 'HIGH' if sm>=13 else 'MID'
                if b!=bucket: continue
                w=0.5**(((len(valid)-2)-i)/36)
                if valid[i+1]['result']=='TÀI': p+=w
                else: x+=w
                ev+=w
            if ev>=1.8: add('sumband',(p-x)/(p+x),.68)
        cur_d=valid[-1].get('dice') or []
        if len(cur_d)>=3:
            def state_parity(d): return sum(int(v)%2==0 for v in d[:3])
            def state_high(d): return sum(int(v)>=4 for v in d[:3])
            for name,fn,wt in [('parity',state_parity,.58),('highcount',state_high,.62)]:
                cur=fn(cur_d); p=x=1.6; ev=0
                for i,r in enumerate(valid[:-1]):
                    d=r.get('dice') or []
                    if len(d)<3 or fn(d)!=cur: continue
                    w=0.5**(((len(valid)-2)-i)/36)
                    if valid[i+1]['result']=='TÀI': p+=w
                    else: x+=w
                    ev+=w
                if ev>=1.8: add(name,(p-x)/(p+x),wt)

    active=[c for c in comps if abs(c[1])>=.035]
    if not active:
        active=[('fallback',vote(seq[-1])*.02,.25)]
    num=sum(sc*wt for _,sc,wt in active); den=sum(abs(wt) for _,_,wt in active) or 1
    raw=num/den
    pos=sum(1 for _,sc,_ in active if sc>0); neg=sum(1 for _,sc,_ in active if sc<0)
    agreement=max(pos,neg)/max(1,len(active))
    H=_entropy(seq[-80:])
    sample_factor=_clamp(n/36,.58,1.0)
    entropy_factor=_clamp(1.25-H*.46,.70,1.0)
    agree_factor=.66 if agreement<.56 else .84 if agreement<.67 else 1.0
    score=_clamp(raw*sample_factor*entropy_factor*agree_factor,-.82,.82)
    # Ensemble disagreement damping: more modules must not create fake certainty.
    _pos=sum(abs(w) for _,v,w in signals if v>0)
    _neg=sum(abs(w) for _,v,w in signals if v<0)
    _agree=max(_pos,_neg)/max(.001,_pos+_neg)
    score=score*(1.0-min(.42,(1.0-_agree)*.72))
    if abs(score)<.014: score=vote(seq[-1])*.014
    pred='TÀI' if score>=0 else 'XỈU'
    evidence=_clamp(abs(score)*1.95+max(0,agreement-.5)*.48,0,1)
    cap=64 if game=='baccarat' else 69
    conf=round(_clamp(50+evidence*(cap-50),51,cap))
    if run>=3: pattern=f"BỆT {seq[-1]} x{run}"
    elif current_flip: pattern='ĐẢO 1-1 / FLIP'
    else: pattern='CẦU HỖN HỢP'
    alt=f"{len(active)} tín hiệu · đồng thuận {round(agreement*100)}% · entropy {H:.3f}"
    return {'sample':n,'score':round(score,4),'prediction':pred,'confidence':conf,'percent':conf,
            'run':run,'pattern':pattern,'alt':alt,'entropy':round(H,4),
            'cycle':{'k':lag,'r':round(best,4)},'agreement':round(agreement,4),
            'engine':'HYBRID MAX-COMPUTE V22','skills':len(active),'totalSkills':37,
            'updated_at':time.time()}


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
            rows.append({'id':str(s),'result':r,'dice':[x for x in (d1,d2,d3) if x is not None],
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
    return settled



def _current_anchor(current_rows):
    nums=[_session_num(r.get('id')) for r in (current_rows or [])
          if r.get('result') in ('TÀI','XỈU') and _session_num(r.get('id')) is not None]
    return str(max(nums)) if nums else None

def _drop_stale_pending(board,target_session):
    tn=_session_num(target_session)
    if tn is None: return
    with sqlite3.connect(DB_PATH) as db:
        for (s,) in db.execute('SELECT session FROM shared_predictions WHERE board=? AND actual IS NULL',(board,)).fetchall():
            sn=_session_num(s)
            if sn is not None and sn!=tn:
                db.execute('DELETE FROM shared_predictions WHERE board=? AND session=?',(board,str(s)))
        db.commit()

def _create_shared_prediction(board, rows, current_session=None):
    if not rows: return None,False
    game=board.split(':',1)[0]
    model=model_snapshot(rows,game)
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
    model=model_snapshot(rows, board.split(':',1)[0])
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
                anchor=_current_anchor(rows)
                await set_state(b,True)
                await refresh_shared_prediction(b,anchor)
            await set_state(board,True)
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
            await store_rows(board,history_rows+current_rows)
            await set_state(board,True)
            await refresh_shared_prediction(board,anchor)
            return

        data,_=await fetch_first(client,[cfg.get('current')]+cfg.get('current_fallbacks',[]))
        rows=parse_xocdia(data) if cfg['kind']=='xocdia' else parse_tx(data)
        anchor=_current_anchor(rows)
        if not rows or anchor is None: raise RuntimeError('current API không có phiên hợp lệ')
        await store_rows(board,rows)
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
      'son789:hu':'SON789 HŨ','son789:md5':'SON789 MD5'
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
    return [x for x in ids if has_access(x)]


def is_admin(chat_id):
    try:return int(chat_id) in ADMIN_IDS
    except:return False


def has_access(chat_id):
    if is_admin(chat_id): return True
    if not BOT_REQUIRE_ACCESS: return True
    with sqlite3.connect(DB_PATH) as db:
        r=db.execute('SELECT enabled FROM bot_access WHERE chat_id=?',(int(chat_id),)).fetchone()
    return bool(r and r[0])


def grant_access(chat_id,admin_id=None,enabled=True):
    with sqlite3.connect(DB_PATH) as db:
        db.execute('''INSERT INTO bot_access(chat_id,enabled,granted_by,updated_at) VALUES(?,?,?,?)
                      ON CONFLICT(chat_id) DO UPDATE SET enabled=excluded.enabled,
                      granted_by=excluded.granted_by,updated_at=excluded.updated_at''',
                   (int(chat_id),1 if enabled else 0,int(admin_id) if admin_id is not None else None,time.time()))
        db.commit()


def admin_help():
    return ('🛠 ADMIN V22\n/grant <user_id> · cấp quyền\n/revoke <user_id> · thu quyền\n'
            '/users · danh sách quyền\n/apis · xem link API\n/ai · giải thích tín hiệu bằng ChatGPT (nếu đã cấu hình)\n'
            '/setapi <board> <current_url> [history_url]\n/resetapi <board>\n'
            'Board ví dụ: sunwin:hu, sunwin:sicbo, lc79:xocdia, max789:md5')

def bot_games_keyboard(chat_id):
    boards=available_bot_boards(); rows=[]
    for i in range(0,len(boards),2):
        row=[]
        for b in boards[i:i+2]:
            dot='🟢' if sub_enabled(chat_id,b) else '⚪'
            row.append({'text':f'{dot} {board_label(b)}','callback_data':'sel|'+b})
        rows.append(row)
    rows.append([{'text':'✅ AUTO ALL','callback_data':'allon'},{'text':'⛔ TẮT ALL','callback_data':'alloff'}])
    return {'inline_keyboard':rows}


def bot_board_keyboard(board):
    return {'inline_keyboard':[
      [{'text':'▶️ AUTO','callback_data':'on|'+board},{'text':'⏹ TẮT','callback_data':'off|'+board}],
      [{'text':'🎯 XEM','callback_data':'now|'+board},{'text':'📜 LS','callback_data':'hist|'+board},{'text':'🎮 GAME','callback_data':'games'}]
    ]}


def format_prediction(board,pred):
    p=pred
    if not p: return f'⏳ {board_label(board)} · chưa có dự đoán.'
    model=p.get('model') or {}
    text=(f"🎮 {board_label(board)}\n"
          f"Phiên #{p.get('session','---')}\n"
          f"Dự đoán: {display_pred(board,p.get('prediction'))} · {p.get('confidence',50)}%\n"
          f"Engine: {model.get('engine','HYBRID MAX-COMPUTE V22')}\n"
          f"Mẫu: {model.get('pattern','---')} · {model.get('sample',0)} phiên")
    df=model.get('dice_forecast') or {}
    if board=='sunwin:sicbo' and df.get('ready'):
        faces=[str(x.get('face','?')) for x in df.get('faces',[])[:3]]
        text += f"\n🎲 Vị dự đoán: {' · '.join(faces)} · vùng tổng {df.get('sum_zone','---')}"
    xf=model.get('xocdia_forecast') or {}
    if board=='lc79:xocdia' and xf:
        text += f"\n⚪🔴 Thế phụ: {xf.get('label','---')}"
    return text


def format_history(board,limit=12):
    h=get_prediction_history(board,limit)
    if not h: return f'📜 {board_label(board)}\nChưa có lịch sử dự đoán chung.'
    lines=[f'📜 {board_label(board)} · {min(limit,len(h))} dự đoán gần nhất']
    for x in h:
        pred=display_pred(board,x['prediction'])
        if x['actual'] is None: mark='⏳'; actual='chờ KQ'
        else:
            mark='✅' if x['ok'] else '❌'; actual=display_pred(board,x['actual'])
        lines.append(f"{mark} #{x['session']} · {pred} → {actual} · {x['confidence']}%")
    return '\n'.join(lines)


async def tg_call(client,method,payload=None):
    if not BOT_TOKEN: return None
    url=f'https://api.telegram.org/bot{BOT_TOKEN}/{method}'
    r=await client.post(url,json=payload or {},timeout=30)
    r.raise_for_status(); data=r.json()
    return data.get('result') if data.get('ok') else None


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
                msg=await tg_call(c,'sendMessage',{'chat_id':chat,'text':text,'disable_web_page_preview':True,'reply_markup':bot_board_keyboard(board)})
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


async def ai_explain_board(client, board):
    if not OPENAI_API_KEY or not OPENAI_MODEL:
        return '🧠 ChatGPT giải thích chưa bật. Admin cần cấu hình OPENAI_API_KEY và OPENAI_MODEL trên Railway.'
    rows=load_rows(board,40)
    pred=get_shared_prediction(board)
    if not rows or not pred:
        return '🧠 Chưa đủ dữ liệu để giải thích.'
    compact=[{'id':r.get('id'),'result':display_pred(board,r.get('result')),'dice':r.get('dice'),'sum':r.get('sum')} for r in rows[-30:]]
    prompt=(f"Phân tích ngắn bằng tiếng Việt cho board {board}. Dữ liệu 30 phiên gần nhất: {json.dumps(compact,ensure_ascii=False)}. "
            f"Dự đoán thống kê hiện tại: {display_pred(board,pred.get('prediction'))}, confidence {pred.get('confidence')}%. "
            "Chỉ giải thích các tín hiệu thống kê/pattern có thể thấy; không tuyên bố chắc thắng và không biến MD5 thành cách suy ra trước kết quả. Tối đa 8 dòng.")
    try:
        r=await client.post('https://api.openai.com/v1/responses',headers={'Authorization':'Bearer '+OPENAI_API_KEY,'Content-Type':'application/json'},
                            json={'model':OPENAI_MODEL,'input':prompt,'max_output_tokens':350},timeout=30)
        r.raise_for_status();data=r.json()
        parts=[]
        for item in data.get('output',[]):
            for c in item.get('content',[]) if isinstance(item,dict) else []:
                if isinstance(c,dict) and c.get('type')=='output_text' and c.get('text'): parts.append(c['text'])
        text='\n'.join(parts).strip()
        return ('🧠 PHÂN TÍCH CHATGPT\n'+text) if text else '🧠 ChatGPT không trả nội dung.'
    except Exception as e:
        return '🧠 ChatGPT lỗi: '+str(e)[:180]


async def bot_handle_message(client,msg):
    chat_id=(msg.get('chat') or {}).get('id'); text=(msg.get('text') or '').strip()
    if not chat_id: return

    if text.startswith('/admin'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':admin_help() if is_admin(chat_id) else '⛔ Không có quyền admin.'});return
    if is_admin(chat_id) and text.startswith('/grant '):
        try: uid=int(text.split(maxsplit=1)[1]);grant_access(uid,chat_id,True);out=f'✅ Đã cấp quyền dự đoán cho {uid}'
        except: out='Cú pháp: /grant <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if is_admin(chat_id) and text.startswith('/revoke '):
        try: uid=int(text.split(maxsplit=1)[1]);grant_access(uid,chat_id,False);out=f'⛔ Đã thu quyền {uid}'
        except: out='Cú pháp: /revoke <user_id>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if is_admin(chat_id) and text.startswith('/users'):
        with sqlite3.connect(DB_PATH) as db: rows=db.execute('SELECT chat_id,enabled,updated_at FROM bot_access ORDER BY updated_at DESC LIMIT 100').fetchall()
        out='👥 QUYỀN DỰ ĐOÁN\n'+('\n'.join(f"{uid} · {'ON' if en else 'OFF'}" for uid,en,_ in rows) if rows else 'Chưa cấp user nào.')
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if is_admin(chat_id) and text.startswith('/apis'):
        lines=['🔗 API V22']
        for b,c in BOARDS.items():
            ec=effective_cfg(b,c); lines.append(f"{b}\n→ {ec.get('current','-')}"+(f"\nH {ec.get('history')}" if ec.get('history') else ''))
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'\n'.join(lines)[:4000]});return
    if is_admin(chat_id) and text.startswith('/setapi '):
        parts=text.split(maxsplit=3)
        if len(parts)<3 or parts[1] not in BOARDS: out='Cú pháp: /setapi <board> <current_url> [history_url]'
        else:
            set_api_override(parts[1],parts[2],parts[3] if len(parts)>3 else None);out=f'✅ Đã đổi API {parts[1]}'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return
    if is_admin(chat_id) and text.startswith('/resetapi '):
        b=text.split(maxsplit=1)[1].strip() if ' ' in text else ''
        if b in BOARDS: reset_api_override(b);out=f'↩️ Đã trả API mặc định {b}'
        else: out='Cú pháp: /resetapi <board>'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':out});return

    if text.startswith('/start') or text.startswith('/games'):
        if not has_access(chat_id):
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'🔒 Tài khoản chưa được cấp quyền dự đoán.\nUser ID: {chat_id}\nGửi ID này cho admin.'});return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'TAIXIUTOOL V22 · HYBRID ALL-GAME\nLIVE API khóa phiên + history/SQLite học tối đa 1000 phiên.','reply_markup':bot_games_keyboard(chat_id)})
        return
    if not has_access(chat_id):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'🔒 Chưa có quyền dự đoán. User ID: {chat_id}'});return
    board=get_selected_board(chat_id)
    if text.startswith('/ai'):
        if not board:
            await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Chưa chọn game. Dùng /games.'});return
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':await ai_explain_board(client,board)});return
    if text.startswith('/history'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_history(board) if board else 'Chưa chọn game. Dùng /games.','reply_markup':bot_board_keyboard(board) if board else bot_games_keyboard(chat_id)});return
    if text.startswith('/status'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_prediction(board,get_shared_prediction(board)) if board else 'Chưa chọn game. Dùng /games.','reply_markup':bot_board_keyboard(board) if board else bot_games_keyboard(chat_id)});return
    await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Dùng /games để chọn game, /status xem dự đoán, /history xem lịch sử.','reply_markup':bot_games_keyboard(chat_id)})


async def bot_handle_callback(client,q):
    qid=q.get('id'); data=q.get('data') or ''; msg=q.get('message') or {}; chat_id=(msg.get('chat') or {}).get('id')
    if not chat_id: return
    try: await tg_call(client,'answerCallbackQuery',{'callback_query_id':qid})
    except: pass
    if not has_access(chat_id):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'🔒 Chưa được cấp quyền dự đoán. User ID: {chat_id}'});return
    if data=='games':
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Chọn game/bàn:','reply_markup':bot_games_keyboard(chat_id)});return
    if data=='allon' or data=='alloff':
        on=data=='allon'
        for b in available_bot_boards(): set_sub(chat_id,b,on)
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'✅ Đã bật auto tất cả game.' if on else '⛔ Đã tắt auto tất cả game.','reply_markup':bot_games_keyboard(chat_id)});return
    if '|' not in data: return
    action,board=data.split('|',1)
    if action=='sel':
        set_selected_board(chat_id,board)
        status='ĐANG BẬT AUTO' if sub_enabled(chat_id,board) else 'AUTO ĐANG TẮT'
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':f'🎮 {board_label(board)}\n{status}\n\n'+format_prediction(board,get_shared_prediction(board)),'reply_markup':bot_board_keyboard(board)});return
    set_selected_board(chat_id,board)
    if action=='on':
        set_sub(chat_id,board,True); txt=f'🔔 Đã bật AUTO {board_label(board)}. Khi engine chung tạo phiên dự đoán mới, bot sẽ gửi.'
    elif action=='off':
        set_sub(chat_id,board,False); txt=f'🔕 Đã tắt AUTO {board_label(board)}.'
    elif action=='now': txt=format_prediction(board,get_shared_prediction(board))
    elif action=='hist': txt=format_history(board)
    else: return
    await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':txt,'reply_markup':bot_board_keyboard(board)})


async def telegram_loop():
    offset=0
    async with httpx.AsyncClient() as client:
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
    _worker_task=asyncio.create_task(worker_loop())
    if BOT_TOKEN:
        _bot_task=asyncio.create_task(telegram_loop())
    yield
    for task in (_worker_task,_bot_task):
        if task:
            task.cancel()
            try: await task
            except BaseException: pass

app=FastAPI(title='TAIXIUTOOL V22 Hybrid All-Game API', lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['GET'],allow_headers=['*'])

@app.get('/api/health')
def health():
    return {'ok':True,'worker_last_cycle':_last_cycle,'poll_seconds':POLL_SECONDS,'db':DB_PATH,'bot_enabled':bool(BOT_TOKEN),'admin_count':len(ADMIN_IDS),'max_history':MAX_HISTORY,'chatgpt_enabled':bool(OPENAI_API_KEY and OPENAI_MODEL),'engine':'HYBRID MAX-COMPUTE V22'}

@app.get('/api/learn/{game}/{table}')
def learn(game:str, table:str, limit:int=Query(1000,ge=20,le=1000), sub:str|None=None):
    if game=='baccarat':
        if sub:
            board=f'baccarat:{sub}'
            rows=load_rows(board,limit)
            return {'game':game,'table':table,'sub':sub,'rows':rows,'model':model_snapshot(rows,game),
                    'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,20)}
        with sqlite3.connect(DB_PATH) as db:
            names=[r[0].split(':',1)[1] for r in db.execute("SELECT DISTINCT board FROM rounds WHERE board LIKE 'baccarat:%' AND board<>'baccarat:main'")]
        tables={}
        for name in names:
            board=f'baccarat:{name}'; rows=load_rows(board,limit)
            tables[name]={'rows':rows,'model':model_snapshot(rows,game),'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,20)}
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
    return {'game':game,'table':table,'rows':rows,'model':model_snapshot(rows,game),'state':state,
            'shared_prediction':get_shared_prediction(board),'prediction_history':get_prediction_history(board,20)}

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
    return FileResponse(BASE_DIR/'index.html',media_type='text/html')

@app.get('/{path:path}')
def static_fallback(path:str):
    p=(BASE_DIR/path).resolve()
    if p.is_file() and BASE_DIR in p.parents:
        return FileResponse(p)
    return FileResponse(BASE_DIR/'index.html',media_type='text/html')

import os, json, math, time, asyncio, sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv('DB_PATH', '/data/taixiutool_learning.db')
POLL_SECONDS = max(2.0, float(os.getenv('POLL_SECONDS', '4')))
MAX_HISTORY = max(300, int(os.getenv('MAX_HISTORY', '800')))
BOT_TOKEN = os.getenv('BOT_TOKEN','').strip()
PUBLIC_URL = os.getenv('PUBLIC_URL','').rstrip('/')
BOT_POLL_TIMEOUT = max(10, int(os.getenv('BOT_POLL_TIMEOUT','20')))

BOARDS = {
    'sunwin:hu': {
        'game':'sunwin','table':'hu','kind':'tx_pair',
        'current':'https://kwinstore.com/sunwin/tx/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
        'history':'https://kwinstore.com/sunwin/tx/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'lc79:hu': {
        'game':'lc79','table':'hu','kind':'tx_pair',
        'current':'https://kwinstore.com/lc79/tx/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
        'history':'https://kwinstore.com/lc79/tx/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'lc79:md5': {
        'game':'lc79','table':'md5','kind':'tx_pair',
        'current':'https://kwinstore.com/lc79/md5/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
        'history':'https://kwinstore.com/lc79/md5/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'betvip:hu': {'game':'betvip','table':'hu','kind':'tx_single','current':'https://paying-hon-bullet-sms.trycloudflare.com/api/tx'},
    'betvip:md5': {'game':'betvip','table':'md5','kind':'tx_single','current':'https://paying-hon-bullet-sms.trycloudflare.com/api/txmd5'},
    'gb68:hu': {'game':'gb68','table':'hu','kind':'tx_single','current':'https://winds-fonts-seq-jaguar.trycloudflare.com/api/68/thuong'},
    'gb68:md5': {
        'game':'gb68','table':'md5','kind':'tx_pair',
        'current':'https://kwinstore.com/68gamebip/md5/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
        'history':'https://kwinstore.com/68gamebip/md5/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    },
    'b52:hu': {'game':'b52','table':'hu','kind':'tx_single','current':'https://volunteers-executives-granted-liz.trycloudflare.com/taixiu'},
    'b52:md5': {'game':'b52','table':'md5','kind':'tx_single','current':'https://volunteers-executives-granted-liz.trycloudflare.com/txmd5'},
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
    for ks in [('dice1','dice2','dice3'),('d1','d2','d3'),('xx1','xx2','xx3'),('xucxac1','xucxac2','xucxac3')]:
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
    for k in ['data','result','results','history','histories','list','items','sessions','rounds','records','rows','games','payload','response']:
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
        out.append({'id':str(sid(o,i+1)),'result':r,'dice':d,'sum':t,'md5':pick(o,['md5','hash','md5_hash','md5Hash','hash_md5','md5Code','md5_code'])})
    def key(x):
        try: return (0,int(x['id']))
        except: return (1,x['id'])
    out.sort(key=key)
    return out


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


def _next_session(rows):
    if not rows: return None
    last=str(rows[-1]['id'])
    try: return str(int(last)+1)
    except: return last+'+1'


def model_snapshot(rows, game=None):
    seq=[r['result'] for r in rows if r.get('result') in ('TÀI','XỈU')]
    if len(seq)<6:
        return {'sample':len(seq),'score':0.0,'prediction':None,'confidence':50,'percent':50,
                'pattern':'ĐANG HỌC','alt':'Chưa đủ mẫu','entropy':round(_entropy(seq),4),
                'cycle':{'k':0,'r':0.0},'agreement':0.5,'engine':'SHARED ADAPTIVE V17',
                'skills':0,'totalSkills':16,'updated_at':time.time()}
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
            'engine':'SHARED ADAPTIVE V17','skills':len(active),'totalSkills':16,
            'updated_at':time.time()}


async def store_rows(board, rows):
    if not rows: return
    async with _db_lock:
        with sqlite3.connect(DB_PATH) as db:
            now=time.time()
            for r in rows:
                d=(r.get('dice') or [])+[None,None,None]
                db.execute('''INSERT OR IGNORE INTO rounds(board,session,result,d1,d2,d3,total,md5,seen_at)
                              VALUES(?,?,?,?,?,?,?,?,?)''',
                           (board,str(r['id']),r['result'],d[0],d[1],d[2],r.get('sum'),r.get('md5'),now))
            # bound rows per board
            db.execute('''DELETE FROM rounds WHERE board=? AND rowid NOT IN
                          (SELECT rowid FROM rounds WHERE board=? ORDER BY seen_at DESC LIMIT ?)''',(board,board,MAX_HISTORY))
            db.commit()


def load_rows(board, limit=500):
    with sqlite3.connect(DB_PATH) as db:
        cur=db.execute('SELECT session,result,d1,d2,d3,total,md5,seen_at FROM rounds WHERE board=? ORDER BY seen_at DESC LIMIT ?',(board,limit))
        rows=[]
        for s,r,d1,d2,d3,total,md5,seen in reversed(cur.fetchall()):
            dice=[x for x in (d1,d2,d3) if x is not None]
            rows.append({'id':s,'result':r,'dice':dice,'sum':total,'md5':md5,'seen_at':seen})
        return rows


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


def _create_shared_prediction(board, rows):
    if not rows: return None,False
    game=board.split(':',1)[0]
    model=model_snapshot(rows,game)
    if not model.get('prediction'): return None,False
    session=_next_session(rows)
    if not session: return None,False
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


async def refresh_shared_prediction(board):
    rows=load_rows(board,MAX_HISTORY)
    settled=_settle_predictions(board,rows)
    if settled and BOT_TOKEN:
        for item in settled:
            asyncio.create_task(bot_clear_settled_prediction(board,item['session']))
    pred,created=_create_shared_prediction(board,rows)
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


async def fetch_json(client,url):
    r=await client.get(url,headers={'Accept':'application/json','Cache-Control':'no-cache'},timeout=3.5)
    r.raise_for_status(); return r.json()


async def poll_board(client, board, cfg):
    try:
        if cfg['kind']=='baccarat':
            data=await fetch_json(client,cfg['current']); tables=parse_baccarat(data)
            for table,rows in tables.items():
                b=f'baccarat:{table}'; await store_rows(b,rows); await set_state(b,True); await refresh_shared_prediction(b)
            await set_state(board,True)
            return
        if cfg['kind']=='tx_pair':
            current, history = await asyncio.gather(fetch_json(client,cfg['current']), fetch_json(client,cfg['history']), return_exceptions=True)
            rows=[]
            if not isinstance(history,Exception): rows.extend(parse_tx(history))
            if not isinstance(current,Exception): rows.extend(parse_tx(current))
            if not rows:
                err = history if isinstance(history,Exception) else current
                raise err if isinstance(err,Exception) else RuntimeError('no data')
        else:
            data=await fetch_json(client,cfg['current']); rows=parse_tx(data)
            if not rows: raise RuntimeError('no parseable rows')
        await store_rows(board,rows); await set_state(board,True); await refresh_shared_prediction(board)
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
      'sunwin:hu':'SUNWIN', 'lc79:hu':'LC79 HŨ', 'lc79:md5':'LC79 MD5',
      'betvip:hu':'BETVIP HŨ','betvip:md5':'BETVIP MD5',
      'gb68:hu':'68GB HŨ','gb68:md5':'68GB MD5',
      'b52:hu':'B52 HŨ','b52:md5':'B52 MD5'
    }
    if board.startswith('baccarat:') and board!='baccarat:main': return 'BACCARAT · '+board.split(':',1)[1]
    return labels.get(board,board.upper())


def display_pred(board,p):
    if not p: return '---'
    if board.startswith('baccarat:'):
        return 'PLAYER' if p=='TÀI' else 'BANKER' if p=='XỈU' else p
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
        return [r[0] for r in db.execute('SELECT chat_id FROM bot_subscriptions WHERE board=? AND enabled=1',(board,))]


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
    if not pred: return f'🎮 {board_label(board)} · ⏳ Chưa đủ dữ liệu'
    m=pred.get('model') or {}
    side=display_pred(board,pred.get('prediction'))
    return (f'🎮 {board_label(board)}  ·  #{pred.get("session","---")}\n'
            f'🎯 {side}  ·  📶 {pred.get("confidence",50)}%\n'
            f'🧠 {m.get("pattern","SHARED")}  ·  🤝 {round(float(m.get("agreement",0))*100)}%\n'
            f'📚 {m.get("sample",0)} phiên')


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


async def bot_handle_message(client,msg):
    chat_id=(msg.get('chat') or {}).get('id'); text=(msg.get('text') or '').strip()
    if not chat_id: return
    if text.startswith('/start') or text.startswith('/games'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'TAIXIUTOOL V17 · WEB ↔ TELEGRAM\nChọn game/bàn. Dự đoán và lịch sử lấy từ cùng một engine + SQLite với web.','reply_markup':bot_games_keyboard(chat_id)})
        return
    board=get_selected_board(chat_id)
    if text.startswith('/history'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_history(board) if board else 'Chưa chọn game. Dùng /games.','reply_markup':bot_board_keyboard(board) if board else bot_games_keyboard(chat_id)})
        return
    if text.startswith('/status'):
        await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':format_prediction(board,get_shared_prediction(board)) if board else 'Chưa chọn game. Dùng /games.','reply_markup':bot_board_keyboard(board) if board else bot_games_keyboard(chat_id)})
        return
    await tg_call(client,'sendMessage',{'chat_id':chat_id,'text':'Dùng /games để chọn game, /status xem dự đoán, /history xem lịch sử.','reply_markup':bot_games_keyboard(chat_id)})


async def bot_handle_callback(client,q):
    qid=q.get('id'); data=q.get('data') or ''; msg=q.get('message') or {}; chat_id=(msg.get('chat') or {}).get('id')
    if not chat_id: return
    try: await tg_call(client,'answerCallbackQuery',{'callback_query_id':qid})
    except: pass
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

app=FastAPI(title='TAIXIUTOOL V17 Web Telegram Shared Engine', lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['GET'],allow_headers=['*'])

@app.get('/api/health')
def health():
    return {'ok':True,'worker_last_cycle':_last_cycle,'poll_seconds':POLL_SECONDS,'db':DB_PATH,'bot_enabled':bool(BOT_TOKEN),'engine':'SHARED ADAPTIVE V17'}

@app.get('/api/learn/{game}/{table}')
def learn(game:str, table:str, limit:int=Query(500,ge=20,le=800), sub:str|None=None):
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

@app.get('/')
def index():
    return FileResponse(BASE_DIR/'index.html',media_type='text/html')

@app.get('/{path:path}')
def static_fallback(path:str):
    p=(BASE_DIR/path).resolve()
    if p.is_file() and BASE_DIR in p.parents:
        return FileResponse(p)
    return FileResponse(BASE_DIR/'index.html',media_type='text/html')

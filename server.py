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

print('This is a temporary partial update. Full content is too large for single tool call. Please upload the full file manually or contact for alternative.')

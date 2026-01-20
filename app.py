# ==========================================
# 1. IMPORTS & SETUP
# ==========================================
import os
import time
import threading
import uuid
import requests
import pandas as pd
import uvicorn
import nest_asyncio
import asyncio
import math
import random
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from supabase import create_client, Client
from fastapi.middleware.cors import CORSMiddleware

# Apply nest_asyncio for compatible event loops
nest_asyncio.apply()

# ==========================================
# 2. CONFIGURATION & SECRETS
# ==========================================

GLOBAL_RUN_ID = str(uuid.uuid4())
print(f"🆔 New Run ID: {GLOBAL_RUN_ID}")

# --- TIMEZONE ---
IST = timezone(timedelta(hours=5, minutes=30))

# --- ENVIRONMENT VARIABLES (Secrets) ---
# Ensure these are set in your environment or Dockerfile
SB_URL_MGR = os.environ.get("SB_URL_MGR", "")
SB_KEY_MGR = os.environ.get("SB_KEY_MGR", "")
SB_URL_HIST = os.environ.get("SB_URL_HIST", "")
SB_KEY_HIST = os.environ.get("SB_KEY_HIST", "")

# --- RUNNER CONFIG ---
try:
    RUNNER_ID = int(os.environ.get("RUNNER_ID", 1))
except:
    RUNNER_ID = 1

inf_env = os.environ.get("INFINITE_MODE", "True").lower()
INFINITE_MODE = inf_env == "true"

print(f"⚙️ Config: Runner={RUNNER_ID} | InfiniteMode={INFINITE_MODE} | Broker=UPSTOX")

# --- SUPABASE CLIENTS ---
if not SB_URL_MGR or not SB_KEY_MGR:
    print("❌ CRITICAL: Supabase Credentials Missing in Environment Variables.")
    sys.exit(1)

sb_mgr: Client = create_client(SB_URL_MGR, SB_KEY_MGR)
sb_hist: Client = create_client(SB_URL_HIST, SB_KEY_HIST)

# --- GLOBAL STATE ---
MASTER_DB = {}
SEARCH_INDEX = []
WORKER_STATUS = { "status": "Booting", "logs": [] }

def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry)
    WORKER_STATUS["logs"].insert(0, entry)
    if len(WORKER_STATUS["logs"]) > 50: WORKER_STATUS["logs"].pop()

# ==========================================
# 3. ADVANCED TOKEN MANAGER (UPSTOX SPECIFIC)
# ==========================================

class TokenManager:
    def __init__(self):
        self.tokens = [] 
        self.blacklist = set()
        self.refresh_tokens()

    def refresh_tokens(self):
        try:
            # Filter specifically for UPSTOX broker and Enabled status
            res = sb_mgr.table("apps").select("*")\
                .eq("broker", "UPSTOX")\
                .eq("is_enabled", True)\
                .order("updated_at", desc=True)\
                .execute()
            
            if res.data:
                # Upstox just needs the access token for Bearer auth
                self.tokens = [r['access_token'] for r in res.data if r.get('access_token')]
                log(f"🔑 Loaded {len(self.tokens)} UPSTOX Tokens.")
            else:
                self.tokens = []
                log("⚠️ No active UPSTOX tokens found in DB (apps table).")
        except Exception as e:
            log(f"❌ Token Fetch Error: {e}")

    def get_token(self, runner_id, retry_random=False):
        available = [t for t in self.tokens if t not in self.blacklist]
        if not available: 
            self.blacklist.clear()
            available = self.tokens
        
        if not available: return None

        if retry_random:
            return random.choice(available)
        
        idx = (runner_id - 1) % len(available)
        return available[idx]

    def report_error(self, token):
        self.blacklist.add(token)
        log(f"⚠️ Token Blacklisted (Auth Error): {token[:10]}...")

token_mgr = TokenManager()

# ==========================================
# 4. NETWORKING
# ==========================================

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.05, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=retries)
session.mount('https://', adapter)

def fetch_api(instrument_keys):
    """
    Fetches quotes for a comma-separated string of instrument keys.
    """
    url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_keys}"
    current_token = token_mgr.get_token(RUNNER_ID, retry_random=False)
    
    for attempt in range(2):
        if not current_token: return None
        try:
            headers = {
                "Authorization": f"Bearer {current_token}",
                "Accept": "application/json"
            }
            r = session.get(url, headers=headers, timeout=2.0)
            
            if r.status_code == 200:
                d = r.json()
                if d.get('status') == 'success': 
                    return d['data']
            
            if r.status_code in [401, 403]:
                token_mgr.report_error(current_token)
                current_token = token_mgr.get_token(RUNNER_ID, retry_random=True)
                continue 
        except Exception as e:
            pass
    return None

# ==========================================
# 5. MASTER DATA & SYMBOLS (FROM UPSTOX APP.PY3)
# ==========================================

# Manual mapping for Index Spot Keys
INDEX_MAP = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX": "BSE_INDEX|SENSEX",
    "BANKEX": "BSE_INDEX|BANKEX",
    "SENSEX50": "BSE_INDEX|SENSEX50"
}

def update_master_db():
    global MASTER_DB, SEARCH_INDEX
    log("📥 Downloading Upstox Master Data (NSE, BSE, MCX)...")
    
    try:
        # 1. NSE
        url_nse = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
        df_nse = pd.read_json(url_nse, compression='gzip')
        
        # 2. BSE
        url_bse = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
        df_bse = pd.read_json(url_bse, compression='gzip')

        # 3. MCX
        url_mcx = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
        df_mcx = pd.read_json(url_mcx, compression='gzip')

        # Combine all exchanges
        df = pd.concat([df_nse, df_bse, df_mcx], ignore_index=True)
        
        # Filter relevant segments
        mask = df['segment'].isin(['NSE_FO', 'BSE_FO', 'MCX_FO', 'NSE_INDEX', 'BSE_INDEX', 'NSE_EQ'])
        df = df[mask]
        
        temp_db = {}
        spot_lookup = {}

        # STEP 1: Build Spot Lookup
        for _, row in df.iterrows():
            seg = row['segment']
            if seg == 'NSE_EQ':
                sym = str(row['trading_symbol']).strip().upper()
                key = row['instrument_key']
                spot_lookup[sym] = key
            elif seg == 'NSE_INDEX' or seg == 'BSE_INDEX':
                sym = str(row['trading_symbol']).strip().upper()
                key = row['instrument_key']
                spot_lookup[sym] = key
                name = str(row['name']).strip().upper()
                spot_lookup[name] = key

        # STEP 2: Build Derivatives Chain
        for _, row in df.iterrows():
            seg = row['segment']
            if seg not in ['NSE_FO', 'BSE_FO', 'MCX_FO']: 
                continue 
            
            sym = row['trading_symbol']
            key = row['instrument_key']
            
            # Identify Root
            root = ""
            us = row.get('underlying_symbol')
            if pd.notna(us) and str(us).strip() != "":
                root = str(us).strip().upper()
            if not root:
                asym = row.get('asset_symbol')
                if pd.notna(asym) and str(asym).strip() != "":
                    root = str(asym).strip().upper()
            if not root:
                nm = row.get('name')
                if pd.notna(nm):
                    root = str(nm).strip().upper()

            clean_root = root.replace(' ', '') 

            # Initialize DB Entry
            if clean_root not in temp_db:
                spot_key = ""
                if clean_root in INDEX_MAP: spot_key = INDEX_MAP[clean_root]
                elif root in spot_lookup: spot_key = spot_lookup[root]
                elif clean_root in spot_lookup: spot_key = spot_lookup[clean_root]
                
                exch_code = seg.split('_')[0] 
                display_name = row['name'] if pd.notna(row['name']) else root

                temp_db[clean_root] = { 
                    "spot": spot_key, 
                    "exch": exch_code, 
                    "name": display_name, 
                    "items": [] 
                }

            inst_type = row.get('instrument_type', '') 
            
            # Parse Expiry
            exp = 0
            raw_exp = row.get('expiry')
            if pd.notna(raw_exp):
                try:
                    if isinstance(raw_exp, (int, float)):
                        exp = int(raw_exp / 1000)
                    elif isinstance(raw_exp, str):
                        dt = datetime.strptime(raw_exp, "%Y-%m-%d")
                        exp = int(dt.replace(tzinfo=None).timestamp())
                except: pass

            strike = float(row.get('strike_price', 0.0))
            
            t_mapped = "FUT"
            if inst_type == "CE": t_mapped = "CE"
            elif inst_type == "PE": t_mapped = "PE"
            elif inst_type in ["FUTIDX", "FUTSTK", "FUTCOM", "FUTBLN", "FUTENR"]: t_mapped = "FUT"
            
            temp_db[clean_root]["items"].append({
                "s": key,       
                "dis": sym,     
                "e": exp,       
                "k": strike,    
                "t": t_mapped   
            })

        # Finalize
        final_db = {}
        for root, data in temp_db.items():
            if not data["items"]: continue
            sorted_items = sorted(data["items"], key=lambda x: (x['e'], x['k']))
            final_db[root] = { 
                "spot": data["spot"], 
                "exch": data["exch"], 
                "items": sorted_items 
            }
            
        MASTER_DB = final_db
        log(f"✅ Master DB Ready: {len(MASTER_DB)} Roots")

    except Exception as e:
        log(f"❌ Master DB Error: {e}")

def get_ltp_sync(instrument_key):
    """
    Get a single LTP synchronously for calculation purposes.
    """
    d = fetch_api(instrument_key)
    if d and instrument_key in d:
        return d[instrument_key].get('last_price', 0)
    return 0

# ==========================================
# 6. DATA ENGINE
# ==========================================

class DataEngine:
    def __init__(self):
        self.reset_storage()
        self.tasks_meta = {}
        self.fetch_list = []

    def reset_storage(self):
        self.storage = {}

    def is_task_scheduled(self, task):
        if INFINITE_MODE: return True
        try:
            sched = task['full_payload'].get('schedule', {})
            start_str = sched.get('start', "09:15")
            end_str = sched.get('end', "15:30")
            
            now = datetime.now(IST)
            s_h, s_m = map(int, start_str.split(':'))
            e_h, e_m = map(int, end_str.split(':'))
            
            start_dt = now.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
            end_dt = now.replace(hour=e_h, minute=e_m, second=0, microsecond=0)
            
            # Start 2 mins early
            start_dt -= timedelta(minutes=2)
            
            if start_dt <= now <= end_dt: return True
        except: return True 
        return False

    def resolve_task_symbols(self, task):
        if not self.is_task_scheduled(task):
            return [], "SKIPPED"

        payload = task['full_payload']
        # Task root usually comes as "NSE:NIFTY" or just "NIFTY". 
        # Upstox MasterDB keys are clean roots (e.g. "NIFTY", "RELIANCE").
        raw_root = payload['root']
        
        # Clean the root to match Master DB keys
        if ':' in raw_root: root_name = raw_root.split(':')[1].upper()
        else: root_name = raw_root.upper()
        
        if root_name not in MASTER_DB: 
            # Try removing spaces just in case
            root_name = root_name.replace(' ', '')
            if root_name not in MASTER_DB:
                return [], "ERROR_ROOT_NOT_FOUND"

        db_entry = MASTER_DB[root_name]
        items = db_entry["items"]
        spot_sym = db_entry["spot"]
        
        # Get Futures
        futs = sorted([x for x in items if x['t'] == 'FUT'], key=lambda x: x['e'])
        future_syms = [x['s'] for x in futs[:3]]
        
        # Determine Reference Symbol for ATM calculation
        ref_sym = spot_sym
        if not ref_sym:
            if futs: ref_sym = futs[0]['s']
            else: return [], "ERROR_NO_REF"
            
        watch_list = []
        if spot_sym: watch_list.append(spot_sym)
        watch_list.extend(future_syms)
        
        config = payload.get('config', {})
        logic = config.get('expiry_logic', 'MARKET_ONLY')
        rng = int(config.get('range', 0))
        expiry_label = "MARKET"
        
        if logic != "MARKET_ONLY":
            ref_ltp = get_ltp_sync(ref_sym)
            if ref_ltp > 0:
                valid_exps = sorted(list(set([x['e'] for x in items if x['t'] in ['CE', 'PE'] and x['e'] > time.time()])))
                target_exp = None
                
                if logic.startswith("IDX_"):
                    try:
                        idx = int(logic.split("_")[1])
                        if idx < len(valid_exps): target_exp = valid_exps[idx]
                    except: pass
                elif logic == "DY_CURRENT":
                    if valid_exps: target_exp = valid_exps[0]
                elif logic == "DY_NEXT":
                    if len(valid_exps) > 1: target_exp = valid_exps[1]
                elif logic == "DY_MONTH_CURR":
                     if valid_exps: target_exp = valid_exps[-1] 

                if target_exp:
                    # Format expiry label YYYY-MM-DD
                    expiry_label = datetime.fromtimestamp(target_exp).strftime('%Y-%m-%d')
                    opts = [x for x in items if x['e'] == target_exp and x['t'] in ['CE', 'PE']]
                    strikes = sorted(list(set([x['k'] for x in opts])))
                    
                    if strikes:
                        atm_strike = min(strikes, key=lambda x: abs(x - ref_ltp))
                        atm_idx = strikes.index(atm_strike)
                        start = max(0, atm_idx - rng)
                        end = min(len(strikes), atm_idx + rng + 1)
                        targets = strikes[start:end]
                        watch_list.extend([x['s'] for x in opts if x['k'] in targets])

        return list(set(watch_list)), expiry_label

    def register_tasks(self, tasks):
        self.fetch_list = []
        self.tasks_meta = {}
        
        for task in tasks:
            t_id = task['task_id']
            syms, label = self.resolve_task_symbols(task)
            
            if syms:
                self.tasks_meta[t_id] = {
                    "root": task['full_payload']['root'], # Keep original root string from task
                    "symbols": syms,
                    "label": label
                }
                self.fetch_list.extend(syms)
        
        self.fetch_list = list(set(self.fetch_list))
        
        self.storage = {}
        for sym in self.fetch_list:
            self.storage[sym] = {
                "p": [None]*6, "v": [None]*6, "o": [None]*6,
                "b": [None]*6, "s": [None]*6
            }
        
        log(f"📋 Monitoring {len(self.fetch_list)} Symbols for {len(self.tasks_meta)} Tasks")

    def fetch_slot(self, slot_index):
        if not self.fetch_list: return
        
        # Upstox supports comma separated keys. 
        # Safe limit is around 50-100.
        chunk_size = 50
        chunks = [self.fetch_list[i:i + chunk_size] for i in range(0, len(self.fetch_list), chunk_size)]
        
        def _req(batch):
            return fetch_api(",".join(batch))

        with ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(_req, chunks))
        
        for res in results:
            if not res: continue 
            # Upstox returns data keyed by instrument_key
            for instr_key, data in res.items():
                if instr_key in self.storage:
                    # Upstox JSON structure mapping based on upstox_data_example.txt
                    self.storage[instr_key]["p"][slot_index] = data.get('last_price')
                    self.storage[instr_key]["v"][slot_index] = data.get('volume')
                    self.storage[instr_key]["o"][slot_index] = data.get('oi')
                    self.storage[instr_key]["b"][slot_index] = data.get('total_buy_quantity')
                    self.storage[instr_key]["s"][slot_index] = data.get('total_sell_quantity')

    def get_bucket_data(self, task_id):
        if task_id not in self.tasks_meta: return None, None
        meta = self.tasks_meta[task_id]
        syms = meta["symbols"]
        label = meta["label"]
        root = meta["root"]
        
        subset = {}
        has_data = False
        for sym in syms:
            if sym in self.storage:
                # Handle Special Naming for Indices exactly like PHP viewer expects
                # PHP Code: saveKey = k.includes('|') ? k.split('|')[1] : k;
                # Upstox Keys: NSE_INDEX|Nifty 50 -> "Nifty 50" or "NSE_FO|..." -> "NSE_FO|..."
                
                save_key = sym
                if "NSE_INDEX" in sym or "BSE_INDEX" in sym:
                     if '|' in sym: save_key = sym.split('|')[1]
                
                # Check for "NA" symbol issue mentioned in prompts
                if save_key == "NA" and '|' in sym:
                     save_key = sym.split('|')[1]

                subset[save_key] = self.storage[sym]
                
                # Check if we actually got data (price not None)
                if any(x is not None for x in self.storage[sym]['p']):
                    has_data = True
        
        if not has_data: return None, None
        return { "root": root, "expiry": label, "data": subset }, label

# ==========================================
# 7. WORKER MAIN LOOP
# ==========================================

def worker_main(run_id):
    log(f"🚀 Worker Started ({run_id}) | Mode: {'INFINITE' if INFINITE_MODE else 'SCHEDULED'}")
    
    # Initial Master DB Load
    update_master_db()
    
    engine = DataEngine()
    active_minute_str = None
    processed_slots = set()
    
    # 3:35 PM IST Cutoff
    stop_hour = 15
    stop_min = 35

    while True:
        try:
            if GLOBAL_RUN_ID != run_id: return 

            now = datetime.now(IST)
            
            # --- AUTO STOP LOGIC (3:35 PM IST) ---
            if not INFINITE_MODE and (now.hour > stop_hour or (now.hour == stop_hour and now.minute >= stop_min)):
                log("🛑 Market Closed (3:35 PM IST). Shutting down.")
                os._exit(0) 

            current_minute_str = now.strftime("%Y-%m-%d %H:%M:00")
            
            # --- MINUTE FLUSH ---
            if active_minute_str and current_minute_str != active_minute_str:
                log(f"💾 End of Minute {active_minute_str}. Flushing...")
                
                # 1. Fetch Tasks (Hot Reload)
                # Note: Table name is worker_tasks_upstox
                res = sb_mgr.table("worker_tasks_upstox").select("*").eq("runner_group", RUNNER_ID).execute()
                tasks = res.data
                
                buckets = []
                # 2. Extract Data
                for t_id in engine.tasks_meta:
                    payload, label = engine.get_bucket_data(t_id)
                    if payload:
                        payload["minute"] = active_minute_str
                        buckets.append(payload)
                
                # 3. Upload to history_buckets_upstox
                if buckets:
                    try:
                        sb_hist.table("history_buckets_upstox").upsert(buckets, on_conflict="root,expiry,minute").execute()
                        log(f"☁️ Uploaded {len(buckets)} Buckets.")
                    except Exception as e:
                        log(f"❌ Upload Error: {e}")
                
                processed_slots = set()
                token_mgr.refresh_tokens()
                
                if tasks:
                    engine.register_tasks(tasks) 
                else:
                    engine.reset_storage()

            active_minute_str = current_minute_str

            # --- SLOT EXECUTION ---
            current_slot = math.floor(now.second / 10)
            if current_slot > 5: current_slot = 5
            
            if current_slot not in processed_slots:
                # Sync alignment
                if now.second % 10 == 0: time.sleep(0.2) 
                
                engine.fetch_slot(current_slot)
                processed_slots.add(current_slot)
            
            time.sleep(0.1)

        except Exception as e:
            log(f"❌ Main Loop Error: {e}")
            time.sleep(5)

# ==========================================
# 8. SERVER STARTUP
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=worker_main, args=(GLOBAL_RUN_ID,), daemon=True)
    t.start()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def status():
    return HTMLResponse(f"""
    <html><body style="background:#111; color:#0f0; font-family:monospace; padding:20px;">
    <h2>🟢 Upstox Worker {RUNNER_ID} Active</h2>
    <div><strong>Run ID:</strong> {GLOBAL_RUN_ID}</div>
    <div><strong>Mode:</strong> {'INFINITE 24/7' if INFINITE_MODE else 'SCHEDULED'}</div>
    <div><strong>Tokens:</strong> {len(token_mgr.tokens)} available</div>
    <hr style="border-color:#333;">
    <div style="white-space: pre-wrap; word-wrap: break-word;">
    {chr(10).join(WORKER_STATUS['logs'])}
    </div>
    </body></html>
    """)

if __name__ == "__main__":
    print("⚠️  Checking for existing process on Port 8000...")
    os.system("fuser -k 8000/tcp") 
    time.sleep(2) 
    
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    asyncio.run(server.serve())

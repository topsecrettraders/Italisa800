# ==============================================================================
# 1. IMPORTS & SETUP
# ==============================================================================
import os
import sys
import time
import threading
import uuid
import json
import requests
import pandas as pd
import datetime
import uvicorn
import nest_asyncio
import asyncio
from collections import defaultdict
from fyers_apiv3.FyersWebsocket import data_ws
from supabase import create_client, Client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

nest_asyncio.apply()

# ==============================================================================
# 2. CONFIGURATION & SECRETS
# ==============================================================================

GLOBAL_RUN_ID = str(uuid.uuid4())
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# --- ENVIRONMENT VARIABLES ---
SB_URL_MGR = os.environ.get("SB_URL_MGR", "")
SB_KEY_MGR = os.environ.get("SB_KEY_MGR", "")
SB_URL_HIST = os.environ.get("SB_URL_HIST_FYERS", "")
SB_KEY_HIST = os.environ.get("SB_KEY_HIST_FYERS", "")

try:
    RUNNER_ID = int(os.environ.get("RUNNER_ID", 1))
except:
    RUNNER_ID = 1

if not SB_URL_MGR or not SB_KEY_MGR:
    print("❌ CRITICAL: Supabase Credentials Missing.")
    sys.exit(1)

sb_mgr: Client = create_client(SB_URL_MGR, SB_KEY_MGR)
sb_hist: Client = create_client(SB_URL_HIST, SB_KEY_HIST)

# ==============================================================================
# 3. GLOBAL STATE & MEMORY
# ==============================================================================

MASTER_DB = {}
STATE_MEMORY = defaultdict(lambda: {"ltp": 0, "vol": 0, "oi": None, "b": 0, "s": 0})
MINUTE_BUFFER = defaultdict(lambda: {"p": [None]*6, "v": [None]*6, "o": [None]*6, "b": [None]*6, "s": [None]*6})
TASK_MAP = {}
ALL_TRACKED_SYMBOLS = set()

# Websocket Control
FS = None
SOCKET_RUNNING = False
WS_THREAD = None

WORKER_STATUS = { "status": "Booting", "logs": [] }

INDEX_MAP = {
    "NIFTY": "NSE:NIFTY50-INDEX", "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "FINNIFTY": "NSE:FINNIFTY-INDEX", "MIDCPNIFTY": "NSE:MIDCAPNIFTY-INDEX",
    "SENSEX": "BSE:SENSEX-INDEX", "BANKEX": "BSE:BANKEX-INDEX",
    "SENSEX50": "BSE:SENSEX50-INDEX"
}

def log(msg):
    ts = datetime.datetime.now(IST).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry, flush=True)
    WORKER_STATUS["logs"].insert(0, entry)
    if len(WORKER_STATUS["logs"]) > 100: WORKER_STATUS["logs"].pop()

# ==============================================================================
# 4. DATA FETCHERS
# ==============================================================================

def get_fyers_token():
    try:
        # Fetch the most recently updated active FYERS token
        res = sb_mgr.table('apps').select("*").eq("broker", "FYERS").eq("is_enabled", True).order("updated_at", desc=True).limit(1).execute()
        if res.data: 
            return f"{res.data[0]['app_id']}:{res.data[0]['access_token']}"
    except Exception as e: 
        log(f"❌ Token Fetch Error: {e}")
    return None

def download_master():
    global MASTER_DB
    log("⏳ Downloading Fyers Master DB...")
    headers = { "User-Agent": "Mozilla/5.0" }
    try:
        df_nse = pd.read_csv("https://public.fyers.in/sym_details/NSE_FO.csv", usecols=[0,1,8,9,13], names=['Token','Desc','Expiry','Symbol','Inst'], header=0, on_bad_lines='skip', storage_options=headers)
        df_nse['Exch'] = 'NSE'
        df_mcx = pd.read_csv("https://public.fyers.in/sym_details/MCX_COM.csv", usecols=[0,1,8,9,13], names=['Token','Desc','Expiry','Symbol','Inst'], header=0, on_bad_lines='skip', storage_options=headers)
        df_mcx['Exch'] = 'MCX'
        df_bse = pd.read_csv("https://public.fyers.in/sym_details/BSE_FO.csv", usecols=[0,1,8,9,13], names=['Token','Desc','Expiry','Symbol','Inst'], header=0, on_bad_lines='skip', storage_options=headers)
        df_bse['Exch'] = 'BSE'

        df = pd.concat([df_nse, df_mcx, df_bse], ignore_index=True)
        
        temp_db = {}
        for _, row in df.iterrows():
            sym = str(row['Symbol'])
            desc = str(row['Desc'])
            
            try: root = desc.strip().split(' ')[0].upper().replace(':','').replace('-','')
            except: root = sym.split(':')[1] if ':' in sym else sym
            
            if root not in temp_db:
                spot_sym = INDEX_MAP.get(root, f"NSE:{root}-EQ")
                if row['Exch'] == "MCX": spot_sym = "MCX" 
                elif row['Exch'] == "BSE": spot_sym = f"BSE:{root}"
                temp_db[root] = { "spot": spot_sym, "exch": row['Exch'], "items": [] }

            try: exp = int(row['Expiry'])
            except: exp = 0
            
            strike = 0.0; opt_type = "FUT"
            if "CE" in sym or "PE" in sym:
                try:
                    parts = desc.strip().split(' ')
                    strike = float(parts[-2])
                    opt_type = parts[-1]
                except: pass
            
            temp_db[root]["items"].append({ "s": sym, "e": exp, "k": strike, "t": opt_type })
            
        MASTER_DB = temp_db
        log(f"✅ Master DB Loaded: {len(MASTER_DB)} roots")
    except Exception as e: 
        log(f"❌ Master DB Error: {e}")

def fetch_price(token, symbol):
    if not symbol: return 0
    try:
        url = f"https://api-t1.fyers.in/data/depth?symbol={symbol}&ohlcv_flag=1"
        res = requests.get(url, headers={"Authorization": token}, timeout=2.0)
        data = res.json()
        if data.get('s') == 'ok' and symbol in data['d']: 
            return data['d'][symbol]['ltp']
    except: pass
    return 0

# ==============================================================================
# 5. TASK RESOLVER ENGINE
# ==============================================================================

def resolve_symbols(token, tasks):
    global TASK_MAP, ALL_TRACKED_SYMBOLS
    if not MASTER_DB: download_master()
    
    new_task_map = {}
    new_tracked_symbols = set()
    
    for t in tasks:
        task_id = t.get('task_id', f"unknown_{int(time.time())}")
        task_subs = [] 
        expiry_label = "MARKET"
        
        raw_root = t.get('full_payload', {}).get('root', '')
        if ':' in raw_root: raw_root = raw_root.split(':')[1]
        if '-' in raw_root: raw_root = raw_root.split('-')[0]
        search = raw_root.upper().replace('50', '') 
        if search == 'BANK': search = 'BANKNIFTY'
        
        db_key = None
        if search in MASTER_DB: db_key = search
        else:
            for k in MASTER_DB:
                if k.endswith(search) or k.startswith(search):
                    db_key = k; break
        
        if not db_key: continue

        data = MASTER_DB[db_key]
        items = data["items"]
        now = time.time() - 86400

        config = t.get('full_payload', {}).get('config', {})
        logic = config.get('expiry_logic', 'MARKET_ONLY')
        rng = int(config.get('range', 18))

        spot_sym = data.get("spot")
        global_ref_price = 0
        using_spot = False
        
        if spot_sym and spot_sym != "MCX":
            global_ref_price = fetch_price(token, spot_sym)
            if global_ref_price > 0: 
                using_spot = True
                task_subs.append(spot_sym)

        all_futs = sorted([x for x in items if x['t'] == 'FUT' and x['e'] > now], key=lambda x: x['e'])
        if logic == 'MARKET_ONLY':
            for f in all_futs[:3]: 
                if f['s'] not in task_subs: task_subs.append(f['s'])
        else:
            for f in all_futs[:3]: 
                if f['s'] not in task_subs: task_subs.append(f['s'])

            all_opts = [x for x in items if x['e'] > now and x['t'] in ['CE','PE']]
            if all_opts:
                unique_exps = sorted(list(set(x['e'] for x in all_opts)))
                exp_idx = 0
                if logic.startswith('IDX_'):
                    try: exp_idx = int(logic.split('_')[1])
                    except: exp_idx = 0
                elif logic == 'DY_NEXT': exp_idx = 1
                
                if exp_idx < len(unique_exps):
                    target_opt_exp = unique_exps[exp_idx]
                    expiry_label = datetime.datetime.fromtimestamp(target_opt_exp).strftime('%Y-%m-%d')
                    current_ref_price = 0
                    
                    if using_spot:
                        current_ref_price = global_ref_price
                    else:
                        opt_dt = datetime.datetime.fromtimestamp(target_opt_exp)
                        opt_month_year = (opt_dt.year, opt_dt.month)
                        
                        matched_fut = None
                        for f in all_futs:
                            fut_dt = datetime.datetime.fromtimestamp(f['e'])
                            if (fut_dt.year, fut_dt.month) == opt_month_year:
                                matched_fut = f; break
                        
                        if matched_fut:
                            fut_price = fetch_price(token, matched_fut['s'])
                            if fut_price > 0: current_ref_price = fut_price
                        
                        if current_ref_price == 0 and all_futs:
                            nearest_fut = all_futs[0]
                            fut_price = fetch_price(token, nearest_fut['s'])
                            if fut_price > 0: current_ref_price = fut_price

                    if current_ref_price > 0:
                        options = [x for x in all_opts if x['e'] == target_opt_exp]
                        strikes = sorted(list(set(x['k'] for x in options)))
                        if strikes:
                            atm = min(strikes, key=lambda x: abs(x - current_ref_price))
                            atm_idx = strikes.index(atm)
                            start = max(0, atm_idx - rng)
                            end = min(len(strikes), atm_idx + rng + 1)
                            target_strikes = strikes[start:end]

                            final_opts = [x['s'] for x in options if x['k'] in target_strikes]
                            for f in final_opts:
                                if f not in task_subs: task_subs.append(f)

        if task_subs:
            new_task_map[task_id] = {
                "root": db_key,
                "symbols": list(set(task_subs)),
                "label": expiry_label
            }
            new_tracked_symbols.update(task_subs)

    TASK_MAP = new_task_map
    ALL_TRACKED_SYMBOLS = new_tracked_symbols
    return list(ALL_TRACKED_SYMBOLS)

# ==============================================================================
# 6. WEBSOCKET MANAGER
# ==============================================================================

def stop_websocket():
    global FS, SOCKET_RUNNING
    if FS and SOCKET_RUNNING:
        log("🛑 Stopping existing Fyers WebSocket connection...")
        try:
            FS.close_connection()
        except Exception as e:
            log(f"⚠️ Error closing WS: {e}")
    SOCKET_RUNNING = False
    FS = None

def start_websocket(token, symbols):
    global FS, SOCKET_RUNNING
    stop_websocket() # Ensure clean state

    if not symbols:
        log("⚠️ No symbols to monitor. Skipping WebSocket connection.")
        return

    def on_message(msg):
        global STATE_MEMORY
        ticks = msg if isinstance(msg, list) else [msg]
        for t in ticks:
            if 'symbol' in t:
                sym = t['symbol']
                state = STATE_MEMORY[sym]
                if 'ltp' in t: state['ltp'] = t['ltp']
                if 'vol_traded_today' in t: state['vol'] = t['vol_traded_today']
                if 'oi' in t: state['oi'] = t['oi']
                if 'tot_buy_qty' in t: state['b'] = t['tot_buy_qty']
                if 'tot_sell_qty' in t: state['s'] = t['tot_sell_qty']

    def on_error(e): 
        log(f"❌ WS Error: {e}")
        
    def on_close(c): 
        global SOCKET_RUNNING
        log("⚠️ WS Connection Closed")
        SOCKET_RUNNING = False

    def on_open(): 
        global SOCKET_RUNNING
        SOCKET_RUNNING = True
        log("✅ Fyers WebSocket Connected. Subscribing to chunks...")
        chunk_size = 50
        for i in range(0, len(symbols), chunk_size):
            batch = symbols[i:i+chunk_size]
            try:
                FS.subscribe(symbols=batch, data_type="SymbolUpdate")
                time.sleep(0.1)
            except Exception as e:
                log(f"❌ WS Subscription Error on batch: {e}")
        log(f"🚀 Successfully subscribed to {len(symbols)} symbols.")

    log("🔌 Initializing new Fyers WebSocket connection...")
    FS = data_ws.FyersDataSocket(
        access_token=token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True, 
        on_connect=on_open, 
        on_close=on_close,
        on_error=on_error, 
        on_message=on_message
    )
    
    # Run WS connection in a separate thread so it doesn't block main loop
    ws_thread = threading.Thread(target=FS.connect, daemon=True)
    ws_thread.start()

# ==============================================================================
# 7. MAIN WORKER LOOP & UPLOAD ENGINE
# ==============================================================================

def worker_main(run_id):
    log(f"🚀 FYERS Worker Started ({run_id}) | Runner: {RUNNER_ID}")
    download_master()
    
    active_minute_str = None
    processed_slots = set()
    last_tasks_hash = None
    
    # 3:35 PM IST Cutoff
    stop_hour = 15
    stop_min = 35

    while True:
        try:
            if GLOBAL_RUN_ID != run_id: return 

            now = datetime.datetime.now(IST)
            if now.hour > stop_hour or (now.hour == stop_hour and now.minute >= stop_min):
                log("🛑 Market Closed (3:35 PM IST). Shutting down.")
                stop_websocket()
                os._exit(0) 

            current_minute_str = now.strftime("%Y-%m-%d %H:%M:00")
            
            # --- MINUTE FLUSH & TASK CHECK ---
            if active_minute_str != current_minute_str:
                if active_minute_str is not None:
                    log(f"💾 End of Minute {active_minute_str}. Formatting Buckets...")
                    
                    # CHANGED: Use a dictionary to eliminate duplicate composite keys natively
                    unique_buckets = {}
                    for task_id, meta in TASK_MAP.items():
                        root = meta["root"]
                        label = meta["label"]
                        task_symbols = meta["symbols"]
                        
                        subset = {}
                        for sym in task_symbols:
                            if sym in MINUTE_BUFFER and any(p is not None for p in MINUTE_BUFFER[sym]['p']):
                                subset[sym] = MINUTE_BUFFER[sym]
                                
                        if subset:
                            # Unique key for DB constraint (root + expiry + minute)
                            bucket_key = (root, label, active_minute_str)
                            
                            # If duplicate exists, this simply overwrites it (keeping the latest one)
                            unique_buckets[bucket_key] = {
                                "root": root,
                                "expiry": label,
                                "minute": active_minute_str,
                                "data": subset
                            }
                            
                    buckets = list(unique_buckets.values())
                            
                    if buckets:
                        try:
                            # Upsert to avoid 23505 constraints
                            sb_hist.table("history_buckets_fyers").upsert(buckets, on_conflict="root,expiry,minute").execute()
                            log(f"☁️ Uploaded {len(buckets)} Fyers Buckets to DB.")
                        except Exception as e:
                            log(f"❌ DB Upload Error: {e}")
                    else:
                        log("⚠️ No active Fyers data collected this minute.")
                
                # Reset Minute state
                MINUTE_BUFFER.clear()
                processed_slots = set()
                active_minute_str = current_minute_str

                # Check for Task Updates every minute
                try:
                    res = sb_mgr.table("worker_tasks").select("*").eq("runner_group", RUNNER_ID).execute()
                    current_tasks = res.data or []
                    
                    # Create a simple hash to check if tasks changed
                    current_tasks_str = json.dumps(current_tasks, sort_keys=True)
                    current_hash = hash(current_tasks_str)
                    
                    if current_hash != last_tasks_hash:
                        log(f"🔄 Task List Updated. Found {len(current_tasks)} active tasks.")
                        token = get_fyers_token()
                        
                        if not token:
                            log("❌ No valid Fyers token found. Cannot start WebSocket.")
                            stop_websocket()
                        else:
                            symbols = resolve_symbols(token, current_tasks)
                            if len(symbols) > 0:
                                log(f"🎯 Resolved {len(symbols)} unique Fyers symbols. Connecting WS...")
                                start_websocket(token, symbols)
                            else:
                                log("⏳ No active symbols mapped. Idling and waiting for tasks...")
                                stop_websocket()
                                
                        last_tasks_hash = current_hash
                except Exception as e:
                    log(f"❌ Task Fetch Error: {e}")

            # --- 10 SECOND SNAPSHOTS ---
            current_slot = now.second // 10
            if current_slot > 5: current_slot = 5
            
            if current_slot not in processed_slots and SOCKET_RUNNING:
                for sym in ALL_TRACKED_SYMBOLS:
                    state = STATE_MEMORY[sym]
                    while len(MINUTE_BUFFER[sym]['p']) <= current_slot:
                         MINUTE_BUFFER[sym]['p'].append(None)
                         MINUTE_BUFFER[sym]['v'].append(None)
                         MINUTE_BUFFER[sym]['o'].append(None)
                         MINUTE_BUFFER[sym]['b'].append(None)
                         MINUTE_BUFFER[sym]['s'].append(None)
                    
                    # Only record if we actually have data
                    if state['ltp'] > 0:
                        MINUTE_BUFFER[sym]['p'][current_slot] = state['ltp']
                        MINUTE_BUFFER[sym]['v'][current_slot] = state['vol']
                        MINUTE_BUFFER[sym]['o'][current_slot] = state['oi']
                        MINUTE_BUFFER[sym]['b'][current_slot] = state['b']
                        MINUTE_BUFFER[sym]['s'][current_slot] = state['s']

                processed_slots.add(current_slot)

            time.sleep(0.5)

        except Exception as e:
            log(f"❌ Main Worker Error: {e}")
            time.sleep(5)

# ==============================================================================
# 8. FASTAPI SERVER (Logging & Health Check)
# ==============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=worker_main, args=(GLOBAL_RUN_ID,), daemon=True)
    t.start()
    yield
    stop_websocket()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def status():
    return HTMLResponse(f"""
    <html><body style="background:#000; color:#00ffcc; font-family:monospace; padding:20px;">
    <h2>🟢 FYERS Worker {RUNNER_ID} Active (v5)</h2>
    <div><strong>Run ID:</strong> {GLOBAL_RUN_ID}</div>
    <div><strong>Tracked Symbols:</strong> {len(ALL_TRACKED_SYMBOLS)}</div>
    <div><strong>WebSocket Connected:</strong> {SOCKET_RUNNING}</div>
    <hr style="border-color:#333;">
    <h3>Live Logs:</h3>
    <div style="white-space: pre-wrap; word-wrap: break-word; background:#111; padding:10px; border:1px solid #333;">
    {chr(10).join(WORKER_STATUS['logs'])}
    </div>
    </body></html>
    """)

if __name__ == "__main__":
    print("⚠️ Checking for existing processes on Port 8000...")
    os.system("fuser -k 8000/tcp") 
    time.sleep(2) 
    
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    asyncio.run(server.serve())

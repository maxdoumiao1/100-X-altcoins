# signals_worker.py
import os, json, asyncio, time
from typing import List, Dict
import httpx

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ------------- ENV CONFIG -------------
SHEET_ID = os.environ.get("GOOGLE_SHEETS_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
LUNARCRUSH_KEY = os.environ.get("LUNARCRUSH_KEY", "").strip()
DUNE_KEY = os.environ.get("DUNE_KEY", "").strip()

INBOX_SHEET = os.environ.get("INBOX_SHEET", "Signals_Inbox")
OUT_SHEET   = os.environ.get("OUT_SHEET",   "Signals")

MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "8"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "20"))

# ------------- SHEETS CLIENT -------------
def _build_client():
    if not SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEETS_ID env")
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON env")
    cred_dict = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(cred_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

sh = _build_client()

# ------------- IO HELPERS -------------
def read_inbox() -> List[Dict]:
    try:
        ws = sh.worksheet(INBOX_SHEET)
    except gspread.WorksheetNotFound:
        return []
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return []
    header = [c.strip().lower() for c in rows[0]]
    def idx(name): 
        try: 
            return header.index(name.lower())
        except ValueError:
            return -1
    i_sym = idx("symbol")
    i_chain = idx("chain")
    i_con = idx("contract")
    out = []
    for r in rows[1:]:
        if not any(r): 
            continue
        sym = (r[i_sym] if i_sym >= 0 else "").strip()
        chain = (r[i_chain] if i_chain >= 0 else "").strip()
        con = (r[i_con] if i_con >= 0 else "").strip()
        if con:
            out.append({"symbol": sym, "chain": chain, "contract": con})
    return out

def write_signals(recs: List[Dict]):
    if not recs:
        return
    try:
        ws = sh.worksheet(OUT_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=OUT_SHEET, rows=1000, cols=20)
    header = [
        "symbol","chain","contract",
        "lc_score","lc_social_24h",
        "dune_whale_score","dune_tx_24h",
        "lc_error","dune_error","signals_at"
    ]
    data = []
    for r in recs:
        data.append([
            r.get("symbol",""), r.get("chain",""), r.get("contract",""),
            r.get("lc_score",""), r.get("lc_social_24h",""),
            r.get("dune_whale_score",""), r.get("dune_tx_24h",""),
            r.get("lc_error",""), r.get("dune_error",""), r.get("signals_at",""),
        ])
    ws.clear()
    ws.update("A1", [header] + data)

# ------------- HTTP HELPERS -------------
async def _retryable(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    backoff = 0.5
    for attempt in range(6):
        try:
            r = await client.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"retryable {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == 5:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8)

async def fetch_lunarcrush(ac: httpx.AsyncClient, symbol: str) -> Dict:
    if not LUNARCRUSH_KEY:
        return {}
    headers = {"Authorization": f"Bearer {LUNARCRUSH_KEY}"}
    url = f"https://api.lunarcrush.com/v2?data=assets&symbol={symbol.upper()}"
    try:
        r = await _retryable(ac, "GET", url, headers=headers)
        data = r.json()
        item = (data.get("data") or [{}])[0]
        return {
            "lc_score": item.get("galaxy_score"),
            "lc_social_24h": item.get("social_volume"),
        }
    except Exception as e:
        return {"lc_error": str(e)[:180]}

async def fetch_dune(ac: httpx.AsyncClient, contract: str) -> Dict:
    if not DUNE_KEY:
        return {}
    headers = {"X-Dune-API-Key": DUNE_KEY}
    url = f"https://api.dune.com/api/v1/whales?contract={contract}"  # TODO: replace
    try:
        r = await _retryable(ac, "GET", url, headers=headers)
        data = r.json()
        return {
            "dune_whale_score": data.get("score"),
            "dune_tx_24h": data.get("tx_24h"),
        }
    except Exception as e:
        return {"dune_error": str(e)[:180]}

async def process_token(ac: httpx.AsyncClient, token: Dict) -> Dict:
    lc, dn = await asyncio.gather(
        fetch_lunarcrush(ac, token.get("symbol","")),
        fetch_dune(ac, token.get("contract","")),
    )
    out = {**token, **lc, **dn, "signals_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    return out

async def main_async():
    tokens = read_inbox()
    print(f"Read {len(tokens)} tokens from inbox")
    if not tokens:
        return {"status": "ok", "count": 0}
    limits = httpx.Limits(max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as ac:
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        async def wrapped(t):
            async with sem:
                return await process_token(ac, t)
        results = await asyncio.gather(*(wrapped(t) for t in tokens))
    write_signals(results)
    print(f"Wrote {len(results)} rows to {OUT_SHEET}")
    return {"status": "ok", "count": len(results)}

def main():
    return asyncio.run(main_async())

if __name__ == "__main__":
    print(main())

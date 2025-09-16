import os, json, asyncio, time
from typing import List, Dict
import httpx

import gspread
from google.oauth2.service_account import Credentials

# ====== ENV ======
SHEET_ID = os.environ.get("GOOGLE_SHEETS_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

LUNARCRUSH_KEY = os.environ.get("LUNARCRUSH_KEY", "").strip()

DUNE_KEY = os.environ.get("DUNE_KEY", "").strip()
DUNE_QUERY_ID = os.environ.get("DUNE_QUERY_ID", "").strip()   # 必填（第1步拿到）
DUNE_POLL_MAX_SEC = int(os.environ.get("DUNE_POLL_MAX_SEC", "60"))

INBOX_SHEET = os.environ.get("INBOX_SHEET", "Signals_Inbox")
OUT_SHEET   = os.environ.get("OUT_SHEET",   "Signals")

MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "8"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))

# ====== Google Sheets client ======
def _build_client():
    if not SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEETS_ID")
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    cred_dict = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(cred_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

sh = _build_client()

# ====== Sheets IO ======
def read_inbox() -> List[Dict]:
    try:
        ws = sh.worksheet(INBOX_SHEET)
    except gspread.WorksheetNotFound:
        return []
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return []
    H = [c.strip().lower() for c in rows[0]]
    def idx(name): 
        try: return H.index(name.lower())
        except ValueError: return -1
    i_sym, i_chain, i_con = idx("symbol"), idx("chain"), idx("contract")
    out = []
    for r in rows[1:]:
        if not any(r): 
            continue
        con = (r[i_con] if i_con >= 0 else "").strip()
        if con:
            out.append({
                "symbol": (r[i_sym] if i_sym>=0 else "").strip(),
                "chain":  (r[i_chain] if i_chain>=0 else "").strip(),
                "contract": con
            })
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

# ====== HTTP helpers ======
async def _retryable(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    backoff = 0.6
    for attempt in range(7):
        try:
            r = await client.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"retryable {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == 6:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8, 8)

# ====== LunarCrush (两个方式都试) ======
async def fetch_lunarcrush(ac: httpx.AsyncClient, symbol: str) -> Dict:
    if not LUNARCRUSH_KEY or not symbol:
        return {}
    sym = symbol.upper().strip()
    try:
        # 方式一：key 放在 query（经典 v2 写法）
        url1 = f"https://api.lunarcrush.com/v2?data=assets&symbol={sym}&key={LUNARCRUSH_KEY}"
        r = await _retryable(ac, "GET", url1)
        data = r.json()
        item = (data.get("data") or [{}])[0]
        out = {
            "lc_score": item.get("galaxy_score"),
            "lc_social_24h": item.get("social_volume") or item.get("social_volume_24h"),
        }
        if out["lc_score"] is not None or out["lc_social_24h"] is not None:
            return out
        # 若字段为空，继续试方式二
    except Exception as e:
        # 再试方式二
        pass

    try:
        # 方式二：Bearer（部分账户/网关需要）
        url2 = f"https://api.lunarcrush.com/v2?data=assets&symbol={sym}"
        r = await _retryable(ac, "GET", url2, headers={"Authorization": f"Bearer {LUNARCRUSH_KEY}"})
        data = r.json()
        item = (data.get("data") or [{}])[0]
        return {
            "lc_score": item.get("galaxy_score"),
            "lc_social_24h": item.get("social_volume") or item.get("social_volume_24h"),
        }
    except Exception as e:
        return {"lc_error": str(e)[:180]}

# ====== Dune（执行 -> 轮询结果） ======
async def fetch_dune(ac: httpx.AsyncClient, contract: str) -> Dict:
    if not DUNE_KEY or not DUNE_QUERY_ID or not contract:
        return {}
    headers = {
        "X-Dune-API-Key": DUNE_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # 1) 执行查询（把合约地址作为参数传入你在 Dune 定义的 :contract）
    try:
        run = await _retryable(
            ac, "POST",
            f"https://api.dune.com/api/v1/query/{DUNE_QUERY_ID}/execute",
            headers=headers,
            json={"query_parameters": {"contract": contract}}
        )
        execution_id = run.json().get("execution_id")
        if not execution_id:
            return {"dune_error": f"No execution_id from Dune"}
    except Exception as e:
        return {"dune_error": f"execute err: {str(e)[:150]}"}

    # 2) 轮询结果（直接 GET results，404/空时重试）
    start = time.time()
    last_err = None
    while time.time() - start < DUNE_POLL_MAX_SEC:
        try:
            res = await ac.get(f"https://api.dune.com/api/v1/execution/{execution_id}/results",
                               headers=headers, timeout=HTTP_TIMEOUT)
            if res.status_code == 200:
                j = res.json()
                rows = (j.get("result") or {}).get("rows") or []
                if rows:
                    row = rows[0]
                    # 兼容不同字段名
                    whale = row.get("whale_score") or row.get("score") or row.get("whale") or 0
                    tx24  = row.get("tx_24h") or row.get("txns_24h") or row.get("tx_count_24h") or 0
                    return {"dune_whale_score": whale, "dune_tx_24h": tx24}
            elif res.status_code == 404:
                last_err = "404 (not ready)"
            else:
                last_err = f"{res.status_code}: {res.text[:120]}"
        except Exception as e:
            last_err = str(e)[:150]
        await asyncio.sleep(2.0)
    return {"dune_error": last_err or "timeout waiting result"}

# ====== Pipeline ======
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

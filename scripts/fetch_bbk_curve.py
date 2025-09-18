#!/usr/bin/env python3
import os, sys, time, json, requests, pandas as pd

HEAD = {"User-Agent": "Mozilla/5.0"}

# Three alternative endpoints per series (some networks/providers hiccup intermittently):
# A) "series_ids" aggregator, B) single-series endpoint, C) editor endpoint (json)
URLS = [
    # A: aggregator (can take multiple, we still call one-by-one for simplicity)
    lambda code: f"https://api.db.nomics.world/v22/series?observations=1&format=json&series_ids=BUBA/BBK01/{code}",
    # B: direct single-series endpoint
    lambda code: f"https://api.db.nomics.world/v22/series/BUBA/BBK01/{code}?observations=1&format=json&facets=0&offset=0",
    # C: editor mirror (also JSON)
    lambda code: f"https://editor.nomics.world/api/series?series_id=BUBA/BBK01/{code}"
]

CODES = {
    "2Y": "WT0202",
    "5Y": "WT0505",
    "7Y_A": "WT0707",
    "7Y_B": "WT7070",
    "10Y": "WT1010",
    "15Y": "WT1515",
    "30Y": "WT3030",
}

def _http_get(url, max_tries=3, sleep=1.5):
    last = None
    for i in range(max_tries):
        try:
            r = requests.get(url, headers=HEAD, timeout=45)
            if r.status_code == 200 and r.text.strip():
                return r
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(sleep)
    raise RuntimeError(last or "empty response")

def _parse_series_json(js):
    # Shape A ("series_ids"): {"series": [{"values":[["2024-01-02",2.3], ...]}]}
    if isinstance(js, dict) and "series" in js and isinstance(js["series"], list) and js["series"]:
        vals = js["series"][0].get("values") or []
        rows = []
        for v in vals:
            if isinstance(v, (list, tuple)) and len(v) >= 2:
                rows.append((v[0], v[1]))
            elif isinstance(v, dict):
                rows.append((v.get("period"), v.get("value")))
        if rows:
            df = pd.DataFrame(rows, columns=["Date", "value"])
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            s = df.dropna(subset=["Date"]).set_index("Date")["value"].sort_index().astype(float)
            return s

    # Shape C (editor API): {"series":{"values":[["2024-01-02",2.3], ...]}}
    if isinstance(js, dict) and "series" in js and isinstance(js["series"], dict):
        vals = js["series"].get("values") or []
        rows = []
        for v in vals:
            if isinstance(v, (list, tuple)) and len(v) >= 2:
                rows.append((v[0], v[1]))
            elif isinstance(v, dict):
                rows.append((v.get("period"), v.get("value")))
        if rows:
            df = pd.DataFrame(rows, columns=["Date", "value"])
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            s = df.dropna(subset=["Date"]).set_index("Date")["value"].sort_index().astype(float)
            return s

    return pd.Series(dtype=float)

def fetch_series(code: str) -> pd.Series:
    errors = []
    for make in URLS:
        url = make(code)
        try:
            r = _http_get(url)
            try:
                js = r.json()
            except Exception:
                # Some endpoints might send JSON with text/plain; attempt json load
                js = json.loads(r.text)
            s = _parse_series_json(js)
            if not s.empty:
                print(f"[OK] {code} via {url}")
                return s
            else:
                errors.append(f"empty via {url}")
        except Exception as e:
            errors.append(f"{url} -> {e}")
    print("[ERR] " + " | ".join(errors), file=sys.stderr)
    return pd.Series(dtype=float)

def main():
    got = {}

    # Anchor 10Y first
    s10 = fetch_series(CODES["10Y"])
    if s10.empty:
        print("ERROR: 10Y WT1010 empty across all endpoints", file=sys.stderr)
        sys.exit(2)
    got["10Y"] = s10

    for lbl, code in [("2Y","WT0202"),("5Y","WT0505"),("15Y","WT1515"),("30Y","WT3030")]:
        s = fetch_series(code)
        if not s.empty:
            got[lbl] = s

    s7a, s7b = fetch_series(CODES["7Y_A"]), fetch_series(CODES["7Y_B"])
    if s7a.size or s7b.size:
        got["7Y"] = s7a if s7a.size >= s7b.size else s7b

    if not got:
        print("ERROR: no series fetched", file=sys.stderr)
        sys.exit(2)

    df = pd.DataFrame(got).sort_index().dropna(how="all")
    df.index.name = "Date"
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/de_bbk_curve.csv", float_format="%.6f", date_format="%Y-%m-%d")
    print("Wrote data/de_bbk_curve.csv | cols:", list(df.columns), "| last:", df.index.max().date())

if __name__ == "__main__":
    main()

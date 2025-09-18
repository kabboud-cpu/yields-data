#!/usr/bin/env python3
import os, sys, io, time, json, requests, pandas as pd

# We’ll try JSON first (with an explicit time window), then two CSV variants.
BBK_DATA     = "https://api.statistiken.bundesbank.de/rest/data/BBK01/{code}?startPeriod={start}"
BBK_DL_HYPH  = "https://api.statistiken.bundesbank.de/rest/download/BBK01/{code}?format=sdmx-csv&startPeriod={start}"
BBK_DL_UND   = "https://api.statistiken.bundesbank.de/rest/download/BBK01/{code}?format=sdmx_csv&startPeriod={start}"

HEAD_JSON = {"Accept":"application/vnd.sdmx.data+json;version=1.0.0", "User-Agent":"Mozilla/5.0"}
HEAD_CSV  = {"Accept":"application/vnd.sdmx.data+csv;version=1.0.0",  "User-Agent":"Mozilla/5.0"}

START = os.getenv("START_DATE", "2020-01-01")

CODES = {
    "2Y": "WT0202",
    "5Y": "WT0505",
    "7Y_A": "WT0707",
    "7Y_B": "WT7070",
    "10Y": "WT1010",
    "15Y": "WT1515",
    "30Y": "WT3030",
}

def _get(url, headers, tries=3, pause=1.2):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=45)
            if r.status_code == 200 and r.text.strip():
                return r
            last = f"HTTP {r.status_code}: {r.text[:180]}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(pause)
    raise RuntimeError(last or "empty response")

def _parse_json(text: str) -> pd.Series:
    js = json.loads(text)
    ds = js.get("dataSets", [{}])[0]
    series = ds.get("series", {})
    if not series:
        return pd.Series(dtype=float)
    key = next(iter(series))
    obs = series[key].get("observations", {})
    times = js["structure"]["dimensions"]["observation"][0]["values"]
    idx = [pd.to_datetime(t["id"], errors="coerce") for t in times]
    s = pd.Series({idx[int(k)]: v[0] for k, v in obs.items()}, dtype=float).dropna().sort_index()
    return s

def _parse_csv(text: str) -> pd.Series:
    df = pd.read_csv(io.StringIO(text))
    cols = {c.lower(): c for c in df.columns}
    dcol = cols.get("time_period") or cols.get("date")
    vcol = cols.get("obs_value")  or cols.get("value") or cols.get("close")
    if not dcol or not vcol:
        return pd.Series(dtype=float)
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    df[vcol] = pd.to_numeric(df[vcol], errors="coerce")
    return df.dropna(subset=[dcol]).set_index(dcol)[vcol].sort_index().astype(float)

def fetch_series(code: str) -> pd.Series:
    # 1) JSON (preferred) with startPeriod
    try:
        r = _get(BBK_DATA.format(code=code, start=START), HEAD_JSON)
        s = _parse_json(r.text)
        if not s.empty:
            print(f"[OK JSON] {code} ({len(s)} pts)")
            return s
        else:
            print(f"[JSON empty] {code}", file=sys.stderr)
    except Exception as e:
        print(f"[JSON fail] {code}: {e}", file=sys.stderr)

    # 2) CSV with hyphen spelling
    try:
        r = _get(BBK_DL_HYPH.format(code=code, start=START), HEAD_CSV)
        s = _parse_csv(r.text)
        if not s.empty:
            print(f"[OK CSV hyphen] {code} ({len(s)} pts)")
            return s
        else:
            print(f"[CSV hyphen empty] {code}", file=sys.stderr)
    except Exception as e:
        print(f"[CSV hyphen fail] {code}: {e}", file=sys.stderr)

    # 3) CSV with underscore spelling (some mirrors accept this)
    try:
        r = _get(BBK_DL_UND.format(code=code, start=START), HEAD_CSV)
        s = _parse_csv(r.text)
        if not s.empty:
            print(f"[OK CSV underscore] {code} ({len(s)} pts)")
            return s
        else:
            print(f"[CSV underscore empty] {code}", file=sys.stderr)
    except Exception as e:
        print(f"[CSV underscore fail] {code}: {e}", file=sys.stderr)

    return pd.Series(dtype=float)

def main():
    got = {}

    # Anchor 10Y first; stop if empty so we don’t commit partials
    s10 = fetch_series(CODES["10Y"])
    if s10.empty:
        print("ERROR: 10Y WT1010 still empty after JSON+CSV variants", file=sys.stderr)
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

#!/usr/bin/env python3
import os, sys, io, time, json, requests, pandas as pd

# Use the correct dataflow (BBSSY) + full series keys from Bundesbank
# We'll try JSON first, then CSV (hyphen spelling). We pass startPeriod to avoid empty responses.

DATA_JSON = "https://api.statistiken.bundesbank.de/rest/data/BBSSY/{series}?startPeriod={start}"
DL_CSV    = "https://api.statistiken.bundesbank.de/rest/download/BBSSY/{series}?format=sdmx-csv&startPeriod={start}"

HEAD_JSON = {"Accept":"application/vnd.sdmx.data+json;version=1.0.0", "User-Agent":"Mozilla/5.0"}
HEAD_CSV  = {"Accept":"application/vnd.sdmx.data+csv;version=1.0.0",  "User-Agent":"Mozilla/5.0"}

START = os.getenv("START_DATE", "2020-01-01")

SERIES = {
    "30Y": "D.REN.EUR.A640.000000WT3030.A",
    "15Y": "D.REN.EUR.A615.000000WT1515.A",
    "10Y": "D.REN.EUR.A630.000000WT1010.A",
    "7Y":  "D.REN.EUR.A607.000000WT7070.A",
    "5Y":  "D.REN.EUR.A620.000000WT0505.A",
    "2Y":  "D.REN.EUR.A610.000000WT0202.A",
}

def _get(url, headers, tries=3, pause=1.0):
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

def fetch(series_key: str) -> pd.Series:
    # JSON first
    try:
        r = _get(DATA_JSON.format(series=series_key, start=START), HEAD_JSON)
        s = _parse_json(r.text)
        if not s.empty:
            print(f"[OK JSON] {series_key} ({len(s)} pts)")
            return s
        else:
            print(f"[JSON empty] {series_key}", file=sys.stderr)
    except Exception as e:
        print(f"[JSON fail] {series_key}: {e}", file=sys.stderr)

    # CSV fallback
    try:
        r = _get(DL_CSV.format(series=series_key, start=START), HEAD_CSV)
        s = _parse_csv(r.text)
        if not s.empty:
            print(f"[OK CSV] {series_key} ({len(s)} pts)")
            return s
        else:
            print(f"[CSV empty] {series_key}", file=sys.stderr)
    except Exception as e:
        print(f"[CSV fail] {series_key}: {e}", file=sys.stderr)

    return pd.Series(dtype=float)

def main():
    got = {}

    # Anchor (10Y) first
    s10 = fetch(SERIES["10Y"])
    if s10.empty:
        print("ERROR: 10Y empty via BBSSY endpoints", file=sys.stderr)
        sys.exit(2)
    got["10Y"] = s10

    # Others
    for lbl in ["2Y","5Y","7Y","15Y","30Y"]:
        s = fetch(SERIES[lbl])
        if not s.empty:
            got[lbl] = s

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

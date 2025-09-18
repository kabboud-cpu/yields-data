#!/usr/bin/env python3
import os, sys, io, time, json, requests, pandas as pd

BBK_DOWNLOAD = "https://api.statistiken.bundesbank.de/rest/download/BBK01/{code}?format=sdmx_csv"
BBK_DATA     = "https://api.statistiken.bundesbank.de/rest/data/BBK01/{code}"
HEAD_CSV  = {"Accept":"application/vnd.sdmx.data+csv;version=1.0.0",  "User-Agent":"Mozilla/5.0"}
HEAD_JSON = {"Accept":"application/vnd.sdmx.data+json;version=1.0.0", "User-Agent":"Mozilla/5.0"}

CODES = {
    "2Y": "WT0202",
    "5Y": "WT0505",
    "7Y_A": "WT0707",     # try both encodings for 7Y
    "7Y_B": "WT7070",
    "10Y": "WT1010",
    "15Y": "WT1515",
    "30Y": "WT3030",
}

def _get(url, headers, tries=3, pause=1.5):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=45)
            if r.status_code == 200 and r.text.strip():
                return r
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(pause)
    raise RuntimeError(last or "empty response")

def _parse_csv(text: str) -> pd.Series:
    df = pd.read_csv(io.StringIO(text))
    cols = {c.lower(): c for c in df.columns}
    dcol = cols.get("time_period") or cols.get("date")
    vcol = cols.get("obs_value")  or cols.get("value") or cols.get("close")
    if not dcol or not vcol:
        return pd.Series(dtype=float)
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    df[vcol] = pd.to_numeric(df[vcol], errors="coerce")
    s = df.dropna(subset=[dcol]).set_index(dcol)[vcol].sort_index()
    return s.astype(float)

def _parse_json(text: str) -> pd.Series:
    js = json.loads(text)
    ds = js.get("dataSets", [{}])[0]
    series = ds.get("series", {})
    if not series:
        return pd.Series(dtype=float)
    key = next(iter(series))
    obs = series[key].get("observations", {})
    times = js["structure"]["dimensions"]["observation"][0]["values"]
    dates = [pd.to_datetime(t["id"], errors="coerce") for t in times]
    s = pd.Series({dates[int(k)]: v[0] for k, v in obs.items()}, dtype=float).dropna().sort_index()
    return s

def fetch_series(code: str) -> pd.Series:
    # 1) try the simpler download CSV endpoint
    try:
        r = _get(BBK_DOWNLOAD.format(code=code), HEAD_CSV)
        s = _parse_csv(r.text)
        if not s.empty:
            print(f"[OK CSV] {code} ({len(s)} pts)")
            return s
        else:
            print(f"[CSV empty] {code}", file=sys.stderr)
    except Exception as e:
        print(f"[CSV fail] {code}: {e}", file=sys.stderr)

    # 2) fallback: SDMX-JSON endpoint
    try:
        r = _get(BBK_DATA.format(code=code), HEAD_JSON)
        s = _parse_json(r.text)
        if not s.empty:
            print(f"[OK JSON] {code} ({len(s)} pts)")
            return s
        else:
            print(f"[JSON empty] {code}", file=sys.stderr)
    except Exception as e:
        print(f"[JSON fail] {code}: {e}", file=sys.stderr)

    return pd.Series(dtype=float)

def main():
    got = {}

    # Anchor 10Y first; stop if empty
    s10 = fetch_series(CODES["10Y"])
    if s10.empty:
        print("ERROR: 10Y WT1010 empty across CSV+JSON endpoints", file=sys.stderr)
        sys.exit(2)
    got["10Y"] = s10

    # Other key points
    for lbl, code in [("2Y","WT0202"),("5Y","WT0505"),("15Y","WT1515"),("30Y","WT3030")]:
        s = fetch_series(code)
        if not s.empty:
            got[lbl] = s

    # 7Y: choose the variant with more points
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

#!/usr/bin/env python3
import os, sys, requests, pandas as pd

# DBnomics v22 API (mirror of Bundesbank BBK01)
SERIES_URL = "https://api.db.nomics.world/v22/series/BUBA/BBK01/{code}?observations=1&format=json&facets=0&offset=0"
HEAD = {"User-Agent": "Mozilla/5.0"}

CODES = {
    "2Y": "WT0202",
    "5Y": "WT0505",
    "7Y_A": "WT0707",   # 7Y appears under two codes; we’ll keep the longer one
    "7Y_B": "WT7070",
    "10Y": "WT1010",
    "15Y": "WT1515",
    "30Y": "WT3030",
}

def fetch_series(code: str) -> pd.Series:
    url = SERIES_URL.format(code=code)
    r = requests.get(url, headers=HEAD, timeout=60)
    r.raise_for_status()
    js = r.json()
    items = js.get("series") or []
    if not items:
        return pd.Series(dtype=float)
    vals = items[0].get("values") or []
    rows = []
    for v in vals:
        # values come as [period, value]
        if isinstance(v, (list, tuple)) and len(v) >= 2:
            rows.append((v[0], v[1]))
        elif isinstance(v, dict):
            rows.append((v.get("period"), v.get("value")))
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["Date", "value"])
    df["Date"]  = pd.to_datetime(df["Date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    s = df.dropna(subset=["Date"]).set_index("Date")["value"].sort_index().astype(float)
    return s

def main():
    got = {}

    # Anchor first (10Y). If this fails, we stop (no partial commits).
    s10 = fetch_series(CODES["10Y"])
    if s10.empty:
        print("ERROR: WT1010 returned no data from DBnomics", file=sys.stderr)
        sys.exit(2)
    got["10Y"] = s10

    # Other fixed points
    for lbl, code in [("2Y","WT0202"),("5Y","WT0505"),("15Y","WT1515"),("30Y","WT3030")]:
        s = fetch_series(code)
        if not s.empty:
            got[lbl] = s

    # 7Y (choose longer series between the two)
    s7a, s7b = fetch_series(CODES["7Y_A"]), fetch_series(CODES["7Y_B"])
    if s7a.size or s7b.size:
        got["7Y"] = s7a if s7a.size >= s7b.size else s7b

    if not got:
        print("ERROR: No series fetched", file=sys.stderr)
        sys.exit(2)

    df = pd.DataFrame(got).sort_index().dropna(how="all")
    df.index.name = "Date"
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/de_bbk_curve.csv", float_format="%.6f", date_format="%Y-%m-%d")
    print("Wrote data/de_bbk_curve.csv | cols:", list(df.columns), "| last:", df.index.max().date())

if __name__ == "__main__":
    main()

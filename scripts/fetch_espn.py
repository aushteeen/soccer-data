"""
fetch_espn.py — build and refresh data/espn/matches.csv (played matches with shots on
target, fouls, yellow/red cards and the referee) and data/espn/fixtures.csv (upcoming
matches) from ESPN's public JSON endpoints, for any of ESPN's 200+ soccer competitions:
domestic leagues, domestic cups, UEFA, CONMEBOL/CONCACAF, MLS, internationals.

Endpoints (no key needed):
  scoreboard  site.api.espn.com/apis/site/v2/sports/soccer/<slug>/scoreboard?dates=YYYYMMDD-YYYYMMDD
              -> events, teams, score, status, per-team shotsOnTarget / foulsCommitted / totalShots
  summary     site.api.espn.com/apis/site/v2/sports/soccer/<slug>/summary?event=<id>
              -> boxscore team stats incl. Yellow Cards / Red Cards / Fouls / ON GOAL, referee, venue
Cards and the referee only exist in the summary, so every completed match costs one
summary request (cached: never re-fetched once stored). Incremental by design: the
state file remembers how far each competition has been pulled.

Usage:
  python3 fetch_espn.py --data data                       # core competitions, current + previous season
  python3 fetch_espn.py --data data --groups all
  python3 fetch_espn.py --data data --competitions eng.1 uefa.champions usa.1 --since 2025-07-01
  python3 fetch_espn.py --data data --source mirror       # sandbox: pull the CSVs from the GitHub mirror
  python3 fetch_espn.py --list-competitions               # print the group definitions

Writes data/espn/matches.csv, data/espn/fixtures.csv, data/espn/state.json, data/espn/coverage.csv.
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"
UA = {"User-Agent": "Mozilla/5.0 (soccer-match-stats-predictor)"}
DEFAULT_MIRROR = "https://raw.githubusercontent.com/aushteeen/soccer-data/main"

GROUPS = {
    "europe_leagues": ["eng.1", "eng.2", "eng.3", "eng.4", "eng.5", "esp.1", "esp.2", "ita.1", "ita.2", "ger.1", "ger.2",
                       "fra.1", "fra.2", "ned.1", "ned.2", "por.1", "bel.1", "tur.1", "gre.1", "sco.1", "sco.2",
                       "aut.1", "swe.1", "den.1", "nor.1", "rus.1"],
    "europe_cups": ["eng.fa", "eng.league_cup", "eng.trophy", "esp.copa_del_rey", "esp.super_cup", "ita.coppa_italia",
                    "ita.super_cup", "ger.dfb_pokal", "ger.super_cup", "fra.coupe_de_france", "fra.super_cup",
                    "ned.cup", "ned.supercup", "por.taca.portugal", "sco.tennents", "sco.cis", "sco.challenge"],
    "uefa": ["uefa.champions", "uefa.champions_qual", "uefa.europa", "uefa.europa_qual", "uefa.europa.conf",
             "uefa.europa.conf_qual", "uefa.super_cup"],
    "americas": ["usa.1", "usa.open", "usa.usl.1", "mex.1", "mex.2", "mex.campeon", "concacaf.champions",
                 "concacaf.leagues.cup", "concacaf.champions_cup", "bra.1", "bra.2", "bra.copa_do_brazil", "arg.1",
                 "arg.copa", "chi.1", "col.1", "col.copa", "uru.1", "per.1", "ecu.1", "par.1", "conmebol.libertadores",
                 "conmebol.sudamericana", "conmebol.recopa"],
    "rest_of_world": ["jpn.1", "ksa.1", "ksa.kings.cup", "aus.1", "chn.1", "ind.1", "rsa.1", "afc.champions",
                      "caf.champions", "caf.confed", "fifa.cwc", "fifa.intercontinental_cup"],
    "international": ["uefa.nations", "uefa.euro", "uefa.euroq", "fifa.world", "fifa.worldq.uefa", "fifa.worldq.conmebol",
                      "fifa.worldq.concacaf", "fifa.worldq.afc", "fifa.worldq.caf", "fifa.friendly", "conmebol.america",
                      "concacaf.gold", "concacaf.nations.league", "caf.nations", "afc.asian.cup"],
}
GROUPS["core"] = GROUPS["europe_leagues"][:20] + GROUPS["europe_cups"] + GROUPS["uefa"] + GROUPS["americas"][:8]
GROUPS["all"] = sorted({s for k, v in GROUPS.items() if k != "core" for s in v})

MATCH_COLS = ["event_id", "competition", "season", "date", "home_id", "home", "away_id", "away", "neutral", "completed",
              "state", "FTHG", "FTAG", "HS", "AS", "HST", "AST", "HF", "AF", "HY", "AY", "HR", "AR", "referee", "venue",
              "attendance", "has_summary", "fetched_at"]
STAT_MAP = {  # ESPN stat name / label (lowercased) -> our column suffix
    "shotsontarget": "ST", "on goal": "ST", "shots on goal": "ST", "shots on target": "ST",
    "foulscommitted": "F", "fouls": "F", "yellowcards": "Y", "yellow cards": "Y", "redcards": "R", "red cards": "R",
    "totalshots": "S", "shots": "S", "total shots": "S",
}


class Blocked(Exception):
    pass


def get_json(url: str, timeout: int = 30, retries: int = 2):
    last = None
    for i in range(retries + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (400, 404):
                return None
            time.sleep(1.5 * (i + 1))
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            msg = str(e)
            if "403" in msg or "Tunnel" in msg or "connect_rejected" in msg:
                raise Blocked(msg)
            time.sleep(1.5 * (i + 1))
    raise Blocked(str(last))


def _num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def parse_scoreboard(js: dict, slug: str, now_iso: str) -> list[dict]:
    out = []
    for ev in js.get("events", []) or []:
        comp = (ev.get("competitions") or [{}])[0]
        st = (ev.get("status") or comp.get("status") or {}).get("type", {})
        rec = {"event_id": str(ev.get("id")), "competition": slug,
               "season": str((ev.get("season") or {}).get("year", "")), "date": ev.get("date"),
               "neutral": bool(comp.get("neutralSite", False)), "completed": bool(st.get("completed", False)),
               "state": st.get("state", ""), "venue": (comp.get("venue") or {}).get("fullName", ""),
               "attendance": comp.get("attendance"), "referee": "", "has_summary": False, "fetched_at": now_iso}
        for c in comp.get("competitors", []) or []:
            side = "home" if c.get("homeAway") == "home" else "away"
            team = c.get("team") or {}
            rec[f"{side}_id"] = str(team.get("id", ""))
            rec[side] = team.get("displayName") or team.get("name") or ""
            rec["FTHG" if side == "home" else "FTAG"] = _num(c.get("score")) if rec["completed"] else None
            pre = "H" if side == "home" else "A"
            for s in c.get("statistics", []) or []:
                key = STAT_MAP.get(str(s.get("name", "")).lower()) or STAT_MAP.get(str(s.get("label", "")).lower())
                if key:
                    rec[f"{pre}{key}"] = _num(s.get("displayValue", s.get("value")))
        if rec.get("home") and rec.get("away"):
            out.append(rec)
    return out


def parse_summary(js: dict) -> dict:
    """Cards, fouls, shots, referee, neutral flag from a summary payload."""
    out = {}
    teams = (js.get("boxscore") or {}).get("teams") or []
    for t in teams:
        side = "home" if t.get("homeAway") == "home" else "away"
        pre = "H" if side == "home" else "A"
        for s in t.get("statistics", []) or []:
            key = STAT_MAP.get(str(s.get("name", "")).lower()) or STAT_MAP.get(str(s.get("label", "")).lower())
            if key:
                out[f"{pre}{key}"] = _num(s.get("displayValue", s.get("value")))
    offs = (js.get("gameInfo") or {}).get("officials") or []
    ref = ""
    for o in offs:
        pos = str((o.get("position") or {}).get("name", o.get("position", ""))).lower()
        if "referee" in pos and "assistant" not in pos and "video" not in pos and "fourth" not in pos:
            ref = o.get("displayName") or o.get("fullName") or ""
            break
    if not ref and offs:
        ref = offs[0].get("displayName") or offs[0].get("fullName") or ""
    out["referee"] = ref.strip()
    hdr = ((js.get("header") or {}).get("competitions") or [{}])[0]
    if "neutralSite" in hdr:
        out["neutral"] = bool(hdr.get("neutralSite"))
    out["has_summary"] = bool(teams)
    return out


def load_csv(path: Path, cols: list[str]) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path, dtype={"event_id": str, "home_id": str, "away_id": str, "season": str})
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df[cols]
    return pd.DataFrame(columns=cols)


def season_start(today: dt.date, back: int = 1) -> dt.date:
    """July 1 of the previous European season start year (covers MLS/Liga calendar seasons too)."""
    y = today.year if today.month >= 7 else today.year - 1
    return dt.date(y - back, 7, 1)


def fetch_direct(a, data: Path, now_iso: str):
    state_path = data / "state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"competitions": {}}
    matches = load_csv(data / "matches.csv", MATCH_COLS)
    by_id = {r["event_id"]: dict(r) for r in matches.to_dict("records")}
    until = dt.date.today() + dt.timedelta(days=a.ahead)
    comps = a.competitions or sorted({s for g in a.groups for s in GROUPS[g]})
    new_events = 0
    for slug in comps:
        st = state["competitions"].setdefault(slug, {})
        start = dt.date.fromisoformat(a.since) if a.since else season_start(dt.date.today())
        if st.get("last_scoreboard") and not a.since:
            start = max(start, dt.date.fromisoformat(st["last_scoreboard"]) - dt.timedelta(days=4))
        # ESPN's scoreboard is reliable for a SINGLE date only (date ranges return nothing for UEFA
        # competitions, and 'limit' is rejected), so query day by day. When the league metadata
        # carries a plain list of match dates, restrict to those; otherwise try every day.
        # The calendar in the league metadata is per SEASON, so probe at the start, at the end
        # and at every 1 Jan / 1 Jul in between and union the match dates we get back.
        probes = {start, until}
        y = start.year
        while y <= until.year:
            for m in (1, 7):
                d = dt.date(y, m, 1)
                if start < d < until:
                    probes.add(d)
            y += 1
        cal = set()
        cal_ok = True
        for pd_ in sorted(probes):
            probe = get_json(f"{BASE}/{slug}/scoreboard?dates={pd_:%Y%m%d}")
            time.sleep(a.delay)
            raw_cal = ((probe or {}).get("leagues") or [{}])[0].get("calendar")
            if isinstance(raw_cal, list) and raw_cal and all(isinstance(x, str) for x in raw_cal):
                cal |= {dt.date.fromisoformat(x[:10]) for x in raw_cal}
            else:
                cal_ok = False     # phase-style calendar (UEFA etc.): no date list -> try every day
        days = [start + dt.timedelta(days=i) for i in range((until - start).days + 1)]
        if cal_ok and cal:
            days = [d for d in days if d in cal] or days
        else:
            cal = None

        def one_day(d):
            try:
                js = get_json(f"{BASE}/{slug}/scoreboard?dates={d:%Y%m%d}")
            except Blocked:
                return d, None
            time.sleep(a.delay)
            return d, (parse_scoreboard(js, slug, now_iso) if js else [])

        n_slug = 0
        blocked = False
        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            for d, recs in ex.map(one_day, days):
                if recs is None:
                    blocked = True
                    continue
                for rec in recs:
                    old = by_id.get(rec["event_id"], {})
                    merged = {**old, **{k: v for k, v in rec.items() if v is not None and v != ""}}
                    merged["has_summary"] = bool(old.get("has_summary", False))
                    merged["referee"] = old.get("referee", "") or ""
                    for k in ("HY", "AY", "HR", "AR"):
                        if old.get(k) is not None and not pd.isna(old.get(k)):
                            merged[k] = old[k]
                    by_id[rec["event_id"]] = merged
                    n_slug += 1
        if blocked:
            raise Blocked("scoreboard requests refused")
        st["last_scoreboard"] = until.isoformat()
        new_events += n_slug
        print(f"  {slug:24s} {n_slug:4d} events over {len(days)} dates{' (calendar)' if cal else ''}", file=sys.stderr)
    if new_events == 0 and not by_id:
        raise RuntimeError("ESPN returned zero events for every competition — refusing to write an empty mirror")

    if not a.no_summaries:
        todo = [eid for eid, r in by_id.items() if r.get("completed") and not r.get("has_summary")]
        print(f"fetching {len(todo)} match summaries (cards, referee) with {a.workers} workers ...", file=sys.stderr)

        def one(eid):
            slug = by_id[eid]["competition"]
            try:
                js = get_json(f"{BASE}/{slug}/summary?event={eid}")
            except Blocked:
                return eid, None
            time.sleep(a.delay)
            return eid, (parse_summary(js) if js else {"has_summary": False})

        done = 0
        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            for eid, res in ex.map(one, todo):
                if res is None:
                    continue
                by_id[eid].update({k: v for k, v in res.items() if v is not None})
                if not res.get("has_summary"):
                    by_id[eid]["has_summary"] = True   # ESPN has no boxscore for this match; don't retry daily
                    by_id[eid]["no_boxscore"] = True
                done += 1
                if done % 200 == 0:
                    print(f"  {done}/{len(todo)}", file=sys.stderr)

    df = pd.DataFrame(list(by_id.values()))
    for c in MATCH_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[MATCH_COLS].sort_values(["date", "competition"]).reset_index(drop=True)
    played = df[df["completed"].astype(bool)]
    fixtures = df[(df["state"] == "pre") & (pd.to_datetime(df["date"], utc=True) >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1))]
    played.to_csv(data / "matches.csv", index=False)
    fixtures[["event_id", "competition", "season", "date", "home_id", "home", "away_id", "away", "neutral", "venue", "referee"]] \
        .to_csv(data / "fixtures.csv", index=False)
    state["fetched_at"] = now_iso
    state["source"] = "direct"
    state_path.write_text(json.dumps(state, indent=1))
    return played, fixtures


def fetch_mirror(a, data: Path, now_iso: str):
    got = {}
    for name in ("matches.csv", "fixtures.csv", "state.json"):
        url = f"{a.mirror}/espn/{name}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                raw = r.read()
        except Exception as e:
            print(f"  mirror {name}: {type(e).__name__} {str(e)[:80]}", file=sys.stderr)
            continue
        (data / name).write_bytes(raw)
        got[name] = len(raw)
    if "matches.csv" not in got:
        return None, None
    state = json.loads((data / "state.json").read_text()) if (data / "state.json").exists() else {}
    state["source"] = "mirror"; state["mirror_fetched_at"] = now_iso
    (data / "state.json").write_text(json.dumps(state, indent=1))
    return load_csv(data / "matches.csv", MATCH_COLS), pd.read_csv(data / "fixtures.csv", dtype=str)


def write_coverage(played: pd.DataFrame, data: Path):
    if played is None or played.empty:
        return
    g = played.groupby("competition")
    cov = pd.DataFrame({
        "matches": g.size(),
        "first": pd.to_datetime(g["date"].min()).dt.date.astype(str),
        "last": pd.to_datetime(g["date"].max()).dt.date.astype(str),
        "sot": g["HST"].apply(lambda s: float(pd.to_numeric(s, errors="coerce").notna().mean())),
        "fouls": g["HF"].apply(lambda s: float(pd.to_numeric(s, errors="coerce").notna().mean())),
        "yellows": g["HY"].apply(lambda s: float(pd.to_numeric(s, errors="coerce").notna().mean())),
        "referee": g["referee"].apply(lambda s: float(s.fillna("").astype(str).str.strip().ne("").mean())),
    }).reset_index()
    cov.to_csv(data / "coverage.csv", index=False)
    print("\ncoverage (share of played matches with the stat recorded):", file=sys.stderr)
    with pd.option_context("display.max_rows", 300, "display.width", 160):
        print(cov.round(2).to_string(index=False), file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--groups", nargs="*", default=["core"], choices=sorted(GROUPS))
    ap.add_argument("--competitions", nargs="*", help="ESPN slugs; overrides --groups")
    ap.add_argument("--since", help="YYYY-MM-DD; default = July 1 of the previous season (full re-pull of scoreboards)")
    ap.add_argument("--ahead", type=int, default=8, help="days ahead to pull fixtures")
    ap.add_argument("--no-summaries", action="store_true", help="skip per-match summaries (no cards/referee)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--delay", type=float, default=0.15, help="seconds between requests per worker")
    ap.add_argument("--mirror", default=DEFAULT_MIRROR)
    ap.add_argument("--source", choices=["auto", "direct", "mirror", "local"], default="auto")
    ap.add_argument("--list-competitions", action="store_true")
    a = ap.parse_args()

    if a.list_competitions:
        for k, v in GROUPS.items():
            print(f"{k}: {' '.join(v)}")
        return 0
    data = Path(a.data) / "espn"
    data.mkdir(parents=True, exist_ok=True)
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes")
    played = fixtures = None
    if a.source in ("auto", "direct"):
        try:
            played, fixtures = fetch_direct(a, data, now_iso)
        except Blocked as e:
            print(f"ESPN not reachable from here ({str(e)[:60]}).", file=sys.stderr)
            if a.source == "direct":
                return 1
    if played is None and a.source in ("auto", "mirror"):
        print(f"trying the GitHub mirror {a.mirror} ...", file=sys.stderr)
        played, fixtures = fetch_mirror(a, data, now_iso)
    if played is None:
        if (data / "matches.csv").exists():
            print("using the local copy already in the data directory (may be stale)", file=sys.stderr)
            played = load_csv(data / "matches.csv", MATCH_COLS)
            fixtures = pd.read_csv(data / "fixtures.csv", dtype=str) if (data / "fixtures.csv").exists() else pd.DataFrame()
        else:
            print("no data: ESPN blocked, mirror unavailable, nothing local. Set up assets/mirror-workflow.yml or run this on a machine with open internet.", file=sys.stderr)
            return 1
    write_coverage(played, data)
    last = pd.to_datetime(played["date"], utc=True).max() if len(played) else None
    print(f"\n{len(played)} played matches, {len(fixtures) if fixtures is not None else 0} fixtures; last played {last.date() if last is not None else '-'};"
          f" summaries missing for {int((~played['has_summary'].astype(bool)).sum()) if 'has_summary' in played else '?'} matches", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

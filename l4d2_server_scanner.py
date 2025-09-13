import argparse
import csv
import html
import json
import re
import socket
import sys
import subprocess
import time
from typing import Dict, List, Optional, Tuple

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://api.steampowered.com/IGameServersService/GetServerList/v1/"
APPID = 550

# Region number references:
#1=???
#5=SEA?
#6=dubai
#7=aus
#8=china
#14=chile
#15=peru
#16=india west
#19=jp
#24=hk
#26=india east

# ---------- Subjective campaign preference (lower is better) ----------
CAMPAIGN_PREFERENCE = {
    1:  4.0, #Dead Center (c1m1_hotel, c1m2_streets, c1m3_mall, c1m4_atrium)
    2:  5.0, #Dark Carnival (c2m1_highway, c2m2_fairgrounds, c2m3_coaster, c2m4_barns, c2m5_concert)
    3:  5.0, #Swamp Fever (c3m1_plankcountry, c3m2_swamp, c3m3_shantytown, c3m4_plantation)
    4:  6.0, #Hard Rain (c4m1_milltown_a, c4m2_sugarmill_a, c4m3_sugarmill_b, c4m4_milltown_b, c4m5_milltown_escape)
    5:  4.0, #The Parish (c5m1_waterfront, c5m2_park, c5m3_cemetery, c5m4_quarter, c5m5_bridge)
    6:  6.0, #The Passing (c6m1_riverbank, c6m2_bedlam, c6m3_port)
    7:  5.0, #The Sacrifice (c7m1_docks, c7m2_barge, c7m3_port)
    8:  3.0, #No Mercy (c8m1_apartment, c8m2_subway, c8m3_sewers, c8m4_interior, c8m5_rooftop)
    9:  5.0, #Crash Course (c9m1_alleys, c9m2_lots)
    10: 2.0, #Death Toll (c10m1_caves, c10m2_drainage, c10m3_ranchhouse, c10m4_mainstreet, c10m5_houseboat)
    11: 2.0, #Dead Air (c11m1_greenhouse, c11m2_offices, c11m3_garage, c11m4_terminal, c11m5_runway)
    12: 4.0, #Blood Harvest (c12m1_hilltop, c12m2_traintunnel, c12m3_bridge, c12m4_barn, c12m5_cornfield)
    13: 5.0, #Cold Stream (c13m1_alpinecreek, c13m2_southpinestream, c13m3_memorialbridge, c13m4_cutthroatcreek)
    14: 5.0, #The Last Stand (c14m1_junkyard, c14m2_lighthouse)
}
CAMPAIGN_MAP_COUNT = {
    1:  4, #Dead Center (c1m1_hotel, c1m2_streets, c1m3_mall, c1m4_atrium)
    2:  5, #Dark Carnival (c2m1_highway, c2m2_fairgrounds, c2m3_coaster, c2m4_barns, c2m5_concert)
    3:  4, #Swamp Fever (c3m1_plankcountry, c3m2_swamp, c3m3_shantytown, c3m4_plantation)
    4:  5, #Hard Rain (c4m1_milltown_a, c4m2_sugarmill_a, c4m3_sugarmill_b, c4m4_milltown_b, c4m5_milltown_escape)
    5:  5, #The Parish (c5m1_waterfront, c5m2_park, c5m3_cemetery, c5m4_quarter, c5m5_bridge)
    6:  3, #The Passing (c6m1_riverbank, c6m2_bedlam, c6m3_port)
    7:  3, #The Sacrifice (c7m1_docks, c7m2_barge, c7m3_port)
    8:  5, #No Mercy (c8m1_apartment, c8m2_subway, c8m3_sewers, c8m4_interior, c8m5_rooftop)
    9:  2, #Crash Course (c9m1_alleys, c9m2_lots)
    10: 5, #Death Toll (c10m1_caves, c10m2_drainage, c10m3_ranchhouse, c10m4_mainstreet, c10m5_houseboat)
    11: 5, #Dead Air (c11m1_greenhouse, c11m2_offices, c11m3_garage, c11m4_terminal, c11m5_runway)
    12: 5, #Blood Harvest (c12m1_hilltop, c12m2_traintunnel, c12m3_bridge, c12m4_barn, c12m5_cornfield)
    13: 4, #Cold Stream (c13m1_alpinecreek, c13m2_southpinestream, c13m3_memorialbridge, c13m4_cutthroatcreek)
    14: 2, #The Last Stand (c14m1_junkyard, c14m2_lighthouse)
}

# ---------- Weights (tweak to taste) ----------
WEIGHT_PLAYERS             = 1.50   # more players among {5,6,7} is better
WEIGHT_PING                = 2.50   # lower ping is better
WEIGHT_MAPS_REMAIN         = 0.90   # earlier map is better (m1 best)
WEIGHT_CAMPAIGN_PREF       = 2.00   # see campaign preference table
WEIGHT_CAMPAIGN_COMPLETION = 2.00   # campaign completion % (to increase favour for fresh, low-map count campaigns)

# ---------- Map parsing ----------
MAP_RE = re.compile(r"^c(?P<campaign>\d{1,2})m(?P<map>\d{1,2})_.*", re.IGNORECASE)

def parse_map(map_str: str) -> Tuple[int, int]:
    """Return (campaign, map). Unknown -> (0, 0)."""
    if not isinstance(map_str, str):
        return (0, 0)
    m = MAP_RE.match(map_str)
    if not m:
        return (0, 0)
    try:
        return (int(m.group("campaign")), int(m.group("map")))
    except Exception:
        return (0, 0)

# ---------- Filtering (ONLY filtering logic lives here) ----------
def filter_servers(raw: List[Dict]) -> List[Dict]:
    """
    Keep only entries that satisfy ALL:
      - name starts with 'Valve Left4Dead 2'
      - 'versus' is in gametype (case-insensitive substring check)
      - players in {5,6,7}
      - secure is True
    """
    out = []
    for s in raw:
        name = s.get("name", "") or ""
        gametype = (s.get("gametype", "") or "").lower()
        players = int(s.get("players", 0) or 0)
        secure = bool(s.get("secure", False))

        if not name.startswith("Valve Left4Dead 2"):
            continue
        if "versus" not in gametype:
            continue
        if players not in (5, 6, 7):
            continue
        if not secure:
            continue

        out.append(s)
    return out

# ---------- Scoring (ONLY scoring/weight logic lives here) ----------
def score_server(entry: Dict) -> float:
    """
    Combine three factors:
      1) Player count: among {5,6,7}, higher is better.
      2) Ping: lower is better, normalized.
      3) Level preference: earlier map is better, plus a campaign preference penalty.
    """
    # 1) players among 5,6,7 -> map to [0..1]: 5 -> 0, 6 -> 0.5, 7 -> 1
    players = int(entry.get("players", 0) or 0)
    players_norm = {5: 0.0, 6: 0.5, 7: 1.0}.get(players, 0.0)

    # 2) ping normalization
    ping = float(entry.get("ping_ms", 9999.0) or 9999.0)
    best_expected_ping = 40.0
    worst_expected_ping = 350.0
    ping_norm = 1.0 - (ping - best_expected_ping) / (worst_expected_ping - best_expected_ping)
    ping_norm = max(0.0, min(1.0, ping_norm))

    # 3) level: prefer earlier map; apply campaign preference penalty
    (campaign, map_idx) = parse_map(entry.get("map", ""))
    maps_in_campaign = CAMPAIGN_MAP_COUNT.get(campaign, 4.0) # default to 4, should never be hit
    maps_left = maps_in_campaign - map_idx
    max_maps_left = 4.0
    maps_left_norm = maps_left / max_maps_left

    # campaign completion: 1.0 on first map, 0.0 on last map
    campaign_completion_norm = 1.0 - ((map_idx - 1.0) / (maps_in_campaign - 1.0))

    pref = CAMPAIGN_PREFERENCE.get(campaign, 5.0)  # default neutral
    pref_clamped = max(1.0, min(10.0, float(pref)))
    campaign_norm = 1.0 - (pref_clamped - 1.0) / 9.0  # 1.0 best when pref==1, 0.0 worst when pref==10

    #print('--------------------------')
    #print(f'map: {entry.get("map", "")} name: {entry.get("name", "")}')
    total_score = (
        (WEIGHT_PLAYERS * players_norm) +
        (WEIGHT_PING * ping_norm) +
        (WEIGHT_MAPS_REMAIN * maps_left_norm) +
        (WEIGHT_CAMPAIGN_PREF * campaign_norm) +
        (WEIGHT_CAMPAIGN_COMPLETION * campaign_completion_norm)
    )
    #print(f'score {total_score}')
    #print(f'players: {WEIGHT_PLAYERS * players_norm}, ping: {WEIGHT_PING * ping_norm}, '
    #      f'map: {(WEIGHT_MAPS_REMAIN * maps_left_norm)}, campaign_pref: {(WEIGHT_CAMPAIGN_PREF * campaign_norm)}, '
    #      f'campaign_comp: {(WEIGHT_CAMPAIGN_COMPLETION * campaign_completion_norm)}')

    return total_score

# ---------- Fetch (multi-pass: direct + proxies) ----------
def _single_fetch(api_key: str, limit: int, request_timeout: float, proxy_url: Optional[str]) -> List[Dict]:
    """One pass fetch. If proxy_url is provided, use it for both http/https."""
    params = {
        "key": api_key,
        "limit": str(limit),
        "filter": r"\appid\550\empty\1\full\0\gametype\versus",
    }
    proxies = None
    if proxy_url:
        # Mimics: curl --proxy "http://user:pass@host:port/"
        proxies = {
            "http": proxy_url,
            "https": proxy_url,  # HTTPS URL over HTTP proxy via CONNECT
        }

    try:
        r = requests.get(API_URL, params=params, timeout=request_timeout, proxies=proxies)
        r.raise_for_status()  # like curl --fail-with-body
        data = r.json()
        return data.get("response", {}).get("servers", [])
    except Exception as e:
        label = f"proxy {proxy_url}" if proxy_url else "direct"
        print(f"Fetch error ({label}):", e, file=sys.stderr)
        return []

def fetch_servers_multi(api_key: str,
                        limit: int,
                        request_timeout: float,
                        proxies: List[str],
                        include_direct: bool = True) -> List[Dict]:
    """
    Run multiple fetch passes in parallel:
      - optional direct (no proxy)
      - each supplied proxy URL
    Then merge & dedupe by (ip, gameport).
    """
    tasks = []
    with ThreadPoolExecutor(max_workers=max(1, (1 if include_direct else 0) + len(proxies))) as ex:
        if include_direct:
            tasks.append(ex.submit(_single_fetch, api_key, limit, request_timeout, None))
        for p in proxies:
            tasks.append(ex.submit(_single_fetch, api_key, limit, request_timeout, p))

        results: List[Dict] = []
        for fut in as_completed(tasks):
            try:
                batch = fut.result() or []
                print(f'{len(batch)}')
                results.extend(batch)
            except Exception as e:
                print("Unexpected fetch error:", e, file=sys.stderr)

    # Deduplicate by (ip, gameport). Steam returns "addr" like "IP:PORT", but keep robust.
    deduped = []
    seen = set()
    for s in results:
        addr = s.get("addr", "")
        ip = None
        port = None
        if addr and ":" in addr:
            ip, port_str = addr.split(":")
            try:
                port = int(s.get("gameport", port_str))
            except Exception:
                # fallback: parse port_str if int failed in gameport
                try:
                    port = int(port_str)
                except Exception:
                    port = None
        else:
            ip = s.get("addr", "")
            try:
                port = int(s.get("gameport", 0))
            except Exception:
                port = None

        key = (ip, port)
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    return deduped

# ---------- ICMP ping by IP only (no ports) ----------
def icmp_ping_ip(ip: str, timeout: float = 1.0) -> float:
    """
    Returns RTT in milliseconds using a single system 'ping' to the IP only.
    No ports are used. Returns 9999.0 on failure.
    Works on Windows, Linux, and macOS (best-effort for timeouts).
    """
    try:
        if sys.platform.startswith("win"):
            cmd = ["ping", "-n", "1", "-w", str(int(timeout * 1000)), ip]
        elif sys.platform == "darwin":
            cmd = ["ping", "-c", "1", "-W", str(int(timeout * 1000)), ip]
        else:
            cmd = ["ping", "-c", "1", "-W", str(max(1, int(round(timeout)))), ip]

        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=max(1.0, timeout + 1.0)
        )
        if out.returncode != 0:
            return 9999.0

        text = out.stdout or ""
        m = re.search(r"time[=<]?\s*([0-9.]+)\s*ms", text, re.IGNORECASE)
        if not m:
            return 9999.0
        return round(float(m.group(1)), 1)
    except Exception:
        return 9999.0

def parallel_icmp(rows: List[Dict], timeout: float, sample: int, workers: int) -> None:
    """
    Ping up to `sample` rows in parallel and write results into row['ping_ms'].
    Rows beyond `sample` remain at their default ping (9999.0).
    """
    n = min(sample, len(rows))
    if n <= 0:
        return

    print(f"Pinging up to {n} servers via ICMP (IP only) with {workers} workers...")
    targets = rows[:n]

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        future_to_row = {
            ex.submit(icmp_ping_ip, r["ip"], timeout=timeout): r
            for r in targets
        }

        completed = 0
        for fut in as_completed(future_to_row):
            r = future_to_row[fut]
            try:
                r["ping_ms"] = float(fut.result())
            except Exception:
                r["ping_ms"] = 9999.0
            completed += 1
            if completed % 50 == 0 or completed == n:
                print(f"  {completed}/{n} pinged")

def generate_ranked_rows(
    api_key: str,
    limit: int = 10000,
    timeout: float = 1.0,
    sample: int = 1500,
    workers: int = 64,
    proxies: Optional[List[str]] = None,
    include_direct: bool = True,
    request_timeout: float = 30.0,
) -> List[Dict]:
    """
    Fetch (multi), merge/dedupe, filter, ping, score, sort, and return the rows.
    """
    proxies = proxies or []
    raw = fetch_servers_multi(
        api_key=api_key,
        limit=limit,
        request_timeout=request_timeout,
        proxies=proxies,
        include_direct=include_direct,
    )
    kept = filter_servers(raw)

    rows: List[Dict] = []
    for s in kept:
        addr = s.get("addr", "")
        if ":" in addr:
            ip, port_str = addr.split(":")
        else:
            ip, port_str = addr, str(s.get("gameport", ""))
        row = {
            "name": s.get("name", ""),
            "map": s.get("map", ""),
            "players": int(s.get("players", 0) or 0),
            "max_players": int(s.get("max_players", 0) or 0),
            "ip": ip,
            "port": int(s.get("gameport", port_str)) if str(s.get("gameport", "")).isdigit() or port_str.isdigit() else s.get("gameport", port_str),
            "secure": bool(s.get("secure", False)),
            "gametype": s.get("gametype", ""),
            "region": int(s.get("region", 0) or 0),
            "ping_ms": 9999.0,
        }
        rows.append(row)

    parallel_icmp(rows, timeout=timeout, sample=sample, workers=workers)

    for r in rows:
        r["score"] = round(score_server(r), 4)
        r["link"] = f"steam://connect/{r['ip']}:{r['port']}"

    rows.sort(key=lambda x: (-x["score"], x["ping_ms"], -x["players"], x["name"]))
    return rows

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True, help="Steam Web API key")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--timeout", type=float, default=1.0, help="ICMP ping timeout (seconds)")
    ap.add_argument("--sample", type=int, default=1500)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--out", type=str, default="valve_versus_ranked")

    # New arguments for multi-pass HTTP fetch
    ap.add_argument("--proxy", action="append", default=[],
                    help="Proxy URL like http://user:pass@ip:port/ (can be supplied multiple times)")
    ap.add_argument("--no-direct", action="store_true",
                    help="Skip the direct (no-proxy) fetch pass")
    ap.add_argument("--request-timeout", type=float, default=30.0,
                    help="HTTP request timeout for Steam API (seconds)")

    args = ap.parse_args()

    print("Fetching (direct/proxy), Merging, Filtering, Pinging, Scoring...")
    rows = generate_ranked_rows(
        api_key=args.key,
        limit=args.limit,
        timeout=args.timeout,
        sample=args.sample,
        workers=args.workers,
        proxies=args.proxy,
        include_direct=(not args.no_direct),
        request_timeout=args.request_timeout,
    )
    if not rows:
        print("No rows after filtering. Nothing to write.")
        return

    # Write CSV
    csv_path = f"{args.out}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "score","name","map","players","max_players","ping_ms","ip","port","secure","gametype","region","link"
        ])
        w.writeheader()
        w.writerows(rows)

    # Write HTML
    html_path = f"{args.out}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("<!doctype html><meta charset='utf-8'><title>L4D2 Valve Versus Ranked</title>")
        f.write("<h1>L4D2 Valve Versus Ranked</h1>")
        f.write("<p>Filters: Valve-official name prefix, gametype contains 'versus', players in {5,6,7}, secure=True.</p>")
        f.write("<p>Ping method: ICMP ping to server IP only (no ports).</p>")
        f.write("<table border='1' cellpadding='4' cellspacing='0'>")
        f.write("<tr><th>#</th><th>Score</th><th>Name</th><th>Map</th><th>Players</th><th>Ping</th><th>Connect</th></tr>")
        for i, r in enumerate(rows, 1):
            f.write("<tr>")
            f.write(f"<td>{i}</td>")
            f.write(f"<td>{r['score']}</td>")
            f.write(f"<td>{html.escape(r['name'])}</td>")
            f.write(f"<td>{html.escape(r['map'])}</td>")
            f.write(f"<td>{r['players']}/{r['max_players']}</td>")
            f.write(f"<td>{r['ping_ms']}</td>")
            f.write(f"<td><a href='{r['link']}'>connect</a></td>")
            f.write("</tr>")
        f.write("</table>")
        f.write("<p>Edit weights and campaign preferences at the top of the script.</p>")

    print(f"Wrote {csv_path} and {html_path}")

if __name__ == "__main__":
    main()

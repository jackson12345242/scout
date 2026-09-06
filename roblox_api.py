"""
roblox_api.py

Thin wrapper around Roblox's public HTTP endpoints, routed through
RoProxy (see note below).

Jobs:
1. discover_candidates() -> a list of universeIds worth checking,
   combining keyword search with Roblox's Discover-page sort
   categories (Popular, Trending, etc.) for wider coverage
2. get_stats(universe_ids) -> live CCU / visits / favorites
3. get_votes(universe_ids) -> upvotes/downvotes
4. get_icons(universe_ids) -> game icon image URLs

Notes on stability:
- `games.roblox.com/v1/games?universeIds=...` (get_stats) is the well
  documented, stable endpoint.
- The search and sorts/list endpoints are undocumented and can change
  shape without notice. Rather than hardcode one exact nested path for
  parsing them, we recursively extract any "universeId" values found
  anywhere in the response (_extract_universe_ids) so a schema change
  is less likely to silently break discovery.
- There is no official "give me every game filtered by CCU/visits"
  endpoint. This is always a sample of the platform, not a full crawl.

Endpoint history (why this file looks the way it does):
- `games.roblox.com/v1/games/sorts` and `.../v1/games/list` were
  Roblox's old Discover-page endpoints. Roblox has since deprecated
  both in favor of a *different host*: `apis.roblox.com/explore-api`.
  The old ones now just 404 -- they're gone, not rate-limited.
- Both the new explore-api endpoints AND the omni-search endpoint
  expect a `sessionId` query param (any GUID-ish string works; it's
  used for Roblox's own analytics). Omitting it doesn't error -- it
  just quietly comes back with zero/empty results, which is why the
  search calls below were returning 0 universeIds instead of failing
  loudly.
"""

import asyncio
import uuid
import aiohttp

# We route through RoProxy instead of hitting roblox.com directly.
# Roblox blocks a lot of datacenter/cloud IP ranges (Railway, Heroku,
# AWS, etc.) from its public endpoints -- that's the instant 429s you'll
# see if you point these at *.roblox.com directly from a cloud host.
# RoProxy is a widely-used community proxy that mirrors these same
# public, unauthenticated endpoints under a different domain to route
# around that block. Caveat: it's a third-party service we don't
# control -- if it goes down, these calls fail until it's back up.
# Since we never send a login cookie (we're only reading public game
# data), there's no credential-leak risk in routing through it.
SEARCH_URL = "https://apis.roproxy.com/search-api/omni-search"
STATS_URL = "https://games.roproxy.com/v1/games"
VOTES_URL = "https://games.roproxy.com/v1/games/votes"
ICONS_URL = "https://thumbnails.roproxy.com/v1/games/icons"

# NOTE: the old games.roblox.com/v1/games/sorts + /v1/games/list pair
# is deprecated (confirmed 404, per Roblox's own deprecation notice).
# The replacement lives under apis.roblox.com/explore-api, not
# games.roblox.com, so the proxy host changes too.
EXPLORE_SORTS_URL = "https://apis.roproxy.com/explore-api/v1/get-sorts"
EXPLORE_SORT_CONTENT_URL = "https://apis.roproxy.com/explore-api/v1/get-sort-content"

# Roblox's search/explore backends want a session id for analytics
# purposes. It doesn't need to be tied to a real login -- any stable
# GUID for the run works -- but omitting it entirely causes these
# endpoints to quietly return empty results rather than erroring.
SESSION_ID = str(uuid.uuid4())

# Seed terms used to pull a spread of candidate games each scan via
# keyword search.
SEED_TERMS = [
    "simulator", "tycoon", "obby", "roleplay", "horror",
    "anime", "fighting", "survival", "adventure", "clicker",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RobloxScoutBot/1.0)"
}


def _extract_universe_ids(obj):
    """
    Recursively walk any nested dict/list JSON structure and collect
    every integer value found under a "universeId" key, wherever it
    appears. This is deliberately schema-agnostic: Roblox's discovery
    and search endpoints are undocumented and reshape their response
    structure over time, so rather than hardcode one exact nested path
    (which breaks silently the moment the shape changes), we just
    harvest the IDs from wherever they show up.
    """
    ids = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "universeId" and isinstance(value, int):
                ids.add(value)
            else:
                ids |= _extract_universe_ids(value)
    elif isinstance(obj, list):
        for item in obj:
            ids |= _extract_universe_ids(item)
    return ids


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: dict, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=15) as resp:
                if resp.status == 429 and attempt < retries:
                    print(f"[roblox_api] {url} 429'd, backing off before retry {attempt + 1}")
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[roblox_api] {url} returned {resp.status}: {body[:300]}")
                    return None
                return await resp.json()
        except Exception as e:
            print(f"[roblox_api] request to {url} failed: {e!r}")
            return None
    return None


async def discover_via_search(session, terms=None, per_term_limit=20):
    """
    Search a set of seed terms and collect unique universeIds using the
    schema-agnostic extractor above.
    """
    terms = terms or SEED_TERMS
    universe_ids = set()

    for term in terms:
        data = await _fetch_json(
            session,
            SEARCH_URL,
            params={
                "searchQuery": term,
                "pageType": "all",
                "sessionId": SESSION_ID,
            },
        )
        if not data:
            print(f"[roblox_api] search for '{term}' returned no data")
            continue

        found = _extract_universe_ids(data)
        universe_ids |= found
        print(f"[roblox_api] search '{term}' -> {len(found)} universeIds")

        await asyncio.sleep(0.5)  # be polite, avoid rate limiting

    return universe_ids


async def discover_via_sorts(session, per_sort_limit=50):
    """
    Roblox's Discover page is built from named "sorts" (Popular,
    Trending, Top Rated, etc.), each returning a batch of games. This
    is a much wider net than keyword search -- typically dozens to a
    few hundred games per sort -- so we pull from every sort we're
    given rather than just one or two.

    This hits the current apis.roblox.com/explore-api endpoints --
    the old games.roblox.com/v1/games/sorts + /list pair Roblox used
    for this is deprecated and just 404s now.
    """
    universe_ids = set()

    sorts_data = await _fetch_json(
        session,
        EXPLORE_SORTS_URL,
        params={"sessionId": SESSION_ID, "device": "computer", "country": "all"},
    )
    if not sorts_data:
        print("[roblox_api] explore-api get-sorts returned no data")
        return universe_ids

    sorts = sorts_data.get("sorts", [])
    print(f"[roblox_api] found {len(sorts)} sort categories")

    for sort in sorts:
        sort_id = sort.get("sortId") or sort.get("token")
        name = sort.get("sortDisplayName") or sort.get("name") or "unknown"
        if not sort_id:
            continue

        content_data = await _fetch_json(
            session,
            EXPLORE_SORT_CONTENT_URL,
            params={
                "sessionId": SESSION_ID,
                "sortId": sort_id,
                "device": "computer",
                "country": "all",
                "maxRows": per_sort_limit,
            },
        )
        if not content_data:
            print(f"[roblox_api] sort '{name}' returned no data")
            continue

        found = _extract_universe_ids(content_data)
        print(f"[roblox_api] sort '{name}' -> {len(found)} universeIds")
        universe_ids |= found

        await asyncio.sleep(0.5)

    return universe_ids


async def discover_candidates(session, terms=None, per_term_limit=20):
    """
    Combines keyword search (genre/theme diversity) with sort-based
    discovery (much larger batches from Roblox's own Discover page) to
    cast as wide a net as we reasonably can. There is no public Roblox
    endpoint that returns literally every game on the platform -- this
    is the closest practical approximation.
    """
    search_ids = await discover_via_search(session, terms, per_term_limit)
    sort_ids = await discover_via_sorts(session)

    universe_ids = search_ids | sort_ids
    print(f"[roblox_api] discover_candidates total unique universeIds: "
          f"{len(universe_ids)} (search: {len(search_ids)}, sorts: {len(sort_ids)})")
    return universe_ids


async def get_stats(session: aiohttp.ClientSession, universe_ids):
    """
    Given an iterable of universeIds, return a list of dicts:
    { id, name, playing, visits, favoritedCount, created, updated }
    Roblox allows batching multiple ids in one call (comma separated),
    capped conservatively at 100 per request here.
    """
    universe_ids = list(universe_ids)
    results = []

    for i in range(0, len(universe_ids), 100):
        chunk = universe_ids[i:i + 100]
        data = await _fetch_json(
            session,
            STATS_URL,
            params={"universeIds": ",".join(str(u) for u in chunk)},
        )
        if not data:
            continue
        results.extend(data.get("data", []))
        await asyncio.sleep(0.3)

    return results


async def get_votes(session: aiohttp.ClientSession, universe_ids):
    """
    Returns { universeId: {"upVotes": int, "downVotes": int} }
    """
    universe_ids = list(universe_ids)
    votes = {}

    for i in range(0, len(universe_ids), 100):
        chunk = universe_ids[i:i + 100]
        data = await _fetch_json(
            session,
            VOTES_URL,
            params={"universeIds": ",".join(str(u) for u in chunk)},
        )
        if not data:
            continue
        for entry in data.get("data", []):
            votes[entry["id"]] = {
                "upVotes": entry.get("upVotes", 0),
                "downVotes": entry.get("downVotes", 0),
            }
        await asyncio.sleep(0.3)

    return votes


async def get_icons(session: aiohttp.ClientSession, universe_ids):
    """
    Returns { universeId: icon_image_url }
    """
    universe_ids = list(universe_ids)
    icons = {}

    for i in range(0, len(universe_ids), 100):
        chunk = universe_ids[i:i + 100]
        data = await _fetch_json(
            session,
            ICONS_URL,
            params={
                "universeIds": ",".join(str(u) for u in chunk),
                "size": "512x512",
                "format": "Png",
                "isCircular": "false",
            },
        )
        if not data:
            continue
        for entry in data.get("data", []):
            if entry.get("state") == "Completed":
                icons[entry["targetId"]] = entry.get("imageUrl")
        await asyncio.sleep(0.3)

    return icons


def apply_filters(games, min_ccu, max_visits):
    """
    games: list of dicts as returned by get_stats()
    Returns only games matching your scouting filters.
    """
    matches = []
    for g in games:
        playing = g.get("playing", 0)
        visits = g.get("visits", 0)
        if playing >= min_ccu and visits <= max_visits:
            matches.append(g)
    return matches


def game_url(game_stats_entry):
    """
    Build a clickable link from a get_stats() entry.
    Roblox links use the *place* id, not the universe id -- the stats
    endpoint returns this as "rootPlaceId".
    """
    place_id = game_stats_entry.get("rootPlaceId")
    if place_id:
        return f"https://www.roblox.com/games/{place_id}"
    return "https://www.roblox.com/discover"

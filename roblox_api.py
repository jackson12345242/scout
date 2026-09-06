"""
roblox_api.py

Thin wrapper around Roblox's public HTTP endpoints.

Two jobs:
1. discover_candidates() -> a list of universeIds worth checking
2. get_stats(universe_ids) -> live CCU / visits / favorites for those ids

Notes on stability:
- `games.roblox.com/v1/games?universeIds=...` (get_stats) is the well
  documented, stable endpoint. It's safe to rely on.
- Roblox does not offer an official "give me every game filtered by
  CCU/visits" endpoint. Discovery is done by searching a rotating list
  of keywords/genres via the public search API and collecting the
  universeIds that come back. This is inherently a *sample*, not a full
  crawl of Roblox -- it will miss games that don't match any seed term.
  If Roblox changes this endpoint's shape, discover_candidates() is the
  only function you should need to fix.
"""

import asyncio
import aiohttp

SEARCH_URL = "https://apis.roblox.com/search-api/omni-search"
STATS_URL = "https://games.roblox.com/v1/games"
VOTES_URL = "https://games.roblox.com/v1/games/votes"
ICONS_URL = "https://thumbnails.roblox.com/v1/games/icons"

# Seed terms used to pull a spread of candidate games each scan.
# Add/remove terms to change what kind of games you surface.
SEED_TERMS = [
    "simulator", "tycoon", "obby", "roleplay", "horror",
    "anime", "fighting", "survival", "adventure", "clicker",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RobloxScoutBot/1.0)"
}


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: dict):
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=15) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"[roblox_api] {url} returned {resp.status}: {body[:300]}")
                return None
            return await resp.json()
    except Exception as e:
        print(f"[roblox_api] request to {url} failed: {e!r}")
        return None


async def discover_candidates(session: aiohttp.ClientSession, terms=None, per_term_limit=20):
    """
    Search a set of seed terms and collect unique universeIds.
    Returns a set of universeIds (ints).
    """
    terms = terms or SEED_TERMS
    universe_ids = set()

    for term in terms:
        data = await _fetch_json(
            session,
            SEARCH_URL,
            params={"searchQuery": term, "pageType": "games"},
        )
        if not data:
            print(f"[roblox_api] search for '{term}' returned no data")
            continue

        found_this_term = 0
        # The search API nests results under searchResults -> contents.
        # We defensively walk the structure since Roblox can reshape this.
        for block in data.get("searchResults", []):
            for item in block.get("contents", [])[:per_term_limit]:
                uid = item.get("universeId")
                if uid:
                    universe_ids.add(uid)
                    found_this_term += 1

        print(f"[roblox_api] search '{term}' -> {found_this_term} universeIds "
              f"(raw keys: {list(data.keys())})")

        await asyncio.sleep(0.5)  # be polite, avoid rate limiting

    print(f"[roblox_api] discover_candidates total unique universeIds: {len(universe_ids)}")
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

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
5. get_social_links(universe_ids) -> attached social links (Discord, etc.)

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
  just quietly comes back with zero/empty results.

On persistent 429s from RoProxy even after batch-size shrinking:
- RoProxy is a free, shared, community-run proxy with its own global
  rate limits that have nothing to do with how politely any single
  bot behaves. A scan that fires off ~16 discovery calls in quick
  succession (10 search terms + 6 sort categories) followed
  immediately by a burst of stats/votes/icons/social-link batch calls
  is a lot of concentrated traffic, and RoProxy will 429 the whole
  burst regardless of retry logic once its own limit is hit.
- The only thing actually in our control is reducing how much and how
  fast we hit it: smaller starting batch size, a real pause between
  every request (not just on retry), and a short cooldown between the
  discovery phase and the stats phase so we're not stacking two bursts
  back to back. None of this *guarantees* RoProxy won't 429 us -- it's
  a third-party service outside our control -- but it meaningfully
  lowers how often we trip its limit.
"""

import asyncio
import random
import uuid
import aiohttp

# We route through RoProxy instead of hitting roblox.com directly.
# Roblox blocks a lot of datacenter/cloud IP ranges (Railway, Heroku,
# AWS, etc.) from its public endpoints -- that's the instant 429s you'll
# see if you point these at *.roblox.com directly from a cloud host.
# RoProxy is a widely-used community proxy that mirrors these same
# public, unauthenticated endpoints under a different domain to route
# around that block. Caveat: it's a third-party service we don't
# control -- if it goes down or rate-limits us, these calls fail until
# it eases up. Since we never send a login cookie (we're only reading
# public game data), there's no credential-leak risk in routing
# through it.
SEARCH_URL = "https://apis.roproxy.com/search-api/omni-search"
STATS_URL = "https://games.roproxy.com/v1/games"
VOTES_URL = "https://games.roproxy.com/v1/games/votes"
ICONS_URL = "https://thumbnails.roproxy.com/v1/games/icons"
SOCIAL_LINKS_URL = "https://games.roproxy.com/v1/games/{universe_id}/social-links/list"

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

# --- Throttling knobs ---
# RoProxy is shared infrastructure with its own rate limits we don't
# control. These pauses are the main lever we actually have to reduce
# how often we trip them. Raise these further if 429s persist even
# after this change.
DELAY_BETWEEN_SEARCH_TERMS = 1.2
DELAY_BETWEEN_SORTS = 1.2
DELAY_BETWEEN_BATCH_CHUNKS = 2.5   # was 1.5 -- stats calls were still 429ing on the very first attempt
COOLDOWN_AFTER_DISCOVERY = 8.0     # was 3.0 -- give RoProxy's bucket more time to recover before the stats burst
DEFAULT_BATCH_SIZE = 40            # confirmed OK -- no more "too many ids" errors at this size

# How many candidates discovery pulls in per cycle. Every candidate
# costs a stats lookup, and stats lookups are what's hitting RoProxy's
# rate limit -- cutting this in roughly half (from ~600 to ~300)
# noticeably improves the odds a scan actually finishes getting stats
# back instead of losing most chunks to 429s. You'll cover less of the
# platform per scan, but every 5 minutes adds up.
DEFAULT_PER_SORT_LIMIT = 25   # was 50
SEARCH_TERM_LIMIT = 6          # was implicitly all 10 terms every cycle


def _extract_universe_ids(obj):
    """
    Recursively walk any nested dict/list JSON structure and collect
    every "universeId" value found, wherever it appears -- as an int
    OR a numeric string (APIs often return large IDs as strings to
    avoid precision loss).
    """
    ids = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "universeId":
                if isinstance(value, int):
                    ids.add(value)
                elif isinstance(value, str) and value.isdigit():
                    ids.add(int(value))
                else:
                    ids |= _extract_universe_ids(value)
            else:
                ids |= _extract_universe_ids(value)
    elif isinstance(obj, list):
        for item in obj:
            ids |= _extract_universe_ids(item)
    return ids


class TooManyIdsError(Exception):
    """
    Raised when Roblox rejects a batched request for having too many
    universeIds in one call (their error code 9). The real cap isn't
    documented and appears to have gotten stricter than the 100/call
    this file used to assume -- rather than hardcode a new guessed
    number that can just as easily drift again, callers that batch IDs
    catch this and retry with a smaller chunk (see _fetch_batched).
    """
    pass


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: dict, retries: int = 3):
    """
    retries defaults to 3 with longer, jittered backoff. Note this
    backoff only kicks in *after* we've already been 429'd once -- the
    real fix for a proxy that's rate-limiting us globally is not
    retrying harder, it's requesting less often in the first place
    (see the DELAY_BETWEEN_* constants used by callers of this
    function).
    """
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=20) as resp:
                if resp.status == 429 and attempt < retries:
                    backoff = 4 * (attempt + 1) + random.uniform(0, 2.0)
                    print(f"[roblox_api] {url} 429'd, backing off {backoff:.1f}s before retry {attempt + 1}")
                    await asyncio.sleep(backoff)
                    continue
                if resp.status == 400:
                    body = await resp.text()
                    if '"code":9' in body or "Too many" in body:
                        raise TooManyIdsError(body[:200])
                    print(f"[roblox_api] {url} returned 400: {body[:300]}")
                    return None
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[roblox_api] {url} returned {resp.status}: {body[:300]}")
                    return None
                return await resp.json()
        except TooManyIdsError:
            raise
        except Exception as e:
            if attempt < retries:
                backoff = 2 * (attempt + 1)
                print(f"[roblox_api] request to {url} failed ({e!r}), retrying in {backoff}s")
                await asyncio.sleep(backoff)
                continue
            print(f"[roblox_api] request to {url} failed: {e!r}")
            return None
    return None


async def _fetch_batched(session, url, ids, static_params, id_param="universeIds", max_batch=DEFAULT_BATCH_SIZE):
    """
    Fetches `ids` against `url` in batches, merging each batch's
    "data" array into one list. If Roblox rejects a batch as having
    too many IDs, the batch size is halved and that same slice is
    retried. A fixed pause happens between every chunk request
    (success or failure) -- not just on retry -- since the point is to
    avoid tripping RoProxy's rate limit in the first place, not just
    to recover gracefully once we have.

    Returns (entries, failed_chunk_count).
    """
    ids = list(ids)
    results = []
    failed = 0
    i = 0
    batch_size = max_batch

    while i < len(ids):
        chunk = ids[i:i + batch_size]
        params = dict(static_params)
        params[id_param] = ",".join(str(u) for u in chunk)

        try:
            data = await _fetch_json(session, url, params)
        except TooManyIdsError:
            if batch_size == 1:
                print(f"[roblox_api] {url} rejected even a single id as 'too many'; skipping id {chunk}")
                failed += 1
                i += 1
                continue
            batch_size = max(1, batch_size // 2)
            print(f"[roblox_api] {url} batch of {len(chunk)} rejected as too many, retrying with batch size {batch_size}")
            continue  # retry same starting index i with the smaller batch_size

        if data is None:
            failed += 1
            i += batch_size
            await asyncio.sleep(DELAY_BETWEEN_BATCH_CHUNKS)
            continue

        results.extend(data.get("data", []))
        i += batch_size
        await asyncio.sleep(DELAY_BETWEEN_BATCH_CHUNKS)

    return results, failed


_search_term_cursor = 0


async def discover_via_search(session, terms=None, per_term_limit=20):
    """
    Search a rotating slice of SEARCH_TERM_LIMIT seed terms and collect
    unique universeIds using the schema-agnostic extractor above.
    Rotates through the full SEED_TERMS list across successive calls
    (via _search_term_cursor) rather than always hitting the same
    subset, so every term still gets used over a few cycles -- just
    fewer terms per single cycle, to cut request volume.
    """
    global _search_term_cursor
    pool = terms or SEED_TERMS
    if len(pool) > SEARCH_TERM_LIMIT:
        start = _search_term_cursor % len(pool)
        selected = [pool[(start + i) % len(pool)] for i in range(SEARCH_TERM_LIMIT)]
        _search_term_cursor = (start + SEARCH_TERM_LIMIT) % len(pool)
    else:
        selected = pool
    terms = selected
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

        await asyncio.sleep(DELAY_BETWEEN_SEARCH_TERMS)

    return universe_ids


async def discover_via_sorts(session, per_sort_limit=DEFAULT_PER_SORT_LIMIT):
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

        await asyncio.sleep(DELAY_BETWEEN_SORTS)

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

    # Give RoProxy a breather before we immediately start hammering it
    # again with stats/votes/icons/social-link batch calls -- discovery
    # alone is already ~16 requests in quick succession.
    print(f"[roblox_api] cooling down {COOLDOWN_AFTER_DISCOVERY}s before stats lookups...")
    await asyncio.sleep(COOLDOWN_AFTER_DISCOVERY)

    return universe_ids


async def get_stats(session: aiohttp.ClientSession, universe_ids):
    """
    Given an iterable of universeIds, return a list of dicts:
    { id, name, playing, visits, favoritedCount, created, updated,
      rootPlaceId, ... } -- rootPlaceId is what game_url() below needs
    to build a real, clickable Roblox game page link (Roblox's game
    pages are keyed by placeId, not universeId).

    Returns a (results, failed_chunks) tuple -- failed_chunks is how
    many of the batch requests came back empty after retries (network
    errors, exhausted 429 retries -- NOT the "too many ids" case, which
    is handled transparently by shrinking the batch instead), so
    callers can tell "0 stats because nothing matched" apart from
    "0 stats because RoProxy dropped every request." Those look
    identical if you only look at len(results).
    """
    return await _fetch_batched(session, STATS_URL, universe_ids, {})


async def get_votes(session: aiohttp.ClientSession, universe_ids):
    """
    Returns { universeId: {"upVotes": int, "downVotes": int} }
    """
    entries, _failed = await _fetch_batched(session, VOTES_URL, universe_ids, {})
    votes = {}
    for entry in entries:
        votes[entry["id"]] = {
            "upVotes": entry.get("upVotes", 0),
            "downVotes": entry.get("downVotes", 0),
        }
    return votes


async def get_icons(session: aiohttp.ClientSession, universe_ids):
    """
    Returns { universeId: icon_image_url }
    """
    entries, _failed = await _fetch_batched(
        session,
        ICONS_URL,
        universe_ids,
        {"size": "512x512", "format": "Png", "isCircular": "false"},
    )
    icons = {}
    for entry in entries:
        if entry.get("state") == "Completed":
            icons[entry["targetId"]] = entry.get("imageUrl")
    return icons


async def get_social_links(session: aiohttp.ClientSession, universe_ids, concurrency=3):
    """
    Returns { universeId: {"discord": url_or_None} }

    Unlike stats/votes/icons, this endpoint is per-universeId (no
    batching), so we cap how many run concurrently to avoid hammering
    RoProxy -- this is only called for games that already passed the
    scouting filters, so the id list here is small (top matches, not
    every candidate). Concurrency dropped from 5 to 3 to go a bit
    easier on RoProxy given the broader 429 issues.
    """
    universe_ids = list(universe_ids)
    results = {}
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_one(uid):
        async with semaphore:
            data = await _fetch_json(
                session,
                SOCIAL_LINKS_URL.format(universe_id=uid),
                params={},
            )
            discord_url = None
            if data:
                for link in data.get("data", []):
                    if str(link.get("type", "")).lower() == "discord":
                        discord_url = link.get("url")
                        break
            results[uid] = {"discord": discord_url}
            await asyncio.sleep(0.4)

    await asyncio.gather(*(fetch_one(uid) for uid in universe_ids))
    found = sum(1 for v in results.values() if v.get("discord"))
    print(f"[roblox_api] get_social_links: {found}/{len(universe_ids)} games have a linked Discord")
    return results


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


def closeness_score(game, min_ccu, max_visits):
    """
    0 means the game actually passes both filters. Anything above 0
    measures how far it missed by, as a fraction of the threshold --
    e.g. 0.2 on the CCU side means it had 20% fewer players than
    min_ccu required. Used only as a fallback ranking for games that
    didn't strictly pass, so we can still surface "closest" candidates
    when nothing clears the bar outright (see rank_by_closeness).
    """
    playing = game.get("playing", 0)
    visits = max(game.get("visits", 0), 0)

    ccu_gap = max(0.0, (min_ccu - playing) / min_ccu) if min_ccu > 0 else 0.0
    visits_gap = max(0.0, (visits - max_visits) / max_visits) if max_visits > 0 else 0.0

    return ccu_gap + visits_gap


def rank_by_closeness(games, min_ccu, max_visits):
    """
    Sorts games (that did NOT pass apply_filters) by how close they
    came to passing, best (smallest gap) first.
    """
    return sorted(games, key=lambda g: closeness_score(g, min_ccu, max_visits))


def game_url(game_stats_entry):
    """
    Link destination for the embed title -- the actual Roblox game
    page, e.g. https://www.roblox.com/games/1234567/Some-Game-Name

    Roblox game pages are keyed by *placeId*, not universeId, even
    though our stats entries are indexed by universeId. The
    games.roblox.com/v1/games (get_stats) response includes
    "rootPlaceId" for exactly this reason, so we use that. The name
    slug in the URL is cosmetic -- Roblox resolves the link correctly
    even with a generic or missing slug, so we don't need to worry
    about exact slugification matching Roblox's own.
    """
    place_id = game_stats_entry.get("rootPlaceId")
    if not place_id:
        # Fall back to a search link if we somehow don't have a
        # rootPlaceId for this entry, so the link still goes somewhere
        # useful instead of 404ing.
        name = game_stats_entry.get("name", "")
        return f"https://www.roblox.com/search/games?keyword={name}".replace(" ", "%20")

    name = game_stats_entry.get("name", "game")
    slug = "-".join(name.split())
    return f"https://www.roblox.com/games/{place_id}/{slug}"

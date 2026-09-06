"""
scoring.py

Builds a 0-100 "worth scouting" score out of data our bot can actually
pull from Roblox's public API. This is NOT the same algorithm as any
third-party tool (like the one in your screenshot) -- we don't have
access to their historical CCU tracking or their internal weighting.
This is our own transparent, tunable version built from:

  - Current CCU              (is it actually getting played right now)
  - Visit headroom           (low visits relative to players = likely
                               undiscovered, not just old and stagnant)
  - Like ratio               (upvotes vs downvotes = quality signal)
  - Favorites ratio          (favorited / visits = retention signal)
  - Growth since first seen  (our bot tracks this itself over repeated
                               scans -- a game we've seen before that's
                               climbing in CCU scores higher)

Weights sum to 100 and live in config.py so you can retune them without
touching this file.
"""

import config


def compute_score(game, vote_data, history_entry):
    """
    game: one entry from roblox_api.get_stats()
    vote_data: {"upVotes": int, "downVotes": int} or None
    history_entry: dict from our own seen_games tracking, or None if
                   this is the first time we've seen this game
                   ({"first_playing": int, "first_seen_iso": str})

    Returns (score: int, breakdown: dict of component -> points)
    """
    w = config.SCORE_WEIGHTS
    breakdown = {}

    playing = game.get("playing", 0)
    visits = max(game.get("visits", 1), 1)  # avoid div by zero
    favorites = game.get("favoritedCount", 0)

    # --- CCU score: scaled against a reference "great" CCU value ---
    ccu_score = min(playing / config.SCORE_CCU_REFERENCE, 1.0) * w["ccu"]
    breakdown["ccu"] = round(ccu_score)

    # --- Visit headroom: high CCU-to-visits ratio = undiscovered gem ---
    ccu_to_visits = playing / visits
    headroom_score = min(ccu_to_visits / config.SCORE_HEADROOM_REFERENCE, 1.0) * w["headroom"]
    breakdown["headroom"] = round(headroom_score)

    # --- Like ratio ---
    if vote_data:
        up = vote_data.get("upVotes", 0)
        down = vote_data.get("downVotes", 0)
        total_votes = up + down
        like_ratio = (up / total_votes) if total_votes > 0 else 0.5  # neutral if no votes yet
    else:
        like_ratio = 0.5
    like_score = like_ratio * w["likes"]
    breakdown["likes"] = round(like_score)

    # --- Favorites ratio ---
    fav_ratio = favorites / visits
    fav_score = min(fav_ratio / config.SCORE_FAVORITES_REFERENCE, 1.0) * w["favorites"]
    breakdown["favorites"] = round(fav_score)

    # --- Growth since we first spotted it ---
    if history_entry and history_entry.get("first_playing", 0) > 0:
        growth = (playing - history_entry["first_playing"]) / history_entry["first_playing"]
        growth_score = max(min(growth / config.SCORE_GROWTH_REFERENCE, 1.0), 0.0) * w["growth"]
    else:
        growth_score = w["growth"] * 0.5  # neutral score until we have a second data point
    breakdown["growth"] = round(growth_score)

    total = round(sum(breakdown.values()))
    total = max(0, min(total, 100))

    return total, breakdown

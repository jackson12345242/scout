"""
Roblox Acquisition Scout Bot

Commands:
  ?scan               -> runs a scan now using the current live filters
  ?scan 50 500000     -> one-off scan with custom min_ccu / max_visits
                          (doesn't change your saved filters)
  ?setfilters 150 150000 -> updates the saved filters used by ?scan
                             (no args) and by auto-scan
  ?filters            -> shows the current saved filters
  ?poll               -> shows when the next auto-scan will run
  ?instapoll          -> skips the timer and forces a scan right now
                          (cancels any scan in progress; restricted to
                          INSTAPOLL_ROLE_ID)

Also runs a background loop every AUTO_SCAN_INTERVAL_MINUTES that posts
the top AUTO_SCAN_POST_LIMIT new matches to ALERT_CHANNEL_ID, or
PRIORITY_CHANNEL_ID if the computed score is >= PRIORITY_SCORE_THRESHOLD.

Unhandled errors (bad commands, network failures, anything that slips
through) get posted as a traceback embed to ERROR_CHANNEL_ID so they
show up in Discord instead of only in process logs.
"""

import asyncio
import json
import os
import sys
import traceback
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import commands, tasks

import config
import roblox_api
import scoring

# Force line-buffered stdout. Without this, print() output can sit in
# a buffer for a long time when it's piped to a log viewer (Docker /
# Railway / Heroku logs aren't a real TTY), which makes a perfectly
# healthy process look "stuck" -- the diagnostic prints below are
# useless if they don't actually show up when they happen. This is
# equivalent to running `python -u`, but doesn't depend on the
# Procfile/start command being set up right.
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=config.COMMAND_PREFIX, intents=intents)


# ---------- persistence: live filters ----------

FILTERS_FILE = "filters.json"


def load_filters():
    if os.path.exists(FILTERS_FILE):
        with open(FILTERS_FILE, "r") as f:
            data = json.load(f)
            return data.get("min_ccu", config.MIN_CCU), data.get("max_visits", config.MAX_VISITS)
    return config.MIN_CCU, config.MAX_VISITS


def save_filters(min_ccu, max_visits):
    with open(FILTERS_FILE, "w") as f:
        json.dump({"min_ccu": min_ccu, "max_visits": max_visits}, f)


current_min_ccu, current_max_visits = load_filters()


# ---------- persistence: seen games + growth history ----------

def load_seen():
    if os.path.exists(config.SEEN_GAMES_FILE):
        with open(config.SEEN_GAMES_FILE, "r") as f:
            return json.load(f)
    return {}


def save_seen(seen):
    with open(config.SEEN_GAMES_FILE, "w") as f:
        json.dump(seen, f)


seen_games = load_seen()  # { "universeId(str)": {"first_playing": int, "first_seen_iso": str, "alerted": bool} }

# A full scan now does real search + sort coverage and can take a
# while, especially with 429 backoffs. Without this lock, a manual
# ?scan overlapping with the auto-scan loop (or the loop firing again
# before a slow previous run finished) causes two scans to run at once
# -- doubling request load against RoProxy and interleaving their
# console output, which is what the duplicated "found N sort
# categories" log lines were.
scan_lock = asyncio.Lock()


def update_history(game):
    """Record first-seen stats for a game, or return its existing history."""
    uid = str(game.get("id"))
    if uid not in seen_games:
        seen_games[uid] = {
            "first_playing": game.get("playing", 0),
            "first_seen_iso": datetime.now(timezone.utc).isoformat(),
            "alerted": False,
        }
    return seen_games[uid]


# ---------- error reporting ----------

async def report_error(source: str, error: Exception):
    """
    Posts a traceback embed to ERROR_CHANNEL_ID. Never raises itself --
    if the error channel is unreachable we fall back to printing, since
    the whole point is to not lose visibility into failures.
    """
    print(f"[error] {source}: {error!r}")

    channel = bot.get_channel(config.ERROR_CHANNEL_ID)
    if channel is None:
        print(f"[error] can't reach ERROR_CHANNEL_ID ({config.ERROR_CHANNEL_ID}) to post this error")
        return

    tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    if len(tb) > 1800:
        tb = "...\n" + tb[-1800:]

    embed = discord.Embed(
        title=f"\u26A0\uFE0F Error in {source}",
        description=f"```py\n{tb}\n```",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    try:
        await channel.send(embed=embed)
    except discord.HTTPException as send_err:
        print(f"[error] failed to post error embed for {source}: {send_err!r}")


# ---------- embed building ----------

def score_color(score):
    if score >= config.PRIORITY_SCORE_THRESHOLD:
        return discord.Color.gold()
    if score >= 60:
        return discord.Color.green()
    if score >= 40:
        return discord.Color.orange()
    return discord.Color.greyple()


def build_embed(game, score, breakdown, votes, icon_url, social=None):
    created = game.get("created", "")[:10]
    updated = game.get("updated", "")[:10]
    creator = game.get("creator", {}).get("name", "Unknown")
    genre = game.get("genre", "All")

    up = votes.get("upVotes", 0) if votes else 0
    down = votes.get("downVotes", 0) if votes else 0
    total_votes = up + down
    like_ratio = (up / total_votes * 100) if total_votes else 0

    if game.get("_is_closest_fill"):
        tag = "\U0001F50D Closest Match (didn't fully clear your filters)"
    else:
        tag = "\U0001F195 Early Discovery!" if _is_new(created) else "\U0001F4C8 Scouting Match"

    embed = discord.Embed(
        title=game.get("name", "Unknown game"),
        url=roblox_api.game_url(game),
        description=f"**{tag}**",
        color=score_color(score),
    )

    gap_note = game.get("_gap_note")
    if gap_note:
        embed.add_field(name="\U0001F4CF Filter Gap", value=gap_note, inline=False)
    if icon_url:
        embed.set_thumbnail(url=icon_url)

    embed.add_field(name="\U0001F9EE Score", value=f"**{score}/100**", inline=True)
    embed.add_field(name="\U0001F194 Universe ID", value=str(game.get("id")), inline=True)
    embed.add_field(name="\U0001F5D3 Created", value=created or "Unknown", inline=True)

    embed.add_field(
        name="\U0001F4CA Current Stats",
        value=f"Players Online: **{game.get('playing', 0):,}**\nTotal Visits: **{game.get('visits', 0):,}**",
        inline=False,
    )
    embed.add_field(
        name="\U0001F44D Ratings",
        value=f"Upvotes: **{up:,}**\nDownvotes: **{down:,}**\nLike Ratio: **{like_ratio:.1f}%**",
        inline=True,
    )
    embed.add_field(
        name="\u2B50 Engagement",
        value=f"Favorites: **{game.get('favoritedCount', 0):,}**",
        inline=True,
    )

    # Discord field removed: get_social_links() is a permanent no-op
    # (the underlying Roblox endpoint 401s without a real authenticated
    # account session -- see its docstring in roblox_api.py) so this
    # would only ever say "Not linked" regardless of the truth, which
    # is worse than not showing it at all.

    embed.add_field(
        name="\U0001F3F7\uFE0F Metadata",
        value=f"Genre: **{genre}**\nCreator: **{creator}**\nUpdated: **{updated or 'Unknown'}**",
        inline=False,
    )
    embed.add_field(
        name="\U0001F9EE Score Breakdown",
        value=(
            f"CCU: {breakdown['ccu']}/{config.SCORE_WEIGHTS['ccu']} \u2022 "
            f"Headroom: {breakdown['headroom']}/{config.SCORE_WEIGHTS['headroom']} \u2022 "
            f"Likes: {breakdown['likes']}/{config.SCORE_WEIGHTS['likes']} \u2022 "
            f"Favorites: {breakdown['favorites']}/{config.SCORE_WEIGHTS['favorites']} \u2022 "
            f"Growth: {breakdown['growth']}/{config.SCORE_WEIGHTS['growth']}"
        ),
        inline=False,
    )

    embed.set_footer(text="Roblox Acquisition Scout")
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def _is_new(created_date_str, days=30):
    if not created_date_str:
        return False
    try:
        created = datetime.strptime(created_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - created).days <= days


# ---------- buttons ----------

class ScoutView(discord.ui.View):
    """Claim / Priority buttons attached to each match message."""

    def __init__(self):
        super().__init__(timeout=None)  # buttons stay live for the runtime of the bot

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success)
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        button.label = f"Claimed by {interaction.user.display_name}"
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="Priority", style=discord.ButtonStyle.primary)
    async def mark_priority(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"\U0001F525 {interaction.user.mention} flagged this as priority.",
            ephemeral=False,
        )


# ---------- scan logic ----------

def _annotate_gap(game, min_ccu, max_visits):
    """
    Builds a short human-readable note on why a "closest match" fill-in
    didn't strictly pass the filters, e.g. "42 CCU (need 100+)" or
    "310,000 visits (need under 200,000)". Stored on the game dict so
    build_embed can show it without needing the raw thresholds passed
    around separately.
    """
    playing = game.get("playing", 0)
    visits = game.get("visits", 0)
    parts = []
    if playing < min_ccu:
        parts.append(f"{playing:,} CCU (need {min_ccu:,}+)")
    if visits > max_visits:
        parts.append(f"{visits:,} visits (need under {max_visits:,})")
    game["_gap_note"] = " \u2022 ".join(parts) if parts else None


async def run_scan(min_ccu, max_visits, status_callback=None, ensure_minimum=3):
    """
    Returns a list of (game, score, breakdown, votes, icon_url, social)
    tuples. Real filter-passing games are always ranked first (best
    score first); if fewer than `ensure_minimum` games strictly pass,
    the list is padded out with the closest non-passing candidates
    (ranked by roblox_api.rank_by_closeness) so there's still something
    to look at instead of an empty scan. Fill-ins are tagged with
    game["_is_closest_fill"] = True and a game["_gap_note"] explaining
    what they missed by, so the embed can be upfront about it instead
    of presenting them as if they'd actually passed.
    status_callback: optional async function(str) to report progress,
                      e.g. ctx.send, so diagnostics show up in Discord.
    """
    async with aiohttp.ClientSession() as session:
        candidate_ids = await roblox_api.discover_candidates(session)
        msg = f"Found {len(candidate_ids)} candidate universeIds from search."
        print(f"[scan] {msg}")
        if status_callback:
            await status_callback(f"Found **{len(candidate_ids)}** candidate universeIds from search.")

        stats, failed_chunks = await roblox_api.get_stats(session, candidate_ids)
        msg = f"Pulled stats for {len(stats)} games ({failed_chunks} failed chunk(s))."
        print(f"[scan] {msg}")
        if status_callback:
            note = ""
            if failed_chunks:
                note = f" ({failed_chunks} batch request(s) failed after retries -- likely rate-limited, not a real 0)"
            await status_callback(f"Pulled stats for **{len(stats)}** games{note}.")

        strict_matches = roblox_api.apply_filters(stats, min_ccu, max_visits)
        print(f"[scan] {len(strict_matches)} passed filters (min_ccu={min_ccu}, max_visits={max_visits}).")
        if status_callback:
            await status_callback(f"**{len(strict_matches)}** passed your CCU/visits filters.")

        selected = list(strict_matches)
        for g in selected:
            g["_is_closest_fill"] = False
            g["_gap_note"] = None

        if len(selected) < ensure_minimum:
            strict_ids = {g["id"] for g in strict_matches}
            remaining_pool = [g for g in stats if g["id"] not in strict_ids]
            needed = ensure_minimum - len(selected)
            filler = roblox_api.rank_by_closeness(remaining_pool, min_ccu, max_visits)[:needed]
            for g in filler:
                g["_is_closest_fill"] = True
                _annotate_gap(g, min_ccu, max_visits)
            print(f"[scan] backfilled {len(filler)} closest-match game(s) since strict matches came up short.")
            if status_callback and filler:
                await status_callback(
                    f"Only **{len(strict_matches)}** strictly passed, so adding **{len(filler)}** "
                    f"closest-match game(s) to round it out."
                )
            selected.extend(filler)

        if not selected:
            return []

        selected_ids = [g["id"] for g in selected]
        votes_by_id = await roblox_api.get_votes(session, selected_ids)
        icons_by_id = await roblox_api.get_icons(session, selected_ids)
        social_by_id = await roblox_api.get_social_links(session, selected_ids)

    results = []
    for game in selected:
        history_entry = update_history(game)  # also seeds history for brand-new games
        votes = votes_by_id.get(game["id"])
        score, breakdown = scoring.compute_score(game, votes, history_entry)
        icon_url = icons_by_id.get(game["id"])
        social = social_by_id.get(game["id"])
        results.append((game, score, breakdown, votes, icon_url, social))

    save_seen(seen_games)  # persist first-seen data collected this scan
    # Real matches always rank above closest-match fill-ins, regardless
    # of score; within each group, highest score first.
    results.sort(key=lambda r: (not r[0]["_is_closest_fill"], r[1]), reverse=True)
    return results


async def post_result(destination, game, score, breakdown, votes, icon_url, social, prefix=None):
    embed = build_embed(game, score, breakdown, votes, icon_url, social)
    await destination.send(content=prefix, embed=embed, view=ScoutView())


# ---------- events & commands ----------

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Usage: `?{ctx.command.name} <min_ccu> <max_visits>`")
        return
    if isinstance(error, commands.BadArgument):
        await ctx.send("Both values need to be whole numbers, e.g. `?setfilters 150 150000`")
        return
    if isinstance(error, commands.MissingRole):
        await ctx.send("You don't have the required role to use that command.")
        return
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send("That command only works in a server, not in DMs.")
        return

    original = getattr(error, "original", error)
    await ctx.send(f"\u26A0\uFE0F Something went wrong running that command -- I've logged the error.")
    await report_error(f"command `?{ctx.command}`", original)


def _check_channel(name, channel_id):
    """
    Startup sanity check. A misconfigured or unreachable channel ID is
    the single most common reason auto-scan looks like it 'does
    nothing' -- _run_auto_scan currently just prints and silently
    returns in that case, which is easy to miss. Surfacing it loudly
    at startup means you find out immediately instead of after
    watching several silent scan cycles go by.
    """
    if channel_id == 0:
        print(f"[startup][WARNING] {name} is not set (env var missing) -- auto-scan posts to this channel will be skipped.")
        return
    channel = bot.get_channel(channel_id)
    if channel is None:
        print(f"[startup][WARNING] {name}={channel_id} -- bot cannot see this channel "
              f"(wrong ID, or bot not added to that server/channel). Posts to it will silently fail.")
    else:
        print(f"[startup] {name}={channel_id} -> #{channel.name} OK")


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    _check_channel("ALERT_CHANNEL_ID", config.ALERT_CHANNEL_ID)
    _check_channel("PRIORITY_CHANNEL_ID", config.PRIORITY_CHANNEL_ID)
    _check_channel("ERROR_CHANNEL_ID", config.ERROR_CHANNEL_ID)
    print(f"[startup] Current filters: min_ccu={current_min_ccu}, max_visits={current_max_visits}")
    if config.AUTO_SCAN_ENABLED and not auto_scan_loop.is_running():
        auto_scan_loop.start()


@bot.command(name="scan")
async def scan(ctx, min_ccu: int = None, max_visits: int = None):
    min_ccu = min_ccu if min_ccu is not None else current_min_ccu
    max_visits = max_visits if max_visits is not None else current_max_visits

    if scan_lock.locked():
        await ctx.send(
            "A scan is already running (manual or auto) -- wait for it to finish before starting another. "
            "Use `?poll` to see when the next auto-scan is due."
        )
        return

    await ctx.send(f"Scanning for games with {min_ccu}+ CCU and under {max_visits:,} visits...")

    try:
        async with scan_lock:
            results = await run_scan(min_ccu, max_visits, status_callback=ctx.send, ensure_minimum=3)
    except Exception as e:
        await ctx.send("\u26A0\uFE0F Scan failed partway through -- I've logged the error.")
        await report_error("`?scan` command", e)
        return

    if not results:
        # Only possible now if discovery itself came back with zero
        # games at all (e.g. RoProxy down) -- ensure_minimum guarantees
        # a result otherwise, even if every one is a closest-match fill-in.
        await ctx.send("No games came back from Roblox at all this scan -- likely a RoProxy outage. Try again shortly.")
        return

    for game, score, breakdown, votes, icon_url, social in results[:10]:
        await post_result(ctx.channel, game, score, breakdown, votes, icon_url, social)


@bot.command(name="setfilters")
async def setfilters(ctx, min_ccu: int, max_visits: int):
    global current_min_ccu, current_max_visits

    if min_ccu < 0 or max_visits < 0:
        await ctx.send("Both values need to be positive numbers.")
        return

    current_min_ccu = min_ccu
    current_max_visits = max_visits
    save_filters(current_min_ccu, current_max_visits)

    await ctx.send(
        f"Filters updated: **{min_ccu}+ CCU** and **under {max_visits:,} visits**. "
        f"This applies to `?scan` (no args) and auto-scan going forward."
    )


@bot.command(name="filters")
async def show_filters(ctx):
    await ctx.send(
        f"Current filters: **{current_min_ccu}+ CCU** and **under {current_max_visits:,} visits**"
    )


@bot.command(name="poll")
async def poll(ctx):
    """Shows when the next auto-scan will run."""
    if not config.AUTO_SCAN_ENABLED:
        await ctx.send("Auto-scan is disabled in config right now.")
        return
    if not auto_scan_loop.is_running():
        await ctx.send("Auto-scan isn't running (it may have crashed -- check the error channel).")
        return

    next_run = auto_scan_loop.next_iteration
    if next_run is None:
        await ctx.send("Auto-scan is running but hasn't scheduled its next run yet -- try again in a moment.")
        return

    ts = int(next_run.timestamp())
    await ctx.send(
        f"Next auto-scan: <t:{ts}:R> (<t:{ts}:T>) -- posting top **{config.AUTO_SCAN_POST_LIMIT}** "
        f"matches every **{config.AUTO_SCAN_INTERVAL_MINUTES}** minutes."
    )


@bot.command(name="instapoll")
@commands.has_role(config.INSTAPOLL_ROLE_ID)
async def instapoll(ctx):
    """
    Forces an immediate scan right now, skipping whatever's left on
    the auto-scan timer. If a scan is currently running (scheduled or
    a previous instapoll), this cancels it and starts a fresh one
    immediately instead of waiting for it to finish -- the 5-minute
    schedule resets from this point forward. Restricted to
    INSTAPOLL_ROLE_ID.
    """
    await ctx.send("Skipping the timer -- running an instant scan now...")

    if auto_scan_loop.is_running():
        auto_scan_loop.restart()
    else:
        auto_scan_loop.start()


@tasks.loop(minutes=config.AUTO_SCAN_INTERVAL_MINUTES)
async def auto_scan_loop():
    # If a manual ?scan (or a slow previous auto-scan) is still running,
    # skip this tick rather than starting a second scan on top of it --
    # see scan_lock's comment above for why that matters.
    if scan_lock.locked():
        print("[auto_scan_loop] skipping this run -- a scan is already in progress")
        return

    # Wrapped in try/except so one bad run (network blip, RoProxy
    # outage, etc.) doesn't silently kill the whole loop -- it just
    # reports the error and waits for the next scheduled interval.
    try:
        async with scan_lock:
            await _run_auto_scan()
    except Exception as e:
        await report_error("auto_scan_loop", e)


async def _run_auto_scan():
    print(f"[auto_scan_loop] starting run (filters: min_ccu={current_min_ccu}, max_visits={current_max_visits})")

    alert_channel = bot.get_channel(config.ALERT_CHANNEL_ID)
    priority_channel = bot.get_channel(config.PRIORITY_CHANNEL_ID)

    if alert_channel is None:
        print(f"[auto_scan_loop][WARNING] ALERT_CHANNEL_ID={config.ALERT_CHANNEL_ID} not set or bot "
              f"can't see that channel; skipping auto-scan post entirely this cycle.")
        return

    results = await run_scan(current_min_ccu, current_max_visits, ensure_minimum=config.AUTO_SCAN_POST_LIMIT)
    strict_count = sum(1 for r in results if not r[0]["_is_closest_fill"])
    print(f"[auto_scan_loop] {len(results)} total ({strict_count} strict, "
          f"{len(results) - strict_count} closest-match fill-ins) this cycle.")

    posted = 0
    already_alerted = 0
    for game, score, breakdown, votes, icon_url, social in results:
        if posted >= config.AUTO_SCAN_POST_LIMIT:
            break

        uid = str(game.get("id"))
        if seen_games.get(uid, {}).get("alerted"):
            already_alerted += 1
            continue  # already alerted on this one before

        if score >= config.PRIORITY_SCORE_THRESHOLD and priority_channel is not None:
            await post_result(priority_channel, game, score, breakdown, votes, icon_url, social,
                               prefix="**\U0001F525 High-priority match**")
        else:
            await post_result(alert_channel, game, score, breakdown, votes, icon_url, social,
                               prefix="**New scouting match**")

        seen_games[uid]["alerted"] = True
        posted += 1

    print(f"[auto_scan_loop] posted {posted}, skipped {already_alerted} already-alerted "
          f"(out of {len(results)} matches).")

    save_seen(seen_games)


if __name__ == "__main__":
    bot.run(config.BOT_TOKEN)

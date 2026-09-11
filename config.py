import os

# --- Discord ---
BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "PUT_YOUR_TOKEN_HERE")
COMMAND_PREFIX = "?"

# Channel ID where normal-score auto-alerts get posted.
ALERT_CHANNEL_ID = int(os.environ.get("ALERT_CHANNEL_ID", "0"))

# Channel ID where high-score (see PRIORITY_SCORE_THRESHOLD) matches get
# posted instead of the regular channel.
PRIORITY_CHANNEL_ID = int(os.environ.get("PRIORITY_CHANNEL_ID", "0"))

# Channel ID where unhandled errors (command errors, auto-scan crashes)
# get posted as a traceback embed, so problems surface in Discord
# instead of only showing up in the process logs.
ERROR_CHANNEL_ID = int(os.environ.get("ERROR_CHANNEL_ID", "1546210133665783858"))

# --- Scouting filters ---
MIN_CCU = 100          # minimum concurrent players
MAX_VISITS = 200_000    # maximum total visits (keeps it "undiscovered")

# --- Scoring ---
# Score >= this routes to PRIORITY_CHANNEL_ID instead of ALERT_CHANNEL_ID.
PRIORITY_SCORE_THRESHOLD = 80

# Weights must sum to 100. Retune these to change what the score rewards.
SCORE_WEIGHTS = {
    "ccu": 30,        # raw concurrent players
    "headroom": 20,   # CCU relative to visits (undiscovered signal)
    "likes": 20,      # upvote ratio
    "favorites": 15,  # favorites relative to visits
    "growth": 15,     # CCU growth since we first spotted this game
}

# Reference values used to normalize each component to 0-1 before
# weighting. Raise these to make the score harder to max out.
SCORE_CCU_REFERENCE = 500          # CCU at/above this scores full marks on the CCU component
SCORE_HEADROOM_REFERENCE = 0.01    # CCU/visits ratio that scores full marks
SCORE_FAVORITES_REFERENCE = 0.05   # favorites/visits ratio that scores full marks
SCORE_GROWTH_REFERENCE = 1.0       # 100% CCU growth since first-seen scores full marks

# --- Auto-scan behavior ---
AUTO_SCAN_ENABLED = True
AUTO_SCAN_INTERVAL_MINUTES = 5   # was 30 -- now scans every 5 minutes
AUTO_SCAN_POST_LIMIT = 3         # was implicitly 10 -- now posts top 3 per run

# Only members with this role can use ?instapoll (force an immediate
# scan cycle instead of waiting for the next scheduled auto-scan).
INSTAPOLL_ROLE_ID = int(os.environ.get("INSTAPOLL_ROLE_ID", "1546134163030020177"))

# File used to remember games we've seen before (for dedup + growth
# tracking). NOTE: on Railway this resets on every redeploy unless you
# attach a persistent volume -- see README.
SEEN_GAMES_FILE = "seen_games.json"

from __future__ import annotations

import asyncio
import discord
import subprocess
import re
import json
import time

from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from dataclasses import dataclass, field
from config.Environment import ADMIN_LOG_CHANNEL, RESULT_CHANNEL, GIT_ORIGIN
from config.SqliteStore import (
    ensure_ranked_storage,
    fetch_active_ranked_matches,
    fetch_pending_ranked_matches,
    fetch_match_history,
    fetch_monthly_ranking,
    fetch_world_ranking,
    generate_monthly_ranking,
    get_current_ranked_month_key,
    get_next_ranked_match_id,
    mark_ranked_match_cancelled,
    mark_ranked_match_result_published,
    persist_pending_ranked_match,
    persist_active_ranked_match,
    persist_ranked_match_result,
    rebuild_current_month_rankings,
)


# TODO:
#  screenshot in result modal per einfügen (strg + v)
#  .
#  wieder nur einmal pro tag? oder ab 2. mal abfrage ob man das möchte, ansonsten beide in die q und der 3. bekommt zufällig einen von den beiden


# =============================
# Konstanten und Parser-Patterns für Queue, Threads und Ergebnisse.
# =============================
QUEUE_EMPTY_TEXT = "kein spieler"
MATCHES_FIELD_NAME = ":fire: Aktuelle Matches"
MENTION_PATTERN = re.compile(r"<@!?(\d+)>")
THREAD_SAFE_PATTERN = re.compile(r"[^a-z0-9-]")
MATCH_ID_PATTERN = re.compile(r"#(\d+)")
SCORE_PATTERN = re.compile(r"^\s*(\d{1,2})\s*[:\-]\s*(\d{1,2})\s*$")
AVERAGE_PATTERN = re.compile(r"^\s*\d+(?:[.,]\d+)?\s*$")
REPO_ROOT = Path(__file__).resolve().parents[1]
LEADERBOARD_FILE = "leaderboard.html"
PLAYER_DATA_FILE = "players.json"
QUEUE_STATE_FILE = REPO_ROOT / "queue_state.json"
RANKED_RESULT_STATUS_SQL = "status IN ('completed', 'confirmed') AND winner_id IS NOT NULL AND loser_id IS NOT NULL"
WITHDRAW_ENABLE_DELAY = timedelta(minutes=5)
QUEUE_WAIT_TIMEOUT = timedelta(minutes=5)
RESULT_SELF_CONFIRM_DELAY = timedelta(minutes=1)

# =============================
# WEBSITE - generate static html for displaying ranking on web
# =============================

def run_git_command(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )


def get_current_git_branch() -> str | None:
    branch_result = run_git_command("branch", "--show-current")
    if branch_result.returncode != 0:
        print(f"git branch failed: {branch_result.stderr.strip()}")
        return None

    branch_name = branch_result.stdout.strip()
    if not branch_name:
        print("git branch failed: no current branch")
        return None

    return branch_name


def upload() -> bool:
    add_result = run_git_command("add", LEADERBOARD_FILE, PLAYER_DATA_FILE)
    if add_result.returncode != 0:
        print(f"git add failed: {add_result.stderr.strip()}")
        return False

    diff_result = run_git_command("diff", "--cached", "--quiet", "--", LEADERBOARD_FILE, PLAYER_DATA_FILE)
    if diff_result.returncode == 0:
        print("leaderboard unchanged")
        return False
    if diff_result.returncode != 1:
        print(f"git diff failed: {diff_result.stderr.strip()}")
        return False

    commit_result = run_git_command("commit", "-m", "update leaderboard")
    if commit_result.returncode != 0:
        print(f"git commit failed: {commit_result.stderr.strip()}")
        return False

    branch_name = get_current_git_branch()
    if branch_name is None:
        return False

    push_result = run_git_command("push", "--set-upstream", GIT_ORIGIN, branch_name)
    if push_result.returncode != 0:
        print(f"git push failed: {push_result.stderr.strip()}")
        return False

    print("update pushed")
    return True


def parse_stored_average(value: str | None) -> float | None:
    if value is None:
        return None

    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None


async def build_player_profiles(
    bot: commands.Bot,
    player_data: list[tuple[int, int, int, int]],
    monthly_data: list[tuple[int, int, int, int]],
    get_player_display,
) -> dict[str, dict]:
    world_ranks = {user_id: rank for rank, (user_id, *_rest) in enumerate(player_data, start=1)}
    monthly_ranks = {user_id: rank for rank, (user_id, *_rest) in enumerate(monthly_data, start=1)}
    world_stats = {user_id: (rating, wins, losses) for user_id, rating, wins, losses in player_data}
    monthly_stats = {user_id: (rating, wins, losses) for user_id, rating, wins, losses in monthly_data}
    db = getattr(bot, "db", None)
    matches_by_player: dict[int, list[dict]] = {}
    averages_by_player: dict[int, list[float]] = {}
    recent_match_counts: dict[int, int] = {}
    all_player_ids = set(world_stats) | set(monthly_stats)

    if db is not None:
        async with db._lock:
            rows = db.connection.execute(
                f"""
                SELECT id, player1_id, player2_id, winner_id, loser_id,
                       platform, score, winner_avg, loser_avg, elo_change,
                       strftime('%Y-%m', timestamp) AS month_key
                FROM matches
                WHERE {RANKED_RESULT_STATUS_SQL}
                ORDER BY id DESC
                """
            ).fetchall()

        opponent_ids: set[int] = set()
        pending_entries: list[tuple[int, dict]] = []

        for row in rows:
            winner_id = int(row["winner_id"])
            loser_id = int(row["loser_id"])
            player_one_id = int(row["player1_id"]) if row["player1_id"] is not None else winner_id
            player_two_id = int(row["player2_id"]) if row["player2_id"] is not None else loser_id
            participants = {player_one_id, player_two_id, winner_id, loser_id}
            all_player_ids.update(participants)

            for user_id in (winner_id, loser_id):
                won = user_id == winner_id
                opponent_id = loser_id if won else winner_id
                opponent_ids.add(opponent_id)

                average_value = row["winner_avg"] if won else row["loser_avg"]
                player_average = "" if average_value is None else str(average_value)
                parsed_average = parse_stored_average(player_average)
                if parsed_average is not None:
                    averages_by_player.setdefault(user_id, []).append(parsed_average)

                if recent_match_counts.get(user_id, 0) >= 5:
                    continue

                elo_change = int(row["elo_change"] or 0)
                if not won:
                    elo_change = -elo_change

                recent_match_counts[user_id] = recent_match_counts.get(user_id, 0) + 1
                pending_entries.append(
                    (
                        user_id,
                        {
                            "matchId": int(row["id"]),
                            "result": "Win" if won else "Loss",
                            "opponentId": opponent_id,
                            "opponentName": None,
                            "score": row["score"] or "N/A",
                            "average": player_average or "N/A",
                            "eloChange": elo_change,
                            "queue": row["platform"],
                        },
                    )
                )

        opponent_names = {
            opponent_id: (await get_player_display(opponent_id))[0]
            for opponent_id in opponent_ids
        }
        for user_id, entry in pending_entries:
            entry["opponentName"] = opponent_names.get(entry["opponentId"], f"User {entry['opponentId']}")
            matches_by_player.setdefault(user_id, []).append(entry)

    player_ids = sorted(all_player_ids, key=lambda user_id: world_ranks.get(user_id, 999999))

    profiles: dict[str, dict] = {}
    for user_id in player_ids:
        name, avatar = await get_player_display(user_id)
        world_rating, world_wins, world_losses = world_stats.get(user_id, (1000, 0, 0))
        monthly_rating, monthly_wins, monthly_losses = monthly_stats.get(user_id, (0, 0, 0))
        averages = averages_by_player.get(user_id, [])
        matches = matches_by_player.get(user_id, [])

        total = world_wins + world_losses

        profiles[str(user_id)] = {
            "userId": user_id,
            "name": name,
            "avatar": avatar,
            "worldRating": world_rating,
            "worldRank": world_ranks.get(user_id),
            "worldWins": world_wins,
            "worldLosses": world_losses,
            "monthlyRating": monthly_rating,
            "monthlyRank": monthly_ranks.get(user_id),
            "monthlyWins": monthly_wins,
            "monthlyLosses": monthly_losses,
            "games": total,
            "winrate": round((world_wins / total) * 100, 1) if total else 0,
            "overallAverage": round(sum(averages) / len(averages), 2) if averages else 0,
            "recentMatches": matches,
        }

    return profiles


async def generate_html(bot: commands.Bot):
    display_cache: dict[int, tuple[str, str]] = {}

    async def find_member(user_id: int) -> discord.Member | None:
        for guild in bot.guilds:
            member = guild.get_member(user_id)
            if member is not None:
                return member

        for guild in bot.guilds:
            try:
                return await guild.fetch_member(user_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue

        return None

    async def get_player_display(user_id: int) -> tuple[str, str]:
        cached = display_cache.get(user_id)
        if cached is not None:
            return cached

        name = f"User {user_id}"
        avatar = "https://cdn.discordapp.com/embed/avatars/0.png"

        member = await find_member(user_id)
        if member is not None:
            name = member.display_name
            avatar = member.display_avatar.url

        display_cache[user_id] = (name, avatar)
        return name, avatar

    # =============================
    # DATA
    # =============================

    print("fetch data")
    player_data = await fetch_world_ranking(bot, limit=None)
    monthly_data = await fetch_monthly_ranking(bot, get_current_ranked_month_key(), limit=None)

    top3 = player_data[:3]

    # =============================
    # HTML START
    # =============================

    html = """
    <html>
    <head>
    <style>
    body {background:#0b0f14;color:white;font-family:Segoe UI;text-align:center;}

    .container {display:flex;justify-content:center;gap:50px;margin-top:40px;}

    .podium {display:flex;justify-content:center;gap:30px;margin-top:30px;}

    .card {background:#161b22;padding:20px;border-radius:20px;width:180px;transition:0.3s;cursor:pointer;}
    .card:hover {transform:scale(1.05);}

    .gold {border:2px solid gold;}
    .silver {border:2px solid silver;}
    .bronze {border:2px solid #cd7f32;}

    .avatar {width:70px;height:70px;border-radius:50%;}

    table {width:100%;border-collapse:collapse;margin-top:20px;}
    td {padding:10px;border-bottom:1px solid #222;}

    tr[data-player-id] {cursor:pointer;}
    tr:hover {background:#161b22;}

    a {color:#58a6ff;text-decoration:none;font-weight:bold;}
    .player-link {color:#58a6ff;background:none;border:0;padding:0;font:inherit;font-weight:bold;cursor:pointer;}
    .player-link:hover {text-decoration:underline;}
    .modal-backdrop {align-items:center;background:rgba(0,0,0,.72);display:none;inset:0;justify-content:center;padding:20px;position:fixed;z-index:1000;}
    .modal-backdrop.open {display:flex;}
    .player-modal {background:#161b22;border:1px solid #30363d;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,.45);max-width:500px;padding:30px;position:relative;text-align:center;width:min(400px, 100%);}
    .modal-close {background:#21262d;border:1px solid #30363d;border-radius:50%;color:white;cursor:pointer;font-size:22px;height:34px;line-height:28px;position:absolute;right:14px;top:14px;width:34px;}
    .modal-avatar {border-radius:50%;height:120px;width:120px;}
    .player-modal h1 {font-size:32px;margin:18px 0;}
    .player-modal p {font-size:16px;margin:10px 0;}
    .player-modal h3 {margin:22px 0 10px;}
    .match-list {display:inline-block;list-style:none;margin:0;padding-left:0;text-align:left;}
    .match-list li {margin:8px 0;}
    .modal-back-link {background:none;border:0;color:#58a6ff;cursor:pointer;font:inherit;font-weight:bold;margin-top:18px;padding:0;text-decoration:none;}
    .modal-back-link:hover {text-decoration:underline;}
    </style>
    </head>

    <body>

    <h1>🏆 RANKED DARTS Dashboard</h1>
    """

    # =============================
    # PODIUM
    # =============================

    print("build podium")
    html += "<div class='podium'>"
    classes = ["gold", "silver", "bronze"]

    for i, (user_id, world_rating, wins, losses) in enumerate(top3):
        name, avatar = await get_player_display(user_id)

        html += f"""
        <div class='card {classes[i]}' data-player-id="{user_id}">
        <img src="{avatar}" class="avatar"><br>
        <h2>#{i+1}</h2>
        <button type="button" class="player-link" data-player-id="{user_id}">{escape(name)}</button>
        <p>{world_rating} ELO</p>
        <p>{wins}W / {losses}L</p>
        </div>
        """

    html += "</div>"

    # =============================
    # TABLES
    # =============================

    html += "<div class='container'>"
    print("build world ranking")
    # 🌍 WORLD
    html += "<div style='width:40%'><h2>🌍 World Ranking</h2><table>"

    for i, (user_id, world_rating, wins, losses) in enumerate(player_data, 1):
        name, avatar = await get_player_display(user_id)

        html += f"""
        <tr data-player-id="{user_id}">
        <td>{i}</td>
        <td><img src="{avatar}" class="avatar"></td>
        <td><button type="button" class="player-link" data-player-id="{user_id}">{escape(name)}</button></td>
        <td>{world_rating}</td>
        <td>{wins}W / {losses}L</td>
        </tr>
        """

    html += "</table></div>"

    print("build monthly ranking")
    # 🗓️ MONTHLY
    html += "<div style='width:40%'><h2>🗓️ Monatsranking</h2><table>"

    for i, (user_id, monthly_rating, wins, losses) in enumerate(monthly_data, 1):
        name, avatar = await get_player_display(user_id)

        html += f"""
        <tr data-player-id="{user_id}">
        <td>{i}</td>
        <td><img src="{avatar}" class="avatar"></td>
        <td><button type="button" class="player-link" data-player-id="{user_id}">{escape(name)}</button></td>
        <td>{monthly_rating}</td>
        <td>{wins}W / {losses}L</td>
        </tr>
        """

    html += "</table></div>"

    html += "</div>"

    # =============================
    # Player Profiles
    # =============================
    print("build player profiles")

    player_profiles = await build_player_profiles(bot, player_data, monthly_data, get_player_display)
    with open(PLAYER_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump({"players": player_profiles}, f, ensure_ascii=False, indent=2)


    # =============================
    # SAVE
    # =============================
    print("save")
    html += """
    <div id="player-modal-backdrop" class="modal-backdrop" aria-hidden="true">
        <div class="player-modal" role="dialog" aria-modal="true" aria-labelledby="modal-player-name">
            <button type="button" id="modal-close" class="modal-close" aria-label="Schliessen">&times;</button>
            <img id="modal-avatar" class="modal-avatar" src="" alt="">
            <h1 id="modal-player-name"></h1>
            <p id="modal-rating"></p>
            <p id="modal-world-rank"></p>
            <p id="modal-monthly-rank"></p>
            <p id="modal-games"></p>
            <p id="modal-winrate"></p>
            <p id="modal-average"></p>
            <h3>🔥 Letzte Matches</h3>
            <ul id="modal-matches" class="match-list"></ul>
        </div>
    </div>
    <script>
    let playerProfiles = {};

    function text(id, value) {
        document.getElementById(id).textContent = value;
    }

    function rankText(value) {
        return value ? "#" + value : "N/A";
    }

    function openPlayerModal(userId, fallbackAvatar) {
        const player = playerProfiles[String(userId)];
        if (!player) {
            return;
        }

        const jsonAvatarIsDefault = player.avatar && player.avatar.includes("/embed/avatars/0.png");
        document.getElementById("modal-avatar").src = jsonAvatarIsDefault && fallbackAvatar ? fallbackAvatar : player.avatar;
        text("modal-player-name", player.name);
        text("modal-rating", "🏆 Rating: " + player.worldRating);
        text("modal-world-rank", "🌍 Worldrank: " + rankText(player.worldRank));
        text("modal-monthly-rank", "🗓️ Monatsrang: " + rankText(player.monthlyRank));
        text("modal-games", "🎯 Spiele: " + player.games);
        text("modal-winrate", "📈 Winrate: " + player.winrate + "%");
        text("modal-average", "🎯 Ø Average: " + player.overallAverage);

        const matches = document.getElementById("modal-matches");
        matches.replaceChildren();
        if (!player.recentMatches.length) {
            const item = document.createElement("li");
            item.textContent = "Keine Matches vorhanden.";
            matches.appendChild(item);
        } else {
            player.recentMatches.forEach(match => {
                const item = document.createElement("li");
                const resultIcon = match.result === "Win" ? "🟢" : "🔴";
                const elo = match.eloChange > 0 ? "+" + match.eloChange : String(match.eloChange);
                item.textContent = resultIcon + " vs " + match.opponentName + " (" + match.score + ") | Avg: " + match.average + " | " + elo + " ELO";
                matches.appendChild(item);
            });
        }

        const backdrop = document.getElementById("player-modal-backdrop");
        backdrop.classList.add("open");
        backdrop.setAttribute("aria-hidden", "false");
    }

    function closePlayerModal() {
        const backdrop = document.getElementById("player-modal-backdrop");
        backdrop.classList.remove("open");
        backdrop.setAttribute("aria-hidden", "true");
    }

    fetch("players.json?v=" + Date.now(), { cache: "no-store" })
        .then(response => response.json())
        .then(data => {
            playerProfiles = data.players || {};
        })
        .catch(error => console.error("players.json konnte nicht geladen werden", error));

    document.addEventListener("click", event => {
        const playerTarget = event.target.closest("[data-player-id]");
        if (playerTarget) {
            const avatar =
                playerTarget.querySelector("img")?.src ||
                playerTarget.closest("tr")?.querySelector("img")?.src ||
                playerTarget.closest(".card")?.querySelector("img")?.src;
            openPlayerModal(playerTarget.dataset.playerId, avatar);
            return;
        }

        if (
            event.target.id === "player-modal-backdrop" ||
            event.target.id === "modal-close" ||
            event.target.id === "modal-back-link"
        ) {
            closePlayerModal();
        }
    });

    document.addEventListener("keydown", event => {
        if (event.key === "Escape") {
            closePlayerModal();
        }
    });
    </script>
    </body></html>
    """

    with open(LEADERBOARD_FILE, "w", encoding="utf-8") as f:
        f.write(html)
        print("html generated")


# =============================
# Datenmodelle für den Laufzeit-Zustand von Panels, Matches und Ergebnissen.
# =============================

@dataclass(slots=True)
class MatchState:
    match_id: int
    queue_name: str
    player_ids: tuple[int, int]
    thread_id: int


@dataclass(slots=True)
class PendingMatchState:
    match_id: int
    queue_name: str
    player_ids: tuple[int, int]
    thread_id: int
    confirmed_user_ids: set[int] = field(default_factory=set)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pending_message_id: int | None = None


@dataclass(slots=True)
class PendingResultState:
    submission_id: int
    match_id: int
    winner_id: int
    score: tuple[int, int]
    score_text: str
    averages: dict[int, str]
    submitted_by: int
    thread_id: int
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    screenshot: discord.Attachment | None = None
    screenshot_url: str | None = None
    confirmation_message_id: int | None = None


@dataclass(slots=True)
class PanelState:
    channel_id: int
    dartcounter_queue: list[int] = field(default_factory=list)
    scolia_queue: list[int] = field(default_factory=list)
    queue_joined_at: dict[int, datetime] = field(default_factory=dict)

    def get_queue(self, queue_name: str) -> list[int]:
        if queue_name == "DartCounter":
            return self.dartcounter_queue
        return self.scolia_queue


# =============================
# Darstellung: Queue-, Match-, Ergebnis- und Ranking-Embeds.
# =============================

def format_queue(queue: list[int]) -> str:
    if not queue:
        return QUEUE_EMPTY_TEXT
    return "\n".join(f"<@{user_id}>" for user_id in queue)


def format_active_matches(matches: list[MatchState]) -> str:
    return "\n".join(
        f"{match.queue_name} #{match.match_id:03d} <@{match.player_ids[0]}> vs <@{match.player_ids[1]}>"
        for match in matches
    )


def format_admin_active_matches(matches: list[MatchState]) -> str:
    if not matches:
        return "Keine aktiven Matches."

    return "\n".join(
        f"#{match.match_id:03d} | {match.queue_name} | <@{match.player_ids[0]}> vs <@{match.player_ids[1]}> | <#{match.thread_id}>"
        for match in matches
    )


def format_admin_pending_matches(matches: list[PendingMatchState]) -> str:
    if not matches:
        return "Keine pending Matches."

    lines: list[str] = []
    for match in matches:
        confirmed = [user_id for user_id in match.player_ids if user_id in match.confirmed_user_ids]
        waiting = [user_id for user_id in match.player_ids if user_id not in match.confirmed_user_ids]
        confirmed_text = ", ".join(f"<@{user_id}>" for user_id in confirmed) or "niemand"
        waiting_text = ", ".join(f"<@{user_id}>" for user_id in waiting) or "niemand"
        created_timestamp = int(match.created_at.timestamp())
        lines.append(
            f"#{match.match_id:03d} | {match.queue_name} | <@{match.player_ids[0]}> vs <@{match.player_ids[1]}> | "
            f"<#{match.thread_id}> | Bestätigt: {confirmed_text} | Wartet: {waiting_text} | Erstellt: <t:{created_timestamp}:R>"
        )

    return "\n".join(lines)


def fit_embed_description(value: str) -> str:
    if len(value) <= 4096:
        return value
    return value[:4092] + "\n..."


def build_admin_matches_embed(*, title: str, description: str, colour: discord.Color) -> discord.Embed:
    return discord.Embed(
        title=title,
        description=fit_embed_description(description),
        colour=colour,
    )


def build_queue_embed(panel_state: PanelState, active_matches: list[MatchState]) -> discord.Embed:
    embed = discord.Embed(
        title=":dart: Dart Matchmaking",
        description="Queue Status\u200b",
        colour=discord.Color.blurple(),
    )
    embed.add_field(name=":dart: DartCounter", value=format_queue(panel_state.dartcounter_queue), inline=False)
    embed.add_field(name=":blue_circle: Scolia", value=format_queue(panel_state.scolia_queue), inline=False)

    if active_matches:
        embed.add_field(name=MATCHES_FIELD_NAME, value=format_active_matches(active_matches), inline=False)

    return embed


def build_pending_match_embed(match: PendingMatchState) -> discord.Embed:
    confirmed_mentions = (
        "\n".join(f"<@{user_id}>" for user_id in match.player_ids if user_id in match.confirmed_user_ids)
        if match.confirmed_user_ids
        else "noch niemand"
    )
    waiting_mentions = (
        "\n".join(f"<@{user_id}>" for user_id in match.player_ids if user_id not in match.confirmed_user_ids)
        if len(match.confirmed_user_ids) < len(match.player_ids)
        else "niemand"
    )

    embed = discord.Embed(
        title=f"Match #{match.match_id:03d} bestätigen",
        description=(
            f"{match.queue_name} Match zwischen <@{match.player_ids[0]}> und <@{match.player_ids[1]}>.\n"
            "Beide Spieler müssen bestätigen, bevor das Match aktiv wird."
        ),
        colour=discord.Color.gold(),
    )
    embed.add_field(name="Bestätigt", value=confirmed_mentions, inline=True)
    embed.add_field(name="Wartet auf", value=waiting_mentions, inline=True)
    return embed


def build_confirmed_match_embed(match: MatchState) -> discord.Embed:
    return discord.Embed(
        title=f"Match #{match.match_id:03d} bestätigt",
        description=(
            f"{match.queue_name} <@{match.player_ids[0]}> vs <@{match.player_ids[1]}>\n"
            "Das Match ist jetzt aktiv."
        ),
        colour=discord.Color.green(),
    )


def build_result_embed(
    match: MatchState,
    result: PendingResultState,
    player_display_names: dict[int, str] | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=f":bar_chart: Match Ergebnis #{match.match_id:03d}",
        description=f"{match.queue_name} <@{match.player_ids[0]}> vs <@{match.player_ids[1]}>",
        colour=discord.Color.dark_green(),
    )
    player_one_id, player_two_id = match.player_ids
    player_display_names = player_display_names or {}
    player_one_name = player_display_names.get(player_one_id, f"Spieler {player_one_id}")
    player_two_name = player_display_names.get(player_two_id, f"Spieler {player_two_id}")
    embed.add_field(name="Eingetragen von", value=f"<@{result.submitted_by}>", inline=False)
    embed.add_field(name="Gewinner", value=f"<@{result.winner_id}>", inline=True)
    embed.add_field(name="Spielstand", value=result.score_text, inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    embed.add_field(name=f"Average {player_one_name}", value=result.averages[player_one_id], inline=True)
    embed.add_field(name=f"Average {player_two_name}", value=result.averages[player_two_id], inline=True)
    if result.screenshot_url is not None:
        embed.set_image(url=result.screenshot_url)
    return embed


def build_withdrawn_match_embed(match_id: int) -> discord.Embed:
    return discord.Embed(
        title=f"Match Ergebnis #{match_id:03d}",
        description="Das Match wurde zurückgezogen.",
        colour=discord.Color.red(),
    )
def build_cancel_match_embed(match_id: int) -> discord.Embed:
    return discord.Embed(
        title=f"Match Ergebnis #{match_id:03d}",
        description="Das Match wurde abgebrochen.",
        colour=discord.Color.red(),
    )


def build_ranking_embed(
    *,
    title: str,
    rows: list[tuple[int, int, int, int]],
    empty_text: str,
) -> discord.Embed:
    embed = discord.Embed(title=title, colour=discord.Color.gold())
    if not rows:
        embed.description = empty_text
        return embed

    lines = [
        f"**{index}.** <@{user_id}> | Rating: **{rating}** | W: {wins} | L: {losses}"
        for index, (user_id, rating, wins, losses) in enumerate(rows, start=1)
    ]
    embed.description = "\n".join(lines)
    return embed


def build_monthly_ranking_embeds(
    *,
    title: str,
    rows: list[tuple[int, int, int, int]],
    empty_text: str,
) -> list[discord.Embed]:
    if not rows:
        embed = discord.Embed(title=title, description=empty_text, colour=discord.Color.gold())
        return [embed]

    chunks: list[list[str]] = []
    current_chunk: list[str] = []
    current_length = 0

    for index, (user_id, points, wins, losses) in enumerate(rows, start=1):
        line = f"**{index}.** <@{user_id}> | Punkte: **{points}** | W: {wins} | L: {losses}"
        line_length = len(line) + 1
        if current_chunk and current_length + line_length > 3900:
            chunks.append(current_chunk)
            current_chunk = []
            current_length = 0

        current_chunk.append(line)
        current_length += line_length

    if current_chunk:
        chunks.append(current_chunk)

    embeds: list[discord.Embed] = []
    page_count = len(chunks)
    for page_index, chunk in enumerate(chunks, start=1):
        page_title = title if page_count == 1 else f"{title} ({page_index}/{page_count})"
        embed = discord.Embed(title=page_title, colour=discord.Color.gold())
        embed.description = "\n".join(chunk)
        embeds.append(embed)

    return embeds


def rank_value(rank: int | None) -> str:
    return f"#{rank}" if rank is not None else "N/A"


def build_stats_embed(
    *,
    player: discord.Member,
    world_rank: int | None,
    monthly_rank: int | None,
    rating: int,
    total: int,
    winrate: int | float,
) -> discord.Embed:
    embed = discord.Embed(title=f"ðŸ“Š Stats von {player.display_name}")

    embed.add_field(name="ðŸ† Rating", value=str(rating), inline=False)
    embed.add_field(name="ðŸŒ Global Rank", value=rank_value(world_rank), inline=False)
    embed.add_field(name="ðŸ—“ï¸ Monthly Rank", value=rank_value(monthly_rank), inline=False)
    embed.add_field(name="ðŸŽ¯ Spiele", value=str(total), inline=False)
    embed.add_field(name="ðŸ“ˆ Winrate", value=f"{winrate}%", inline=False)
    return embed


# =============================
# Parser und Normalisierung für Queue-Embeds, Thread-Namen und Formularwerte.
# =============================

def parse_queue(value: str) -> list[int]:
    if value.strip().lower() == QUEUE_EMPTY_TEXT:
        return []

    user_ids: list[int] = []
    for match in MENTION_PATTERN.finditer(value):
        user_id = int(match.group(1))
        if user_id not in user_ids:
            user_ids.append(user_id)
    return user_ids


def parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def unique_int_list(value: object) -> list[int]:
    if not isinstance(value, list):
        return []

    result: list[int] = []
    for item in value:
        try:
            user_id = int(item)
        except (TypeError, ValueError):
            continue
        if user_id not in result:
            result.append(user_id)
    return result


def panel_state_to_json(panel_state: PanelState) -> dict:
    queued_user_ids = set(panel_state.dartcounter_queue) | set(panel_state.scolia_queue)
    joined_at = {
        str(user_id): panel_state.queue_joined_at[user_id].isoformat()
        for user_id in queued_user_ids
        if user_id in panel_state.queue_joined_at
    }
    return {
        "channel_id": panel_state.channel_id,
        "dartcounter_queue": panel_state.dartcounter_queue,
        "scolia_queue": panel_state.scolia_queue,
        "joined_at": joined_at,
    }


def panel_state_from_json(value: object) -> PanelState | None:
    if not isinstance(value, dict):
        return None

    try:
        channel_id = int(value["channel_id"])
    except (KeyError, TypeError, ValueError):
        return None

    state = PanelState(
        channel_id=channel_id,
        dartcounter_queue=unique_int_list(value.get("dartcounter_queue")),
        scolia_queue=unique_int_list(value.get("scolia_queue")),
    )

    joined_at = value.get("joined_at")
    if isinstance(joined_at, dict):
        for user_id_text, timestamp in joined_at.items():
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            parsed = parse_utc_datetime(str(timestamp))
            if parsed is not None:
                state.queue_joined_at[user_id] = parsed

    now = datetime.now(timezone.utc)
    for user_id in set(state.dartcounter_queue) | set(state.scolia_queue):
        state.queue_joined_at.setdefault(user_id, now)

    return state


def panel_state_from_embed(message: discord.Message) -> PanelState:
    state = PanelState(channel_id=message.channel.id)

    if not message.embeds:
        return state

    for field in message.embeds[0].fields:
        if "DartCounter" in field.name:
            state.dartcounter_queue = parse_queue(field.value)
        elif "Scolia" in field.name:
            state.scolia_queue = parse_queue(field.value)

    return state


def is_queue_panel_message(message: discord.Message) -> bool:
    if not message.embeds:
        return False

    embed = message.embeds[0]
    if embed.title != ":dart: Dart Matchmaking":
        return False

    field_names = [field.name for field in embed.fields]
    return any("DartCounter" in name for name in field_names) and any("Scolia" in name for name in field_names)


def normalize_thread_part(value: str) -> str:
    normalized = value.lower().replace(" ", "-")
    normalized = THREAD_SAFE_PATTERN.sub("", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "spieler"


def normalize_average(value: str) -> str | None:
    stripped = value.strip()
    if not AVERAGE_PATTERN.fullmatch(stripped):
        return None
    return stripped.replace(".", ",")


def parse_best_of_seven_score(value: str) -> tuple[int, int] | None:
    match = SCORE_PATTERN.fullmatch(value)
    if match is None:
        return None

    left_score = int(match.group(1))
    right_score = int(match.group(2))
    if left_score == 4 and 0 <= right_score <= 3:
        return left_score, right_score
    if right_score == 4 and 0 <= left_score <= 3:
        return left_score, right_score
    return None


def parse_user_id_from_mention(value: str) -> int | None:
    match = MENTION_PATTERN.search(value)
    if match is None:
        return None
    return int(match.group(1))


def parse_user_ids_from_lines(value: str) -> set[int]:
    return {int(match.group(1)) for match in MENTION_PATTERN.finditer(value)}


def interaction_user_is_admin(interaction: discord.Interaction) -> bool:
    permissions = getattr(interaction.user, "guild_permissions", None)
    return bool(permissions is not None and permissions.administrator)


def shorten_label(value: str, limit: int = 28) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


# =============================
# Match-Bestätigung: Buttons für Annahme oder Rückzug eines neuen Matches.
# =============================

class PendingMatchView(discord.ui.View):
    def __init__(
        self,
        cog: Ranked,
        match_id: int | None = None,
        *,
        withdraw_enabled: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id
        self.set_withdraw_button_disabled(not withdraw_enabled)

    def set_withdraw_button_disabled(self, disabled: bool) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.custom_id == "ranked:pending_withdraw":
                item.disabled = disabled
                return

    def resolve_match_id(self, interaction: discord.Interaction) -> int | None:
        if self.match_id is not None:
            return self.match_id
        if isinstance(interaction.channel, discord.Thread):
            pending_match = self.cog.get_pending_match_by_thread_id(interaction.channel.id)
            if pending_match is not None:
                return pending_match.match_id
        message = interaction.message
        if message is not None and message.embeds:
            title = message.embeds[0].title or ""
            match = MATCH_ID_PATTERN.search(title)
            if match is not None:
                return int(match.group(1))
        return None

    def sync_confirmations_from_message(self, pending_match: PendingMatchState, interaction: discord.Interaction) -> None:
        message = interaction.message
        if message is None or not message.embeds:
            return

        for field in message.embeds[0].fields:
            if "best" in field.name.casefold():
                pending_match.confirmed_user_ids = parse_user_ids_from_lines(field.value)
                break

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return False

        self.match_id = match_id
        match = self.cog.pending_matches.get(match_id)
        if match is None:
            match = await self.cog.recover_pending_match_from_message(interaction.message, expected_match_id=match_id)
        if match is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return False

        self.sync_confirmations_from_message(match, interaction)

        if interaction.user.id in match.player_ids:
            return True

        await interaction.response.send_message("Nur die beiden Spieler können hier reagieren.", ephemeral=True)
        return False

    @discord.ui.button(label="Bestätigen", style=discord.ButtonStyle.success, custom_id="ranked:pending_confirm")
    async def confirm_callback(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return

        self.match_id = match_id
        pending_match = self.cog.pending_matches.get(self.match_id)
        if pending_match is None:
            pending_match = await self.cog.recover_pending_match_from_message(interaction.message, expected_match_id=match_id)
        if pending_match is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return

        self.sync_confirmations_from_message(pending_match, interaction)

        if interaction.user.id in pending_match.confirmed_user_ids:
            await interaction.response.send_message("Du hast dieses Match bereits bestätigt.", ephemeral=True)
            return

        pending_match.confirmed_user_ids.add(interaction.user.id)
        await self.cog.log_match_player_confirmed(pending_match, interaction.user.id)

        if len(pending_match.confirmed_user_ids) < 2:
            await interaction.response.edit_message(embed=build_pending_match_embed(pending_match), view=self)
            await interaction.followup.send(
                "Du hast das Match bestätigt. Wenn dein Gegner das Match nicht innerhalb von 5 Minuten bestätigt, "
                "kannst du den Button \"Match zurückziehen\" drücken.",
                ephemeral=True,
            )
            return

        active_match = await self.cog.confirm_pending_match(self.match_id)
        if active_match is None:
            await interaction.response.send_message("Das Match konnte nicht bestätigt werden.", ephemeral=True)
            return

        self.stop()
        await interaction.response.edit_message(embed=build_confirmed_match_embed(active_match), view=None)
        thread = await self.cog.fetch_thread(active_match.thread_id)
        if thread is not None:
            await thread.send(
                "Wenn euer Match beendet ist, könnt ihr hier das Ergebnis eintragen: Alternativ mit /result",
                view=ResultEntryView(self.cog, active_match.match_id),
            )
        await self.cog.refresh_panels(refresh_all=True)

    @discord.ui.button(
        label="Match zurückziehen",
        style=discord.ButtonStyle.danger,
        custom_id="ranked:pending_withdraw",
    )
    async def withdraw_callback(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return

        pending_match = self.cog.pending_matches.get(match_id)
        if pending_match is None:
            pending_match = await self.cog.recover_pending_match_from_message(interaction.message, expected_match_id=match_id)
        if pending_match is None:
            await interaction.response.send_message("Dieses Match ist nicht mehr offen.", ephemeral=True)
            return

        if not self.cog.is_pending_match_withdraw_enabled(pending_match):
            await interaction.response.send_message(
                "Der Match-Rückzug ist erst 5 Minuten nach Match-Erstellung möglich.",
                ephemeral=True,
            )
            return

        await self.cog.cancel_pending_match(interaction, pending_match)


# =============================
# Ergebnis-Bestätigung: Gegenspieler prueft und bestätigt den Vorschlag.
# =============================

class ResultConfirmationView(discord.ui.View):
    def __init__(
        self,
        cog: Ranked,
        match_id: int | None = None,
        submission_id: int | None = None,
        confirmer_id: int | None = None,
    ) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id
        self.submission_id = submission_id
        self.confirmer_id = confirmer_id

    def resolve_match_id(self, interaction: discord.Interaction) -> int | None:
        if self.match_id is not None:
            return self.match_id
        if isinstance(interaction.channel, discord.Thread):
            match = self.cog.get_active_match_by_thread_id(interaction.channel.id)
            if match is not None:
                return match.match_id
        return None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.response.send_message("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return False
        self.match_id = match_id
        match = self.cog.active_matches.get(match_id)
        result = self.cog.get_pending_result_for_confirmation(match_id, interaction.message)
        if result is None or match is None:
            await interaction.response.send_message("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return False
        if self.submission_id is not None and result.submission_id != self.submission_id:
            await interaction.response.send_message("Es gibt bereits einen neueren Ergebnisvorschlag.", ephemeral=True)
            return False
        self.submission_id = result.submission_id
        if self.confirmer_id is None:
            self.confirmer_id = self.cog.parse_confirmer_id_from_confirmation_message(interaction.message)
        custom_id = ""
        if isinstance(interaction.data, dict):
            custom_id = str(interaction.data.get("custom_id") or "")
        if custom_id == "ranked:result_confirm" and interaction_user_is_admin(interaction):
            return True
        if interaction.user.id not in match.player_ids:
            await interaction.response.send_message("Nur die beiden Spieler können das Ergebnis bestätigen.", ephemeral=True)
            return False
        if self.confirmer_id is None:
            await interaction.response.send_message("Der bestätigende Spieler konnte nicht ermittelt werden.", ephemeral=True)
            return False
        can_confirm_as_submitter = (
            custom_id == "ranked:result_confirm"
            and interaction.user.id == result.submitted_by
            and self.cog.is_result_self_confirm_available(result)
        )
        if interaction.user.id != self.confirmer_id and not can_confirm_as_submitter:
            if custom_id == "ranked:result_confirm" and interaction.user.id == result.submitted_by:
                await interaction.response.send_message(
                    "Du kannst dein eigenes Ergebnis erst nach 1 Minute selbst bestätigen.",
                    ephemeral=True,
                )
                return False
            if custom_id == "ranked:result_dispute":
                await interaction.response.send_message(
                    f"Nur <@{self.confirmer_id}> kann diesem Ergebnis widersprechen.",
                    ephemeral=True,
                )
                return False
            await interaction.response.send_message(
                f"Nur <@{self.confirmer_id}> kann dieses Ergebnis bestätigen.",
                ephemeral=True,
            )
            return False

        return True

    @discord.ui.button(
        label="Ergebnis bestätigen",
        style=discord.ButtonStyle.success,
        custom_id="ranked:result_confirm",
    )
    async def confirm_callback(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await interaction.response.defer(ephemeral=True, thinking=True)
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.followup.send("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return
        self.match_id = match_id
        match = self.cog.active_matches.get(match_id)
        result = self.cog.get_pending_result_for_confirmation(match_id, interaction.message)
        if (
            result is None
            or match is None
            or (self.submission_id is not None and result.submission_id != self.submission_id)
        ):
            await interaction.followup.send("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return
        self.submission_id = result.submission_id
        results_channel = await self.cog.fetch_results_channel()
        if results_channel is None:
            await self.cog.send_admin_log(
                "Ergebnis-Channel fehlt",
                f"{self.cog.describe_match(match)}\nDer Ergebnis-Channel konnte beim Bestätigen nicht gefunden werden.",
                colour=discord.Color.red(),
            )
            await interaction.followup.send("Der Ergebnis-Channel konnte nicht gefunden werden.", ephemeral=True)
            return
        if interaction.guild_id is None:
            await self.cog.send_admin_log(
                "Guild-ID fehlt",
                f"{self.cog.describe_match(match)}\nDie Guild-ID konnte beim Ergebnis-Bestätigen nicht aufgelöst werden.",
                colour=discord.Color.red(),
            )
            await interaction.followup.send("Guild-ID konnte nicht aufgelöst werden.", ephemeral=True)
            return
        persisted, already_published = await persist_ranked_match_result(
            self.cog.bot,
            match,
            result,
            guild_id=interaction.guild_id,
            confirmed_by=interaction.user.id,
        )
        if not persisted:
            await self.cog.log_result_persist_failed(match, result)
            await interaction.followup.send(
                "Das Ergebnis konnte nicht in der Datenbank gespeichert werden. Match bleibt offen.",
                ephemeral=True,
            )
            return
        if already_published:
            self.stop()
            self.cog.remove_pending_result(self.match_id)
            self.cog.active_matches.pop(self.match_id, None)
            await self.cog.refresh_panels(refresh_all=True)
            thread = await self.cog.fetch_thread(match.thread_id)
            if thread is not None:
                try:
                    await thread.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
            await interaction.followup.send("Dieses Ergebnis wurde bereits verarbeitet. Match wurde geschlossen.", ephemeral=True)
            return

        try:
            results_message = await self.cog.send_result_message(results_channel, match, result)
        except discord.HTTPException:
            await self.cog.log_result_publish_failed(match, result)
            await interaction.followup.send(
                "Das Ergebnis wurde gespeichert, aber nicht in den Ergebnis-Channel gesendet. Bitte erneut bestätigen.",
                ephemeral=True,
            )
            return

        await mark_ranked_match_result_published(self.cog.bot, match.match_id, results_channel.id, results_message.id)
        await self.cog.log_result_confirmed(match, result, interaction.user.id)

        self.stop()
        self.cog.remove_pending_result(self.match_id)
        self.cog.active_matches.pop(self.match_id, None)
        await self.cog.refresh_panels(refresh_all=True)

        thread = await self.cog.fetch_thread(match.thread_id)
        if thread is not None:
            try:
                await thread.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        await generate_html(self.cog.bot)
        upload()
        await interaction.followup.send("Ergebnis bestätigt und gepostet.", ephemeral=True)

    @discord.ui.button(
        label="Ergebnis widersprechen",
        style=discord.ButtonStyle.danger,
        custom_id="ranked:result_dispute",
    )
    async def dispute_callback(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        match_id = self.resolve_match_id(interaction)
        if match_id is None:
            await interaction.response.send_message("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return

        self.match_id = match_id
        match = self.cog.active_matches.get(match_id)
        result = self.cog.get_pending_result_for_confirmation(match_id, interaction.message)
        if (
            result is None
            or match is None
            or (self.submission_id is not None and result.submission_id != self.submission_id)
        ):
            await interaction.response.send_message("Dieses Ergebnis ist nicht mehr offen.", ephemeral=True)
            return

        self.stop()
        await self.cog.log_result_disputed(match, result, interaction.user.id)
        self.cog.remove_pending_result(match_id)
        await interaction.response.edit_message(content="Dem Ergebnis wurde widersprochen.", view=None)
        if not await self.cog.post_result_entry_button(match):
            await self.cog.send_admin_log(
                "Ergebnis-Button konnte nicht erneut gesendet werden",
                f"{self.cog.describe_match(match)}\nNach Widerspruch konnte der Ergebnis-posten-Button nicht gepostet werden.",
                colour=discord.Color.red(),
            )
            await interaction.followup.send(
                "Der Ergebnis-posten-Button konnte nicht erneut gesendet werden. Nutzt bitte /result im Match-Thread.",
                ephemeral=True,
            )


# =============================
# Ergebnis-Erfassung: Button im Match-Thread und Modal fuer Score, Average und Screenshot.
# =============================

class ResultEntryView(discord.ui.View):
    def __init__(self, cog: Ranked, match_id: int | None = None) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id

    @discord.ui.button(
        label="Ergebnis posten",
        style=discord.ButtonStyle.success,
        custom_id="ranked:result_entry",
    )
    async def post_result(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.match_id is None:
            if not isinstance(interaction.channel, discord.Thread):
                match = None
            else:
                match = self.cog.get_active_match_by_thread_id(interaction.channel.id)
        else:
            match = self.cog.get_active_match_by_id(self.match_id)

        if match is None:
            await interaction.response.send_message(
                "In diesem Thread wurde kein aktives Match gefunden.",
                ephemeral=True,
            )
            return

        if interaction.user.id not in match.player_ids:
            await interaction.response.send_message(
                "Nur die beiden Match-Spieler dürfen das Ergebnis eintragen.",
                ephemeral=True,
            )
            return

        if self.cog.pending_results.get(match.match_id) is not None:
            await interaction.response.send_message(
                "Für dieses Match wurde bereits ein Ergebnis eingetragen und wartet auf Bestätigung.",
                ephemeral=True,
            )
            return

        message = interaction.message
        button.disabled = True
        await self.cog.open_result_modal(
            interaction,
            match_id=match.match_id,
            entry_message_id=message.id if message is not None else None,
        )
        if message is not None:
            try:
                await message.edit(view=self)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass


class ResultModal(discord.ui.Modal):
    def __init__(
        self,
        cog: Ranked,
        match: MatchState,
        guild: discord.Guild,
        entry_message_id: int | None = None,
        submitted_by_id: int | None = None,
    ) -> None:
        super().__init__(title=f"Ergebnis Match #{match.match_id:03d}")
        self.cog = cog
        self.match = match
        self.guild = guild
        self.entry_message_id = entry_message_id
        self.submitted_by_id = submitted_by_id

        player_one = guild.get_member(match.player_ids[0])
        player_two = guild.get_member(match.player_ids[1])
        player_one_name = shorten_label(player_one.display_name if player_one else f"Spieler {match.player_ids[0]}")
        player_two_name = shorten_label(player_two.display_name if player_two else f"Spieler {match.player_ids[1]}")
        score_player_one_name = shorten_label(player_one_name, 14)
        score_player_two_name = shorten_label(player_two_name, 14)

        self.winner_select = discord.ui.Select(
            placeholder="Gewinner auswählen",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label=player_one_name, value=str(match.player_ids[0])),
                discord.SelectOption(label=player_two_name, value=str(match.player_ids[1])),
            ],
        )
        self.score_input = discord.ui.TextInput(
            label=f"Spielstand ({score_player_one_name}:{score_player_two_name})",
            placeholder="z. B. 4:2",
            required=True,
            max_length=10,
        )
        self.average_one_input = discord.ui.TextInput(
            label=f"Average {player_one_name}",
            placeholder="z. B. 54,32 oder 54.32",
            required=True,
            max_length=20,
        )
        self.average_two_input = discord.ui.TextInput(
            label=f"Average {player_two_name}",
            placeholder="z. B. 48,76 oder 48.76",
            required=True,
            max_length=20,
        )
        self.screenshot_upload = discord.ui.FileUpload(
            required=True,
            min_values=1,
            max_values=1,
        )

        self.add_item(discord.ui.Label(text="Gewinner", component=self.winner_select))
        self.add_item(self.score_input)
        self.add_item(self.average_one_input)
        self.add_item(self.average_two_input)
        self.add_item(discord.ui.Label(text="Screenshot", component=self.screenshot_upload))

    async def restore_result_entry_button(self) -> None:
        if self.entry_message_id is None:
            return

        thread = await self.cog.fetch_thread(self.match.thread_id)
        if thread is None:
            return

        try:
            message = await thread.fetch_message(self.entry_message_id)
            await message.edit(view=ResultEntryView(self.cog, self.match.match_id))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not self.winner_select.values:
            await self.restore_result_entry_button()
            await interaction.response.send_message("Bitte wähle einen Gewinner aus.", ephemeral=True)
            return

        score = parse_best_of_seven_score(self.score_input.value)
        if score is None:
            await self.restore_result_entry_button()
            await interaction.response.send_message(
                "Bitte gib einen gültigen Best-of-7-Spielstand ein, z. B. 4:0 bis 4:3.",
                ephemeral=True,
            )
            return

        average_one = normalize_average(self.average_one_input.value)
        average_two = normalize_average(self.average_two_input.value)
        if average_one is None or average_two is None:
            await self.restore_result_entry_button()
            await interaction.response.send_message(
                "Die Averages müssen numerisch sein. Punkt und Komma sind erlaubt.",
                ephemeral=True,
            )
            return

        winner_id = int(self.winner_select.values[0])
        player_one_id, player_two_id = self.match.player_ids
        left_score, right_score = score

        if winner_id == player_one_id and left_score != 4:
            await self.restore_result_entry_button()
            await interaction.response.send_message(
                "Der ausgewählte Gewinner passt nicht zum Spielstand.",
                ephemeral=True,
            )
            return

        if winner_id == player_two_id and right_score != 4:
            await self.restore_result_entry_button()
            await interaction.response.send_message(
                "Der ausgewählte Gewinner passt nicht zum Spielstand.",
                ephemeral=True,
            )
            return

        screenshot = self.screenshot_upload.values[0] if self.screenshot_upload.values else None
        # if screenshot is None:
        #     await self.restore_result_entry_button()
        #     await interaction.response.send_message(
        #         "Bitte hänge einen Screenshot an.",
        #         ephemeral=True,
        #     )
        #     return

        submission_id = self.cog.next_result_submission_id
        self.cog.next_result_submission_id += 1

        previous_result = self.cog.pending_results.get(self.match.match_id)
        if previous_result is not None:
            await self.cog.mark_result_submission_obsolete(previous_result)

        pending_result = PendingResultState(
            submission_id=submission_id,
            match_id=self.match.match_id,
            winner_id=winner_id,
            score=score,
            score_text=f"{left_score}:{right_score}",
            averages={
                player_one_id: average_one,
                player_two_id: average_two,
            },
            submitted_by=self.submitted_by_id or interaction.user.id,
            thread_id=self.match.thread_id,
            screenshot=screenshot,
            screenshot_url=screenshot.url,
        )
        self.cog.pending_results[self.match.match_id] = pending_result

        thread = await self.cog.fetch_thread(self.match.thread_id)
        if thread is None:
            await self.cog.send_admin_log(
                "Match-Thread fehlt",
                f"{self.cog.describe_match(self.match)}\nDer Ergebnisvorschlag konnte nicht gesendet werden, weil der Thread nicht gefunden wurde.",
                colour=discord.Color.red(),
            )
            self.cog.remove_pending_result(self.match.match_id)
            await self.restore_result_entry_button()
            await interaction.response.send_message("Der Match-Thread konnte nicht gefunden werden.", ephemeral=True)
            return

        submitter_id = pending_result.submitted_by
        confirmer_id = player_two_id if submitter_id == player_one_id else player_one_id
        confirmation_view = ResultConfirmationView(self.cog, self.match.match_id, submission_id, confirmer_id)

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            message = await self.cog.send_result_message(
                thread,
                self.match,
                pending_result,
                content=f"<@{confirmer_id}>, bitte bestätige dieses Ergebnis.",
                view=confirmation_view,
            )
        except discord.HTTPException:
            await self.cog.send_admin_log(
                "Ergebnisvorschlag konnte nicht gesendet werden",
                f"{self.cog.describe_match(self.match)}\nDer Ergebnisvorschlag konnte nicht im Match-Thread gepostet werden.",
                colour=discord.Color.red(),
            )
            self.cog.remove_pending_result(self.match.match_id)
            await self.restore_result_entry_button()
            await interaction.followup.send("Das Ergebnis konnte nicht im Match-Thread gesendet werden.", ephemeral=True)
            return

        pending_result.confirmation_message_id = message.id
        self.cog.schedule_result_self_confirm_notification(pending_result)
        await self.cog.log_result_submitted(self.match, pending_result)
        await interaction.followup.send("Ergebnis zur Bestätigung in den Match-Thread gesendet.", ephemeral=True)


# =============================
# Queue-Panel: Beitreten, Verlassen und automatisches Starten passender Matches.
# =============================

class QueuePanel(discord.ui.View):
    def __init__(self, cog: Ranked, panel_state: PanelState | None = None) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        if panel_state is not None:
            self.set_both_button_disabled(bool(panel_state.dartcounter_queue and panel_state.scolia_queue))

    def set_both_button_disabled(self, disabled: bool) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.custom_id == "queue_panel:both_join":
                item.disabled = disabled
                return

    @staticmethod
    def find_joined_queue(panel_state: PanelState, user_id: int) -> str | None:
        in_dartcounter = user_id in panel_state.dartcounter_queue
        in_scolia = user_id in panel_state.scolia_queue
        if in_dartcounter and in_scolia:
            return "Beides"
        if in_dartcounter:
            return "DartCounter"
        if in_scolia:
            return "Scolia"
        return None

    @staticmethod
    def has_waiting_opponent(queue: list[int], user_id: int) -> bool:
        return any(queued_user_id != user_id for queued_user_id in queue)

    async def update_queue(self,  interaction: discord.Interaction, *, queue_name: str | None, join: bool) -> None:
        message = interaction.message
        if message is None:
            await interaction.response.send_message("Das Queue-Embed konnte nicht gelesen werden.", ephemeral=True)
            return

        panel_state = self.cog.get_or_create_panel_state(message)
        user_id = interaction.user.id
        joined_queue = self.find_joined_queue(panel_state, user_id)

        if join:
            if queue_name is None:
                await interaction.response.send_message("Keine Queue ausgewählt.", ephemeral=True)
                return

            queue = panel_state.get_queue(queue_name)

            if self.cog.is_user_locked(user_id):
                await interaction.response.send_message(
                    "Du bist bereits in einem offenen oder aktiven Match und kannst keiner Queue beitreten.",
                    ephemeral=True,
                    delete_after=10,
                )
                return

            if user_id in queue:
                await interaction.response.send_message(
                    f"Du bist bereits in der {queue_name}-Queue.",
                    ephemeral=True,
                    delete_after=10,
                )
                return

            if joined_queue is not None and joined_queue != queue_name:
                await interaction.response.send_message(
                    f"Du bist bereits in der {joined_queue}-Queue. Verlasse sie zuerst, bevor du wechselst.",
                    ephemeral=True,
                    delete_after=10,
                )
                return

            queue.append(user_id)
            self.cog.schedule_queue_timeout(message.id, user_id, notify_interaction=interaction)
            match_started = await self.cog.try_start_matches(message, panel_state, queue_name)
        else:
            if joined_queue is None:
                await interaction.response.send_message(
                    "Du bist aktuell in keiner Queue.",
                    ephemeral=True,
                    delete_after=10,
                )
                return

            panel_state.dartcounter_queue[:] = [
                queued_user_id for queued_user_id in panel_state.dartcounter_queue if queued_user_id != user_id
            ]
            panel_state.scolia_queue[:] = [
                queued_user_id for queued_user_id in panel_state.scolia_queue if queued_user_id != user_id
            ]
            self.cog.cancel_queue_timeout(message.id, user_id)
            match_started = False

        self.cog.persist_queue_state()
        await self.cog.refresh_panels(
            interaction=interaction,
            current_message_id=message.id,
            refresh_all=match_started,
        )

    async def join_both_queues(self, interaction: discord.Interaction) -> None:
        message = interaction.message
        if message is None:
            await interaction.response.send_message("Das Queue-Embed konnte nicht gelesen werden.", ephemeral=True)
            return

        panel_state = self.cog.get_or_create_panel_state(message)
        user_id = interaction.user.id

        if self.cog.is_user_locked(user_id):
            await interaction.response.send_message(
                "Du bist bereits in einem offenen oder aktiven Match und kannst keiner Queue beitreten.",
                ephemeral=True,
                delete_after=10,
            )
            return

        dartcounter_has_opponent = self.has_waiting_opponent(panel_state.dartcounter_queue, user_id)
        scolia_has_opponent = self.has_waiting_opponent(panel_state.scolia_queue, user_id)
        if dartcounter_has_opponent and scolia_has_opponent:
            await interaction.response.send_message(
                "Beides ist gerade nicht möglich, weil in beiden Queues schon jemand wartet.",
                ephemeral=True,
                delete_after=10,
            )
            return

        if user_id not in panel_state.dartcounter_queue:
            panel_state.dartcounter_queue.append(user_id)
        if user_id not in panel_state.scolia_queue:
            panel_state.scolia_queue.append(user_id)
        self.cog.schedule_queue_timeout(message.id, user_id, notify_interaction=interaction)

        if dartcounter_has_opponent:
            match_started = await self.cog.try_start_matches(message, panel_state, "DartCounter")
        elif scolia_has_opponent:
            match_started = await self.cog.try_start_matches(message, panel_state, "Scolia")
        else:
            match_started = False

        self.cog.persist_queue_state()
        await self.cog.refresh_panels(
            interaction=interaction,
            current_message_id=message.id,
            refresh_all=match_started,
        )

    @discord.ui.button(
        label="DartCounter",
        style=discord.ButtonStyle.success,
        custom_id="queue_panel:dartcounter_join",
        row=0,
    )
    async def dartcounter_join(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self.update_queue(interaction, queue_name="DartCounter", join=True)

    @discord.ui.button(
        label="Scolia",
        style=discord.ButtonStyle.primary,
        custom_id="queue_panel:scolia_join",
        row=0,
    )
    async def scolia_join(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self.update_queue(interaction, queue_name="Scolia", join=True)

    @discord.ui.button(
        label="Beides",
        style=discord.ButtonStyle.secondary,
        custom_id="queue_panel:both_join",
        row=0,
    )
    async def both_join(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self.join_both_queues(interaction)

    @discord.ui.button(
        label="Leave",
        style=discord.ButtonStyle.danger,
        custom_id="queue_panel:leave",
        row=0,
    )
    async def leave_queue(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self.update_queue(interaction, queue_name=None, join=False)


# =============================
# Ranked-Cog: verbindet UI, Match-Status, Discord-Threads und Ranked-DB-Helper.
# =============================
class Ranked(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.active_matches: dict[int, MatchState] = {}
        self.pending_matches: dict[int, PendingMatchState] = {}
        self.pending_results: dict[int, PendingResultState] = {}
        self.next_match_id = 1
        self.next_result_submission_id = 1
        self.panel_states: dict[int, PanelState] = {}
        self.panel_restore_attempted = False
        self.queue_timeout_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}
        self.pending_withdraw_tasks: dict[int, asyncio.Task[None]] = {}
        self.result_self_confirm_tasks: dict[int, asyncio.Task[None]] = {}

    async def cog_load(self) -> None:
        await ensure_ranked_storage(self.bot)
        await self.restore_pending_matches()
        await self.restore_active_matches()
        await self.restore_queue_state_from_file()
        self.bot.add_view(QueuePanel(self))
        self.bot.add_view(ResultEntryView(self))
        self.bot.add_view(PendingMatchView(self))
        self.bot.add_view(ResultConfirmationView(self))

    async def cog_unload(self) -> None:
        for task in self.queue_timeout_tasks.values():
            task.cancel()
        self.queue_timeout_tasks.clear()
        for task in self.pending_withdraw_tasks.values():
            task.cancel()
        self.pending_withdraw_tasks.clear()
        for task in self.result_self_confirm_tasks.values():
            task.cancel()
        self.result_self_confirm_tasks.clear()

    # In-Memory-Zustand fuer Panels, Queues und laufende Matches.
    def get_or_create_panel_state(self, message: discord.Message) -> PanelState:
        panel_state = self.panel_states.get(message.id)
        if panel_state is None:
            panel_state = panel_state_from_embed(message)
            self.panel_states[message.id] = panel_state
        return panel_state

    def persist_queue_state(self) -> None:
        data = {
            "panels": {
                str(message_id): panel_state_to_json(panel_state)
                for message_id, panel_state in self.panel_states.items()
                if panel_state.dartcounter_queue or panel_state.scolia_queue
            }
        }

        try:
            QUEUE_STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except OSError as exc:
            print(f"Queue state persistence failed: {type(exc).__name__}: {exc}")

    async def restore_queue_state_from_file(self) -> None:
        if not QUEUE_STATE_FILE.exists():
            return

        try:
            raw_data = json.loads(QUEUE_STATE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Queue state restore failed: {type(exc).__name__}: {exc}")
            return

        panels = raw_data.get("panels") if isinstance(raw_data, dict) else None
        if not isinstance(panels, dict):
            return

        now = datetime.now(timezone.utc)
        stale_message_ids: list[int] = []

        for message_id_text, panel_data in panels.items():
            try:
                message_id = int(message_id_text)
            except (TypeError, ValueError):
                continue

            panel_state = panel_state_from_json(panel_data)
            if panel_state is None:
                continue

            queued_user_ids = set(panel_state.dartcounter_queue) | set(panel_state.scolia_queue)
            active_user_ids: set[int] = set()
            for user_id in queued_user_ids:
                joined_at = panel_state.queue_joined_at.get(user_id, now)
                if now < joined_at + QUEUE_WAIT_TIMEOUT and not self.is_user_locked(user_id):
                    active_user_ids.add(user_id)

            panel_state.dartcounter_queue[:] = [
                user_id for user_id in panel_state.dartcounter_queue if user_id in active_user_ids
            ]
            panel_state.scolia_queue[:] = [
                user_id for user_id in panel_state.scolia_queue if user_id in active_user_ids
            ]
            panel_state.queue_joined_at = {
                user_id: joined_at
                for user_id, joined_at in panel_state.queue_joined_at.items()
                if user_id in active_user_ids
            }

            if not panel_state.dartcounter_queue and not panel_state.scolia_queue:
                stale_message_ids.append(message_id)
                continue

            self.panel_states[message_id] = panel_state
            for user_id in active_user_ids:
                self.schedule_queue_timeout(message_id, user_id, panel_state.queue_joined_at.get(user_id, now))

        if self.panel_states:
            await self.refresh_panels(refresh_all=True)

        if stale_message_ids or self.panel_states:
            self.persist_queue_state()

    def get_active_match_by_thread_id(self, thread_id: int) -> MatchState | None:
        for match in self.active_matches.values():
            if match.thread_id == thread_id:
                return match
        return None

    def get_active_match_by_id(self, match_id: int) -> MatchState | None:
        return self.active_matches.get(match_id)

    def get_pending_match_by_thread_id(self, thread_id: int) -> PendingMatchState | None:
        for match in self.pending_matches.values():
            if match.thread_id == thread_id:
                return match
        return None

    def get_match_by_thread_id(self, thread_id: int) -> PendingMatchState | MatchState | None:
        pending_match = self.get_pending_match_by_thread_id(thread_id)
        if pending_match is not None:
            return pending_match
        return self.get_active_match_by_thread_id(thread_id)

    def get_match_by_id(self, match_id: int) -> PendingMatchState | MatchState | None:
        pending_match = self.pending_matches.get(match_id)
        if pending_match is not None:
            return pending_match
        return self.active_matches.get(match_id)

    @staticmethod
    def parse_db_timestamp(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return datetime.now(timezone.utc)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def is_pending_match_withdraw_enabled(self, match: PendingMatchState) -> bool:
        return datetime.now(timezone.utc) >= match.created_at + WITHDRAW_ENABLE_DELAY

    @staticmethod
    def is_result_self_confirm_available(result: PendingResultState) -> bool:
        return datetime.now(timezone.utc) >= result.submitted_at + RESULT_SELF_CONFIRM_DELAY

    def is_database_available(self) -> bool:
        return getattr(self.bot, "db", None) is not None

    async def ensure_database_available(self, interaction: discord.Interaction) -> bool:
        if self.is_database_available():
            return True

        await interaction.response.send_message("Die Datenbank ist aktuell nicht verfuegbar.", ephemeral=True)
        return False

    @staticmethod
    def get_result_confirmer_id(match: MatchState, result: PendingResultState) -> int:
        player_one_id, player_two_id = match.player_ids
        return player_two_id if result.submitted_by == player_one_id else player_one_id

    def get_match_player_display_names(self, match: MatchState) -> dict[int, str]:
        display_names: dict[int, str] = {}
        for guild in self.bot.guilds:
            for user_id in match.player_ids:
                if user_id in display_names:
                    continue
                member = guild.get_member(user_id)
                if member is not None:
                    display_names[user_id] = member.display_name
        return display_names

    def build_pending_match_view(self, match: PendingMatchState) -> PendingMatchView:
        return PendingMatchView(
            self,
            match.match_id,
            withdraw_enabled=self.is_pending_match_withdraw_enabled(match),
        )

    async def cancel_pending_match(self, interaction: discord.Interaction, pending_match: PendingMatchState) -> None:
        if len(pending_match.confirmed_user_ids) == 0:
            await interaction.response.send_message(
                "Dieses Match kann erst vom bestätigenden Spieler abgebrochen werden.",
                ephemeral=True,
            )
            return

        if len(pending_match.confirmed_user_ids) > 1:
            await interaction.response.send_message(
                "Dieses Match wurde bereits von beiden Spielern bestätigt und kann hier nicht abgebrochen werden.",
                ephemeral=True,
            )
            return

        if interaction.user.id not in pending_match.confirmed_user_ids:
            await interaction.response.send_message(
                "Nur der Spieler, der dieses Match bestätigt hat, kann es abbrechen.",
                ephemeral=True,
            )
            return

        thread = await self.fetch_thread(pending_match.thread_id)
        if thread is None:
            await interaction.response.send_message(
                "Der Match-Thread konnte nicht gefunden werden. Match bleibt offen.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message("Match wird abgebrochen und nicht gewertet.", ephemeral=True)
        try:
            await thread.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            await interaction.followup.send(
                "Der Match-Thread konnte nicht gelöscht werden. Match bleibt offen.",
                ephemeral=True,
            )
            return

        results_channel = await self.fetch_results_channel()
        if results_channel is not None:
            try:
                await results_channel.send(embed=build_cancel_match_embed(pending_match.match_id))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                await self.send_admin_log(
                    "Cancel-Posting fehlgeschlagen",
                    f"{self.describe_match(pending_match)}\nDas Cancel-Embed konnte nicht in den Ergebnis-Channel gesendet werden.",
                    colour=discord.Color.red(),
                )

        self.pending_matches.pop(pending_match.match_id, None)
        self.cancel_pending_withdraw_activation(pending_match.match_id)
        await mark_ranked_match_cancelled(self.bot, pending_match.match_id)
        await self.log_pending_match_withdrawn(pending_match, interaction.user.id)

    async def cancel_match_as_admin(
        self,
        interaction: discord.Interaction,
        match: PendingMatchState | MatchState,
    ) -> None:
        thread = await self.fetch_thread(match.thread_id)
        if thread is None:
            await interaction.response.send_message(
                "Der Match-Thread konnte nicht gefunden werden. Match bleibt offen.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Match #{match.match_id:03d} wird abgebrochen und nicht gewertet.",
            ephemeral=True,
        )

        try:
            await thread.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            await interaction.followup.send(
                "Der Match-Thread konnte nicht gelöscht werden. Match bleibt offen.",
                ephemeral=True,
            )
            return

        results_channel = await self.fetch_results_channel()
        if results_channel is not None:
            try:
                await results_channel.send(embed=build_cancel_match_embed(match.match_id))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                await self.send_admin_log(
                    "Cancel-Posting fehlgeschlagen",
                    f"{self.describe_match(match)}\nDas Cancel-Embed konnte nicht in den Ergebnis-Channel gesendet werden.",
                    colour=discord.Color.red(),
                )

        self.pending_matches.pop(match.match_id, None)
        self.active_matches.pop(match.match_id, None)
        self.remove_pending_result(match.match_id)
        self.cancel_pending_withdraw_activation(match.match_id)
        await mark_ranked_match_cancelled(self.bot, match.match_id)
        await self.refresh_panels(refresh_all=True)
        await self.log_admin_match_cancelled(match, interaction.user.id)

    def cancel_pending_withdraw_activation(self, match_id: int) -> None:
        task = self.pending_withdraw_tasks.pop(match_id, None)
        if task is not None:
            task.cancel()

    def cancel_result_self_confirm_notification(self, match_id: int) -> None:
        task = self.result_self_confirm_tasks.pop(match_id, None)
        if task is not None:
            task.cancel()

    def schedule_pending_withdraw_activation(self, match: PendingMatchState) -> None:
        self.cancel_pending_withdraw_activation(match.match_id)
        delay_seconds = max(
            (match.created_at + WITHDRAW_ENABLE_DELAY - datetime.now(timezone.utc)).total_seconds(),
            0.0,
        )
        self.pending_withdraw_tasks[match.match_id] = asyncio.create_task(
            self.enable_pending_withdraw_button_after_delay(match.match_id, delay_seconds),
        )

    async def enable_pending_withdraw_button_after_delay(self, match_id: int, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            match = self.pending_matches.get(match_id)
            if match is None:
                return
            await self.repair_pending_match_view(match)
        except asyncio.CancelledError:
            pass
        finally:
            self.pending_withdraw_tasks.pop(match_id, None)

    def schedule_result_self_confirm_notification(self, result: PendingResultState) -> None:
        self.cancel_result_self_confirm_notification(result.match_id)
        delay_seconds = max(
            (result.submitted_at + RESULT_SELF_CONFIRM_DELAY - datetime.now(timezone.utc)).total_seconds(),
            0.0,
        )
        self.result_self_confirm_tasks[result.match_id] = asyncio.create_task(
            self.notify_result_self_confirm_available(result.match_id, result.submission_id, delay_seconds),
        )

    async def notify_result_self_confirm_available(
        self,
        match_id: int,
        submission_id: int,
        delay_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            result = self.pending_results.get(match_id)
            match = self.active_matches.get(match_id)
            if result is None or match is None or result.submission_id != submission_id:
                return

            await self.log_result_self_confirm_available(match, result)

            thread = await self.fetch_thread(result.thread_id)
            if thread is None:
                return

            try:
                await thread.send(
                    f"<@{result.submitted_by}>, <@{self.get_result_confirmer_id(match, result)}> hat noch nicht bestätigt."
                    "Du kannst dein Ergebnis jetzt selbst bestätigen."
                )
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        except asyncio.CancelledError:
            pass
        finally:
            current_task = asyncio.current_task()
            if self.result_self_confirm_tasks.get(match_id) is current_task:
                self.result_self_confirm_tasks.pop(match_id, None)

    async def recover_pending_match_from_message(
        self,
        message: discord.Message | None,
        *,
        expected_match_id: int | None = None,
    ) -> PendingMatchState | None:
        if message is None:
            return None
        if not isinstance(message.channel, discord.Thread):
            return None
        if not message.embeds:
            return None

        embed = message.embeds[0]
        title = embed.title or ""
        description = embed.description or ""

        match_id_match = MATCH_ID_PATTERN.search(title)
        if match_id_match is None:
            return None
        match_id = int(match_id_match.group(1))
        if expected_match_id is not None and match_id != expected_match_id:
            return None

        players = [int(user_id) for user_id in MENTION_PATTERN.findall(description)]
        unique_players: list[int] = []
        for player_id in players:
            if player_id not in unique_players:
                unique_players.append(player_id)
        if len(unique_players) != 2:
            return None

        queue_name = "Ranked"
        if " Match zwischen " in description:
            queue_name = description.split(" Match zwischen ", maxsplit=1)[0].strip() or "Ranked"

        confirmed_user_ids: set[int] = set()
        for field in embed.fields:
            if "best" in field.name.casefold():
                confirmed_user_ids = parse_user_ids_from_lines(field.value)
                break

        recovered_match = PendingMatchState(
            match_id=match_id,
            queue_name=queue_name,
            player_ids=(unique_players[0], unique_players[1]),
            thread_id=message.channel.id,
            confirmed_user_ids=confirmed_user_ids,
            created_at=message.created_at.astimezone(timezone.utc),
            pending_message_id=message.id,
        )

        self.pending_matches[match_id] = recovered_match
        self.next_match_id = max(self.next_match_id, match_id + 1)
        await persist_pending_ranked_match(self.bot, recovered_match)
        self.schedule_pending_withdraw_activation(recovered_match)
        return recovered_match

    @staticmethod
    def parse_confirmer_id_from_confirmation_message(message: discord.Message | None) -> int | None:
        if message is None or message.content is None:
            return None
        return parse_user_id_from_mention(message.content)

    @staticmethod
    def parse_pending_result_from_message(
        message: discord.Message | None,
        *,
        match: MatchState,
    ) -> PendingResultState | None:
        if message is None or not message.embeds:
            return None

        embed = message.embeds[0]
        winner_id: int | None = None
        score: tuple[int, int] | None = None
        submitted_by: int | None = None
        averages: dict[int, str] = {}
        player_one_id, player_two_id = match.player_ids
        average_player_ids = [player_one_id, player_two_id]

        for field in embed.fields:
            field_name = field.name.casefold()
            if field_name == "eingetragen von":
                submitted_by = parse_user_id_from_mention(field.value)
            elif field.name == "Gewinner":
                winner_id = parse_user_id_from_mention(field.value)
            elif field.name == "Spielstand":
                score = parse_best_of_seven_score(field.value)
            elif field_name.startswith("average "):
                normalized_average = normalize_average(field.value)
                if normalized_average is None:
                    continue

                mentioned_user_id = parse_user_id_from_mention(field.name)
                if mentioned_user_id in match.player_ids:
                    averages[mentioned_user_id] = normalized_average
                elif average_player_ids:
                    averages[average_player_ids.pop(0)] = normalized_average

        if winner_id is None or score is None:
            return None

        if len(averages) != 2:
            return None

        if submitted_by not in match.player_ids:
            confirmer_id = Ranked.parse_confirmer_id_from_confirmation_message(message)
            if confirmer_id == player_one_id:
                submitted_by = player_two_id
            elif confirmer_id == player_two_id:
                submitted_by = player_one_id

        if submitted_by not in match.player_ids:
            return None

        screenshot = message.attachments[0] if message.attachments else None
        screenshot_url = screenshot.url if screenshot is not None else None
        if screenshot_url is None and embed.image is not None:
            screenshot_url = embed.image.url

        submission_id = message.id
        return PendingResultState(
            submission_id=submission_id,
            match_id=match.match_id,
            winner_id=winner_id,
            score=score,
            score_text=f"{score[0]}:{score[1]}",
            averages=averages,
            submitted_by=submitted_by,
            thread_id=match.thread_id,
            submitted_at=message.created_at.astimezone(timezone.utc),
            screenshot=screenshot,
            screenshot_url=screenshot_url,
            confirmation_message_id=message.id,
        )

    def get_pending_result_for_confirmation(
        self,
        match_id: int,
        message: discord.Message | None,
    ) -> PendingResultState | None:
        result = self.pending_results.get(match_id)
        if result is not None:
            return result

        match = self.active_matches.get(match_id)
        if match is None:
            return None

        parsed_result = self.parse_pending_result_from_message(message, match=match)
        if parsed_result is None:
            return None

        self.pending_results[match_id] = parsed_result
        self.next_result_submission_id = max(self.next_result_submission_id, parsed_result.submission_id + 1)
        self.schedule_result_self_confirm_notification(parsed_result)
        return parsed_result

    def remove_pending_result(self, match_id: int) -> PendingResultState | None:
        self.cancel_result_self_confirm_notification(match_id)
        return self.pending_results.pop(match_id, None)

    async def restore_active_matches(self) -> None:
        restored_matches = await fetch_active_ranked_matches(self.bot)
        for restored_match in restored_matches:
            match = MatchState(
                match_id=restored_match["match_id"],
                queue_name=restored_match["queue_name"],
                player_ids=restored_match["player_ids"],
                thread_id=restored_match["thread_id"],
            )
            if await self.fetch_thread(match.thread_id) is None:
                continue
            self.active_matches[match.match_id] = match
            self.next_match_id = max(self.next_match_id, match.match_id + 1)

    async def restore_pending_matches(self) -> None:
        restored_matches = await fetch_pending_ranked_matches(self.bot)
        for restored_match in restored_matches:
            match = PendingMatchState(
                match_id=restored_match["match_id"],
                queue_name=restored_match["queue_name"],
                player_ids=restored_match["player_ids"],
                thread_id=restored_match["thread_id"],
                created_at=self.parse_db_timestamp(restored_match.get("created_at")),
            )
            if await self.fetch_thread(match.thread_id) is None:
                await mark_ranked_match_cancelled(self.bot, match.match_id)
                continue
            self.pending_matches[match.match_id] = match
            self.next_match_id = max(self.next_match_id, match.match_id + 1)
            await self.repair_pending_match_view(match)
            self.schedule_pending_withdraw_activation(match)

    async def repair_pending_match_view(self, match: PendingMatchState) -> None:
        thread = await self.fetch_thread(match.thread_id)
        if thread is None:
            return

        target_message: discord.Message | None = None
        title_token = f"#{match.match_id:03d}"
        if match.pending_message_id is not None:
            try:
                target_message = await thread.fetch_message(match.pending_message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                target_message = None

        if target_message is None:
            try:
                async for message in thread.history(limit=50):
                    bot_user = self.bot.user
                    if bot_user is None or message.author.id != bot_user.id:
                        continue
                    if not message.embeds:
                        continue

                    embed = message.embeds[0]
                    title = (embed.title or "").casefold()
                    if title_token in title and "best" in title:
                        target_message = message
                        break
            except (discord.Forbidden, discord.HTTPException):
                target_message = None

        if target_message is not None:
            for field in target_message.embeds[0].fields if target_message.embeds else []:
                if "best" in field.name.casefold():
                    match.confirmed_user_ids = parse_user_ids_from_lines(field.value)
                    break
            match.pending_message_id = target_message.id
            try:
                await target_message.edit(embed=build_pending_match_embed(match), view=self.build_pending_match_view(match))
                return
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        try:
            posted = await thread.send(embed=build_pending_match_embed(match), view=self.build_pending_match_view(match))
            match.pending_message_id = posted.id
        except (discord.Forbidden, discord.HTTPException):
            pass

    def is_user_locked(self, user_id: int) -> bool:
        if any(user_id in match.player_ids for match in self.active_matches.values()):
            return True
        return any(user_id in match.player_ids for match in self.pending_matches.values())

    def schedule_queue_timeout(
        self,
        message_id: int,
        user_id: int,
        joined_at: datetime | None = None,
        notify_interaction: discord.Interaction | None = None,
    ) -> None:
        panel_state = self.panel_states.get(message_id)
        if joined_at is None:
            joined_at = datetime.now(timezone.utc)
        if panel_state is not None:
            panel_state.queue_joined_at[user_id] = joined_at

        self.cancel_queue_timeout(message_id, user_id, forget_join_time=False)
        delay_seconds = max((joined_at + QUEUE_WAIT_TIMEOUT - datetime.now(timezone.utc)).total_seconds(), 0.0)
        self.queue_timeout_tasks[(message_id, user_id)] = asyncio.create_task(
            self.remove_from_queue_after_timeout(message_id, user_id, delay_seconds, notify_interaction),
        )

    def cancel_queue_timeout(self, message_id: int, user_id: int, *, forget_join_time: bool = True) -> None:
        task = self.queue_timeout_tasks.pop((message_id, user_id), None)
        if task is not None:
            task.cancel()
        if forget_join_time:
            panel_state = self.panel_states.get(message_id)
            if panel_state is not None:
                panel_state.queue_joined_at.pop(user_id, None)

    def cancel_queue_timeouts_for_players(self, player_ids: set[int]) -> None:
        for key in list(self.queue_timeout_tasks):
            _message_id, user_id = key
            if user_id in player_ids:
                task = self.queue_timeout_tasks.pop(key)
                task.cancel()
        for panel_state in self.panel_states.values():
            for user_id in player_ids:
                panel_state.queue_joined_at.pop(user_id, None)

    async def remove_from_queue_after_timeout(
        self,
        message_id: int,
        user_id: int,
        delay_seconds: float,
        notify_interaction: discord.Interaction | None = None,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            panel_state = self.panel_states.get(message_id)
            if panel_state is None:
                return

            was_removed = False
            for queue in (panel_state.dartcounter_queue, panel_state.scolia_queue):
                original_length = len(queue)
                queue[:] = [queued_user_id for queued_user_id in queue if queued_user_id != user_id]
                was_removed = was_removed or len(queue) != original_length

            if was_removed:
                panel_state.queue_joined_at.pop(user_id, None)
                self.persist_queue_state()
                await self.refresh_panels(refresh_all=True)
                if notify_interaction is not None:
                    try:
                        await notify_interaction.followup.send(
                            "Du bist aus der Queue, weil seit 5 Minuten kein Match gefunden wurde.",
                            ephemeral=True,
                            delete_after=60,
                        )
                    except discord.HTTPException:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            current_task = asyncio.current_task()
            if self.queue_timeout_tasks.get((message_id, user_id)) is current_task:
                self.queue_timeout_tasks.pop((message_id, user_id), None)

    def remove_players_from_all_queues(self, player_ids: tuple[int, int]) -> None:
        matched_players = set(player_ids)
        self.cancel_queue_timeouts_for_players(matched_players)

        for panel_state in self.panel_states.values():
            panel_state.dartcounter_queue[:] = [
                user_id for user_id in panel_state.dartcounter_queue if user_id not in matched_players
            ]
            panel_state.scolia_queue[:] = [
                user_id for user_id in panel_state.scolia_queue if user_id not in matched_players
            ]
        self.persist_queue_state()

    # Match-Erstellung und Statuswechsel von offen zu aktiv.
    async def create_pending_match(
        self,
        queue_message: discord.Message,
        *,
        queue_name: str,
        player_ids: tuple[int, int],
    ) -> bool:
        guild = queue_message.guild
        if guild is None:
            return False

        player_one = guild.get_member(player_ids[0])
        player_two = guild.get_member(player_ids[1])
        if player_one is None or player_two is None:
            return False

        match_id, self.next_match_id = await get_next_ranked_match_id(self.bot, self.next_match_id)

        thread_name = (
            f"match-{match_id:03d}-"
            f"{normalize_thread_part(player_one.display_name)}-"
            f"{normalize_thread_part(player_two.display_name)}"
        )[:100]

        if not isinstance(queue_message.channel, discord.TextChannel):
            return False

        try:
            thread = await queue_message.channel.create_thread(
                name=thread_name,
                type=discord.ChannelType.private_thread,
                auto_archive_duration=60,
                invitable=False,
            )
            await thread.add_user(player_one)
            await thread.add_user(player_two)
        except (discord.Forbidden, discord.HTTPException):
            try:
                if "thread" in locals():
                    await thread.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
            return False

        pending_match = PendingMatchState(
            match_id=match_id,
            queue_name=queue_name,
            player_ids=player_ids,
            thread_id=thread.id,
            created_at=datetime.now(timezone.utc),
        )
        self.pending_matches[match_id] = pending_match
        self.remove_players_from_all_queues(player_ids)
        await persist_pending_ranked_match(self.bot, pending_match)

        view = self.build_pending_match_view(pending_match)
        pending_message = await thread.send(
            embed=build_pending_match_embed(pending_match),
            view=view,
        )
        pending_match.pending_message_id = pending_message.id
        self.schedule_pending_withdraw_activation(pending_match)
        await self.log_match_created(pending_match)
        # await thread.send(
        #     content=(
        #         f"{player_one.mention} {player_two.mention}\n"
        #         "Bestätigt dieses Match oder zieht es zurueck."
        #     ),
        #     embed=build_pending_match_embed(pending_match),
        #     view=view,
        # )
        return True

    async def try_start_matches(
        self,
        queue_message: discord.Message,
        panel_state: PanelState,
        queue_name: str,
    ) -> bool:
        queue = panel_state.get_queue(queue_name)
        match_started = False

        while len(queue) >= 2:
            player_one = queue.pop(0)
            player_two = queue.pop(0)
            player_ids = (player_one, player_two)

            pending_created = await self.create_pending_match(
                queue_message,
                queue_name=queue_name,
                player_ids=player_ids,
            )
            if not pending_created:
                queue.insert(0, player_two)
                queue.insert(0, player_one)
                break

            match_started = True

        return match_started

    async def confirm_pending_match(self, match_id: int) -> MatchState | None:
        pending_match = self.pending_matches.pop(match_id, None)
        if pending_match is None:
            return None
        self.cancel_pending_withdraw_activation(match_id)

        active_match = MatchState(
            match_id=pending_match.match_id,
            queue_name=pending_match.queue_name,
            player_ids=pending_match.player_ids,
            thread_id=pending_match.thread_id,
        )
        self.active_matches[match_id] = active_match
        await persist_active_ranked_match(self.bot, active_match)
        await self.log_match_activated(active_match)
        return active_match

    def build_embed_for_panel(self, message_id: int) -> discord.Embed:
        panel_state = self.panel_states[message_id]
        active_matches = sorted(self.active_matches.values(), key=lambda match: match.match_id)
        return build_queue_embed(panel_state, active_matches)

    # Discord-Objekte sicher nachladen, ohne bei fehlenden Rechten abzubrechen.
    async def fetch_panel_message(self, channel_id: int, message_id: int) -> discord.Message | None:
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return None

        if not isinstance(channel, discord.TextChannel):
            return None

        try:
            return await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    async def restore_queue_panels_from_messages(self) -> None:
        if self.panel_restore_attempted:
            return
        self.panel_restore_attempted = True

        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                try:
                    async for message in channel.history(limit=200):
                        if is_queue_panel_message(message):
                            self.panel_states[message.id] = panel_state_from_embed(message)
                except (discord.Forbidden, discord.HTTPException):
                    continue

    async def fetch_thread(self, thread_id: int) -> discord.Thread | None:
        thread = self.bot.get_channel(thread_id)
        if isinstance(thread, discord.Thread):
            return thread

        try:
            fetched = await self.bot.fetch_channel(thread_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

        if isinstance(fetched, discord.Thread):
            return fetched
        return None

    async def fetch_results_channel(self) -> discord.TextChannel | None:
        channel = self.bot.get_channel(RESULT_CHANNEL)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched = await self.bot.fetch_channel(RESULT_CHANNEL)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

        if isinstance(fetched, discord.TextChannel):
            return fetched
        return None

    async def fetch_admin_log_channel(self) -> discord.abc.Messageable | None:
        channel = self.bot.get_channel(ADMIN_LOG_CHANNEL)
        if isinstance(channel, discord.abc.Messageable):
            return channel

        try:
            fetched = await self.bot.fetch_channel(ADMIN_LOG_CHANNEL)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

        if isinstance(fetched, discord.abc.Messageable):
            return fetched
        return None

    async def send_admin_log(
        self,
        title: str,
        description: str,
        *,
        colour: discord.Color = discord.Color.blurple(),
    ) -> None:
        channel = await self.fetch_admin_log_channel()
        if channel is None:
            return

        embed = discord.Embed(
            title=title,
            description=fit_embed_description(description),
            colour=colour,
            timestamp=datetime.now(timezone.utc),
        )

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            pass

    @staticmethod
    def describe_match(match: PendingMatchState | MatchState) -> str:
        return (
            f"Match: `#{match.match_id:03d}`\n"
            f"Queue: `{match.queue_name}`\n"
            f"Spieler: <@{match.player_ids[0]}> vs <@{match.player_ids[1]}>\n"
            f"Thread: <#{match.thread_id}>"
        )

    async def log_match_created(self, match: PendingMatchState) -> None:
        await self.send_admin_log("Match erstellt", self.describe_match(match), colour=discord.Color.gold())

    async def log_match_player_confirmed(self, match: PendingMatchState, user_id: int) -> None:
        await self.send_admin_log(
            "Match bestätigt",
            f"{self.describe_match(match)}\nBestätigt von: <@{user_id}>",
            colour=discord.Color.gold(),
        )

    async def log_match_activated(self, match: MatchState) -> None:
        await self.send_admin_log("Match aktiv", self.describe_match(match), colour=discord.Color.green())

    async def log_pending_match_withdrawn(self, match: PendingMatchState, user_id: int) -> None:
        await self.send_admin_log(
            "Pending Match zurückgezogen",
            f"{self.describe_match(match)}\nZurückgezogen von: <@{user_id}>",
            colour=discord.Color.red(),
        )

    async def log_admin_match_cancelled(self, match: PendingMatchState | MatchState, user_id: int) -> None:
        await self.send_admin_log(
            "Match von Admin abgebrochen",
            f"{self.describe_match(match)}\nAdmin: <@{user_id}>",
            colour=discord.Color.red(),
        )

    async def log_result_submitted(self, match: MatchState, result: PendingResultState) -> None:
        confirmer_id = self.get_result_confirmer_id(match, result)
        await self.send_admin_log(
            "Ergebnis eingetragen",
            (
                f"{self.describe_match(match)}\n"
                f"Eingetragen von: <@{result.submitted_by}>\n"
                f"Wartet auf: <@{confirmer_id}>\n"
                f"Gewinner: <@{result.winner_id}>\n"
                f"Spielstand: `{result.score_text}`"
            ),
            colour=discord.Color.blurple(),
        )

    async def log_result_confirmed(self, match: MatchState, result: PendingResultState, user_id: int) -> None:
        self_confirmed = user_id == result.submitted_by
        await self.send_admin_log(
            "Ergebnis bestätigt",
            (
                f"{self.describe_match(match)}\n"
                f"Bestätigt von: <@{user_id}>\n"
                f"Selbstbestätigung: `{'ja' if self_confirmed else 'nein'}`\n"
                f"Gewinner: <@{result.winner_id}>\n"
                f"Spielstand: `{result.score_text}`"
            ),
            colour=discord.Color.green(),
        )

    async def log_result_disputed(self, match: MatchState, result: PendingResultState, user_id: int) -> None:
        await self.send_admin_log(
            "Ergebnis widersprochen",
            (
                f"{self.describe_match(match)}\n"
                f"Widersprochen von: <@{user_id}>\n"
                f"Eingetragen von: <@{result.submitted_by}>\n"
                f"Gewinner im Vorschlag: <@{result.winner_id}>\n"
                f"Spielstand im Vorschlag: `{result.score_text}`"
            ),
            colour=discord.Color.orange(),
        )

    async def log_result_self_confirm_available(self, match: MatchState, result: PendingResultState) -> None:
        await self.send_admin_log(
            "Selbstbestätigung freigeschaltet",
            (
                f"{self.describe_match(match)}\n"
                f"Einreicher: <@{result.submitted_by}>\n"
                f"Ursprünglich wartend auf: <@{self.get_result_confirmer_id(match, result)}>"
            ),
            colour=discord.Color.orange(),
        )

    async def log_result_publish_failed(self, match: MatchState, result: PendingResultState) -> None:
        await self.send_admin_log(
            "Ergebnis-Posting fehlgeschlagen",
            (
                f"{self.describe_match(match)}\n"
                f"Ergebnis wurde gespeichert, konnte aber nicht in den Ergebnis-Channel gesendet werden.\n"
                f"Gewinner: <@{result.winner_id}>\n"
                f"Spielstand: `{result.score_text}`"
            ),
            colour=discord.Color.red(),
        )

    async def log_result_persist_failed(self, match: MatchState, result: PendingResultState) -> None:
        await self.send_admin_log(
            "Ergebnis-Speicherung fehlgeschlagen",
            (
                f"{self.describe_match(match)}\n"
                f"Bestätigung konnte nicht in der Datenbank gespeichert werden.\n"
                f"Gewinner: <@{result.winner_id}>\n"
                f"Spielstand: `{result.score_text}`"
            ),
            colour=discord.Color.red(),
        )

    async def send_result_message(
        self,
        channel: discord.abc.Messageable,
        match: MatchState,
        result: PendingResultState,
        *,
        content: str | None = None,
        view: discord.ui.View | None = None,
    ) -> discord.Message:
        embed = build_result_embed(match, result, self.get_match_player_display_names(match))
        if result.screenshot is None:
            return await channel.send(content=content, embed=embed, view=view)

        try:
            file = await result.screenshot.to_file()
        except discord.HTTPException:
            if result.screenshot_url is not None:
                embed.set_image(url=result.screenshot_url)
                return await channel.send(content=content, embed=embed, view=view)
            raise

        embed.set_image(url=f"attachment://{file.filename}")
        message = await channel.send(content=content, embed=embed, file=file, view=view)
        if message.attachments:
            result.screenshot_url = message.attachments[0].url
        return message

    async def post_result_entry_button(self, match: MatchState) -> bool:
        thread = await self.fetch_thread(match.thread_id)
        if thread is None:
            return False

        try:
            await thread.send(
                "Das Ergebnis wurde widersprochen. Bitte tragt das Ergebnis erneut ein.",
                view=ResultEntryView(self, match.match_id),
            )
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return False

        return True

    # Ergebnisvorschläge und Panel-Aktualisierung nach Interaktionen.
    async def mark_result_submission_obsolete(self, result: PendingResultState) -> None:
        if result.confirmation_message_id is None:
            return

        thread = await self.fetch_thread(result.thread_id)
        if thread is None:
            return

        try:
            message = await thread.fetch_message(result.confirmation_message_id)
            await message.edit(content="Veralteter Ergebnisvorschlag.", view=None)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    async def refresh_panels(
        self,
        *,
        interaction: discord.Interaction | None = None,
        current_message_id: int | None = None,
        refresh_all: bool = True,
    ) -> None:
        stale_message_ids: list[int] = []

        if current_message_id is not None and current_message_id in self.panel_states and interaction is not None:
            await interaction.response.edit_message(
                embed=self.build_embed_for_panel(current_message_id),
                view=QueuePanel(self, self.panel_states[current_message_id]),
            )
        elif interaction is not None and not interaction.response.is_done():
            await interaction.response.send_message("Queue aktualisiert.", ephemeral=True)

        if not refresh_all:
            return

        if not self.panel_states:
            await self.restore_queue_panels_from_messages()

        for message_id, panel_state in self.panel_states.items():
            if message_id == current_message_id:
                continue

            message = await self.fetch_panel_message(panel_state.channel_id, message_id)
            if message is None:
                stale_message_ids.append(message_id)
                continue

            try:
                await message.edit(embed=self.build_embed_for_panel(message_id), view=QueuePanel(self, panel_state))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                stale_message_ids.append(message_id)

        for message_id in stale_message_ids:
            self.panel_states.pop(message_id, None)
        if stale_message_ids:
            self.persist_queue_state()

    async def open_result_modal(
        self,
        interaction: discord.Interaction,
        *,
        match_id: int | None = None,
        entry_message_id: int | None = None,
        submitted_by_member: discord.Member | None = None,
    ) -> None:
        if interaction.guild is None or not isinstance(interaction.channel, discord.Thread):
            await interaction.response.send_message(
                "Dieser Befehl funktioniert nur in einem aktiven Match-Thread.",
                ephemeral=True,
            )
            return

        match = self.get_active_match_by_id(match_id) if match_id is not None else self.get_active_match_by_thread_id(interaction.channel.id)
        if match is None or match.thread_id != interaction.channel.id:
            await interaction.response.send_message(
                "In diesem Thread wurde kein aktives Match gefunden.",
                ephemeral=True,
            )
            return

        is_admin = interaction_user_is_admin(interaction)
        submitted_by_id = submitted_by_member.id if submitted_by_member is not None else interaction.user.id

        if interaction.user.id not in match.player_ids and not is_admin:
            await interaction.response.send_message(
                "Nur die beiden Match-Spieler dürfen das Ergebnis eintragen.",
                ephemeral=True,
            )
            return

        if submitted_by_member is not None and not is_admin:
            await interaction.response.send_message(
                "Nur Admins duerfen das Ergebnis fuer einen anderen Spieler eintragen.",
                ephemeral=True,
            )
            return

        if submitted_by_id not in match.player_ids:
            await interaction.response.send_message(
                "Bitte gib einen der beiden Match-Spieler an, fuer den das Ergebnis eingetragen wird.",
                ephemeral=True,
            )
            return

        try:
            await interaction.response.send_modal(
                ResultModal(
                    self,
                    match,
                    interaction.guild,
                    entry_message_id,
                    submitted_by_id=submitted_by_id,
                )
            )
        except Exception as exc:
            print(f"Result modal failed: {type(exc).__name__}: {exc}")
            if entry_message_id is not None:
                thread = await self.fetch_thread(match.thread_id)
                if thread is not None:
                    try:
                        message = await thread.fetch_message(entry_message_id)
                        await message.edit(view=ResultEntryView(self, match.match_id))
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        pass

            if interaction.response.is_done():
                await interaction.followup.send("Das Ergebnisformular konnte nicht geöffnet werden.", ephemeral=True)
            else:
                await interaction.response.send_message("Das Ergebnisformular konnte nicht geöffnet werden.", ephemeral=True)

    # Slash-Commands fuer Admins.
    @app_commands.command(name="queue_panel", description="Sendet das Queue-Panel in den Chat")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def queue_panel(self, interaction: discord.Interaction) -> None:
        panel_state = PanelState(channel_id=interaction.channel_id)
        embed = build_queue_embed(panel_state, sorted(self.active_matches.values(), key=lambda match: match.match_id))
        view = QueuePanel(self, panel_state)

        await interaction.response.send_message(embed=embed, view=view)
        message = await interaction.original_response()
        self.panel_states[message.id] = panel_state

    @app_commands.command(name="matches", description="Zeigt aktive und pending Matches getrennt an")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def matches(self, interaction: discord.Interaction) -> None:
        active_matches = sorted(self.active_matches.values(), key=lambda match: match.match_id)
        pending_matches = sorted(self.pending_matches.values(), key=lambda match: match.match_id)
        embeds = [
            build_admin_matches_embed(
                title="Aktive Matches",
                description=format_admin_active_matches(active_matches),
                colour=discord.Color.green(),
            ),
            build_admin_matches_embed(
                title="Pending Matches",
                description=format_admin_pending_matches(pending_matches),
                colour=discord.Color.gold(),
            ),
        ]
        await interaction.response.send_message(embeds=embeds, ephemeral=True)

    @app_commands.command(name="cancel_match",description="Bricht ein Match als Admin ab",)
    @app_commands.describe(match_id="Match-ID, wenn der Command nicht im Match-Thread ausgeführt wird")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def cancel_match(self, interaction: discord.Interaction, match_id: int | None = None) -> None:
        match: PendingMatchState | MatchState | None = None

        if match_id is not None:
            match = self.get_match_by_id(match_id)
        elif isinstance(interaction.channel, discord.Thread):
            match = self.get_match_by_thread_id(interaction.channel.id)
        else:
            await interaction.response.send_message(
                "Bitte gib eine Match-ID an oder führe den Command direkt im Match-Thread aus.",
                ephemeral=True,
            )
            return

        if match is None:
            await interaction.response.send_message(
                "Es wurde kein aktives oder pending Match gefunden.",
                ephemeral=True,
            )
            return

        await self.cancel_match_as_admin(interaction, match)

    @app_commands.command(name="world_ranking", description="Zeigt das aktuelle World Ranking")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def world_ranking(self, interaction: discord.Interaction) -> None:
        if not await self.ensure_database_available(interaction):
            return

        rows = await fetch_world_ranking(self.bot)
        embed = build_ranking_embed(
            title="World Ranking",
            rows=rows,
            empty_text="Noch keine Ranked-Ergebnisse vorhanden.",
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="monthly_ranking", description="Zeigt das aktuelle Monatsranking")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def monthly_ranking(self, interaction: discord.Interaction) -> None:
        if not await self.ensure_database_available(interaction):
            return

        rows = await fetch_monthly_ranking(self.bot)
        embed = build_ranking_embed(
            title="Monatsranking",
            rows=rows,
            empty_text="Für diesen Monat gibt es noch keine Ranked-Ergebnisse.",
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(
        name="monthly_ranking_monat",
        description="Generiert das Monatsranking fuer einen angegebenen Monat",
    )
    @app_commands.describe(
        monat="Monat als Zahl von 1 bis 12",
        jahr="Jahr, z.B. 2026",
    )
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def monthly_ranking_monat(
        self,
        interaction: discord.Interaction,
        monat: app_commands.Range[int, 1, 12],
        jahr: app_commands.Range[int, 2000, 2100],
    ) -> None:
        if not await self.ensure_database_available(interaction):
            return

        month_key = datetime(int(jahr), int(monat), 1, tzinfo=timezone.utc).date()
        month_label = f"{int(monat):02d}/{int(jahr)}"
        started_at = time.perf_counter()
        print(f"[monthly_ranking_command] start month={month_label} user={interaction.user.id}")
        await interaction.response.defer(thinking=True)

        print(f"[monthly_ranking_command] fetching rows month={month_label} limit=10")
        rows = await generate_monthly_ranking(self.bot, month_key, limit=10)
        print(f"[monthly_ranking_command] rows fetched month={month_label} players={len(rows)}")

        print(f"[monthly_ranking_command] building embeds month={month_label}")
        embeds = build_monthly_ranking_embeds(
            title=f"Monatsranking {month_label} - Top 10",
            rows=rows,
            empty_text="Fuer diesen Monat gibt es keine Ranked-Ergebnisse.",
        )
        print(f"[monthly_ranking_command] embeds built month={month_label} embeds={len(embeds)}")

        print(f"[monthly_ranking_command] sending embed month={month_label}")
        await interaction.followup.send(embed=embeds[0])

        duration = time.perf_counter() - started_at
        print(f"[monthly_ranking_command] done month={month_label} duration={duration:.3f}s")

    @app_commands.command(name="rebuild_monthly_ranking", description="Berechnet das aktuelle Monatsranking neu")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def rebuild_monthly_ranking(self, interaction: discord.Interaction) -> None:
        if not await self.ensure_database_available(interaction):
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        rebuilt_players = await rebuild_current_month_rankings(self.bot)
        await generate_html(self.bot)
        upload()
        await interaction.followup.send(
            f"Monatsranking wurde neu berechnet. Spieler im aktuellen Monat: {rebuilt_players}",
            ephemeral=True,
        )

    @app_commands.command(name="update_leaderboard", description="Aktualisiert das HTML-Ranking")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def update_leaderboard(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message("Leaderboard wird aktualisiert...", ephemeral=True)
        await generate_html(self.bot)
        upload()
        await interaction.edit_original_response(
            content="Aktualisierung ist fertig. https://stefankulik.github.io/discord-bot/"
        )

    # Slash-Commands fuer User.
    @app_commands.command(name="result", description="Ã–ffnet im Match-Thread das Ergebnisformular")
    @app_commands.describe(spieler="Nur fuer Admins: Match-Spieler, fuer den das Ergebnis eingetragen wird")
    @app_commands.guild_only()
    async def result(self, interaction: discord.Interaction, spieler: discord.Member | None = None) -> None:
        await self.open_result_modal(interaction, submitted_by_member=spieler)

    @app_commands.command(name="stats", description="Zeigt die Ranked-Stats eines Spielers")
    @app_commands.describe(player="Der Spieler dessen Statistiken angezeigt werden sollen")
    @app_commands.guild_only()
    async def stats(self, interaction: discord.Interaction, player: discord.Member) -> None:
        if not await self.ensure_database_available(interaction):
            return

        world_data = await fetch_world_ranking(self.bot, limit=None)
        monthly_data = await fetch_monthly_ranking(self.bot, get_current_ranked_month_key(), limit=None)

        world_ranks = {
            user_id: rank
            for rank, (user_id, _rating, _wins, _losses) in enumerate(world_data, start=1)
        }
        monthly_ranks = {
            user_id: rank
            for rank, (user_id, _rating, _wins, _losses) in enumerate(monthly_data, start=1)
        }
        world_stats = {
            user_id: (rating, wins, losses)
            for user_id, rating, wins, losses in world_data
        }

        rating, wins, losses = world_stats.get(player.id, (1000, 0, 0))
        total = wins + losses
        winrate = round((wins / total) * 100, 1) if total else 0

        embed = build_stats_embed(
            player=player,
            world_rank=world_ranks.get(player.id),
            monthly_rank=monthly_ranks.get(player.id),
            rating=rating,
            total=total,
            winrate=winrate,
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="history", description="Match History des Users")
    @app_commands.describe(player="Der Spieler dessen Match History angezeigt werden soll")
    @app_commands.guild_only()
    async def history(self, interaction: discord.Interaction, player: discord.Member):

        matches = await fetch_match_history(self.bot, player)

        if not matches:
            await interaction.response.send_message("Keine Matches gefunden.")
            return

        text = f"ðŸ“œ Match History von {player.display_name}:\n\n"

        for p1, p2, winner, score, platform, elo_gain in matches:

            opponent_id = p2 if player.id == p1 else p1
            opponent = interaction.guild.get_member(opponent_id)

            name = opponent.display_name if opponent else f"User {opponent_id}"

            elo_gain = elo_gain if elo_gain else 0

            if winner == player.id:
                result = "ðŸ† Win"
                elo_text = f"+{elo_gain}"
            else:
                result = "âŒ Loss"
                elo_text = f"-{elo_gain}"

            text += f"{result} vs {name} ({platform}) ({elo_text} ELO)\n"
            text += f"Score: {score}\n\n"

        await interaction.response.send_message(text)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Ranked(bot))

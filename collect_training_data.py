"""
ydkball — Training Data Collector
========================================
Fetches game IDs for one or more seasons, runs the possession pipeline
on each game, and saves results as JSONL files for model training.

Usage:
    python collect_training_data.py --seasons 2022-23 2023-24 --output data/possessions/

Each line of the output JSONL is one possession (via possession_to_dict).
Failed games are logged and skipped — resume is supported via a progress file.
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from nba_api.stats.endpoints import leaguegamelog
from possession_pipeline import build_possessions, possession_to_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Season → game ID fetching
# ---------------------------------------------------------------------------

SEASON_TYPE = "Regular Season"   # change to "Playoffs" for postseason


def fetch_game_ids(season: str) -> list[str]:
    """
    Return all unique game IDs for a season string like "2023-24".
    Uses LeagueGameLog which returns one row per team per game,
    so we deduplicate.
    """
    log.info(f"Fetching game IDs for {season}...")
    df = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=SEASON_TYPE,
        league_id="00",
    ).get_data_frames()[0]
    time.sleep(0.6)

    game_ids = df["GAME_ID"].unique().tolist()
    log.info(f"  Found {len(game_ids)} games for {season}")
    return game_ids


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress(progress_file: Path) -> set[str]:
    """Return set of game_ids already processed."""
    if not progress_file.exists():
        return set()
    with open(progress_file) as f:
        return set(line.strip() for line in f if line.strip())


def mark_done(progress_file: Path, game_id: str):
    with open(progress_file, "a") as f:
        f.write(game_id + "\n")


# ---------------------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------------------

def collect(seasons: list[str], output_dir: str, delay: float = 0.8):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    progress_file = out / "progress.txt"
    failed_file = out / "failed.txt"
    done = load_progress(progress_file)
    log.info(f"Already processed: {len(done)} games")

    for season in seasons:
        game_ids = fetch_game_ids(season)
        season_file = out / f"{season.replace('-', '_')}.jsonl"

        with open(season_file, "a") as out_f:
            for game_id in game_ids:
                if game_id in done:
                    continue

                try:
                    possessions = build_possessions(game_id, delay=delay)

                    for p in possessions:
                        out_f.write(json.dumps(possession_to_dict(p)) + "\n")

                    out_f.flush()
                    mark_done(progress_file, game_id)
                    log.info(f"  ✓ {game_id} — {len(possessions)} possessions")

                except Exception as e:
                    log.warning(f"  ✗ {game_id} failed: {e}")
                    with open(failed_file, "a") as ff:
                        ff.write(f"{game_id}\t{e}\n")

                time.sleep(delay)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect possession training data")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=["2023-24"],
        help='Season strings e.g. "2022-23 2023-24"',
    )
    parser.add_argument(
        "--output",
        default="data/possessions",
        help="Output directory for JSONL files",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.8,
        help="Seconds between API calls",
    )
    args = parser.parse_args()
    collect(args.seasons, args.output, args.delay)
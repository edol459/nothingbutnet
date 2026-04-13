"""
ydkball — Expected Possession Value (EPV) Model
================================================
Trains a gradient-boosting classifier on historical possession data to
predict the expected points scored on any possession given the game state
at possession start:

    features: period, clock_seconds, score_margin_offense, game_time_fraction
    target:   points_scored  (0 | 1 | 2 | 3)

After training the model is:
  1. Saved to backend/ingest/epv_model.pkl
  2. Used to backfill possessions.expected_points in the DB

Usage:
    python backend/ingest/train_ev_model.py
    python backend/ingest/train_ev_model.py --seasons 2023-24 2024-25
    python backend/ingest/train_ev_model.py --backfill-only   # skip re-training
"""

import argparse
import logging
import os
import pickle
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss
from sklearn.preprocessing import label_binarize

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH = Path(__file__).parent / "epv_model.pkl"

# Max absolute score margin we clip to — prevents extreme outlier states
MARGIN_CLIP = 40


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


# ── Feature engineering ───────────────────────────────────────────────────────

def build_features(rows) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert possession DB rows into feature matrix X and label vector y.

    Features (all continuous after encoding):
      0  period_norm          period / 5  (handles OT gracefully)
      1  clock_fraction       clock_seconds / period_length  (0=end, 1=fresh)
      2  game_time_fraction   game_seconds_start / 2880  (0=start, 1=end of reg)
      3  margin_clipped       score_margin clipped to [-MARGIN_CLIP, +MARGIN_CLIP], normalised
      4  is_ot                1 if period > 4
      5  under_two            1 if clock_seconds <= 120 (last 2 min of period)
      6  under_thirty         1 if clock_seconds <= 30
    """
    X, y = [], []
    for r in rows:
        period = int(r["period"])
        clock  = float(r["start_clock_seconds"])
        gtime  = float(r["game_seconds_start"])
        margin = int(r["score_margin_offense"])
        pts    = int(r["points_scored"])

        period_length = 300.0 if period > 4 else 720.0
        game_total    = 4 * 720.0  # regulation reference (OT pushes > 1.0, that's fine)

        X.append([
            period / 5.0,
            clock / period_length,
            min(gtime / game_total, 1.5),          # allow OT to exceed 1.0
            np.clip(margin, -MARGIN_CLIP, MARGIN_CLIP) / MARGIN_CLIP,
            float(period > 4),
            float(clock <= 120),
            float(clock <= 30),
        ])
        # Note: the possession pipeline splits and-1 plays into two records —
        # the made FG (points_scored = 2 or 3) and the free throw sequence
        # (points_scored = 0 or 1) — so no single row will ever have pts > 3.
        # The cap below is purely defensive against unexpected data anomalies.
        y.append(min(pts, 4))

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.int8)


# ── Model training ────────────────────────────────────────────────────────────

def train(seasons: list[str]) -> GradientBoostingClassifier:
    log.info(f"Loading possession data for seasons: {seasons}")

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT period, start_clock_seconds, game_seconds_start,
               score_margin_offense, points_scored
        FROM possessions
        WHERE season = ANY(%s)
          AND end_reason NOT IN ('end_game', 'fouled')
          AND NOT (
            ABS(score_margin_offense) > 15
            AND game_seconds_start > 3 * 720
          )
    """, (seasons,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    log.info(f"  Loaded {len(rows):,} possessions")
    if len(rows) < 5000:
        raise RuntimeError(
            f"Only {len(rows)} possessions available — run collect_to_db.py first "
            "to populate possession data before training."
        )

    X, y = build_features(rows)
    log.info(f"  Class distribution: {dict(zip(*np.unique(y, return_counts=True)))}")

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.1, random_state=42, stratify=y
    )

    log.info("Training GradientBoostingClassifier...")
    clf = GradientBoostingClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=4,
        subsample=0.8,
        min_samples_leaf=50,
        random_state=42,
    )
    clf.fit(X_train, y_train)

    # Evaluate
    classes = clf.classes_
    proba_val = clf.predict_proba(X_val)
    ll = log_loss(y_val, proba_val, labels=classes)

    # Compute expected value = sum(class * P(class))
    ev_pred  = proba_val @ classes.astype(float)
    ev_actual = y_val.astype(float)
    rmse = np.sqrt(np.mean((ev_pred - ev_actual) ** 2))
    bias = np.mean(ev_pred) - np.mean(ev_actual)

    log.info(f"  Validation log-loss: {ll:.4f}")
    log.info(f"  EV RMSE:  {rmse:.4f} pts/possession")
    log.info(f"  EV bias:  {bias:+.4f} (ideal = 0)")
    log.info(f"  Mean predicted EV: {np.mean(ev_pred):.4f}")
    log.info(f"  Mean actual EV:    {np.mean(ev_actual):.4f}")

    # Save model
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": clf, "classes": classes}, f)
    log.info(f"Model saved to {MODEL_PATH}")

    return clf, classes


# ── Load model ────────────────────────────────────────────────────────────────

def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. "
            "Run train_ev_model.py first."
        )
    with open(MODEL_PATH, "rb") as f:
        payload = pickle.load(f)
    return payload["model"], payload["classes"]


# ── Predict expected value for a single possession state ─────────────────────

def predict_ev(model, classes, period: int, clock_seconds: float,
               game_seconds: float, score_margin: int) -> float:
    """Return expected points for a single possession state."""
    period_length = 300.0 if period > 4 else 720.0
    game_total    = 4 * 720.0
    X = np.array([[
        period / 5.0,
        clock_seconds / period_length,
        min(game_seconds / game_total, 1.5),
        np.clip(score_margin, -MARGIN_CLIP, MARGIN_CLIP) / MARGIN_CLIP,
        float(period > 4),
        float(clock_seconds <= 120),
        float(clock_seconds <= 30),
    ]], dtype=np.float32)
    proba = model.predict_proba(X)[0]
    return float(proba @ classes.astype(float))


# ── Backfill DB ───────────────────────────────────────────────────────────────

def backfill(model, classes, seasons: list[str], batch_size: int = 5000):
    """
    Reset expected_points to NULL for all possessions in the given seasons,
    then backfill non-garbage-time possessions with fresh model predictions.
    Garbage-time possessions (margin > 15 past Q3) are left NULL and therefore
    excluded from PVA attribution.
    """
    log.info("Starting DB backfill of expected_points...")

    conn = get_conn()
    cur  = conn.cursor()

    # Reset all existing values so the garbage-time filter takes effect cleanly
    cur.execute(
        "UPDATE possessions SET expected_points = NULL WHERE season = ANY(%s)",
        (seasons,)
    )
    conn.commit()
    log.info("  Reset expected_points to NULL for all possessions in specified seasons")

    cur.execute("""
        SELECT id, period, start_clock_seconds, game_seconds_start, score_margin_offense
        FROM possessions
        WHERE expected_points IS NULL
          AND season = ANY(%s)
          AND NOT (
            ABS(score_margin_offense) > 15
            AND game_seconds_start > 3 * 720
          )
        ORDER BY id
    """, (seasons,))
    rows = cur.fetchall()
    log.info(f"  {len(rows):,} non-garbage possessions to backfill")

    if not rows:
        log.info("  Nothing to backfill.")
        cur.close(); conn.close()
        return

    # Build feature matrix for all rows at once (fast batch predict)
    period_lengths = np.where(
        np.array([r["period"] for r in rows]) > 4, 300.0, 720.0
    )
    game_total = 4 * 720.0
    periods    = np.array([r["period"] for r in rows], dtype=np.float32)
    clocks     = np.array([r["start_clock_seconds"] for r in rows], dtype=np.float32)
    gtimes     = np.array([r["game_seconds_start"] for r in rows], dtype=np.float32)
    margins    = np.clip(
        np.array([r["score_margin_offense"] for r in rows], dtype=np.float32),
        -MARGIN_CLIP, MARGIN_CLIP
    ) / MARGIN_CLIP

    X = np.column_stack([
        periods / 5.0,
        clocks / period_lengths,
        np.minimum(gtimes / game_total, 1.5),
        margins,
        (periods > 4).astype(np.float32),
        (clocks <= 120).astype(np.float32),
        (clocks <= 30).astype(np.float32),
    ])

    log.info(f"  Running batch prediction on {len(rows):,} possessions...")
    proba_all = model.predict_proba(X)
    ev_all    = proba_all @ classes.astype(float)

    # Write in batches
    cur2 = conn.cursor()
    for i in range(0, len(rows), batch_size):
        batch_rows  = rows[i : i + batch_size]
        batch_ev    = ev_all[i : i + batch_size]
        update_data = [(float(ev), int(row["id"])) for ev, row in zip(batch_ev, batch_rows)]
        psycopg2.extras.execute_values(
            cur2,
            "UPDATE possessions SET expected_points = data.ev FROM (VALUES %s) AS data(ev, id) WHERE possessions.id = data.id",
            update_data,
            template="(%s::real, %s::bigint)",
        )
        conn.commit()
        log.info(f"  Wrote batch {i // batch_size + 1} ({min(i + batch_size, len(rows)):,}/{len(rows):,})")

    cur.close(); cur2.close(); conn.close()
    log.info("Backfill complete.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train EPV model and backfill DB")
    parser.add_argument(
        "--seasons", nargs="+", default=["2023-24", "2024-25"],
        help="Seasons to use for training AND backfill"
    )
    parser.add_argument(
        "--backfill-only", action="store_true",
        help="Skip training, use existing model.pkl for backfill"
    )
    parser.add_argument(
        "--train-only", action="store_true",
        help="Train but do not backfill"
    )
    args = parser.parse_args()

    if args.backfill_only:
        model, classes = load_model()
    else:
        model, classes = train(args.seasons)

    if not args.train_only:
        backfill(model, classes, args.seasons)


if __name__ == "__main__":
    main()

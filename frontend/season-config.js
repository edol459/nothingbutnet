// ── Season utilities (shared across all pages) ────────────────
// Include this before any page script that needs season helpers.

(function () {
  const _API = window.location.protocol === 'file:' ? 'http://localhost:5000' : '';

  /**
   * Returns the current NBA season string, e.g. "2025-26".
   * Oct–Dec → the new season that just started (e.g. Oct 2025 → "2025-26").
   * Jan–Sep → the season that started last October (e.g. Mar 2026 → "2025-26").
   */
  function computeCurrentSeason() {
    const now = new Date();
    const y = now.getFullYear(), m = now.getMonth() + 1; // 1-indexed month
    if (m >= 10) return `${y}-${String(y + 1).slice(2)}`;
    return `${y - 1}-${String(y).slice(2)}`;
  }

  /**
   * Returns "Playoffs" during late April – June, else "Regular Season".
   * Approximate heuristic; the server's /api/current-season is authoritative.
   */
  function computeCurrentSeasonType() {
    const now = new Date();
    const m = now.getMonth() + 1, d = now.getDate();
    if (m === 5 || m === 6 || (m === 4 && d >= 20)) return 'Playoffs';
    if (m === 4 && d >= 13) return 'PlayIn';
    return 'Regular Season';
}

  /**
   * Populate a <select> with distinct seasons fetched from /api/seasons.
   *
   * If currentSeason has no rows yet, the newest season that DOES is selected
   * instead. The calendar flips to a new season weeks before opening night
   * (Oct 1 for the NBA, May 1 for the WNBA), and offering a season with nothing
   * in it just renders empty tables until the first games are played.
   *
   * Resolves with the season actually selected, so callers can sync their own
   * state to it. Never rejects (falls back to existing options).
   */
  async function populateSeasonDropdown(selectEl, currentSeason, source = 'stats') {
    try {
      const res = await fetch(`${_API}/api/seasons?source=${source}`);
      const data = await res.json();
      const all = [...new Set((data.seasons || []).map(s => s.season))].sort().reverse();
      if (!all.length) return currentSeason;
      const selected = all.includes(currentSeason) ? currentSeason : all[0];
      selectEl.innerHTML = all
        .map(s => `<option value="${s}"${s === selected ? ' selected' : ''}>${s.replace('-', '–')}</option>`)
        .join('');
      return selected;
    } catch {
      // On network error keep existing options, just try to select the right one
      for (const opt of selectEl.options) opt.selected = (opt.value === currentSeason);
      return currentSeason;
    }
  }

  // Expose on window so inline page scripts can call them
  window.computeCurrentSeason     = computeCurrentSeason;
  window.computeCurrentSeasonType = computeCurrentSeasonType;
  window.populateSeasonDropdown   = populateSeasonDropdown;
})();

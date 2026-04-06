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
    if ((m === 4 && d >= 20) || m === 5 || m === 6) return 'Playoffs';
    return 'Regular Season';
  }

  /**
   * Populate a <select> with distinct seasons fetched from /api/seasons.
   * If currentSeason is not yet in the DB (e.g. early October), it is prepended.
   * Resolves when the dropdown is ready; never rejects (falls back to existing options).
   */
  async function populateSeasonDropdown(selectEl, currentSeason) {
    try {
      const res = await fetch(`${_API}/api/seasons`);
      const data = await res.json();
      const all = [...new Set((data.seasons || []).map(s => s.season))].sort().reverse();
      if (!all.includes(currentSeason)) all.unshift(currentSeason);
      selectEl.innerHTML = all
        .map(s => `<option value="${s}"${s === currentSeason ? ' selected' : ''}>${s.replace('-', '–')}</option>`)
        .join('');
    } catch {
      // On network error keep existing options, just try to select the right one
      for (const opt of selectEl.options) opt.selected = (opt.value === currentSeason);
    }
  }

  // Expose on window so inline page scripts can call them
  window.computeCurrentSeason     = computeCurrentSeason;
  window.computeCurrentSeasonType = computeCurrentSeasonType;
  window.populateSeasonDropdown   = populateSeasonDropdown;
})();

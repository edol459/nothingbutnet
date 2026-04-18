/**
 * team-colors.js — NBA & WNBA Team Color Map
 * ============================================
 * Primary and secondary colors for each team.
 * Usage: const c = TEAM_COLORS['LAL']; // { primary: '#552583', secondary: '#FDB927' }
 */

const TEAM_COLORS = {
  // ── NBA ──────────────────────────────────────────
  ATL: { primary: '#E03A3E', secondary: '#C1D32F' },
  BOS: { primary: '#007A33', secondary: '#BA9653' },
  BKN: { primary: '#000000', secondary: '#FFFFFF' },
  CHA: { primary: '#00788C', secondary: '#1D1160' },
  CHI: { primary: '#CE1141', secondary: '#000000' },
  CLE: { primary: '#6F263D', secondary: '#FFB81C' },
  DAL: { primary: '#00538C', secondary: '#002B5E' },
  DEN: { primary: '#0E2240', secondary: '#FEC524' },
  DET: { primary: '#C8102E', secondary: '#1D42BA' },
  GSW: { primary: '#1D428A', secondary: '#FFC72C' },
  HOU: { primary: '#CE1141', secondary: '#000000' },
  IND: { primary: '#FDBB30', secondary: '#002d62' },
  LAC: { primary: '#C8102E', secondary: '#1D428A' },
  LAL: { primary: '#552583', secondary: '#FDB927' },
  MEM: { primary: '#5D76A9', secondary: '#12173F' },
  MIA: { primary: '#98002E', secondary: '#F9A01B' },
  MIL: { primary: '#00471B', secondary: '#EEE1C6' },
  MIN: { primary: '#0C2340', secondary: '#236192' },
  NOP: { primary: '#0C2340', secondary: '#C8102E' },
  NYK: { primary: '#F58426', secondary: '#006bb6' },
  OKC: { primary: '#007AC1', secondary: '#EF6B2D' },
  ORL: { primary: '#0077C0', secondary: '#C4CED4' },
  PHI: { primary: '#006BB6', secondary: '#ED174C' },
  PHX: { primary: '#1D1160', secondary: '#E56020' },
  POR: { primary: '#E03A3E', secondary: '#000000' },
  SAC: { primary: '#5A2D81', secondary: '#63727A' },
  SAS: { primary: '#919699', secondary: '#000000' },
  TOR: { primary: '#CE1141', secondary: '#000000' },
  UTA: { primary: '#002B5C', secondary: '#F9A01B' },
  WAS: { primary: '#002B5C', secondary: '#E31837' },

  // ── WNBA ─────────────────────────────────────────
  ACE: { primary: '#A6192E', secondary: '#000000' },
  SKY: { primary: '#418FDE', secondary: '#FFCD00' },
  SUN: { primary: '#F05023', secondary: '#0A2240' },
  WNG: { primary: '#CB6015', secondary: '#201747' },
  FVR: { primary: '#2C5234', secondary: '#BE3A34' },
  DRM: { primary: '#552583', secondary: '#FDB927' },
  LNX: { primary: '#236192', secondary: '#0C2340' },
  LIB: { primary: '#6ECEB2', secondary: '#000000' },
  MYS: { primary: '#0C2340', secondary: '#78BE20' },
  STM: { primary: '#2C5234', secondary: '#FFC72C' },
  SPK: { primary: '#2C5234', secondary: '#FEE11A' },
  VAL: { primary: '#E31837', secondary: '#002B5C' },
  APH: { primary: '#CB6015', secondary: '#000000' },
};

/**
 * Get a team's color with fallback.
 * @param {string} abbr - Team abbreviation
 * @param {'primary'|'secondary'} type
 * @returns {string} CSS color string
 */
function getTeamColor(abbr, type = 'primary') {
  const colors = TEAM_COLORS[(abbr || '').toUpperCase()];
  if (!colors) return type === 'primary' ? '#333333' : '#888888';
  return colors[type] || colors.primary;
}

/**
 * Parse a hex color to [r, g, b].
 */
function hexToRgb(hex) {
  const h = hex.replace('#', '');
  const n = parseInt(h.length === 3
    ? h.split('').map(c => c + c).join('')
    : h, 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

/**
 * Perceptual color distance (0 = identical, higher = more different).
 * Uses weighted Euclidean distance in RGB space.
 */
function colorDistance(hex1, hex2) {
  const [r1, g1, b1] = hexToRgb(hex1);
  const [r2, g2, b2] = hexToRgb(hex2);
  const rMean = (r1 + r2) / 2;
  const dr = r1 - r2, dg = g1 - g2, db = b1 - b2;
  return Math.sqrt(
    (2 + rMean / 256) * dr * dr +
    4 * dg * dg +
    (2 + (255 - rMean) / 256) * db * db
  );
}

/**
 * Resolve the best contrasting color pair for two teams.
 * If the away team's primary is too similar to the home team's primary,
 * the away team falls back to their secondary color.
 *
 * @param {string} awayAbbr
 * @param {string} homeAbbr
 * @returns {{ awayColor: string, homeColor: string }}
 */
function resolveTeamColors(awayAbbr, homeAbbr) {
  const SIMILARITY_THRESHOLD = 100;

  const awayPrimary   = getTeamColor(awayAbbr, 'primary');
  const awaySecondary = getTeamColor(awayAbbr, 'secondary');
  const homePrimary   = getTeamColor(homeAbbr, 'primary');

  const homeColor = homePrimary;
  const dist = colorDistance(awayPrimary, homePrimary);
  const awayColor = dist < SIMILARITY_THRESHOLD ? awaySecondary : awayPrimary;

  return { awayColor, homeColor };
}

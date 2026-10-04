/* Shared NBA/WNBA image helpers (headshots + team logos), matching the CDN
   conventions used across the site. Load before page scripts that use them. */
(function () {
  var NBA = {ATL:1610612737,BKN:1610612751,BOS:1610612738,CHA:1610612766,CHI:1610612741,
    CLE:1610612739,DAL:1610612742,DEN:1610612743,DET:1610612765,GSW:1610612744,HOU:1610612745,
    IND:1610612754,LAC:1610612746,LAL:1610612747,MEM:1610612763,MIA:1610612748,MIL:1610612749,
    MIN:1610612750,NOP:1610612740,NYK:1610612752,OKC:1610612760,ORL:1610612753,PHI:1610612755,
    PHX:1610612756,POR:1610612757,SAC:1610612758,SAS:1610612759,TOR:1610612761,UTA:1610612762,WAS:1610612764};
  var WNBA = {ATL:1611661330,CHI:1611661329,CON:1611661323,DAL:1611661321,GS:1611661331,GSV:1611661331,
    IND:1611661325,LA:1611661320,LAS:1611661320,LV:1611661319,LVA:1611661319,MIN:1611661324,
    NY:1611661313,NYL:1611661313,PHX:1611661317,POR:1611661327,PDX:1611661327,SEA:1611661328,
    TOR:1611661332,WSH:1611661322,WAS:1611661322};

  /* ── Defunct & relocated franchises ────────────────────────────────────────
     Historical games carry the abbreviation the game was played under, so the
     archive surfaces VAN, SEA, NJN, CHH, NOH and NOK. None of them are in the
     NBA's logo CDN: season-scoped paths (…/<id>/2007/L/logo.svg) 403, and the
     /global/ path serves the CURRENT franchise mark — a 2007 Sonics game would
     render a Thunder logo, which is worse than no logo at all.

     So these get a generated monogram in the franchise's own colors instead.
     It is honest, needs no external asset, and because it is returned as a
     data: URI from ydkTeamLogo() every existing caller keeps working unchanged
     (<img src>, CSS background-image, OG cards).

     `successor` is the franchise that carries the lineage today. Note CHH maps
     to CHA, not NOP: the NBA credits the 1988-2002 Hornets history to the
     current Charlotte team, while the Pelicans' record starts in 2002.

     Colors are the franchises' primary brand pairs; several predate official
     hex publication and are the widely-used approximations, in the same spirit
     as the approximated WNBA expansion colors in team-colors.js. */
  var DEFUNCT = {
    SEA: {name: 'Seattle SuperSonics',                years: '1967-2008',
          primary: '#00653A', secondary: '#FFC200', successor: 'OKC'},
    VAN: {name: 'Vancouver Grizzlies',                years: '1995-2001',
          primary: '#00A8A9', secondary: '#B4975A', successor: 'MEM'},
    NJN: {name: 'New Jersey Nets',                    years: '1977-2012',
          primary: '#002A60', secondary: '#CD1041', successor: 'BKN'},
    CHH: {name: 'Charlotte Hornets',                  years: '1988-2002',
          primary: '#00778B', secondary: '#1D1160', successor: 'CHA'},
    NOH: {name: 'New Orleans Hornets',                years: '2002-2013',
          primary: '#00778B', secondary: '#B4975A', successor: 'NOP'},
    NOK: {name: 'New Orleans/Oklahoma City Hornets',  years: '2005-2007',
          primary: '#00778B', secondary: '#B4975A', successor: 'NOP'},
    // The 1983-96 backfill reaches back past two more relocations. The other codes
    // that era uses — GOS, PHL, SAN, UTH — are the SAME franchises in the same
    // cities under different feed abbreviations, so the ingest normalises those to
    // GSW/PHI/SAS/UTA rather than inventing defunct teams for them.
    KCK: {name: 'Kansas City Kings',                  years: '1975-1985',
          primary: '#0033A0', secondary: '#E35205', successor: 'SAC'},
    SDC: {name: 'San Diego Clippers',                 years: '1978-1984',
          primary: '#C8102E', secondary: '#002F6C', successor: 'LAC'},
  };

  function luminance(hex) {
    var h = String(hex || '').replace('#', '');
    if (h.length === 3) h = h.split('').map(function (c) { return c + c; }).join('');
    var n = parseInt(h, 16);
    if (isNaN(n)) return 0;
    // Rec. 601 luma — good enough to choose between light and dark ink.
    return (0.299 * ((n >> 16) & 255) + 0.587 * ((n >> 8) & 255) + 0.114 * (n & 255)) / 255;
  }

  /* Ink that stays legible on `bg`: the team's own secondary when it contrasts,
     otherwise plain white/black. Keeps NJN red-on-navy but avoids CHH's purple
     disappearing into its teal. */
  function readableInk(bg, secondary) {
    var lb = luminance(bg);
    if (secondary && Math.abs(luminance(secondary) - lb) > 0.35) return secondary;
    return lb > 0.55 ? '#111111' : '#FFFFFF';
  }

  function monogram(abbr, team) {
    var ink = readableInk(team.primary, team.secondary);
    var svg =
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" role="img" ' +
           'aria-label="' + team.name + '">' +
        '<circle cx="50" cy="50" r="50" fill="' + team.primary + '"/>' +
        '<text x="50" y="50" fill="' + ink + '" ' +
              'font-family="Helvetica Neue,Helvetica,Arial,sans-serif" ' +
              'font-size="33" font-weight="700" letter-spacing="-1.5" ' +
              'text-anchor="middle" dominant-baseline="central">' + abbr + '</text>' +
      '</svg>';
    return 'data:image/svg+xml,' + encodeURIComponent(svg);
  }

  // Built once at load — these never change, and callers hit them per render.
  var DEFUNCT_MARKS = {};
  Object.keys(DEFUNCT).forEach(function (abbr) {
    DEFUNCT_MARKS[abbr] = monogram(abbr, DEFUNCT[abbr]);
  });

  // Exposed so team-colors.js can resolve historical abbrs without duplicating
  // the hexes, and so pages can link a defunct team through to its successor.
  window.YDK_DEFUNCT_TEAMS = DEFUNCT;

  window.ydkHeadshot = function (pid, league) {
    if (pid == null) return null;
    return league === 'wnba'
      ? 'https://ak-static.cms.nba.com/wp-content/uploads/headshots/wnba/latest/260x190/' + pid + '.png'
      : 'https://cdn.nba.com/headshots/nba/latest/260x190/' + pid + '.png';
  };
  window.ydkTeamLogo = function (abbr, league) {
    abbr = (abbr || '').toUpperCase();
    var id = league === 'wnba' ? WNBA[abbr] : NBA[abbr];
    if (id) {
      return league === 'wnba'
        ? 'https://cdn.wnba.com/logos/wnba/' + id + '/global/L/logo.svg'
        : 'https://cdn.nba.com/logos/nba/' + id + '/global/L/logo.svg';
    }
    // SEA is a live WNBA team (the Storm) and a defunct NBA one — only fall
    // through to the historical mark for NBA.
    if (league !== 'wnba' && DEFUNCT_MARKS[abbr]) return DEFUNCT_MARKS[abbr];
    return null;
  };

  /* Full name for a team abbreviation, historical ones included.
     Returns the abbr unchanged if we don't know it, so it is safe to render. */
  window.ydkTeamName = function (abbr, league) {
    abbr = (abbr || '').toUpperCase();
    if (league !== 'wnba' && DEFUNCT[abbr]) return DEFUNCT[abbr].name;
    return abbr;
  };

  /* True for abbreviations that no longer exist in the league. Pages use this
     to suppress "go to team page" links, which are keyed to current abbrs. */
  window.ydkIsDefunctTeam = function (abbr, league) {
    return league !== 'wnba' && !!DEFUNCT[(abbr || '').toUpperCase()];
  };

  /* The franchise carrying this abbr's lineage today (SEA -> OKC), or the abbr
     itself when it is already current. */
  window.ydkCurrentFranchise = function (abbr, league) {
    abbr = (abbr || '').toUpperCase();
    if (league !== 'wnba' && DEFUNCT[abbr]) return DEFUNCT[abbr].successor;
    return abbr;
  };
})();

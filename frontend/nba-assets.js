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

  window.ydkHeadshot = function (pid, league) {
    if (pid == null) return null;
    return league === 'wnba'
      ? 'https://ak-static.cms.nba.com/wp-content/uploads/headshots/wnba/latest/260x190/' + pid + '.png'
      : 'https://cdn.nba.com/headshots/nba/latest/260x190/' + pid + '.png';
  };
  window.ydkTeamLogo = function (abbr, league) {
    abbr = (abbr || '').toUpperCase();
    var id = league === 'wnba' ? WNBA[abbr] : NBA[abbr];
    if (!id) return null;
    return league === 'wnba'
      ? 'https://cdn.wnba.com/logos/wnba/' + id + '/global/L/logo.svg'
      : 'https://cdn.nba.com/logos/nba/' + id + '/global/L/logo.svg';
  };
})();

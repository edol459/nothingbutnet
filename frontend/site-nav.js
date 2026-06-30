/* ============================================================
   ydkball — canonical site header behaviour
   - highlights the active link from the URL
   - loads the auth slot (avatar -> /profile, else Sign in)
   - keeps --nav-h in sync with the rendered header height
   Markup lives inline on each page (so the header shows even if
   this script fails); this only enhances it.
   ============================================================ */
(function () {
  function init() {
    var host = document.querySelector('.site-nav');
    if (!host) return;

    // Active link
    var path = location.pathname.replace(/\/+$/, '') || '/';
    var links = host.querySelectorAll('.site-nav__links a');
    for (var i = 0; i < links.length; i++) {
      var href = (links[i].getAttribute('href') || '/').replace(/\/+$/, '') || '/';
      var on = href === '/' ? path === '/' : (path === href || path.indexOf(href + '/') === 0);
      links[i].classList.toggle('active', on);
    }

    // Keep --nav-h equal to the real rendered header height
    function syncH() {
      document.documentElement.style.setProperty('--nav-h', host.offsetHeight + 'px');
    }
    syncH();
    window.addEventListener('resize', syncH);
    window.addEventListener('orientationchange', function () { setTimeout(syncH, 120); });

    // Auth slot
    var slot = host.querySelector('#nav-auth-slot');
    fetch('/auth/me')
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (!slot) return;
        var u = d && d.user;
        if (u) {
          if (typeof u.night_mode !== 'undefined') {
            localStorage.setItem('ydkball_night', u.night_mode ? '1' : '0');
            document.documentElement.classList.toggle('night-mode', !!u.night_mode);
          }
          if (u.avatar_url) {
            slot.innerHTML = '<a href="/profile"><img class="site-nav__avatar" src="' + u.avatar_url + '" alt="Profile"></a>';
          } else {
            var initial = ((u.display_name || '?').charAt(0) || '?').toUpperCase();
            slot.innerHTML = '<a href="/profile"><span class="site-nav__avatar site-nav__avatar--initial">' + initial + '</span></a>';
          }
        } else {
          slot.innerHTML = '<a class="site-nav__signin" href="/auth/google/login?next=' + encodeURIComponent(location.pathname) + '">Sign in</a>';
        }
        syncH();
      })
      .catch(function () {
        if (slot) slot.innerHTML = '<a class="site-nav__signin" href="/auth/google/login">Sign in</a>';
      });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

/* ──────────────────────────────────────────────────────────
 * On a Journey · 統一ナビゲーション挿入
 * 各ページの <header> 内の `.nav-links` または `[data-site-nav]`
 * を見つけて、共通ナビに置き換える。
 * ナビ項目の正本は ITEMS だけ（他のファイルに項目を書かない）。
 * ツール内のサブページへのリンクは `[data-sub-nav]`（置換の対象外）に置く。
 *
 * 公開: window.SiteNav.ITEMS（項目）・window.SiteNav.apply()（置換の再実行。
 *       ヘッダーを後から描画するページは、描画の後に呼ぶ）
 *
 * GitHub Pages の base path（/On-a-journey/）を前提に絶対パスで構築。
 * ────────────────────────────────────────────────────────── */
(function () {
  var BASE = '/On-a-journey';

  var ITEMS = [
    { key: 'home',         label: 'HOME',             href: BASE + '/' },
    { key: 'tanuki-score', label: 'TANUKI SCORE',      href: BASE + '/value-monitor/tanuki_score/' },
    { key: 'tanuki',       label: 'TANUKI Valuation',  href: BASE + '/value-monitor/tanuki_valuation/' },
    { key: 'eps',          label: 'EPS Analyzer',      href: BASE + '/value-monitor/adjusted_eps_analyzer/' },
    { key: 'stonks',       label: 'STONKS SILO',       href: BASE + '/value-monitor/stonks-silo/' },
    { key: 'mp',           label: 'Market Pulse',      href: BASE + '/market-monitor/market-pulse/' },
    { key: 'macro',        label: 'MACRO PULSE',       href: BASE + '/market-monitor/macro-pulse/' },
    { key: 'hype',         label: 'HYPE CORE',         href: BASE + '/value-monitor/hypecore/' },
    { key: 'portfolio',    label: 'PORTFOLIO',         href: BASE + '/portfolio/' },
    { key: 'tail',         label: 'TANUKI TAIL',       href: BASE + '/portfolio/tail/' }
  ];

  function build(activeKey) {
    var nav = document.createElement('nav');
    nav.className = 'site-nav';
    ITEMS.forEach(function (it, i) {
      if (i > 0) {
        var sep = document.createElement('span');
        sep.className = 'nav-sep';
        nav.appendChild(sep);
      }
      var a = document.createElement('a');
      a.href = it.href;
      a.textContent = it.label;
      if (it.key === activeKey) a.className = 'active';
      nav.appendChild(a);
    });
    return nav;
  }

  function init() {
    var active = document.body.getAttribute('data-tool') || 'home';
    // 既存の .nav-links / [data-site-nav] を全部置換
    var targets = document.querySelectorAll('.nav-links, [data-site-nav]');
    targets.forEach(function (t) {
      var fresh = build(active);
      t.parentNode.replaceChild(fresh, t);
      centerActive(fresh);
    });
  }

  // NAV-MOBILE-1: スマホ幅（site-theme.css の 640px 以下）ではナビが横スクロールの1行になるので、
  // 現在のツールをナビの中央に寄せる。scrollIntoView はページ（window）も横に動かすため使わず、ナビの scrollLeft だけ変える
  // ヘッダーが後から表示される（認証の画面の後）・フォントの読み込みや隣の要素でナビの幅が後から変わるページがあるため、
  // ナビ・現在のツールの大きさが変わるたびに寄せ直す。人がナビを触ったら寄せ直しをやめる
  function centerActive(nav) {
    if (!window.matchMedia || !window.matchMedia('(max-width: 640px)').matches) return;
    var a = nav.querySelector('a.active');
    if (!a) return;
    var center = function () {
      var nr = nav.getBoundingClientRect();
      if (!nr.width) return;
      var ar = a.getBoundingClientRect();
      nav.scrollLeft += (ar.left + ar.width / 2) - (nr.left + nr.width / 2);
    };
    center();
    if (!window.ResizeObserver) return;
    var ro = new ResizeObserver(center);
    ro.observe(nav);
    ro.observe(a);
    ['pointerdown', 'touchstart', 'wheel'].forEach(function (ev) {
      nav.addEventListener(ev, function () { ro.disconnect(); }, { once: true, passive: true });
    });
  }

  window.SiteNav = { ITEMS: ITEMS, apply: init };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

/* ──────────────────────────────────────────────────────────
 * On a Journey · 銘柄横断リンク（NAV-LINK-INTEGRITY-1）
 * 個別銘柄ページの `[data-ticker-links]` に、同じ銘柄の各ツールのページへの
 * 切替行「TANUKI / EPS / HYPE / TAIL」を描画する。
 * - 現在のツール（<body data-tool>）は active 表示（リンクにしない）
 * - 登録銘柄リストにその銘柄が無いツールはグレーアウト（リンクにしない）
 * - リストを読めなかったツールはリンクとして出す
 *
 * 使い方:
 *   <div data-ticker-links></div>
 *   <script src="/On-a-journey/common/ticker-links.js"></script>
 *   URL の ?ticker=X があれば自動で描画する。ヘッダーを後から描画するページ・
 *   ページ内で銘柄を切り替えるページは、描画の後に TickerLinks.render(X) を呼ぶ。
 *
 * 登録銘柄リストの判定元（フロントエンドが読めるもの。config/cik_lookup.csv は読まない）:
 *   TANUKI tanuki_valuation/data/tickers.json（tickers）
 *   EPS    adjusted_eps_analyzer/data/summary.json（tickers[].ticker）
 *   HYPE   hypecore/data/tickers.json（tickers）
 *   TAIL   portfolio/tail/data/positions_index.json（positions の {T}_thesis.json）
 *
 * GitHub Pages の base path（/On-a-journey/）を前提に絶対パスで構築。
 * ────────────────────────────────────────────────────────── */
(function () {
  var BASE = '/On-a-journey';

  var TOOLS = [
    { key: 'tanuki', label: 'TANUKI', page: BASE + '/value-monitor/tanuki_valuation/stock.html',
      list: BASE + '/value-monitor/tanuki_valuation/data/tickers.json',
      pick: function (d) { return d.tickers; } },
    { key: 'eps', label: 'EPS', page: BASE + '/value-monitor/adjusted_eps_analyzer/stock.html',
      list: BASE + '/value-monitor/adjusted_eps_analyzer/data/summary.json',
      pick: function (d) { return d.tickers.map(function (r) { return r.ticker; }); } },
    { key: 'hype', label: 'HYPE', page: BASE + '/value-monitor/hypecore/detail.html',
      list: BASE + '/value-monitor/hypecore/data/tickers.json',
      pick: function (d) { return d.tickers; } },
    { key: 'tail', label: 'TAIL', page: BASE + '/portfolio/tail/detail.html',
      list: BASE + '/portfolio/tail/data/positions_index.json',
      pick: function (d) { return d.positions.map(function (f) { return f.replace(/_thesis\.json$/, ''); }); } }
  ];

  // ツールごとの登録銘柄（大文字の配列）。読めなければ null（＝判定しない）
  var listsPromise = null;
  function loadLists() {
    if (!listsPromise) {
      listsPromise = Promise.all(TOOLS.map(function (t) {
        return fetch(t.list, { cache: 'no-cache' })
          .then(function (r) { return r.ok ? r.json() : null; })
          .then(function (d) {
            if (!d) return null;
            return t.pick(d).map(function (x) { return String(x).toUpperCase(); });
          })
          .catch(function () { return null; });
      }));
    }
    return listsPromise;
  }

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function draw(targets, ticker, lists) {
    var current = document.body.getAttribute('data-tool');
    targets.forEach(function (box) {
      box.innerHTML = '';
      box.appendChild(el('span', 'tl-label', ticker));
      TOOLS.forEach(function (t, i) {
        var list = lists[i];
        var item;
        if (t.key === current) {
          item = el('span', 'tl-item active', t.label);
        } else if (list && list.indexOf(ticker) < 0) {
          item = el('span', 'tl-item off', t.label);
          item.title = t.label + 'の対象外の銘柄';
        } else {
          item = el('a', 'tl-item', t.label);
          item.href = t.page + '?ticker=' + encodeURIComponent(ticker);
        }
        box.appendChild(item);
      });
    });
  }

  function render(ticker) {
    var t = ticker || new URLSearchParams(location.search).get('ticker');
    if (!t) return Promise.resolve();
    t = String(t).trim().toUpperCase();
    if (!t) return Promise.resolve();
    return loadLists().then(function (lists) {
      var targets = Array.prototype.slice.call(document.querySelectorAll('[data-ticker-links]'));
      draw(targets, t, lists);
    });
  }

  window.TickerLinks = { render: render };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { render(); });
  } else {
    render();
  }
})();

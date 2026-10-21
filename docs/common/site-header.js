/* ──────────────────────────────────────────────────────────
 * On a Journey · 統一ヘッダー（ロゴ・ドット・タイトル・サブタイトル）挿入
 * 各ページの <header> 内の `a.logo` を見つけて、共通の
 * <div class="site-header-inner"> に置き換える。
 *
 * ナビゲーションリンク（.nav-links）は引き続き site-nav.js が管理する。
 * 本スクリプトはロゴ画像・タイトルドット・タイトル文字・サブタイトルのみを担当し、
 * バージョン表記（Koichi式 vX.X 等の装飾的バッジ）は撤廃方針のため生成しない。
 *
 * 使い方:
 *   <header>
 *     <a class="logo">（中身は問わない。置換時に読み捨てる）</a>
 *     <nav class="nav-links">...</nav>
 *   </header>
 *   <script src=".../common/site-header.js"></script>
 *
 * タイトル・サブタイトル・アクセント色は <body data-tool="..."> の値から
 * 自動決定される（アクセント色は site-theme.css 側の body[data-tool] マッピング経由）。
 * 個別ページで上書きしたい場合は元の `a.logo` に以下の data-* 属性を付与する：
 *   data-title="カスタムタイトル"
 *   data-subtitle="カスタムサブタイトル"
 *   data-no-subtitle（属性が存在するだけでサブタイトル非表示）
 *
 * GitHub Pages の base path（/On-a-journey/）を前提に絶対パスで構築。
 * ────────────────────────────────────────────────────────── */
(function () {
  var BASE = '/On-a-journey';

  // SYSTEM_MAP.md の「システム一覧と責任範囲」表と同一の文言を用いる
  var TOOL_META = {
    'home':         { title: 'ON A JOURNEY',     subtitle: 'PERSONAL INVESTMENT TOOLS' },
    'tanuki':       { title: 'TANUKI VALUATION',  subtitle: 'DCF理論株価・RICE投資効率' },
    'tanuki-score': { title: 'TANUKI SCORE',      subtitle: '多銘柄比較・最終投資判断' },
    'eps':          { title: 'EPS ANALYZER',      subtitle: 'GAAP/Non-GAAP乖離・割安発掘' },
    'stonks':       { title: 'STONKS SILO',       subtitle: '赤字企業の投資適合性評価' },
    'hype':         { title: 'HYPE CORE',         subtitle: '期待プレミアム・フェーズ判定' },
    'mp':           { title: 'MARKET PULSE',      subtitle: '市場センチメント・資金フロー' },
    'macro':        { title: 'MACRO PULSE',       subtitle: 'マクロ環境・景気後退リスク' },
    'portfolio':    { title: 'PORTFOLIO',         subtitle: '保有ポートフォリオ管理' },
    'tail':         { title: 'TANUKI TAIL',       subtitle: '長期投資テーゼ管理' }
  };

  function build(tool, opts) {
    var meta = TOOL_META[tool] || TOOL_META.home;
    var title = (opts && opts.title) || meta.title;
    var subtitle = (opts && opts.noSubtitle) ? null : ((opts && opts.subtitle) || meta.subtitle);

    var group = document.createElement('div');
    group.className = 'site-header-inner';

    var a = document.createElement('a');
    a.className = 'logo';
    a.href = BASE + '/';
    a.setAttribute('aria-label', 'Go to home');

    var img = document.createElement('img');
    img.className = 'logo-img';
    img.src = BASE + '/common/images/logo.png';
    img.alt = 'TANUKI';
    a.appendChild(img);

    var dot = document.createElement('span');
    dot.className = 'logo-dot';
    a.appendChild(dot);

    var titleSpan = document.createElement('span');
    titleSpan.className = 'logo-title';
    titleSpan.textContent = title;
    a.appendChild(titleSpan);

    group.appendChild(a);

    if (subtitle) {
      var sub = document.createElement('span');
      sub.className = 'logo-subtitle';
      sub.textContent = subtitle;
      group.appendChild(sub);
    }

    return group;
  }

  function init() {
    var tool = document.body.getAttribute('data-tool') || 'home';
    var targets = document.querySelectorAll('header a.logo');
    targets.forEach(function (t) {
      var opts = {
        title: t.getAttribute('data-title'),
        subtitle: t.getAttribute('data-subtitle'),
        noSubtitle: t.hasAttribute('data-no-subtitle')
      };
      var fresh = build(tool, opts);
      t.parentNode.replaceChild(fresh, t);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

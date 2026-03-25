import os
import sys
import urllib.request
import feedparser
import yfinance as yf
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
import json
import csv
import re
import requests   # 追加: requests で非ASCII URLを扱う

# --- 必須環境変数チェック ---
required_env_vars = ["XAI_API_KEY", "GMAIL_USER", "GMAIL_PASSWORD"]
missing = [v for v in required_env_vars if not os.getenv(v)]
if missing:
    print(f"[ERROR] 必須環境変数が設定されていません: {', '.join(missing)}")
    sys.exit(1)

# --- 設定 ---
XAI_API_KEY  = os.getenv("XAI_API_KEY")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
# RSSファイルはスクリプトと同じ scr/ ディレクトリに配置
RSS_LIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "02_market_rss.txt")

JST = timezone(timedelta(hours=9))

# データ保存先（GitHub Pages 配信対象の docs/data/）
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = os.path.join(REPO_ROOT, "docs", "market-monitor", "data")
JSON_PATH = os.path.join(DATA_DIR, "market_data.json")
CSV_PATH = os.path.join(DATA_DIR, "market_data.csv")

# CSVのカラム定義（必要に応じて拡張）
CSV_COLUMNS = [
    "date", "judgment",
    "VIX指数_value", "VIX指数_change", "VIX指数_change_percent",
    "日経平均_value", "日経平均_change", "日経平均_change_percent",
    "ドル円_value", "ドル円_change", "ドル円_change_percent",
    "米10年債_value", "米10年債_change", "米10年債_change_percent",
    "S&P500_value", "S&P500_change", "S&P500_change_percent",
    "WTI原油_value", "WTI原油_change", "WTI原油_change_percent",
    "金（GOLD）_value", "金（GOLD）_change", "金（GOLD）_change_percent",
    "HYG（ハイイールド債ETF）_value", "HYG（ハイイールド債ETF）_change", "HYG（ハイイールド債ETF）_change_percent",
    "LQD（投資適格債ETF）_value", "LQD（投資適格債ETF）_change", "LQD（投資適格債ETF）_change_percent",
    "NYSE Composite_value", "NYSE Composite_change_percent", "NYSE Composite_volume_ratio",
    "S&P500グロース(IVW)_value", "S&P500グロース(IVW)_change_percent",
    "S&P500バリュー(IVE)_value", "S&P500バリュー(IVE)_change_percent",
    "Russell2000小型(RUT)_value", "Russell2000小型(RUT)_change_percent",
    "グロース対バリュー比_diff_percent",
    "大型対小型比_diff_percent",
    "HYG対LQD比_value", "HYG対LQD比_change",
    "summary"
]


def fetch_hist(ticker, period="5d"):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period)
        return hist if not hist.empty else None
    except Exception:
        return None


def format_line(name, hist):
    if hist is None:
        return f"● {name}: 取得制限あり\n"
    try:
        latest = hist['Close'].iloc[-1]
        last_date = hist.index[-1].strftime('%m/%d')
        diff, pct, vol_msg = 0.0, 0.0, ""
        if len(hist) >= 2:
            prev = hist['Close'].iloc[-2]
            diff = latest - prev
            pct = (diff / prev) * 100
            vol_latest = hist['Volume'].iloc[-1]
            vol_prev = hist['Volume'].iloc[-2]
            if vol_latest > 0 and vol_prev > 0:
                vol_msg = f" | 前日比出来高比:{vol_latest / vol_prev:.2f}"
        return f"● {name}: {latest:.2f} [{diff:+.2f} ({pct:+.2f}%){vol_msg}] ({last_date} 確定)\n"
    except Exception as e:
        return f"● {name}: 解析エラー ({e})\n"


def get_realtime_data():
    """表示用テキストと構造化データを返す"""
    summary = ""
    data = {}

    # 主要指標
    main_tickers = {
        "米10年債": "^TNX",
        "VIX指数": "^VIX",
        "ドル円": "JPY=X",
        "日経平均": "^N225",
        "S&P500": "^GSPC",
        "WTI原油": "CL=F",
        "金（GOLD）": "GC=F",
    }
    for name, ticker in main_tickers.items():
        hist = fetch_hist(ticker)
        summary += format_line(name, hist)
        if hist is not None and len(hist) >= 2:
            latest = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            change = latest - prev
            change_percent = (change / prev) * 100
            vol_latest = hist['Volume'].iloc[-1]
            vol_prev = hist['Volume'].iloc[-2]
            volume_ratio = vol_latest / vol_prev if vol_prev > 0 else None
            data[name] = {
                "value": round(latest, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 2),
                "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
                "date": hist.index[-1].strftime('%Y-%m-%d')
            }
        else:
            data[name] = None

    # NYSE Composite
    summary += "\n--- NYSE騰落統計（代替指標） ---\n"
    nya_hist = fetch_hist("^NYA")
    sp_hist = fetch_hist("^GSPC")
    nya_data = None
    if nya_hist is not None and len(nya_hist) >= 2:
        nya_latest = nya_hist['Close'].iloc[-1]
        nya_prev = nya_hist['Close'].iloc[-2]
        nya_pct = (nya_latest - nya_prev) / nya_prev * 100
        vol_latest = nya_hist['Volume'].iloc[-1]
        vol_prev = nya_hist['Volume'].iloc[-2]
        vol_ratio = vol_latest / vol_prev if vol_prev > 0 else 0
        last_date = nya_hist.index[-1].strftime('%m/%d')
        summary += f"● NYSE Composite(^NYA): {nya_latest:.2f} [{nya_pct:+.2f}%] | 前日比出来高比:{vol_ratio:.2f} ({last_date} 確定)\n"
        if sp_hist is not None and len(sp_hist) >= 2:
            sp_pct = (sp_hist['Close'].iloc[-1] - sp_hist['Close'].iloc[-2]) / sp_hist['Close'].iloc[-2] * 100
            divergence = nya_pct - sp_pct
            summary += f"● NYA対S&P500乖離（騰落代理）: {divergence:+.2f}%pt"
            if divergence < -0.5:
                summary += " → 中小型株が大型株を下回る＝市場内部の弱さ\n"
            elif divergence > 0.5:
                summary += " → 中小型株が大型株を上回る＝市場の広がり確認\n"
            else:
                summary += " → 概ね連動\n"
        nya_data = {
            "value": round(nya_latest, 2),
            "change_percent": round(nya_pct, 2),
            "volume_ratio": round(vol_ratio, 2),
            "date": nya_hist.index[-1].strftime('%Y-%m-%d')
        }
        if sp_hist is not None and len(sp_hist) >= 2:
            nya_data["divergence_vs_sp"] = round(divergence, 2)
    else:
        summary += "● NYSE騰落統計（代替）: 取得制限あり\n"
    data["NYSE Composite"] = nya_data

    # スタイル・規模
    summary += "\n--- スタイル・規模間相対パフォーマンス ---\n"
    style_tickers = {
        "S&P500グロース(IVW)": "IVW",
        "S&P500バリュー(IVE)": "IVE",
        "Russell2000小型(RUT)": "^RUT",
    }
    style_data = {}
    for name, ticker in style_tickers.items():
        hist = fetch_hist(ticker)
        summary += format_line(name, hist)
        if hist is not None and len(hist) >= 2:
            latest = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            pct = (latest - prev) / prev * 100
            style_data[name] = pct
            data[name] = {
                "value": round(latest, 2),
                "change_percent": round(pct, 2),
                "date": hist.index[-1].strftime('%Y-%m-%d')
            }
        else:
            data[name] = None

    if "S&P500グロース(IVW)" in style_data and "S&P500バリュー(IVE)" in style_data:
        gv_diff = style_data["S&P500グロース(IVW)"] - style_data["S&P500バリュー(IVE)"]
        direction = "グロース優勢（リスクオン）" if gv_diff > 0 else "バリュー優勢（ディフェンシブ）"
        summary += f"  グロース対バリュー比（日次）: {gv_diff:+.2f}%pt → {direction}\n"
        data["グロース対バリュー比"] = {"diff_percent": round(gv_diff, 2)}

    sp500_hist = fetch_hist("^GSPC")
    if sp500_hist is not None and "Russell2000小型(RUT)" in style_data and len(sp500_hist) >= 2:
        sp_pct = (sp500_hist['Close'].iloc[-1] - sp500_hist['Close'].iloc[-2]) / sp500_hist['Close'].iloc[-2] * 100
        lsv_diff = sp_pct - style_data["Russell2000小型(RUT)"]
        direction = "大型優勢（質への逃避）" if lsv_diff > 0 else "小型優勢（リスク選好）"
        summary += f"  大型対小型比（日次、S&P500対RUT）: {lsv_diff:+.2f}%pt → {direction}\n"
        data["大型対小型比"] = {"diff_percent": round(lsv_diff, 2)}

    # クレジット
    summary += "\n--- クレジット・金融コンディション ---\n"
    hyg_hist = fetch_hist("HYG")
    lqd_hist = fetch_hist("LQD")
    summary += format_line("HYG（ハイイールド債ETF）", hyg_hist)
    summary += format_line("LQD（投資適格債ETF）", lqd_hist)

    if hyg_hist is not None and lqd_hist is not None:
        for hist, name in [(hyg_hist, "HYG（ハイイールド債ETF）"), (lqd_hist, "LQD（投資適格債ETF）")]:
            latest = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            change = latest - prev
            change_percent = (change / prev) * 100
            vol_latest = hist['Volume'].iloc[-1]
            vol_prev = hist['Volume'].iloc[-2]
            volume_ratio = vol_latest / vol_prev if vol_prev > 0 else None
            data[name] = {
                "value": round(latest, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 2),
                "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
                "date": hist.index[-1].strftime('%Y-%m-%d')
            }
        try:
            ratio_now = hyg_hist['Close'].iloc[-1] / lqd_hist['Close'].iloc[-1]
            ratio_prev = hyg_hist['Close'].iloc[-2] / lqd_hist['Close'].iloc[-2]
            ratio_chg = ratio_now - ratio_prev
            last_date = hyg_hist.index[-1].strftime('%m/%d')
            direction = "HY優勢＝リスクオン" if ratio_chg > 0 else "スプレッド拡大示唆＝リスクオフ"
            summary += f"● HYG対LQD比（クレジット代理）: {ratio_now:.4f} [{ratio_chg:+.6f}] ({last_date} 確定) → {direction}\n"
            data["HYG対LQD比"] = {
                "value": round(ratio_now, 4),
                "change": round(ratio_chg, 6),
                "date": hyg_hist.index[-1].strftime('%Y-%m-%d')
            }
        except Exception as e:
            summary += f"● HYG/LQD比率: 計算エラー ({e})\n"
    else:
        data["HYG（ハイイールド債ETF）"] = None
        data["LQD（投資適格債ETF）"] = None

    return summary, data


def get_market_news():
    """RSSフィードからニュース取得（requests使用）"""
    if not os.path.exists(RSS_LIST_FILE):
        print(f"[WARN] RSSファイルが見つかりません: {RSS_LIST_FILE}")
        return []
    with open(RSS_LIST_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    all_entries = []
    for url in urls:
        try:
            # requests で取得（非ASCII文字列でも自動処理）
            response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            for e in feed.entries[:8]:
                all_entries.append(f"T: {e.title}\nS: {e.get('summary', '')}")
        except Exception as e:
            print(f"[WARN] RSS取得失敗: {url} ({e})")
    print(f"[INFO] RSS取得件数: {len(all_entries)} 件")
    return all_entries


def analyse_market(realtime_data, news_context):
    """xAI Grok API（OpenAI互換エンドポイント）で市場分析を実行する"""
    news_section = news_context if news_context.strip() else "（ニュース取得なし）"
    prompt = f"""
あなたはプロの機関投資家専属アナリストだ。以下の最新数値と需給・ニュースを統合し報告せよ。

1. 市場フェーズ判定（晴れ・曇り・嵐）
結論（例：「判定：曇り」）を必ず冒頭の一文に置き、その後に根拠を続けよ。
根拠には必ずVIXと前日比出来高比を含めること。価格下落＋出来高増なら「嵐」の予兆として厳しく判定せよ。

2. 金利・債券（米10年債）

3. 恐怖指数・心理（VIX）

4. 通貨の勢い（ドル円）

5. 指数・需給（日経平均、S&P500、NYSE騰落統計）
出来高比1.1以上かつ価格下落があればディストリビューション（大口売り抜け）の疑いを指摘せよ。
安値圏からの反発局面において、反発4日目以降に出来高増加を伴う大幅上昇（+1.7%以上）が確認された場合はフォロースルーデイ（買い転換シグナル）として明示せよ。
NYSE騰落比率が指数と逆行していればヒンデンブルグ・オーメン的な市場内部の脆弱性を指摘せよ。

6. スタイル・規模間相対パフォーマンス（グロース対バリュー比、大型対小型比）
グロース対バリュー比（日次）とその解釈（リスクオン/ディフェンシブ）を一行で述べよ。
大型対小型比（日次）と質への逃避の有無を一行で述べよ。
これを踏まえ指数コメントと接続し、市場の立体的な需給構造を考察せよ。

7. コモディティ（原油、金）
金対原油比の方向にも触れ、インフレヘッジ需要とリスク回避の強弱を読み解け。
原油下落時は「地政学リスクの緩和」か「需要減退懸念」かを必ず区別して明記せよ。

8. クレジット・金融コンディション（HYG、LQD、HYG対LQD比）
以下の三点を必ず個別に一行ずつ明記した上で総合判定せよ。
  株（S&P500の方向）→ リスクオン/リスクオフ
  債券（米10年債利回りの方向）→ リスクオン/リスクオフ
  クレジット（HYG対LQD比の方向）→ リスクオン/リスクオフ
週次レベルの変化でも有意な動きがあれば言及せよ。

9. 短期警戒ポイント（重要イベント）
冒頭の説明文は不要。イベントの列挙から直接始めよ。
今後5営業日以内に予定される具体的なイベントを列挙し、各イベントに「予想値・前回値・市場への影響シナリオ」を一行で添えよ。
ニュースから読み取れる情報がない場合でも、現在の市場フェーズに照らして最も注視すべき指標を根拠とともに示せ。
「地政学的リスク」「各国中央銀行の発言」等の汎用表現のみの列挙は禁止。

10. 総評・相関分析（需給面からの踏み込んだ考察）

制約：
- 各項目（1〜10）の冒頭に、その項目に関連する数値を「● 指標名: 数値 増減 前日比出来高比 確定日」の形式で必ず1行書くこと。冒頭に全指標をまとめて列挙することは禁止。各項目内に分散して記載せよ。
- 比較・相対表現は必ず「○○対△△比」の形式で統一すること（例：グロース対バリュー比、大型対小型比、HYG対LQD比）。
- スタイル・規模比較は日次変化に基づく分析である旨を明記すること。
- 総評では現在のデータから導ける具体的なシグナルや閾値を示せ。「○○する可能性も否定できない」等の汎用的な免責表現は使用禁止。
- 地政学リスクに言及する場合は必ず具体的な地域・事象・発言者を明記せよ。「地政学リスク」単独の抽象表現は禁止。
- 出力は必ず日本語（ひらがな・カタカナ・漢字・英数字・記号）のみ使用すること。韓国語・中国語・その他外国語文字の混入は厳禁。
- Markdown記法（##、**、--- 等）は一切使用禁止。プレーンテキストのみで出力せよ。
- 仮想通貨は無視。日本語回答。最後に俳句を一句（5-7-5）のみ添えること。複数句・改行は禁止し必ず一行で書くこと。音数（5-7-5）を厳守せよ。
- 総評・各項目の締め文として「注意が必要である」「注視が必要である」「懸念される」等の汎用表現で終えることは禁止。必ず具体的なシグナルや水準で締めくくれ。

【最新データ】:
{realtime_data}

【背景ニュース】:
{news_section}
"""
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    models = ["grok-3-mini", "grok-3", "grok-2-1212"]
    last_error = None
    for model in models:
        try:
            print(f"[INFO] Grokモデル試行中: {model}")
            resp = requests.post(url, headers=headers, json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4096,
                "temperature": 0.3,
            }, timeout=120)
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            print(f"[OK] Grokモデル成功: {model}")
            return text
        except Exception as e:
            print(f"[WARN] Grokモデル失敗 ({model}): {e}")
            last_error = e
    print("[ERROR] すべてのGrokモデルで失敗しました")
    raise last_error


def extract_judgment(report_text):
    # 全角・半角コロンどちらにも対応。文字列中の最初の判定を返す。
    match = re.search(r'判定[：:]\s*(嵐|曇り|晴れ)', report_text)
    return match.group(1) if match else "不明"


def save_data_to_json_and_csv(report_text, structured_data):
    os.makedirs(DATA_DIR, exist_ok=True)
    jst_now = datetime.now(JST)
    date_str = jst_now.strftime('%Y-%m-%dT%H:%M:%S+09:00')
    judgment = extract_judgment(report_text)

    # JSON
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    all_data = []
                else:
                    f.seek(0)
                    all_data = json.load(f)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[WARN] JSONファイルが破損しています。新しく作成します: {JSON_PATH} (エラー: {e})")
            all_data = []
    else:
        all_data = []

    new_entry = {
        "date": date_str,
        "judgment": judgment,
        "indicators": structured_data,
        "summary": report_text
    }
    all_data.append(new_entry)

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] JSON保存完了: {JSON_PATH} (全{len(all_data)}件)")

    # CSV
    row = {"date": date_str, "judgment": judgment}
    for key, value in structured_data.items():
        if value is None:
            continue
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                col_name = f"{key}_{subkey}"
                row[col_name] = subvalue
        else:
            row[key] = value
    row["summary"] = report_text

    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"[INFO] CSV保存完了: {CSV_PATH}")


def send_email(body):
    jst_now = datetime.now(JST)
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = f"🦝02_【市況分析】 {jst_now.strftime('%m/%d %H:%M')}"
    msg['From'], msg['To'] = GMAIL_USER, GMAIL_USER
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(GMAIL_USER, GMAIL_PASSWORD)
            smtp.send_message(msg)
        print("[INFO] メール送信成功")
    except Exception as e:
        print(f"[ERROR] メール送信失敗: {e}")
        raise


if __name__ == "__main__":
    realtime_text, structured_data = get_realtime_data()
    news = get_market_news()
    if not news:
        print("[WARN] ニュースなしで分析を実行します。")
    report = analyse_market(realtime_text, "\n".join(news))
    save_data_to_json_and_csv(report, structured_data)
    if GMAIL_USER and GMAIL_PASSWORD:
        send_email(report)
    else:
        print("[INFO] メール送信スキップ（認証情報なし）")
# Market Data Daily の外部起動（Cloudflare Worker）設定手順

Koichiさんが行う作業の手順書です（Claude Codeはデプロイ・トークンの登録を行いません）。

## 何をするものか

GitHubのschedule（cron）は、2026-10-01・10-02の2晩とも夜の枠（20:47〜23:17 UTC）で1本も起動しませんでした。
このWorkerが外から`Market_Data_Daily_Update.yml`を起動します。

| 時刻（UTC、平日） | 日本時間 | すること |
|---|---|---|
| 20:25・20:55・21:25 | 5:25・5:55・6:25 | `workflow_dispatch`（ref=kaihatsu、guard=true）を呼ぶ。応答が204以外ならDiscordに通知 |
| 21:50 | 6:50 | その日の20:00 UTC以降に作られて成功した実行が無ければ、もう一度起動してDiscordに通知 |

- 起動はguard=trueなので、scheduleと同じガードを通ります。夏時間（引け20:00 UTC）は20:25で取得し、
  冬時間（引け21:00 UTC）は20:25・20:55が「引けから20分経っていない」で何もせず、21:25で取得します。取得済みなら後の起動は何もしません。
- NYSEの休場日（`lib.js`の`NYSE_HOLIDAYS`、2026〜2028年）は何もしません。表に無い年になると、21:50に毎日Discordで知らせます。
- GitHubのschedule（20:47〜23:17、01:47・02:17 UTC）は保険として残っています。Workerが失敗した日も、scheduleが遅れて起動すれば取得されます。
- 無料プランで動きます: Cron Triggersは無料プランでアカウントあたり5本まで、ここでは3本を使います（`wrangler.toml`）。
  1回の実行のCPU時間は数ms（GitHub APIの応答待ちはCPU時間に入りません）。

ファイル: `worker.js`（入口）・`lib.js`（処理）・`lib.test.js`（`node --test`）・`wrangler.toml`（名前・cron）。

## 1. 事前準備: Node.js

Workerのデプロイに`wrangler`（CloudflareのCLI）を使います。PowerShellで:

```powershell
winget install OpenJS.NodeJS.LTS
```

インストール後、新しいターミナルで`node --version`が表示されればOKです。
（Node.jsを入れたくない場合は、末尾の「ダッシュボードだけで設定する場合」を参照）

## 2. Cloudflareのアカウント作成

1. https://dash.cloudflare.com/sign-up でアカウントを作る（無料プランのまま。ドメインの登録は不要）
2. メールアドレスの確認を済ませる
3. ターミナルで、このフォルダに移動してログインする（ブラウザが開くので許可する）

```powershell
cd C:\Users\shigi\Documents\On-a-journey-git\tools\external_trigger
npx wrangler login
```

## 3. GitHubのfine-grained tokenの発行

1. GitHub右上のアイコン → Settings → Developer settings → Personal access tokens → **Fine-grained tokens** → Generate new token
2. 次のとおり設定する
   - Token name: `on-a-journey-market-data-trigger`
   - Resource owner: `Koichi-Shigihara2`
   - Expiration: **Custom → 発行日の1年後**（期限の日付をカレンダーに入れておく）
   - Repository access: **Only select repositories → `On-a-journey`** だけ
   - Permissions → Repository permissions: **Actions → Read and write** だけ
     （Metadata: Read-only は自動で付きます。それ以外は「No access」のまま）
3. Generate token を押し、表示されたトークン（`github_pat_...`）をコピーする（画面を閉じると二度と表示されません）

このトークンでできるのは、On-a-journeyのワークフローの起動・取り消し・再実行・有効/無効の切り替えと、実行履歴の閲覧です。
コードのpush・secretの閲覧はできません。

## 4. トークンの動作確認（デプロイ前）

Git Bashで、トークンが正しく使えるかを先に確かめます（トークンが履歴に残らないよう`read -s`で入力します）。

```bash
read -s TOKEN   # 貼り付けてEnter（画面には表示されない）
curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/Koichi-Shigihara2/On-a-journey/actions/workflows/Market_Data_Daily_Update.yml/dispatches \
  -d '{"ref":"kaihatsu","inputs":{"guard":"true"}}'
unset TOKEN
```

- `204`が出ればOK。GitHubのActionsに`workflow_dispatch`の「Market Data Daily Update」が1本でき、ガードの判定
  （休日なら「NYSE休場日」、取得済みなら「終値はそろっている」）で何もせずcancelledになります。これで問題ありません
- `401`: トークンの貼り間違い・期限切れ / `403`・`404`: Repository accessかActionsの権限の設定を見直す

## 5. Workerのデプロイとsecretの登録

```powershell
cd C:\Users\shigi\Documents\On-a-journey-git\tools\external_trigger
npx wrangler deploy
npx wrangler secret put GH_DISPATCH_TOKEN    # 3.のトークンを貼り付ける
npx wrangler secret put DISCORD_WEB_HOOK     # DiscordのwebhookのURL（GitHubのsecret DISCORD_WEB_HOOKと同じものでよい）
```

- `wrangler deploy`の出力の最後に、3本のschedule（`25 20,21 * * MON-FRI`・`55 20 * * MON-FRI`・`50 21 * * MON-FRI`）が表示されます
- cronの反映には最大15分ほどかかります
- secretの値はCloudflareに暗号化して保存され、ダッシュボードでも再表示されません。リポジトリには入れないでください
  （このフォルダの`.dev.vars`は`.gitignore`済みです）

## 6. 動作確認

1. Cloudflareのダッシュボード → Workers & Pages → `on-a-journey-market-data-trigger` → Settings → Trigger Events に3本のcronがあること
2. 次の平日の朝（日本時間5:25以降）、GitHubのActionsで「Market Data Daily Update」の`workflow_dispatch`の実行が
   20:25・20:55・21:25 UTC頃にできていること。そのうち1本（夏時間は20:25）が取得し、下流（Market Pulse・Stonks Silo・TANUKI）が続くこと
3. Workerのログ: ダッシュボードの Observability（Logs）か、その時刻に`npx wrangler tail`で
   `workflow_dispatch → HTTP 204`・`成功した実行あり` が出ていること
4. 失敗時の通知の確認（任意）: 一時的に`npx wrangler secret put GH_DISPATCH_TOKEN`で誤った値を入れると、次の起動時刻に
   「起動に失敗: HTTP 401」がDiscordに届きます。確認後は正しいトークンに戻してください

手元でcronの処理を1回だけ試す場合（実際にworkflow_dispatchを呼びます。guard=trueなので取得済みなら何もしません）:

```powershell
# .dev.vars に  GH_DISPATCH_TOKEN=github_pat_...  と  DISCORD_WEB_HOOK=https://...  を書く（コミットしない）
npx wrangler dev
# 別のターミナルで（古いwranglerでは `npx wrangler dev --test-scheduled` と `/__scheduled?cron=...`）
curl "http://localhost:8787/cdn-cgi/local/scheduled?cron=25+20,21+*+*+MON-FRI"
```

起動するか21:50の確認をするかは、cronの文字列ではなく実行した時刻（UTCの21:45〜21:59なら確認）で決まります。

## 7. トークンの期限が切れたとき・更新するとき

- GitHubは期限の7日前ごろにメールで知らせます。期限が切れると、Workerの起動が`HTTP 401`になり、Discordに
  「起動に失敗: HTTP 401（トークンの期限切れ・取り消しの可能性…）」が届きます。この間もGitHubのscheduleが保険で動きます
- 更新の手順:
  1. GitHub → Settings → Developer settings → Fine-grained tokens → 該当のトークン → **Regenerate token**
     （期限を1年後に設定。権限・対象リポジトリはそのまま引き継がれる）
  2. `npx wrangler secret put GH_DISPATCH_TOKEN` で新しいトークンを登録する（すぐに有効になる。デプロイし直しは不要）
  3. 「4. トークンの動作確認」と同じcurlで`204`を確かめる
- トークンが漏れた疑いがあるときは、GitHubでそのトークンを **Delete** し、新しく発行して同じように登録します

## 8. 止めたいとき

- 一時的に止める: ダッシュボード → Worker → Settings → Trigger Events でcronを削除する（または`wrangler.toml`のcronを空にして`npx wrangler deploy`）
- 完全にやめる: `npx wrangler delete` でWorkerを消し、GitHubのトークンもDeleteする。GitHubのscheduleだけの運用に戻ります

## 休場日の表の更新（年に1回）

`lib.js`の`NYSE_HOLIDAYS`に翌年分を追記し、`python -m pytest tests/test_external_trigger_worker.py`
（pandas_market_calendarsのNYSEカレンダーと一致するかを確かめる）を通してから`npx wrangler deploy`します。

## ダッシュボードだけで設定する場合（Node.jsを使わない）

1. Workers & Pages → Create → Worker → 名前`on-a-journey-market-data-trigger`で作成（Hello Worldのまま）
2. Edit code で`worker.js`と`lib.js`の2ファイルを作り、このフォルダの内容を貼り付けてDeploy
3. Settings → Trigger Events → Cron Triggers に3本（`25 20,21 * * MON-FRI`・`55 20 * * MON-FRI`・`50 21 * * MON-FRI`）を追加
4. Settings → Variables and Secrets に`GH_DISPATCH_TOKEN`・`DISCORD_WEB_HOOK`をSecretとして追加
5. Settings → Domains & Routes で workers.dev を無効にする

この場合、`wrangler.toml`の内容は反映されないため、cron・設定を変えたときはダッシュボードでも同じように直してください。

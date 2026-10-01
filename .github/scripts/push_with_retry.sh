#!/usr/bin/env bash
# botのcommitをpushする共通の手順（全ワークフローの「git pull --rebase → git push」を置き換える）。
#
# 夜間は複数のワークフローがほぼ同時にkaihatsuへpushするため、pull --rebaseとpushの間に別のpushが入ると
# 拒否される（2026-09-30 23:57、Market PulseのpushとStonks Siloのpushが重なりStonks Siloの結果が保存されなかった）。
# pull --rebase と push を最大 PUSH_RETRY_MAX 回（既定3回）、間隔を空けて繰り返す。
# rebaseが衝突で止まった場合は中断してから次の回に進む。全回失敗したら終了コード1（ジョブを失敗にする）。
#
# 使い方: bash .github/scripts/push_with_retry.sh [ブランチ名（既定kaihatsu）]
# 環境変数: PUSH_RETRY_MAX（回数）・PUSH_RETRY_WAIT（1回目の後の待ち秒数。n回目の後はn倍、既定15秒）
set -u

branch="${1:-kaihatsu}"
max="${PUSH_RETRY_MAX:-3}"
wait="${PUSH_RETRY_WAIT:-15}"

for i in $(seq 1 "$max"); do
  if git pull --rebase origin "$branch" && git push origin "$branch"; then
    [ "$i" -gt 1 ] && echo "[push_with_retry] ${i}回目で成功"
    exit 0
  fi
  git rebase --abort >/dev/null 2>&1 || true
  if [ "$i" -lt "$max" ]; then
    echo "[push_with_retry] ${i}回目のpull --rebase/pushが失敗。$((wait * i))秒後に再試行"
    sleep $((wait * i))
  fi
done
echo "::error::[push_with_retry] ${max}回試してpushできませんでした（${branch}）"
exit 1

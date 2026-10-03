// tools/external_trigger/lib.js（worker.jsの中身。node --testで試せるよう分けている）
//
// Market Data Daily Updateの外部起動（Cloudflare Workers Cron Triggers、2026-10-03）。
// GitHubのscheduleは2026-10-01・10-02の2晩とも夜の枠（20:47〜23:17 UTC）で1本も起動しなかったため、
// 外からworkflow_dispatch（ref=kaihatsu、guard=true）で起動する。scheduleは保険として残している。
//
//   平日 20:25・20:55・21:25 UTC: workflow_dispatchを呼ぶ。応答が204以外ならDiscordに通知する
//     （ワークフロー側のガードが、引けから20分経っていない・取得済みの起動を何もせず終わらせる。
//       夏時間〈引け20:00 UTC〉は20:25、冬時間〈引け21:00 UTC〉は21:25で取得する）
//   平日 21:50 UTC: その日の引けの後（20:00 UTC以降）に作られて成功した実行が無ければ、もう一度起動してDiscordに通知する
//   NYSEの休場日（下のNYSE_HOLIDAYS）は何もしない
//
// secret（wrangler secret putで登録。値はリポジトリに置かない）:
//   GH_DISPATCH_TOKEN: fine-grained token（このリポジトリのみ・Actions: Read and write）
//   DISCORD_WEB_HOOK : Discordのwebhook URL（未登録なら通知はログに出すだけ）
// 設定手順はREADME.md。

export const OWNER = "Koichi-Shigihara2";
export const REPO = "On-a-journey";
export const WORKFLOW = "Market_Data_Daily_Update.yml";
export const REF = "kaihatsu";

// 引けの後の実行だけを数えるための基準（夏時間の引け20:00 UTC。冬時間は21:00だが、20時台の起動はガードで何もしないので成功にならない）。
// 同じUTCの日でも00:00〜19:59 UTCに作られた実行は前の晩（前日の足）の分なので数えない
export const AFTER_CLOSE_UTC = "20:00:00Z";

// NYSEの終日休場日（平日のみ）。tests/test_external_trigger_worker.pyがpandas_market_calendarsのNYSEカレンダーと一致することを確かめる。
// 年が足りなくなったら追記する（載っていない年は21:50の確認のたびにDiscordで知らせる）
export const NYSE_HOLIDAYS = {
  2026: ["01-01", "01-19", "02-16", "04-03", "05-25", "06-19", "07-03", "09-07", "11-26", "12-25"],
  2027: ["01-01", "01-18", "02-15", "03-26", "05-31", "06-18", "07-05", "09-06", "11-25", "12-24"],
  2028: ["01-17", "02-21", "04-14", "05-29", "06-19", "07-04", "09-04", "11-23", "12-25"],
};

const API = "https://api.github.com";

function ghHeaders(env) {
  return {
    Authorization: `Bearer ${env.GH_DISPATCH_TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "On-a-journey-external-trigger/1.0", // GitHub APIはUser-Agentが無いと拒否する
  };
}

export function utcDay(date) {
  return date.toISOString().slice(0, 10);
}

export function isHoliday(date) {
  const list = NYSE_HOLIDAYS[date.getUTCFullYear()];
  return Boolean(list && list.includes(utcDay(date).slice(5)));
}

export function isDeadlineCheck(date) {
  return date.getUTCHours() === 21 && date.getUTCMinutes() >= 45;
}

export async function dispatch(env, fetchFn) {
  const res = await fetchFn(`${API}/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
    method: "POST",
    headers: { ...ghHeaders(env), "Content-Type": "application/json" },
    body: JSON.stringify({ ref: REF, inputs: { guard: "true" } }),
  });
  return res.status;
}

// その日の引けの後に作られて成功した実行の数。確認できなければnull
export async function successfulRunsSinceClose(env, fetchFn, date) {
  const params = new URLSearchParams({
    branch: REF,
    status: "success",
    created: `>=${utcDay(date)}T${AFTER_CLOSE_UTC}`,
    per_page: "10",
  });
  const res = await fetchFn(`${API}/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/runs?${params}`, {
    headers: ghHeaders(env),
  });
  if (res.status !== 200) return { count: null, status: res.status };
  const body = await res.json();
  return { count: body.total_count ?? (body.workflow_runs || []).length, status: res.status };
}

export async function notify(env, fetchFn, text) {
  const content = `[Market Data Daily 外部起動] ${text}`;
  if (!env.DISCORD_WEB_HOOK) {
    console.log(`(DISCORD_WEB_HOOK未登録) ${content}`);
    return false;
  }
  try {
    const res = await fetchFn(env.DISCORD_WEB_HOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json", "User-Agent": "On-a-journey-notifier/1.0" },
      body: JSON.stringify({ content }),
    });
    return res.status === 200 || res.status === 204;
  } catch (e) {
    console.log(`Discord送信エラー: ${e && e.name}`); // webhook URL（トークンを含む）は出さない
    return false;
  }
}

function dispatchFailureHint(status) {
  if (status === 401) return "（トークンの期限切れ・取り消しの可能性。README.mdの「トークンの更新」を参照）";
  if (status === 403 || status === 404) return "（トークンの権限・対象リポジトリの設定を確認）";
  if (status === 422) return "（ワークフローの入力・ref=kaihatsuを確認）";
  return "";
}

export async function handleScheduled(date, env, fetchFn) {
  const day = utcDay(date);
  if (isHoliday(date)) {
    console.log(`${day}: NYSE休場日のため何もしない`);
    return { action: "holiday" };
  }
  const yearNote = NYSE_HOLIDAYS[date.getUTCFullYear()]
    ? ""
    : `\n※ ${date.getUTCFullYear()}年のNYSE休場日がworker.jsに無い（休場日も起動する。NYSE_HOLIDAYSに追記してデプロイし直す）`;

  if (!isDeadlineCheck(date)) {
    const status = await dispatch(env, fetchFn);
    console.log(`${date.toISOString()}: workflow_dispatch → HTTP ${status}`);
    if (status !== 204) {
      await notify(env, fetchFn, `${date.toISOString()} の起動に失敗: HTTP ${status}${dispatchFailureHint(status)}。GitHubのscheduleが保険で動く${yearNote}`);
    }
    return { action: "dispatch", status };
  }

  const check = await successfulRunsSinceClose(env, fetchFn, date);
  if (check.count && check.count > 0) {
    console.log(`${day}: 成功した実行あり（${check.count}本）`);
    if (yearNote) await notify(env, fetchFn, yearNote.trim());
    return { action: "deadline_ok", count: check.count };
  }
  const status = await dispatch(env, fetchFn);
  const why = check.count === null
    ? `実行の一覧を確認できなかった（HTTP ${check.status}）`
    : `${day} ${AFTER_CLOSE_UTC.slice(0, 5)} UTC以降に成功した実行が無い`;
  await notify(env, fetchFn, `21:50 UTCの確認: ${why}。もう一度起動した → HTTP ${status}${status === 204 ? "" : dispatchFailureHint(status)}${yearNote}`);
  return { action: "deadline_redispatch", status, count: check.count };
}

// tools/external_trigger/lib.test.js（node --test で実行。tests/test_external_trigger_worker.pyからも呼ぶ）
import assert from "node:assert/strict";
import { test } from "node:test";

import { handleScheduled, isHoliday, successfulRunsSinceClose } from "./lib.js";

const ENV = { GH_DISPATCH_TOKEN: "dummy-token", DISCORD_WEB_HOOK: "https://discord.invalid/api/webhooks/0/x" };

// 呼ばれたURL・中身を記録し、URLに応じた応答を返すfetch
function fakeFetch({ dispatchStatus = 204, runsStatus = 200, runsTotal = 0 } = {}) {
  const calls = [];
  const fn = async (url, init = {}) => {
    calls.push({ url, init });
    if (url.endsWith("/dispatches")) return { status: dispatchStatus };
    if (url.includes("/runs?")) return { status: runsStatus, json: async () => ({ total_count: runsTotal, workflow_runs: [] }) };
    if (url.startsWith("https://discord.invalid/")) return { status: 204 };
    throw new Error(`unexpected url ${url}`);
  };
  fn.calls = calls;
  fn.dispatches = () => calls.filter((c) => c.url.endsWith("/dispatches"));
  fn.discord = () => calls.filter((c) => c.url.startsWith("https://discord.invalid/"));
  return fn;
}

test("20:25 UTC: workflow_dispatchをref=kaihatsu・guard=trueで呼び、204なら通知しない", async () => {
  const f = fakeFetch();
  const r = await handleScheduled(new Date("2026-10-05T20:25:00Z"), ENV, f);
  assert.equal(r.action, "dispatch");
  assert.equal(f.dispatches().length, 1);
  const { url, init } = f.dispatches()[0];
  assert.equal(url, "https://api.github.com/repos/Koichi-Shigihara2/On-a-journey/actions/workflows/Market_Data_Daily_Update.yml/dispatches");
  assert.equal(init.method, "POST");
  assert.deepEqual(JSON.parse(init.body), { ref: "kaihatsu", inputs: { guard: "true" } });
  assert.equal(init.headers.Authorization, "Bearer dummy-token");
  assert.ok(init.headers["User-Agent"]);
  assert.equal(f.discord().length, 0);
});

test("起動の応答が204以外ならDiscordに通知する（401はトークンの更新を案内）", async () => {
  const f = fakeFetch({ dispatchStatus: 401 });
  const r = await handleScheduled(new Date("2026-10-05T20:55:00Z"), ENV, f);
  assert.equal(r.status, 401);
  assert.equal(f.discord().length, 1);
  const content = JSON.parse(f.discord()[0].init.body).content;
  assert.match(content, /HTTP 401/);
  assert.match(content, /トークン/);
  assert.ok(!content.includes("dummy-token"));
});

test("21:50 UTC: 引けの後に成功した実行があれば何もしない", async () => {
  const f = fakeFetch({ runsTotal: 1 });
  const r = await handleScheduled(new Date("2026-10-05T21:50:00Z"), ENV, f);
  assert.equal(r.action, "deadline_ok");
  assert.equal(f.dispatches().length, 0);
  assert.equal(f.discord().length, 0);
});

test("21:50 UTC: 成功した実行が無ければもう一度起動して通知する", async () => {
  const f = fakeFetch({ runsTotal: 0 });
  const r = await handleScheduled(new Date("2026-10-05T21:50:00Z"), ENV, f);
  assert.equal(r.action, "deadline_redispatch");
  assert.equal(f.dispatches().length, 1);
  assert.equal(f.discord().length, 1);
  assert.match(JSON.parse(f.discord()[0].init.body).content, /もう一度起動した → HTTP 204/);
});

test("21:50 UTC: 実行の一覧を確認できなければ（HTTP 401等）起動して通知する", async () => {
  const f = fakeFetch({ runsStatus: 401 });
  const r = await handleScheduled(new Date("2026-10-05T21:50:00Z"), ENV, f);
  assert.equal(r.action, "deadline_redispatch");
  assert.equal(r.count, null);
  assert.match(JSON.parse(f.discord()[0].init.body).content, /確認できなかった（HTTP 401）/);
});

test("21:50の確認は、その日の20:00 UTC以降に作られて成功したkaihatsuの実行だけを数える", async () => {
  const f = fakeFetch({ runsTotal: 0 });
  await successfulRunsSinceClose(ENV, f, new Date("2026-10-05T21:50:00Z"));
  const q = new URL(f.calls[0].url).searchParams;
  assert.equal(q.get("created"), ">=2026-10-05T20:00:00Z");
  assert.equal(q.get("status"), "success");
  assert.equal(q.get("branch"), "kaihatsu");
});

test("NYSEの休場日は何もしない", async () => {
  assert.equal(isHoliday(new Date("2026-11-26T20:25:00Z")), true); // 感謝祭
  assert.equal(isHoliday(new Date("2026-10-05T20:25:00Z")), false);
  const f = fakeFetch();
  const r = await handleScheduled(new Date("2026-11-26T21:50:00Z"), ENV, f);
  assert.equal(r.action, "holiday");
  assert.equal(f.calls.length, 0);
});

test("休場日の表に無い年は、起動はしつつ21:50にDiscordで知らせる", async () => {
  const f = fakeFetch({ runsTotal: 1 });
  const r = await handleScheduled(new Date("2029-10-01T21:50:00Z"), ENV, f);
  assert.equal(r.action, "deadline_ok");
  assert.equal(f.discord().length, 1);
  assert.match(JSON.parse(f.discord()[0].init.body).content, /2029年のNYSE休場日/);
});

test("DISCORD_WEB_HOOKが未登録でも落ちない", async () => {
  const f = fakeFetch({ dispatchStatus: 500 });
  const r = await handleScheduled(new Date("2026-10-05T20:25:00Z"), { GH_DISPATCH_TOKEN: "t" }, f);
  assert.equal(r.status, 500);
  assert.equal(f.discord().length, 0);
});

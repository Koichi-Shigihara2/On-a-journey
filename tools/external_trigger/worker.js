// tools/external_trigger/worker.js
//
// Market Data Daily Updateの外部起動（Cloudflare Workers Cron Triggers）。処理はlib.js、設定手順はREADME.md。
// メインモジュールの名前付きexportはWorkersでハンドラ・クラスとして扱われるため、ここではdefaultだけをexportする。

import { handleScheduled } from "./lib.js";

export default {
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(handleScheduled(new Date(controller.scheduledTime), env, fetch));
  },
};

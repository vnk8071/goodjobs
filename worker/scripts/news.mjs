#!/usr/bin/env node
/**
 * Trigger the news desk manually.
 *
 * Usage:
 *   NEWS_SECRET=... node scripts/news.mjs            # 2 articles (auto topics)
 *   NEWS_SECRET=... node scripts/news.mjs --topic ai-va-nghe-nghiep --per-run 1
 *   NEWS_SECRET=... node scripts/news.mjs --no-force   # skip if already ran today
 */

const BASE = (process.env.JOBS_WORKER_URL || "https://goodjobs.io.vn").replace(/\/+$/, "");
const SECRET = process.env.NEWS_SECRET || "";

const args = process.argv.slice(2);
const topicIndex = args.indexOf("--topic");
const perRunIndex = args.indexOf("--per-run");
const topic = topicIndex >= 0 ? args[topicIndex + 1] : "";
const perRun = perRunIndex >= 0 ? Number(args[perRunIndex + 1]) : undefined;
const force = !args.includes("--no-force");

if (!SECRET) {
  console.error("NEWS_SECRET is required");
  process.exit(1);
}

const response = await fetch(`${BASE}/api/news/generate`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: `Bearer ${SECRET}`,
  },
  body: JSON.stringify({ force, topic, perRun }),
});

const data = await response.json().catch(() => ({}));
console.log(JSON.stringify(data, null, 2));
if (!response.ok || data.ok === false) process.exit(1);

#!/usr/bin/env node
/**
 * Sync jobs from the Good Jobs API (Redis cache) into Cloudflare D1 via the
 * Worker's /api/jobs/sync endpoint.
 *
 * Run locally:
 *   JOBS_WORKER_URL=https://goodjobs.io.vn SYNC_SECRET=... node worker/scripts/sync.mjs
 *
 * GitHub Actions runs this on a schedule (see .github/workflows/sync-d1.yml).
 * Search still reads from Redis; D1 only powers permanent /jobs/<slug> pages.
 */

import { createHash } from "node:crypto";

const API_BASE = (process.env.JOBS_API_BASE || "https://api.goodjobs.io.vn").replace(/\/+$/, "");
const WORKER_URL = (process.env.JOBS_WORKER_URL || "").replace(/\/+$/, "");
const SYNC_SECRET = process.env.SYNC_SECRET || "";
const BATCH_SIZE = Math.max(1, Math.min(20, Number(process.env.BATCH_SIZE) || 10));

if (!WORKER_URL || !SYNC_SECRET) {
  console.error("Missing JOBS_WORKER_URL or SYNC_SECRET.");
  process.exit(1);
}

function sha256(value) {
  return createHash("sha256").update(String(value ?? "")).digest("hex");
}

function slugify(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function normalizeJob(job) {
  return {
    title: String(job.title || "").trim(),
    company: String(job.company || "").trim(),
    location: String(job.location || "").trim(),
    source: String(job.source || "").trim(),
    posted: String(job.posted || "").trim(),
    posted_ts: Number(job.posted_ts) || 0,
    posted_date: String(job.posted_date || "").trim(),
    description: String(job.description || ""),
    skills: Array.isArray(job.skills) ? job.skills : [],
    logo: String(job.logo || "").trim(),
    link: String(job.link || "").trim(),
  };
}

function contentHash(job) {
  // Keep this key order identical to contentHash() in worker/src/index.js.
  return sha256(JSON.stringify({
    title: job.title,
    company: job.company,
    location: job.location,
    source: job.source,
    posted: job.posted,
    posted_ts: Number(job.posted_ts) || 0,
    posted_date: job.posted_date,
    description: job.description,
    skills: job.skills,
    logo: job.logo,
  }));
}

const INVALID_POSTED = /^(?:-|—|none|null|n\/a|unknown|not specified)?$/i;

function relativeFromTs(ts) {
  const diff = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (diff < 86400) return "Today";
  const days = Math.floor(diff / 86400);
  if (days === 1) return "1 day ago";
  if (days < 7) return `${days} days ago`;
  const weeks = Math.max(1, Math.floor(days / 7));
  if (days < 30) return `${weeks} week${weeks !== 1 ? "s" : ""} ago`;
  const months = Math.max(1, Math.floor(days / 30));
  if (days < 365) return `${months} month${months !== 1 ? "s" : ""} ago`;
  const years = Math.max(1, Math.floor(days / 365));
  return `${years} year${years !== 1 ? "s" : ""} ago`;
}

/** Fall back to the cache/scrape time when the source has no posted date. */
function normalizePosted(job, raw) {
  let ts = Number(job.posted_ts) || 0;
  if (!ts && job.posted_date) {
    const parsed = Date.parse(`${String(job.posted_date).slice(0, 10)}T00:00:00Z`);
    if (!Number.isNaN(parsed)) ts = Math.floor(parsed / 1000);
  }
  if (!ts) ts = Number(raw._scraped_ts) || Math.floor(Date.now() / 1000);
  const posted = job.posted && !INVALID_POSTED.test(String(job.posted).trim())
    ? job.posted
    : relativeFromTs(ts);
  return {
    posted,
    posted_ts: ts,
    posted_date: job.posted_date || new Date(ts * 1000).toISOString().slice(0, 10),
  };
}

function prepareJob(raw) {
  const job = normalizeJob(raw);
  if (!job.link || !job.title) return null;
  const id = sha256(job.link).slice(0, 24);
  const slug = `${slugify(job.title) || "job"}-${id.slice(0, 8)}`;
  return {
    id,
    slug,
    title: job.title,
    company: job.company,
    location: job.location,
    source: job.source,
    ...normalizePosted(job, raw),
    description: job.description,
    skills: job.skills,
    logo: job.logo,
    link: job.link,
    content_hash: contentHash(job),
  };
}

async function fetchJobs() {
  const response = await fetch(`${API_BASE}/export-jobs?n=100000`, {
    headers: { "User-Agent": "GoodJobs-D1-Sync/1.0" },
    signal: AbortSignal.timeout(90000),
  });
  if (!response.ok) {
    throw new Error(`export-jobs failed: ${response.status} ${response.statusText}`);
  }
  const data = await response.json();
  if (!Array.isArray(data.jobs)) throw new Error("export-jobs returned no jobs array");
  return data.jobs;
}

async function postBatch(batch, attempt = 1) {
  try {
    const response = await fetch(`${WORKER_URL}/api/jobs/sync`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${SYNC_SECRET}`,
      },
      body: JSON.stringify({ jobs: batch }),
      signal: AbortSignal.timeout(60000),
    });
    if (!response.ok) {
      if (attempt < 4 && (response.status === 429 || response.status >= 500)) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 1500));
        return postBatch(batch, attempt + 1);
      }
      const text = await response.text().catch(() => "");
      throw new Error(`sync batch failed: ${response.status} ${text.slice(0, 200)}`);
    }
    return response.json();
  } catch (error) {
    if (attempt < 4) {
      console.warn(`  batch attempt ${attempt} failed (${error.message || error}); retrying...`);
      await new Promise((resolve) => setTimeout(resolve, attempt * 1500));
      return postBatch(batch, attempt + 1);
    }
    throw error;
  }
}

async function main() {
  console.log(`Fetching jobs from ${API_BASE}/export-jobs ...`);
  const rawJobs = await fetchJobs();
  const jobs = rawJobs.map(prepareJob).filter(Boolean);
  console.log(`Prepared ${jobs.length} jobs (${rawJobs.length - jobs.length} skipped).`);

  let written = 0;
  let skipped = 0;
  for (let i = 0; i < jobs.length; i += BATCH_SIZE) {
    const batch = jobs.slice(i, i + BATCH_SIZE);
    const result = await postBatch(batch);
    written += Number(result.written) || 0;
    skipped += Number(result.skipped) || 0;
    console.log(`  ${i + batch.length}/${jobs.length} — wrote ${written}, skipped ${skipped}`);
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
  console.log(`Done. Wrote ${written}, skipped ${skipped}.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

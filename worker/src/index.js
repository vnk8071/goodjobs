/**
 * Good Jobs — Cloudflare Worker + D1
 *
 * Serves permanent job pages at /jobs/<slug> and the small API used by the
 * homepage share button. Search itself still uses Redis via the FastAPI
 * backend; D1 is the durable store for job pages and sharing.
 */

import { handleNewsApi, handleNewsRequest, runNewsJob } from "./news/index.js";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

function json(data, status = 200, env = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...CORS_HEADERS,
    },
  });
}

function html(body, status = 200, env = {}) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=300, s-maxage=3600",
      ...CORS_HEADERS,
    },
  });
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => {
    switch (char) {
      case "&": return "&amp;";
      case "<": return "&lt;";
      case ">": return "&gt;";
      case '"': return "&quot;";
      default: return "&#39;";
    }
  });
}

function plainText(value) {
  return String(value ?? "")
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/\s*(p|div|li|h[1-6]|tr|section|article)\s*>/gi, "\n")
    .replace(/<\s*li[^>]*>/gi, "• ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function paragraphs(text, limit = 12000) {
  const cleaned = text.slice(0, limit);
  return cleaned
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => `<p>${esc(part).replace(/\n/g, "<br>")}</p>`)
    .join("");
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

async function sha256Hex(value) {
  const bytes = new TextEncoder().encode(String(value ?? ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function jobIdentity(job) {
  const id = (await sha256Hex(job.link || "")).slice(0, 24);
  return { id, slug: `${slugify(job.title) || "job"}-${id.slice(0, 8)}` };
}

async function contentHash(job) {
  const stable = {
    title: job.title || "",
    company: job.company || "",
    location: job.location || "",
    source: job.source || "",
    posted: job.posted || "",
    posted_ts: Number(job.posted_ts) || 0,
    posted_date: job.posted_date || "",
    description: job.description || "",
    skills: job.skills || [],
    logo: job.logo || "",
  };
  return sha256Hex(JSON.stringify(stable));
}

function parsedSkills(job) {
  try {
    const value = JSON.parse(job.skills || "[]");
    return Array.isArray(value) ? value.filter(Boolean) : [];
  } catch {
    return [];
  }
}

/** Some sources store "N/A"/"Unknown" as the company; don't show that to users. */
function displayCompany(job) {
  const value = String(job?.company ?? "").trim();
  if (!value || /^(n\/?a|unknown|none|null|-+)$/i.test(value)) {
    return "Company not specified";
  }
  return value;
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

function pageStyles() {
  return `
    :root {
      color-scheme: light;
      --bg: #ffffff; --card: #ffffff; --border: #e5e5e5;
      --text: #1a1a1a; --muted: #666666; --primary: #2563eb;
      --primary-soft: #dbeafe; --primary-strong: #1d4ed8;
    }
    html[data-theme="dark"] {
      color-scheme: dark;
      --bg: #0f0f0f; --card: #1a1a1a; --border: #2a2a2a;
      --text: #f5f5f5; --muted: #a3a3a3; --primary: #60a5fa;
      --primary-soft: rgba(59,130,246,.15); --primary-strong: #93c5fd;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; background: var(--bg); color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      line-height: 1.6; letter-spacing: -.01em;
    }
    a { color: var(--primary); }
    .wrap { width: min(920px, calc(100% - 2rem)); margin: 0 auto; }
    header { border-bottom: 1px solid var(--border); background: var(--card); }
    .header-inner { display: flex; align-items: center; justify-content: space-between; gap: 1rem; padding: 1rem 0; }
    .brand { display: inline-flex; align-items: center; gap: .65rem; color: var(--text); text-decoration: none; font-weight: 800; font-size: 1.15rem; }
    .brand img { width: 36px; height: 36px; border-radius: 9px; }
    .nav { font-size: .84rem; color: var(--muted); text-decoration: none; }
    .nav:hover { color: var(--primary); }
    main { padding: 2.25rem 0 3rem; }
    .back { display: inline-block; margin-bottom: 1.25rem; font-size: .84rem; color: var(--muted); text-decoration: none; }
    .back:hover { color: var(--primary); }
    .job-card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: clamp(1.25rem, 3vw, 2.25rem); box-shadow: 0 16px 48px -30px rgba(15,23,42,.25); }
    .job-head { display: flex; gap: 1rem; align-items: center; margin-bottom: 1rem; }
    .job-logo { width: 54px; height: 54px; border-radius: 12px; object-fit: contain; background: #fff; border: 1px solid var(--border); }
    h1 { font-size: clamp(1.5rem, 3vw, 2.1rem); line-height: 1.15; letter-spacing: -.035em; margin: 0 0 .35rem; }
    .company { font-size: 1rem; font-weight: 600; color: var(--text); margin: 0; }
    .meta { display: flex; flex-wrap: wrap; gap: .5rem 1rem; margin: .75rem 0 1rem; color: var(--muted); font-size: .84rem; }
    .meta span { display: inline-flex; align-items: center; gap: .35rem; }
    .badges { display: flex; flex-wrap: wrap; gap: .4rem; margin-bottom: 1.25rem; }
    .badge { display: inline-flex; padding: .25rem .65rem; border-radius: 99px; background: var(--primary-soft); color: var(--primary-strong); font-size: .74rem; font-weight: 600; }
    .actions { display: flex; flex-wrap: wrap; gap: .65rem; margin: 1.25rem 0 1.5rem; }
    .btn { display: inline-flex; align-items: center; justify-content: center; gap: .45rem; min-height: 44px; padding: 0 1.2rem; border-radius: 10px; font-size: .9rem; font-weight: 700; text-decoration: none; }
    .btn-primary { background: var(--primary); color: #fff; box-shadow: 0 8px 22px -8px rgba(37,99,235,.55); }
    .btn-primary:hover { background: var(--primary-strong); }
    .btn-ghost { border: 1px solid var(--border); color: var(--text); background: var(--card); }
    .btn-ghost:hover { border-color: var(--primary); color: var(--primary); }
    .divider { border: 0; border-top: 1px solid var(--border); margin: 1.5rem 0; }
    h2 { font-size: 1.1rem; letter-spacing: -.02em; margin: 1.5rem 0 .65rem; }
    .description p { margin: 0 0 .8rem; color: var(--text); }
    .description { font-size: .93rem; }
    footer { border-top: 1px solid var(--border); padding: 1.5rem 0 2rem; color: var(--muted); font-size: .78rem; }
    footer .wrap { display: flex; flex-wrap: wrap; justify-content: space-between; gap: .75rem; }
    .not-found { text-align: center; padding: 4rem 0; }
    .not-found h1 { margin-bottom: .5rem; }
  `;
}

function layout({ title, description, canonical, bodyHtml, jsonLd }) {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script>
    (function () {
      try {
        var stored = localStorage.getItem("theme");
        document.documentElement.setAttribute("data-theme", stored === "dark" ? "dark" : "light");
      } catch (e) {
        document.documentElement.setAttribute("data-theme", "light");
      }
    })();
  </script>
  <title>${esc(title)}</title>
  <meta name="description" content="${esc(description)}">
  <link rel="canonical" href="${esc(canonical)}">
  <meta name="robots" content="index,follow,max-image-preview:large">
  <meta property="og:type" content="website">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(description)}">
  <meta property="og:url" content="${esc(canonical)}">
  <meta property="og:image" content="https://goodjobs.io.vn/og-image.jpg">
  <meta name="twitter:card" content="summary_large_image">
  <link rel="icon" type="image/svg+xml" href="/favicon.svg">
  <style>${pageStyles()}</style>
  ${jsonLd ? `<script type="application/ld+json">${JSON.stringify(jsonLd)}</script>` : ""}
</head>
<body>
  <header>
    <div class="wrap header-inner">
      <a class="brand" href="/"><img src="/favicon.svg" alt="Good Jobs" width="36" height="36">Good Jobs</a>
      <a class="nav" href="/">Search jobs →</a>
    </div>
  </header>
  <main class="wrap">${bodyHtml}</main>
  <footer>
    <div class="wrap">
      <span>Good Jobs — job aggregation for Vietnam and beyond.</span>
      <span><a href="/">goodjobs.io.vn</a></span>
    </div>
  </footer>
</body>
</html>`;
}

function jobPageHtml(job, canonical) {
  const skills = parsedSkills(job);
  const text = plainText(job.description);
  const logo = job.logo
    ? `<img class="job-logo" src="${esc(job.logo)}" alt="${esc(displayCompany(job))}">`
    : `<img class="job-logo" src="/favicon.svg" alt="Good Jobs">`;
  const description = text
    ? paragraphs(text)
    : `<p>No description is available for this listing yet. Open the original posting for full details.</p>`;
  const metaDescription = text.slice(0, 158) || `${job.title} at ${displayCompany(job)} in ${job.location || "Vietnam"}.`;
  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    title: job.title,
    description: text.slice(0, 5000),
    datePosted: job.posted_date || (job.posted_ts ? new Date(job.posted_ts * 1000).toISOString().slice(0, 10) : undefined),
    hiringOrganization: { "@type": "Organization", name: displayCompany(job) },
    jobLocation: {
      "@type": "Place",
      address: { "@type": "PostalAddress", addressLocality: job.location || "Vietnam", addressCountry: "VN" },
    },
    url: canonical,
    directApply: false,
  };
  const body = `
    <a class="back" href="/">← Back to Good Jobs</a>
    <article class="job-card">
      <div class="job-head">
        ${logo}
        <div>
          <h1>${esc(job.title)}</h1>
          <p class="company">${esc(displayCompany(job))}</p>
        </div>
      </div>
      <div class="meta">
        <span>📍 ${esc(job.location || "Vietnam")}</span>
        <span>🕒 ${esc(job.posted || "Recently")}</span>
        <span>🌐 ${esc(job.source || "Good Jobs")}</span>
      </div>
      ${skills.length ? `<div class="badges">${skills.slice(0, 20).map((skill) => `<span class="badge">${esc(skill)}</span>`).join("")}</div>` : ""}
      <div class="actions">
        <a class="btn btn-primary" href="${esc(job.link)}" target="_blank" rel="noopener noreferrer">Apply now ↗</a>
        <a class="btn btn-ghost" href="/?kw=${encodeURIComponent(job.title)}">Search similar jobs</a>
      </div>
      <hr class="divider">
      <h2>Job description</h2>
      <div class="description">${description}</div>
    </article>`;
  return { body, metaDescription, jsonLd };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (path.startsWith("/api/news/")) {
      return handleNewsApi(request, env, url);
    }

    if (path === "/news" || path.startsWith("/news/") || path === "/sitemap-news.xml") {
      return handleNewsRequest(request, env, url);
    }

    if (path.startsWith("/api/jobs/")) {
      if (path === "/api/jobs/ensure") return handleEnsure(url, env);
      if (path === "/api/jobs/by-link") return handleByLink(url, env);
      if (path === "/api/jobs/sync" && request.method === "POST") return handleSync(request, env);
      if (path === "/api/jobs/health") return handleHealth(env);
      const slug = decodeURIComponent(path.slice("/api/jobs/".length));
      return handleApiJob(slug, env);
    }

    if (path.startsWith("/jobs/")) {
      const slug = decodeURIComponent(path.slice("/jobs/".length)).replace(/\/+$/, "");
      return handleJobPage(slug, url, env);
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(
      runNewsJob(env)
        .then((result) => console.log("news run", JSON.stringify(result).slice(0, 2000)))
        .catch((error) => console.error("news run failed", String(error))),
    );
  },
};

async function handleHealth(env) {
  try {
    const row = await env.DB.prepare("SELECT COUNT(*) AS count FROM jobs").first();
    return json({ ok: true, jobs: row?.count ?? 0 }, 200, env);
  } catch (error) {
    return json({ ok: false, error: String(error) }, 500, env);
  }
}

async function handleApiJob(slug, env) {
  if (!slug) return json({ error: "slug is required" }, 400, env);
  const job = await env.DB.prepare("SELECT * FROM jobs WHERE slug = ?").bind(slug).first();
  if (!job) return json({ error: "job not found" }, 404, env);
  return json({ ...job, skills: parsedSkills(job) }, 200, env);
}

async function handleByLink(url, env) {
  const link = (url.searchParams.get("link") || "").trim();
  if (!link) return json({ error: "link is required" }, 400, env);
  const row = await env.DB.prepare("SELECT slug FROM jobs WHERE link = ?").bind(link).first();
  if (!row) return json({ error: "job not found" }, 404, env);
  return json({ slug: row.slug, url: `${url.origin}/jobs/${row.slug}` }, 200, env);
}

async function handleEnsure(url, env) {
  const link = (url.searchParams.get("link") || "").trim();
  const cacheKey = (url.searchParams.get("key") || "").trim();
  if (!link.startsWith("http")) return json({ error: "invalid link" }, 400, env);

  const existing = await env.DB.prepare("SELECT slug FROM jobs WHERE link = ?").bind(link).first();
  if (existing) {
    return json({ slug: existing.slug, url: `${url.origin}/jobs/${existing.slug}` }, 200, env);
  }

  const apiBase = env.JOBS_API_BASE || "https://api.goodjobs.io.vn";
  let backendUrl = `${apiBase}/job?link=${encodeURIComponent(link)}`;
  if (cacheKey) backendUrl += `&key=${encodeURIComponent(cacheKey)}`;
  const response = await fetch(backendUrl, {
    headers: { "User-Agent": "GoodJobs-Worker/1.0" },
  });
  if (!response.ok) return json({ error: "job not found" }, 404, env);

  const raw = normalizeJob(await response.json());
  if (!raw.link || !raw.title) return json({ error: "invalid job" }, 422, env);

  const identity = await jobIdentity(raw);
  const hash = await contentHash(raw);
  await env.DB.prepare(
    `INSERT INTO jobs
      (id, slug, link, title, company, location, source, posted, posted_ts, posted_date, description, skills, logo, content_hash)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(link) DO NOTHING`,
  ).bind(
    identity.id, identity.slug, raw.link, raw.title, raw.company, raw.location,
    raw.source, raw.posted, raw.posted_ts, raw.posted_date, raw.description,
    JSON.stringify(raw.skills), raw.logo, hash,
  ).run();

  const row = await env.DB.prepare("SELECT slug FROM jobs WHERE link = ?").bind(link).first();
  const slug = row?.slug || identity.slug;
  return json({ slug, url: `${url.origin}/jobs/${slug}` }, 200, env);
}

async function handleSync(request, env) {
  const auth = request.headers.get("Authorization") || "";
  if (!env.SYNC_SECRET || auth !== `Bearer ${env.SYNC_SECRET}`) {
    return json({ error: "unauthorized" }, 401, env);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "invalid json" }, 400, env);
  }
  const jobs = Array.isArray(body?.jobs) ? body.jobs.slice(0, 100) : [];
  if (jobs.length === 0) return json({ written: 0, skipped: 0 }, 200, env);

  const links = jobs.map((job) => job.link).filter(Boolean);
  const existing = new Map();
  for (let i = 0; i < links.length; i += 50) {
    const chunk = links.slice(i, i + 50);
    const placeholders = chunk.map(() => "?").join(",");
    const result = await env.DB.prepare(
      `SELECT link, content_hash FROM jobs WHERE link IN (${placeholders})`,
    ).bind(...chunk).all();
    for (const row of result.results || []) existing.set(row.link, row.content_hash);
  }

  const statements = [];
  for (const job of jobs) {
    if (!job.link || !job.title || !job.slug || !job.id) continue;
    if (existing.get(job.link) === job.content_hash) continue;
    statements.push(
      env.DB.prepare(
        `INSERT INTO jobs
          (id, slug, link, title, company, location, source, posted, posted_ts, posted_date, description, skills, logo, content_hash)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(link) DO UPDATE SET
           title=excluded.title, company=excluded.company, location=excluded.location,
           source=excluded.source, posted=excluded.posted, posted_ts=excluded.posted_ts,
           posted_date=excluded.posted_date, description=excluded.description,
           skills=excluded.skills, logo=excluded.logo, content_hash=excluded.content_hash,
           updated_at=datetime('now')`,
      ).bind(
        job.id, job.slug, job.link, job.title, job.company || "", job.location || "",
        job.source || "", job.posted || "", Number(job.posted_ts) || 0, job.posted_date || "",
        job.description || "", JSON.stringify(job.skills || []), job.logo || "",
        job.content_hash || "",
      ),
    );
  }

  for (let i = 0; i < statements.length; i += 50) {
    await env.DB.batch(statements.slice(i, i + 50));
  }
  return json({ written: statements.length, skipped: jobs.length - statements.length }, 200, env);
}

async function handleJobPage(slug, url, env) {
  if (!slug) return html("Not found", 404, env);
  const job = await env.DB.prepare("SELECT * FROM jobs WHERE slug = ?").bind(slug).first();
  if (!job) {
    const link = (url.searchParams.get("link") || "").trim();
    if (link) {
      const ensureUrl = new URL(url);
      ensureUrl.pathname = "/api/jobs/ensure";
      ensureUrl.search = `?link=${encodeURIComponent(link)}`;
      const response = await handleEnsure(ensureUrl, env);
      if (response.ok) {
        const data = await response.json();
        return Response.redirect(data.url, 302);
      }
    }
    const body = `
      <div class="not-found">
        <h1>Job not found</h1>
        <p>This job may have expired or the link is incorrect.</p>
        <a class="btn btn-primary" href="/">Search jobs on Good Jobs</a>
      </div>`;
    return html(layout({
      title: "Job not found — Good Jobs",
      description: "This job listing is no longer available.",
      canonical: `${url.origin}/jobs/${slug}`,
      bodyHtml: body,
      jsonLd: null,
    }), 404, env);
  }

  const canonical = `${url.origin}/jobs/${job.slug}`;
  const page = jobPageHtml(job, canonical);
  return html(layout({
    title: `${job.title} — ${displayCompany(job)}`,
    description: page.metaDescription,
    canonical,
    bodyHtml: page.body,
    jsonLd: page.jsonLd,
  }), 200, env);
}

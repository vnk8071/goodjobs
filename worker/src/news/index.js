/** News routing: pages, RSS, sitemap and the generation/admin API. */

import { runNewsJob, publishToLinkedIn } from "./generate.js";
import { debugFeed, debugSources } from "./sources.js";
import { topicBySlug } from "./config.js";

export { runNewsJob };
import { renderArticlePage, renderNewsIndex, renderNewsNotFound, renderNewsSitemap, renderRss } from "./render.js";
import { html, json, xml } from "./util.js";

const PAGE_SIZE = 10;

function authorized(request, url, env) {
  const secret = env.NEWS_SECRET || env.SYNC_SECRET || "";
  if (!secret) return false;
  const header = request.headers.get("Authorization") || "";
  const bearer = header.startsWith("Bearer ") ? header.slice(7) : "";
  const query = url.searchParams.get("secret") || "";
  return bearer === secret || query === secret;
}

async function getPublished(env, limit, offset = 0, category = "") {
  const sql = category
    ? "SELECT * FROM news_articles WHERE status = 'published' AND category = ? ORDER BY published_at DESC LIMIT ? OFFSET ?"
    : "SELECT * FROM news_articles WHERE status = 'published' ORDER BY published_at DESC LIMIT ? OFFSET ?";
  const stmt = env.DB.prepare(sql);
  const bound = category ? stmt.bind(category, limit, offset) : stmt.bind(limit, offset);
  const result = await bound.all();
  return result.results || [];
}

async function countPublished(env, category = "") {
  const sql = category
    ? "SELECT COUNT(*) AS n FROM news_articles WHERE status = 'published' AND category = ?"
    : "SELECT COUNT(*) AS n FROM news_articles WHERE status = 'published'";
  const stmt = env.DB.prepare(sql);
  const row = category ? await stmt.bind(category).first() : await stmt.first();
  return Number(row?.n) || 0;
}

async function categoryCounts(env) {
  try {
    const result = await env.DB.prepare(
      "SELECT category, COUNT(*) AS n FROM news_articles WHERE status = 'published' GROUP BY category ORDER BY n DESC",
    ).all();
    return (result.results || []).map((r) => ({ category: r.category, count: Number(r.n) || 0 }));
  } catch {
    return [];
  }
}

async function renderIndex(env, url) {
  const page = Math.max(1, Number(url.searchParams.get("page")) || 1);
  const category = (url.searchParams.get("category") || "").trim();
  const [total, categories] = await Promise.all([
    countPublished(env, category),
    categoryCounts(env),
  ]);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const articles = await getPublished(env, PAGE_SIZE, (safePage - 1) * PAGE_SIZE, category);
  const body = renderNewsIndex({
    articles,
    page: safePage,
    totalPages,
    categories,
    activeCategory: category,
  });
  return html(body, 200, "public, max-age=300, s-maxage=900");
}

async function renderArticle(env, slug) {
  const article = await env.DB.prepare(
    "SELECT * FROM news_articles WHERE slug = ? AND status = 'published'",
  )
    .bind(slug)
    .first();
  if (!article) return html(renderNewsNotFound(), 404, "no-store");

  let related = [];
  try {
    const result = await env.DB.prepare(
      "SELECT slug, title, category, published_at FROM news_articles WHERE status = 'published' AND slug <> ? AND category = ? ORDER BY published_at DESC LIMIT 3",
    )
      .bind(slug, article.category)
      .all();
    related = result.results || [];
    if (related.length < 3) {
      const more = await env.DB.prepare(
        "SELECT slug, title, category, published_at FROM news_articles WHERE status = 'published' AND slug <> ? ORDER BY published_at DESC LIMIT 6",
      )
        .bind(slug)
        .all();
      const seen = new Set(related.map((r) => r.slug));
      for (const row of more.results || []) {
        if (related.length >= 3) break;
        if (seen.has(row.slug)) continue;
        related.push(row);
      }
    }
  } catch {
    related = [];
  }

  let sources = [];
  try {
    sources = JSON.parse(article.sources || "[]");
    if (!Array.isArray(sources)) sources = [];
  } catch {
    sources = [];
  }

  return html(renderArticlePage(article, related, sources), 200, "public, max-age=300, s-maxage=1800");
}

export async function handleNewsRequest(request, env, url) {
  const path = url.pathname;

  if (path === "/news/rss.xml") {
    const articles = await getPublished(env, 30);
    return xml(renderRss(articles));
  }

  if (path === "/sitemap-news.xml") {
    const articles = await getPublished(env, 1000);
    return xml(renderNewsSitemap(articles));
  }

  if (path === "/news" || path === "/news/") {
    return renderIndex(env, url);
  }

  if (path.startsWith("/news/")) {
    const slug = decodeURIComponent(path.slice("/news/".length)).replace(/\/+$/, "");
    if (!slug) return renderIndex(env, url);
    return renderArticle(env, slug);
  }

  return html(renderNewsNotFound(), 404, "no-store");
}

export async function handleNewsApi(request, env, url) {
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204 });
  }

  const path = url.pathname;

  if (path === "/api/news/health") {
    try {
      const published = await countPublished(env);
      const drafts = await env.DB.prepare("SELECT COUNT(*) AS n FROM news_articles WHERE status = 'draft'").first();
      const lastRun = await env.DB.prepare(
        "SELECT run_date, status, generated, published, skipped, finished_at FROM news_runs ORDER BY started_at DESC LIMIT 1",
      ).first();
      return json({ ok: true, published, drafts: Number(drafts?.n) || 0, lastRun: lastRun || null });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  if (path === "/api/news/list") {
    const limit = Math.max(1, Math.min(50, Number(url.searchParams.get("limit")) || 20));
    const status = (url.searchParams.get("status") || "published").trim();
    const isAuthed = authorized(request, url, env);
    if (status !== "published" && !isAuthed) {
      return json({ error: "unauthorized" }, 401);
    }
    try {
      const stmt = env.DB.prepare(
        "SELECT id, slug, title, dek, category, status, published_at, linkedin_status FROM news_articles WHERE status = ? ORDER BY published_at DESC LIMIT ?",
      );
      const result = await stmt.bind(status, limit).all();
      return json({ ok: true, articles: result.results || [] });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  if (path === "/api/news/debug-sources") {
    if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
    const topic = topicBySlug(url.searchParams.get("topic") || "");
    if (!topic) return json({ error: "unknown topic" }, 400);
    try {
      return json({ ok: true, topic: topic.slug, feeds: await debugSources(topic) });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  if (path === "/api/news/debug-feed") {
    if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
    const target = url.searchParams.get("url") || "";
    try {
      return json({ ok: true, ...(await debugFeed(target)) });
    } catch (error) {
      return json({ ok: false, error: String(error).slice(0, 300) }, 400);
    }
  }

  if (path === "/api/news/linkedin-drafts") {
    if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
    const limit = Math.max(1, Math.min(50, Number(url.searchParams.get("limit")) || 10));
    try {
      const result = await env.DB.prepare(
        "SELECT slug, title, linkedin_text, linkedin_status, published_at FROM news_articles WHERE status = 'published' ORDER BY published_at DESC LIMIT ?",
      )
        .bind(limit)
        .all();
      return json({ ok: true, drafts: result.results || [] });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  if (path === "/api/news/generate" && request.method === "POST") {
    if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
    let body = {};
    try {
      body = await request.json();
    } catch {
      body = {};
    }
    try {
      const result = await runNewsJob(env, {
        force: body.force === true || url.searchParams.get("force") === "1",
        perRun: body.perRun,
        topicSlug: body.topic || url.searchParams.get("topic") || "",
      });
      return json({ ok: true, ...result });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  if (path === "/api/news/linkedin-post" && request.method === "POST") {
    if (!authorized(request, url, env)) return json({ error: "unauthorized" }, 401);
    let body = {};
    try {
      body = await request.json();
    } catch {
      body = {};
    }
    const slug = String(body.slug || "").trim();
    if (!slug) return json({ error: "slug is required" }, 400);
    if (!env.LINKEDIN_ACCESS_TOKEN || !env.LINKEDIN_ORG_ID) {
      return json({ error: "LinkedIn API is not configured", hint: "Set LINKEDIN_ACCESS_TOKEN and LINKEDIN_ORG_ID secrets" }, 400);
    }
    try {
      const article = await env.DB.prepare("SELECT * FROM news_articles WHERE slug = ?").bind(slug).first();
      if (!article) return json({ error: "article not found" }, 404);
      const result = await publishToLinkedIn(env, article);
      await env.DB.prepare("UPDATE news_articles SET linkedin_status = ?, linkedin_url = ?, updated_at = ? WHERE slug = ?")
        .bind(result.status, result.postId ? `urn:li:share:${result.postId}` : "", new Date().toISOString(), slug)
        .run();
      return json({ ok: result.status === "posted", ...result });
    } catch (error) {
      return json({ ok: false, error: String(error) }, 500);
    }
  }

  return json({ error: "not found" }, 404);
}

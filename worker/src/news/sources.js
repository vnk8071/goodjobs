/** News discovery: Google News RSS + curated Vietnamese RSS feeds. */

import { BING_NEWS_URL, CURATED_FEEDS } from "./config.js";
import { decodeEntities, normalizeTitle, sha256Hex, stripHtml } from "./util.js";

const USER_AGENT = "GoodJobsNewsBot/1.0 (+https://goodjobs.io.vn/news)";
const MAX_AGE_DAYS = 10;

function bingNewsUrl(query) {
  return `${BING_NEWS_URL}${encodeURIComponent(query)}`;
}

function googleNewsUrl(query) {
  const params = new URLSearchParams({
    q: `${query} when:7d`,
    hl: "vi",
    gl: "VN",
    ceid: "VN:vi",
  });
  return `https://news.google.com/rss/search?${params.toString()}`;
}

async function fetchText(url, attempts = 2) {
  let lastError = null;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT, Accept: "application/rss+xml, application/xml, text/xml" },
        signal: controller.signal,
        cf: { cacheTtl: 600, cacheEverything: true },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (error) {
      lastError = error;
      if (attempt === 0) await new Promise((resolve) => setTimeout(resolve, 700));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error("fetch failed");
}

function tagValue(block, tag) {
  const re = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)</${tag}>`, "i");
  const match = block.match(re);
  return match ? decodeEntities(match[1]).trim() : "";
}

function tagAttr(block, tag, attr) {
  const re = new RegExp(`<${tag}\\b[^>]*\\b${attr}=["']([^"']+)["'][^>]*>`, "i");
  const match = block.match(re);
  return match ? decodeEntities(match[1]).trim() : "";
}

function parseRss(xml) {
  const items = [];
  const re = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
  let match;
  while ((match = re.exec(xml))) {
    const block = match[1];
    const title = tagValue(block, "title");
    const link = tagValue(block, "link") || tagAttr(block, "link", "href");
    if (!title || !link) continue;
    const publisher = tagValue(block, "source");
    const pubDate = tagValue(block, "pubDate") || tagValue(block, "published");
    const summary = stripHtml(tagValue(block, "description") || tagValue(block, "summary")).slice(0, 320);
    items.push({ title, link, publisher, pubDate, summary });
  }
  return items;
}

function cleanTitle(title, publisher) {
  let value = stripHtml(title);
  const suffix = publisher ? ` - ${publisher}` : "";
  if (suffix && value.toLowerCase().endsWith(suffix.toLowerCase())) {
    value = value.slice(0, -suffix.length);
  }
  return value.replace(/\s+/g, " ").trim();
}

/** Unwrap Bing News redirect links to the publisher URL. */
function cleanLink(link) {
  try {
    const u = new URL(link);
    if (u.hostname.endsWith("bing.com") && u.pathname.includes("apiclick")) {
      const target = u.searchParams.get("url");
      if (target) return decodeURIComponent(target);
    }
  } catch {
    // keep the original link
  }
  return link;
}

function isRelevant(item, topic) {
  const haystack = normalizeTitle(`${item.title} ${item.summary || ""}`);
  return topic.keywords.some((kw) => haystack.includes(normalizeTitle(kw)));
}

function publishedDate(item) {
  const d = item.pubDate ? new Date(item.pubDate) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
}

async function collectFeed(url, feedName = "") {
  try {
    const xml = await fetchText(url);
    return parseRss(xml).map((item) => ({ ...item, feedName }));
  } catch (error) {
    console.warn("rss fetch failed", url, String(error).slice(0, 160));
    return [];
  }
}

const DEBUG_HOSTS = new Set([
  "news.google.com", "bing.com", "vnexpress.net", "vietnamnet.vn", "thanhnien.vn",
  "dantri.com.vn", "tuoitre.vn", "laodong.vn", "cafef.vn", "znews.vn",
]);

/** Fetch an allow-listed feed and report whether it parses. */
export async function debugFeed(url) {
  const parsed = new URL(url);
  const host = parsed.hostname.replace(/^www\./, "");
  if (!DEBUG_HOSTS.has(host)) throw new Error(`host not allowed: ${host}`);
  const xml = await fetchText(url, 1);
  const items = parseRss(xml);
  return { url, items: items.length, sample: items[0]?.title?.slice(0, 120) || "" };
}

/** Diagnostic helper: one line per feed with HTTP/parse status. */
export async function debugSources(topic) {
  const feeds = CURATED_FEEDS.filter((f) => f.topics.includes(topic.slug));
  const targets = [
    { name: "Google News", url: googleNewsUrl(topic.query) },
    { name: "Bing News", url: bingNewsUrl(topic.bingQuery || topic.name) },
    ...feeds.map((f) => ({ name: f.name, url: f.url })),
  ];
  const results = [];
  for (const target of targets) {
    try {
      const xml = await fetchText(target.url);
      const items = parseRss(xml);
      results.push({ ...target, ok: true, items: items.length, sample: items[0]?.title?.slice(0, 90) || "" });
    } catch (error) {
      results.push({ ...target, ok: false, error: String(error).slice(0, 200) });
    }
  }
  return results;
}

/**
 * Discover recent, relevant articles for one topic.
 * Google News supplies breadth; curated feeds supply stable direct links.
 */
function relevanceScore(item, topic) {
  const haystack = normalizeTitle(`${item.title} ${item.summary || ""}`);
  let score = 0;
  for (const kw of topic.keywords) {
    if (haystack.includes(normalizeTitle(kw))) score += 1;
  }
  return score;
}

/**
 * Discover recent, relevant articles for one topic.
 * Google News supplies breadth (query already scopes the topic); curated feeds
 * supply stable direct links. Items are ranked by keyword relevance, then date.
 */
export async function discoverCandidates(topic, limit = 8) {
  const feeds = CURATED_FEEDS.filter((f) => f.topics.includes(topic.slug));
  const requests = [
    collectFeed(googleNewsUrl(topic.query), "Google News"),
    collectFeed(bingNewsUrl(topic.bingQuery || topic.name), "Bing News"),
    ...feeds.map((f) => collectFeed(f.url, f.name)),
  ];
  const settled = await Promise.allSettled(requests);

  const now = Date.now();
  const cutoff = now - MAX_AGE_DAYS * 86400 * 1000;
  const seen = new Set();
  const collected = [];

  for (const result of settled) {
    if (result.status !== "fulfilled") continue;
    for (const raw of result.value) {
      const published = publishedDate(raw);
      if (published && published.getTime() < cutoff) continue;

      const searchFeed = raw.feedName === "Google News" || raw.feedName === "Bing News";
      let publisher = raw.publisher || raw.feedName || "Nguồn";
      let title = cleanTitle(raw.title, raw.publisher || "");
      if (!raw.publisher && raw.feedName === "Bing News" && title.includes(" - ")) {
        const cut = title.lastIndexOf(" - ");
        publisher = title.slice(cut + 3).trim();
        title = title.slice(0, cut).trim();
      }
      if (title.length < 20) continue;

      const key = normalizeTitle(title);
      if (seen.has(key)) continue;
      seen.add(key);

      const score = relevanceScore({ ...raw, title }, topic);
      // Curated feeds are broad (business/tech); keep only on-topic pieces.
      if (!searchFeed && score === 0) continue;

      collected.push({
        title,
        link: cleanLink(raw.link),
        publisher,
        summary: raw.summary,
        searchFeed,
        score,
        publishedAt: (published || new Date()).toISOString(),
      });
    }
  }

  const positive = collected.filter((item) => item.score > 0);
  const zeroGoogle = collected.filter((item) => item.score === 0 && item.searchFeed);
  const pool = positive.length >= limit ? positive : positive.concat(zeroGoogle);
  pool.sort((a, b) => b.score - a.score || new Date(b.publishedAt) - new Date(a.publishedAt));

  let picked = pool.slice(0, limit);
  const publishers = new Set(picked.map((item) => item.publisher).filter(Boolean));
  if (publishers.size < 2) {
    const alternative = pool.slice(limit).find((item) => item.publisher && !publishers.has(item.publisher));
    if (alternative && picked.length > 0) picked[picked.length - 1] = alternative;
  }

  const withKeys = [];
  for (const item of picked) {
    withKeys.push({
      title: item.title,
      link: item.link,
      publisher: item.publisher,
      summary: item.summary,
      publishedAt: item.publishedAt,
      sourceKey: (await sha256Hex(`${item.publisher}|${normalizeTitle(item.title)}`)).slice(0, 32),
    });
  }
  return withKeys;
}

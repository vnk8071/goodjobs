/** News generation pipeline: signals → RSS → Workers AI → quality gate → D1. */

import { expandDraft, generateDraft } from "./ai.js";
import {
  DEFAULT_ARTICLES_PER_RUN,
  MAX_ARTICLES_PER_RUN,
  NEWS_AUTHOR,
  pickTopics,
  topicBySlug,
} from "./config.js";
import { collectSignals, numberVariants, signalNumbers } from "./signals.js";
import { discoverCandidates } from "./sources.js";
import {
  ictDateKey,
  ictIso,
  formatNumber,
  normalizeTitle,
  readingMinutes,
  sha256Hex,
  slugify,
  titleSimilarity,
  truncate,
} from "./util.js";

const MIN_BODY_CHARS = 1000;
const MIN_SOURCES = 2;
const MAX_TITLE_SIMILARITY = 0.72;

function buildBodyHtml(sections) {
  return sections
    .map(
      (section) =>
        `<section><h2>${escapeHtml(section.heading)}</h2>${section.paragraphs
          .map((p) => `<p>${escapeHtml(p)}</p>`)
          .join("")}</section>`,
    )
    .join("");
}

function escapeHtml(value) {
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

function sourceObjects(draft, candidates) {
  const picked = [];
  const seenLinks = new Set();
  for (const index of draft.sources) {
    const candidate = candidates[index - 1];
    if (!candidate || seenLinks.has(candidate.link)) continue;
    seenLinks.add(candidate.link);
    picked.push({
      title: candidate.title,
      publisher: candidate.publisher,
      url: candidate.link,
      publishedAt: candidate.publishedAt,
    });
  }
  return picked;
}

function dataParagraph(signals) {
  const parts = [
    `Theo dữ liệu Good Jobs cập nhật hôm nay, hệ thống đang lưu ${formatNumber(signals.total)} việc làm, với ${formatNumber(signals.recent)} tin tuyển dụng mới trong 7 ngày qua`,
  ];
  if (typeof signals.deltaPct === "number") {
    parts.push(`(${signals.deltaPct >= 0 ? "+" : ""}${signals.deltaPct}% so với tuần trước)`);
  }
  parts.push(".");
  if (signals.topSkills?.length) {
    parts.push(`Các kỹ năng được yêu cầu nhiều nhất gồm ${signals.topSkills.slice(0, 5).map((s) => s.skill).join(", ")}.`);
  }
  if (signals.categories?.length) {
    parts.push(`Nhóm nghề nổi bật: ${signals.categories.slice(0, 4).map((c) => c.name).join(", ")}.`);
  }
  return parts.join(" ").replace(/\s+/g, " ").trim();
}

/** Repair a model draft before the quality gate (never publish a thin, data-less piece). */
function enrichDraft(draft, signals) {
  if (!draft || !Array.isArray(draft.sections) || draft.sections.length === 0) return draft;
  if (draft.sections.length > 5) draft.sections = draft.sections.slice(0, 5);

  const bodyText = draft.sections.flatMap((s) => s.paragraphs).join("\n\n");
  const variants = signalNumbers(signals).flatMap(numberVariants);
  const used = new Set(variants.filter((n) => bodyText.includes(n)));
  if (signals.recent > 0 && used.size < 2) {
    const paragraph = dataParagraph(signals);
    if (draft.sections.length < 3) {
      draft.sections.push({ heading: "Dữ liệu thị trường", paragraphs: [paragraph] });
    } else {
      draft.sections[0].paragraphs.unshift(paragraph);
    }
  }

  if (draft.takeaways.length < 3) {
    const sentences = draft.sections
      .flatMap((s) => s.paragraphs)
      .map((p) => String(p).split(/(?<=\.)\s+/)[0])
      .filter(Boolean)
      .slice(0, 3);
    draft.takeaways = [...new Set([...draft.takeaways, ...sentences])].slice(0, 4);
  }

  if (draft.metaDescription.length < 100) {
    draft.metaDescription = truncate(`${draft.title}. ${draft.dek} Đọc phân tích trên Good Jobs.`, 158);
  }

  if (draft.linkedinText.length < 300) {
    const bullets = draft.takeaways.slice(0, 3).map((t) => `• ${truncate(t, 160)}`).join("\n");
    draft.linkedinText = `${draft.dek}\n\n${bullets}\n\nChi tiết:`;
  }

  return draft;
}

function gateArticle(draft, signals, candidates, sources, existingTitles) {
  const errors = [];
  const bodyText = draft.sections.flatMap((s) => s.paragraphs).join("\n\n");

  if (draft.title.length < 30 || draft.title.length > 110) errors.push("tiêu đề phải dài 30-110 ký tự và cụ thể");
  if (draft.dek.length < 80 || draft.dek.length > 280) errors.push("sapo phải dài 80-280 ký tự");
  if (draft.metaDescription.length < 100 || draft.metaDescription.length > 180) {
    errors.push("meta description phải dài 100-180 ký tự");
  }
  if (draft.sections.length < 3 || draft.sections.length > 5) errors.push("cần 3-5 mục H2");
  if (bodyText.length < MIN_BODY_CHARS) errors.push(`thân bài quá ngắn (${bodyText.length} ký tự, cần ≥ ${MIN_BODY_CHARS})`);
  if (draft.takeaways.length < 3) errors.push("cần ít nhất 3 ý chính");
  if (draft.tags.length < 2) errors.push("cần ít nhất 2 thẻ");
  if (draft.linkedinText.length < 300 || draft.linkedinText.length > 2600) {
    errors.push("linkedin_text phải dài 300-2600 ký tự");
  }

  const publishers = new Set(sources.map((s) => s.publisher).filter(Boolean));
  if (sources.length < MIN_SOURCES) errors.push(`cần ít nhất ${MIN_SOURCES} nguồn hợp lệ`);
  if (publishers.size < 2) errors.push("cần ít nhất 2 nhà xuất bản khác nhau");

  const numbers = signalNumbers(signals).flatMap(numberVariants);
  const used = new Set(numbers.filter((n) => bodyText.includes(n)));
  if (signals.recent > 0 && used.size < 2) errors.push("bài phải trích ít nhất 2 số liệu cụ thể từ dữ liệu Good Jobs");

  if (/\b(tôi là (một )?(ai|trợ lý|mô hình)|as an ai language model|mô hình ngôn ngữ)\b/i.test(bodyText)) {
    errors.push("không được tự nhận là AI/mô hình ngôn ngữ");
  }

  for (const candidate of candidates) {
    if (titleSimilarity(draft.title, candidate.title) >= 0.8) {
      errors.push("tiêu đề quá giống tiêu đề nguồn; hãy viết tiêu đề phân tích riêng");
      break;
    }
  }

  for (const existing of existingTitles) {
    if (titleSimilarity(draft.title, existing) >= MAX_TITLE_SIMILARITY) {
      errors.push("tiêu đề trùng với bài đã đăng gần đây");
      break;
    }
  }

  return { ok: errors.length === 0, errors, bodyText };
}

async function loadExistingTitles(env) {
  try {
    const result = await env.DB.prepare(
      "SELECT title FROM news_articles ORDER BY published_at DESC LIMIT 80",
    ).all();
    return (result.results || []).map((r) => String(r.title || ""));
  } catch {
    return [];
  }
}

async function filterUnseen(env, candidates) {
  if (candidates.length === 0) return [];
  try {
    const placeholders = candidates.map(() => "?").join(",");
    const result = await env.DB.prepare(
      `SELECT source_key FROM news_seen WHERE source_key IN (${placeholders})`,
    )
      .bind(...candidates.map((c) => c.sourceKey))
      .all();
    const seen = new Set((result.results || []).map((r) => r.source_key));
    return candidates.filter((c) => !seen.has(c.sourceKey));
  } catch {
    return candidates;
  }
}

async function markSeen(env, candidates, sources) {
  const used = new Set(sources.map((s) => s.url));
  for (const candidate of candidates) {
    if (!used.has(candidate.link)) continue;
    try {
      await env.DB.prepare(
        `INSERT OR IGNORE INTO news_seen (source_key, title, publisher, url, seen_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
        .bind(candidate.sourceKey, candidate.title, candidate.publisher, candidate.link, ictIso())
        .run();
    } catch {
      // non-fatal
    }
  }
}

function buildLinkedInText(draft, url) {
  const hashtags = (draft.tags || [])
    .slice(0, 5)
    .map((tag) => `#${normalizeTitle(tag).replace(/\s+/g, "")}`)
    .join(" ");
  const base = draft.linkedinText
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/#[^\s#]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const withCta = /Chi tiết:?\s*$/i.test(base) ? base : `${base}\n\nChi tiết:`;
  return truncate(`${withCta}\n\n${url}${hashtags ? `\n\n${hashtags}` : ""}`, 2900);
}

export async function publishToLinkedIn(env, article) {
  if (!env.LINKEDIN_ACCESS_TOKEN || !env.LINKEDIN_ORG_ID) {
    return { status: "draft", reason: "linkedin-not-configured" };
  }
  const linkedinText = article.linkedinText || article.linkedin_text || "";
  const url = article.url || `${env.SITE_URL || "https://goodjobs.io.vn"}/news/${article.slug}`;
  try {
    const response = await fetch("https://api.linkedin.com/rest/posts", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.LINKEDIN_ACCESS_TOKEN}`,
        "Content-Type": "application/json",
        "Linkedin-Version": env.LINKEDIN_VERSION || "202610",
        "X-Restli-Protocol-Version": "2.0.0",
      },
      body: JSON.stringify({
        author: `urn:li:organization:${env.LINKEDIN_ORG_ID}`,
        commentary: linkedinText,
        visibility: "PUBLIC",
        distribution: {
          feedDistribution: "MAIN_FEED",
          targetEntities: [],
          thirdPartyDistributionChannels: [],
        },
        content: {
          article: {
            source: url,
            title: article.title,
            description: article.dek,
          },
        },
        lifecycleState: "PUBLISHED",
        isReshareDisabledByAuthor: false,
      }),
    });
    if (!response.ok) {
      const detail = (await response.text().catch(() => "")).slice(0, 300);
      return { status: `failed:${response.status}`, reason: detail };
    }
    const postId = response.headers.get("x-restli-id") || "";
    return { status: "posted", postId };
  } catch (error) {
    return { status: "failed:network", reason: String(error).slice(0, 200) };
  }
}

async function insertArticle(env, record) {
  await env.DB.prepare(
    `INSERT INTO news_articles
      (id, slug, title, dek, meta_description, body_html, body_text, category, tags, sources, signals,
       takeaways, search_keyword, linkedin_text, linkedin_status, linkedin_url, lang, author, status,
       content_hash, published_at, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      record.id,
      record.slug,
      record.title,
      record.dek,
      record.metaDescription,
      record.bodyHtml,
      record.bodyText,
      record.category,
      JSON.stringify(record.tags),
      JSON.stringify(record.sources),
      JSON.stringify(record.signals),
      JSON.stringify(record.takeaways),
      record.searchKeyword,
      record.linkedinText,
      record.linkedinStatus,
      record.linkedinUrl,
      "vi",
      NEWS_AUTHOR,
      record.status,
      record.contentHash,
      record.publishedAt,
      record.publishedAt,
      record.publishedAt,
    )
    .run();
}

async function generateForTopic(env, { topic, signals, existingTitles }) {
  const discovered = await discoverCandidates(topic, 10);
  if (discovered.length < 2) {
    return { topic: topic.slug, status: "skipped", reason: "not-enough-sources", discovered: discovered.length };
  }

  const candidates = (await filterUnseen(env, discovered)).slice(0, 8);
  if (candidates.length < 2) {
    return { topic: topic.slug, status: "skipped", reason: "all-sources-seen", discovered: discovered.length };
  }

  let draft;
  let model;
  let feedback = "";
  let gate;
  let sources = [];
  let aiAttempts = [];

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const tooShort = attempt > 0 && gate?.errors.some((e) => e.includes("quá ngắn"));
    const generated = tooShort
      ? await expandDraft(env, { draft, feedback })
      : await generateDraft(env, { topic, signals, candidates, feedback });
    draft = enrichDraft(generated.draft, signals);
    model = generated.model;
    aiAttempts = [...(aiAttempts || []), ...(generated.attempts || [])];
    sources = sourceObjects(draft, candidates);
    gate = gateArticle(draft, signals, candidates, sources, existingTitles);
    if (gate.ok) break;
    feedback = `- ${gate.errors.join("\n- ")}`;
    if (gate.errors.some((e) => e.includes("quá ngắn"))) {
      feedback += "\n- Viết lại: mỗi mục 2 đoạn, mỗi đoạn 60-90 từ, tổng 350-500 từ.";
    }
  }

  if (!draft || !gate) {
    return { topic: topic.slug, status: "failed", reason: "no-draft", model, aiAttempts };
  }

  const bodyHtml = buildBodyHtml(draft.sections);
  const bodyText = draft.sections.flatMap((s) => s.paragraphs).join("\n\n");
  const hash = (await sha256Hex(`${draft.title}|${bodyText}`)).slice(0, 10);
  const slug = `${slugify(draft.title) || "tin-tuc"}-${hash.slice(0, 6)}`;
  const publishedAt = ictIso();
  const url = `${env.SITE_URL || "https://goodjobs.io.vn"}/news/${slug}`;
  const searchKeyword = draft.searchKeyword || topic.searchKeyword;

  const contentHash = (await sha256Hex(JSON.stringify({ title: draft.title, dek: draft.dek, bodyText }))).slice(0, 32);
  const linkedinText = buildLinkedInText(draft, url);

  const record = {
    id: crypto.randomUUID(),
    slug,
    title: draft.title,
    dek: draft.dek,
    metaDescription: draft.metaDescription || draft.dek,
    bodyHtml,
    bodyText,
    category: topic.name,
    tags: draft.tags,
    sources,
    signals: {
      total: signals.total,
      recent: signals.recent,
      previous: signals.previous,
      deltaPct: signals.deltaPct,
      topSkills: (signals.topSkills || []).slice(0, 6),
      categories: (signals.categories || []).slice(0, 5),
      topLocations: (signals.topLocations || []).slice(0, 4),
      windowDays: signals.windowDays,
    },
    takeaways: draft.takeaways,
    searchKeyword,
    linkedinText,
    linkedinStatus: "draft",
    linkedinUrl: "",
    status: gate.ok ? "published" : "draft",
    contentHash,
    publishedAt,
    url,
    readingMinutes: readingMinutes(bodyText),
    model,
  };

  if (!gate.ok) {
    record.metaDescription = truncate(record.dek, 158);
    await insertArticle(env, record);
    return { topic: topic.slug, status: "draft", reason: gate.errors.join("; "), slug, model, aiAttempts };
  }

  const linkedin = await publishToLinkedIn(env, record);
  record.linkedinStatus = linkedin.status;
  record.linkedinUrl = linkedin.postId ? `urn:li:share:${linkedin.postId}` : "";

  await insertArticle(env, record);
  await markSeen(env, candidates, sources);

  return {
    topic: topic.slug,
    status: "published",
    slug,
    title: record.title,
    url,
    model,
    aiAttempts,
    sources: sources.length,
    words: bodyText.split(/\s+/).filter(Boolean).length,
    linkedin: linkedin.status,
  };
}

function runSummaryId(runDate) {
  return `run-${runDate}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Generate and (after the quality gate) publish today's news.
 * Idempotent per ICT day unless `force` is set.
 */
export async function runNewsJob(env, options = {}) {
  const runDate = ictDateKey();
  const perRun = Math.max(1, Math.min(MAX_ARTICLES_PER_RUN, Number(options.perRun) || DEFAULT_ARTICLES_PER_RUN));

  if (!options.force) {
    try {
      const existing = await env.DB.prepare(
        "SELECT id, generated, published FROM news_runs WHERE run_date = ? AND status = 'ok' ORDER BY started_at DESC LIMIT 1",
      )
        .bind(runDate)
        .first();
      if (existing) {
        return { skipped: "already-ran-today", runDate, runId: existing.id };
      }
    } catch {
      // table missing on a fresh DB — continue, schema is applied by deploy
    }
  }

  const runId = runSummaryId(runDate);
  const startedAt = ictIso();
  try {
    await env.DB.prepare(
      "INSERT INTO news_runs (id, run_date, status, started_at) VALUES (?, ?, 'running', ?)",
    )
      .bind(runId, runDate, startedAt)
      .run();
  } catch {
    // non-fatal; the run still proceeds
  }

  const signals = await collectSignals(env);
  const existingTitles = await loadExistingTitles(env);
  const topics = options.topicSlug
    ? [topicBySlug(options.topicSlug)].filter(Boolean)
    : pickTopics(runDate, perRun);
  const results = [];

  for (const topic of topics) {
    try {
      const result = await generateForTopic(env, { topic, signals, existingTitles });
      results.push(result);
      if (result.status === "published" && result.title) existingTitles.push(result.title);
    } catch (error) {
      results.push({ topic: topic.slug, status: "failed", reason: String(error).slice(0, 300) });
    }
  }

  const published = results.filter((r) => r.status === "published").length;
  const skipped = results.filter((r) => r.status === "skipped").length;
  const finishedAt = ictIso();
  const notes = JSON.stringify(results).slice(0, 4000);

  try {
    await env.DB.prepare(
      `UPDATE news_runs SET status = ?, generated = ?, published = ?, skipped = ?, notes = ?, finished_at = ?
       WHERE id = ?`,
    )
      .bind("ok", results.length, published, skipped, notes, finishedAt, runId)
      .run();
  } catch {
    // non-fatal
  }

  return {
    runId,
    runDate,
    published,
    skipped,
    results,
    signals: {
      total: signals.total,
      recent: signals.recent,
      previous: signals.previous,
      deltaPct: signals.deltaPct,
      topSkills: signals.topSkills.slice(0, 5).map((s) => s.skill),
    },
  };
}

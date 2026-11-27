/** News generation pipeline: RSS → Workers AI → quality gate → D1. */

import { expandDraft, generateDraft, generateShareDraft } from "./ai.js";
import {
  DEFAULT_ARTICLES_PER_RUN,
  MAX_ARTICLES_PER_RUN,
  NEWS_AUTHOR,
  SHARE_CATEGORY,
  pickTopics,
  topicBySlug,
} from "./config.js";
import { discoverCandidates, isJobMarketRelevant, isSafeArticleLink, mainstreamPublisher } from "./sources.js";
import {
  ictDateKey,
  ictIso,
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

/** Repair a model draft before the quality gate (never publish a thin piece). */
export function enrichDraft(draft) {
  if (!draft || !Array.isArray(draft.sections) || draft.sections.length === 0) return draft;
  if (draft.sections.length > 4) draft.sections = draft.sections.slice(0, 4);

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

/** Vietnamese number formatting: "." groups thousands, "," is the decimal mark. */
function numberValue(token) {
  const value = Number(token.replace(/\./g, "").replace(/,/g, "."));
  return Number.isFinite(value) ? value : NaN;
}

/** Numbers in the article must be quoted from the press (small counts and years are fine). */
function pressNumberErrors(draft, candidates, bodyText) {
  const pressTokens = new Set(
    candidates.map((c) => `${c.title || ""} ${c.summary || ""}`).join(" ").match(/\d+(?:[.,]\d+)*/g) || [],
  );
  const scanned = [
    draft.title,
    draft.dek,
    bodyText,
    (draft.takeaways || []).join(" "),
    draft.linkedinText || "",
  ].join(" ");
  const tokens = new Set(scanned.match(/\d+(?:[.,]\d+)*/g) || []);
  const offending = [];
  for (const token of tokens) {
    const value = numberValue(token);
    const isSmall = Number.isFinite(value) && value <= 10;
    const isYear = /^\d{4}$/.test(token) && Number(token) >= 1990 && Number(token) <= 2099;
    const quotedFromPress = pressTokens.has(token);
    if (!isSmall && !isYear && !quotedFromPress) offending.push(token);
    if (offending.length >= 3) break;
  }
  return offending.map((token) => `số liệu "${token}" không có trong tin báo chí`);
}

export function gateArticle(draft, candidates, sources, existingTitles) {
  const errors = [];
  const bodyText = draft.sections.flatMap((s) => s.paragraphs).join("\n\n");

  if (draft.title.length < 30 || draft.title.length > 110) errors.push("tiêu đề phải dài 30-110 ký tự và cụ thể");
  if (draft.dek.length < 80 || draft.dek.length > 280) errors.push("sapo phải dài 80-280 ký tự");
  if (draft.metaDescription.length < 100 || draft.metaDescription.length > 180) {
    errors.push("meta description phải dài 100-180 ký tự");
  }
  if (draft.sections.length < 3 || draft.sections.length > 4) errors.push("cần 3-4 mục H2");
  if (bodyText.length < MIN_BODY_CHARS) errors.push(`thân bài quá ngắn (${bodyText.length} ký tự, cần ≥ ${MIN_BODY_CHARS})`);
  if (draft.takeaways.length < 3) errors.push("cần ít nhất 3 ý chính");
  if (draft.tags.length < 2) errors.push("cần ít nhất 2 thẻ");
  if (draft.linkedinText.length < 300 || draft.linkedinText.length > 2600) {
    errors.push("linkedin_text phải dài 300-2600 ký tự");
  }

  const publishers = new Set(sources.map((s) => s.publisher).filter(Boolean));
  if (sources.length < MIN_SOURCES) errors.push(`cần ít nhất ${MIN_SOURCES} nguồn hợp lệ`);
  if (publishers.size < 2) errors.push("cần ít nhất 2 nhà xuất bản khác nhau");

  // Attribution: the body must name the papers it draws on.
  const bodyNormalized = normalizeTitle(bodyText);
  const namedPublishers = new Set(
    [...publishers].filter((name) => bodyNormalized.includes(normalizeTitle(name))),
  );
  if (namedPublishers.size < Math.min(2, publishers.size)) {
    errors.push("cần ghi tên ít nhất 2 báo trong bài");
  }

  errors.push(...pressNumberErrors(draft, candidates, bodyText));

  // Model output must not smuggle links or handles into our own pages/posts.
  const linkScan = `${bodyText} ${(draft.takeaways || []).join(" ")} ${draft.linkedinText || ""}`;
  if (/https?:\/\/|www\.|@/i.test(linkScan)) {
    errors.push("không được chèn liên kết hoặc tài khoản");
  }

  if (/\b(tôi là (một )?(ai|trợ lý|mô hình)|as an ai language model|mô hình ngôn ngữ)\b/i.test(bodyText)) {
    errors.push("không được tự nhận là AI/mô hình ngôn ngữ");
  }

  // No repeated paragraphs.
  const paragraphs = draft.sections.flatMap((s) => s.paragraphs);
  let repeated = false;
  for (let i = 0; i < paragraphs.length && !repeated; i += 1) {
    for (let j = i + 1; j < paragraphs.length; j += 1) {
      if (titleSimilarity(paragraphs[i], paragraphs[j]) >= 0.6) {
        repeated = true;
        break;
      }
    }
  }
  if (repeated) errors.push("các đoạn đang lặp ý; viết lại cho khác nhau");

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
      "SELECT title FROM news_articles WHERE status = 'published' ORDER BY published_at DESC LIMIT 80",
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
  let base = draft.linkedinText
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/#[^\s#]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  // The model is asked to close with a bare "Chi tiết:" CTA and nothing
  // after it, but sometimes appends stray/garbled text past it instead of
  // stopping there. We always rebuild our own clean CTA + URL below, so
  // cut everything from the first "Chi tiết:" onward rather than only
  // checking whether it's already at the very end — otherwise a dangling
  // "Chi tiết: <garbage>" is left in place and our own "Chi tiết:" line
  // gets appended right after it, duplicating the CTA in the live post.
  const ctaIdx = base.search(/Chi ti[ếe]t\s*:?/i);
  if (ctaIdx >= 0) base = base.slice(0, ctaIdx).trim();
  // LinkedIn "little text" needs our own prose escaped; the URL/hashtags below are ours.
  base = escapeLittleText(base);
  return truncate(`${base}\n\nChi tiết:\n\n${url}${hashtags ? `\n\n${hashtags}` : ""}`, 2900);
}

/** LinkedIn "little text" escaping: our own prose only, never URLs or hashtags. */
export function escapeLittleText(text) {
  return String(text ?? "").replace(/[\\|{}@[\]()<>*_~]/g, (char) => `\\${char}`);
}

/**
 * Rank pool candidates for a "share the original article" post (pure, no I/O).
 * Only links on allow-listed Vietnamese outlets qualify; the publisher label
 * comes from the host, never from the feed (which can be spoofed).
 */
export function pickShareCandidate(pool, existingTitles = []) {
  const stamp = (value) => {
    const d = value ? new Date(value) : null;
    return d && !Number.isNaN(d.getTime()) ? d.getTime() : 0;
  };
  const ordered = [...(pool || [])].sort((a, b) => stamp(b?.publishedAt) - stamp(a?.publishedAt));
  const seen = new Set();
  const picked = [];
  for (const item of ordered) {
    if (!item) continue;
    if (item.sourceKey) {
      if (seen.has(item.sourceKey)) continue;
      seen.add(item.sourceKey);
    }

    if (!isSafeArticleLink(item.link)) continue;
    // A share is about one article, so its headline itself must be about jobs.
    if (!isJobMarketRelevant({ title: item.title, summary: "" })) continue;
    let link;
    try {
      link = new URL(item.link).toString();
    } catch {
      continue;
    }
    const publisher = mainstreamPublisher(link);
    if (!publisher) continue;

    if ((existingTitles || []).some((title) => title && titleSimilarity(item.title, title) >= MAX_TITLE_SIMILARITY)) {
      continue;
    }

    picked.push({ ...item, link, publisher });
    if (picked.length >= 2) break;
  }
  return picked;
}

/** Article body for a share post: original link first, then our own view. */
export function buildShareBodyHtml(item, paragraphs) {
  const source = `<p class="share-source"><strong>Bài gốc:</strong> <a href="${escapeHtml(item.link)}" target="_blank" rel="noopener noreferrer nofollow">${escapeHtml(item.title)}</a> — ${escapeHtml(item.publisher)}</p>`;
  const body = (paragraphs || []).map((p) => `<p>${escapeHtml(p)}</p>`).join("");
  return `${source}<h2>Góc nhìn Good Jobs</h2>${body}`;
}

function eightWordRunCopied(item, ours) {
  const sourceWords = normalizeTitle(`${item.title || ""} ${item.summary || ""}`).split(" ").filter(Boolean);
  const haystack = ` ${normalizeTitle(ours)} `;
  for (let i = 0; i + 8 <= sourceWords.length; i += 1) {
    if (haystack.includes(` ${sourceWords.slice(i, i + 8).join(" ")} `)) return true;
  }
  return false;
}

/** Quality gate for a share post: short, our own voice, no retelling. */
export function gateShare(draft, item, existingTitles = []) {
  const errors = [];
  const title = String(draft?.title || "");
  const paragraphs = Array.isArray(draft?.paragraphs) ? draft.paragraphs : [];
  const linkedinText = String(draft?.linkedinText || "");
  const bodyText = paragraphs.join(" ");

  if (title.length < 20 || title.length > 120) errors.push("tiêu đề phải dài 20-120 ký tự");
  if (paragraphs.length < 2) errors.push("cần ít nhất 2 đoạn");
  if (bodyText.length < 300 || bodyText.length > 1600) {
    errors.push(`phần bình luận phải dài 300-1600 ký tự (hiện ${bodyText.length})`);
  }
  if (titleSimilarity(title, item?.title || "") >= MAX_TITLE_SIMILARITY) errors.push("tiêu đề trùng tiêu đề bài gốc");
  for (const existing of existingTitles || []) {
    if (existing && titleSimilarity(title, existing) >= MAX_TITLE_SIMILARITY) {
      errors.push("tiêu đề trùng với bài đã đăng gần đây");
      break;
    }
  }

  if (eightWordRunCopied(item || {}, `${bodyText} ${linkedinText}`)) {
    errors.push("không được kể lại hay chép nội dung bài gốc");
  }

  // A company post must not carry third-party links or handles from the model.
  if (/https?:\/\/|www\.|@/i.test(`${bodyText} ${linkedinText}`)) {
    errors.push("không được chèn liên kết hoặc tài khoản");
  }
  if (!isJobMarketRelevant({ title: "", summary: bodyText })) {
    errors.push("bình luận phải nói về việc làm");
  }

  const normalized = normalizeTitle(`${bodyText} ${linkedinText}`);
  const selfRefs = ["mo hinh ngon ngu", "toi la ai", "toi la mot ai", "la mot mo hinh", "trong bai viet nay"];
  if (selfRefs.some((phrase) => normalized.includes(phrase))) {
    errors.push("không được tự nhận là AI hoặc nhắc tới bài viết");
  }

  if (linkedinText.length < 150) errors.push("linkedin_text phải dài ít nhất 150 ký tự");

  return { ok: errors.length === 0, errors };
}

/** LinkedIn commentary for a share post: original article link first. */
export function buildShareLinkedInText(draft, item, pageUrl) {
  const LIMIT = 2900;
  const hashtags = [];
  for (const tag of (draft?.tags || []).slice(0, 5)) {
    const handle = `#${normalizeTitle(tag).replace(/\s+/g, "")}`;
    if (handle.length > 1 && !hashtags.includes(handle)) hashtags.push(handle);
  }
  if (!hashtags.includes("#goodjobs")) hashtags.push("#goodjobs");
  const tagLine = hashtags.join(" ");

  const first = `Đọc bài gốc trên ${escapeLittleText(item.publisher)}: ${item.link}`;
  const last = `Góc nhìn đầy đủ của Good Jobs: ${pageUrl}`;
  const base = escapeLittleText(
    String(draft?.linkedinText || "")
      .replace(/https?:\/\/\S+/g, " ")
      .replace(/#[^\s#]+/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
  );

  const compose = (text, tags = tagLine) => [first, text, last, tags].filter(Boolean).join("\n\n");

  const full = compose(base);
  if (full.length <= LIMIT) return full;

  // Too long: shorten only our own commentary, never the two URLs.
  const fixed = compose("").length;
  const budget = Math.max(0, LIMIT - fixed - 3);
  return compose(truncate(base, budget).trim());
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
            source: article.previewUrl || url,
            title: article.previewTitle || article.title,
            description: article.previewDescription ?? article.dek,
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

async function generateForTopic(env, { topic, existingTitles, pool }) {
  const discovered = await discoverCandidates(topic, 10);
  let candidates = [];
  // Fill the share pool before any early return: a topic with a single unseen
  // candidate cannot produce an article but is still a valid share source.
  if (discovered.length > 0) {
    const unseen = await filterUnseen(env, discovered);
    if (Array.isArray(pool)) {
      pool.push(...unseen.map((c) => ({ ...c, topicSlug: topic.slug, searchKeyword: topic.searchKeyword })));
    }
    candidates = unseen.slice(0, 8);
  }
  if (discovered.length < 2) {
    return { topic: topic.slug, status: "skipped", reason: "not-enough-sources", discovered: discovered.length };
  }
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
      : await generateDraft(env, { topic, candidates, feedback });
    draft = enrichDraft(generated.draft);
    model = generated.model;
    aiAttempts = [...(aiAttempts || []), ...(generated.attempts || [])];
    sources = sourceObjects(draft, candidates);
    gate = gateArticle(draft, candidates, sources, existingTitles);
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
    signals: {},
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
  // x-restli-id on LinkedIn's /rest/posts response is already the full
  // post URN (e.g. "urn:li:share:7528...") — prefixing it again produced
  // "urn:li:share:urn:li:share:7528..." in linkedin_url.
  record.linkedinUrl = linkedin.postId || "";

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

/**
 * Fallback: publish at most one short "Điểm tin" post sharing a recent news
 * article, with Good Jobs's own commentary and the original link first.
 */
export async function shareOriginal(env, { pool, existingTitles }) {
  const candidates = pickShareCandidate(pool || [], existingTitles || []);
  if (candidates.length === 0) return { status: "share-skipped", reason: "no-candidate" };

  let lastReason = "no-candidate";
  for (const item of candidates) {
    let feedback = "";
    for (let attempt = 0; attempt < 2; attempt += 1) {
      let generated;
      try {
        generated = await generateShareDraft(env, { item, feedback });
      } catch (error) {
        lastReason = String(error?.message || error).slice(0, 240);
        break;
      }

      const draft = generated.draft;
      const gate = gateShare(draft, item, existingTitles || []);
      if (!gate.ok) {
        feedback = `- ${gate.errors.join("\n- ")}`;
        lastReason = gate.errors.join("; ");
        continue;
      }

      const paragraphs = draft.paragraphs;
      const bodyText = paragraphs.join("\n\n");
      const hash = (await sha256Hex(`${draft.title}|${bodyText}`)).slice(0, 10);
      const slug = `${slugify(draft.title) || "diem-tin"}-${hash.slice(0, 6)}`;
      const publishedAt = ictIso();
      const url = `${env.SITE_URL || "https://goodjobs.io.vn"}/news/${slug}`;
      const dek = truncate(`Góc nhìn của Good Jobs về bài "${item.title}" trên ${item.publisher}.`, 220);
      const contentHash = (await sha256Hex(JSON.stringify({ title: draft.title, dek, bodyText }))).slice(0, 32);
      const sources = [{ title: item.title, url: item.link, publisher: item.publisher }];

      const record = {
        id: crypto.randomUUID(),
        slug,
        title: draft.title,
        dek,
        metaDescription: truncate(dek, 158),
        bodyHtml: buildShareBodyHtml(item, paragraphs),
        bodyText,
        category: SHARE_CATEGORY,
        tags: draft.tags,
        sources,
        signals: {},
        takeaways: [],
        searchKeyword: item.searchKeyword || "việc làm",
        linkedinText: buildShareLinkedInText(draft, item, url),
        linkedinStatus: "draft",
        linkedinUrl: "",
        status: "published",
        contentHash,
        publishedAt,
        url,
      };

      const linkedin = await publishToLinkedIn(env, {
        ...record,
        previewUrl: item.link,
        previewTitle: item.title,
        previewDescription: item.publisher,
      });
      record.linkedinStatus = linkedin.status;
      record.linkedinUrl = linkedin.postId || "";

      await insertArticle(env, record);
      await markSeen(env, [item], record.sources);

      return {
        topic: item.topicSlug,
        status: "shared",
        slug,
        title: record.title,
        url,
        source: item.link,
        publisher: item.publisher,
        model: generated.model,
        linkedin: linkedin.status,
      };
    }
  }

  return { status: "share-skipped", reason: lastReason || "no-candidate" };
}

/** Fill `pool` with unseen candidates per topic without generating articles. */
async function collectSharePool(env, topics, pool) {
  for (const topic of topics) {
    try {
      const discovered = await discoverCandidates(topic, 10);
      if (discovered.length === 0) continue;
      const unseen = await filterUnseen(env, discovered);
      pool.push(...unseen.map((c) => ({ ...c, topicSlug: topic.slug, searchKeyword: topic.searchKeyword })));
    } catch (error) {
      console.warn("share pool collection failed", topic.slug, String(error).slice(0, 160));
    }
  }
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

  const existingTitles = await loadExistingTitles(env);
  const topics = options.topicSlug
    ? [topicBySlug(options.topicSlug)].filter(Boolean)
    : pickTopics(runDate, perRun);
  const results = [];
  const pool = [];

  if (options.shareOnly) {
    await collectSharePool(env, topics, pool);
  } else {
    for (const topic of topics) {
      try {
        const result = await generateForTopic(env, { topic, existingTitles, pool });
        results.push(result);
        if (result.status === "published" && result.title) existingTitles.push(result.title);
      } catch (error) {
        results.push({ topic: topic.slug, status: "failed", reason: String(error).slice(0, 300) });
      }
    }
  }

  const publishedTopics = results.filter((r) => r.status === "published").length;
  if (publishedTopics === 0 && options.share !== false) {
    try {
      results.push(await shareOriginal(env, { pool, existingTitles }));
    } catch (error) {
      results.push({ status: "share-skipped", reason: String(error).slice(0, 300) });
    }
  }

  const published = results.filter((r) => r.status === "published" || r.status === "shared").length;
  const shared = results.filter((r) => r.status === "shared").length;
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
    shared,
    skipped,
    results,
  };
}

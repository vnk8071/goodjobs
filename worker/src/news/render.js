/** Server-rendered HTML for /news, article pages, RSS and the news sitemap. */

import {
  SITE_URL,
  esc,
  formatNumber,
  formatViDate,
  formatViDateLong,
  readingMinutes,
  safeJsonLd,
} from "./util.js";

const ADSENSE_CLIENT = "ca-pub-1563090184756838";

function parseArray(value) {
  try {
    const parsed = JSON.parse(value || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function parseObject(value) {
  try {
    const parsed = JSON.parse(value || "{}");
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function newsStyles() {
  return `
    :root {
      color-scheme: light;
      --bg: #ffffff;
      --card: #ffffff;
      --border: #e6e6e3;
      --text: #1a1a1a;
      --muted: #6b6b6b;
      --primary: #2563eb;
      --primary-soft: #e8efff;
      --primary-strong: #1d4ed8;
      --serif: Georgia, "Times New Roman", "Noto Serif", serif;
    }
    html[data-theme="dark"] {
      color-scheme: dark;
      --bg: #101012;
      --card: #18181b;
      --border: #2c2c30;
      --text: #f3f3f2;
      --muted: #a1a1aa;
      --primary: #7aa2ff;
      --primary-soft: rgba(85, 130, 240, .16);
      --primary-strong: #a9c1ff;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      line-height: 1.65;
      letter-spacing: -.005em;
    }
    a { color: var(--primary); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .wrap { width: min(1120px, calc(100% - 2rem)); margin: 0 auto; }

    header.site {
      border-bottom: 1px solid var(--border);
      background: var(--card);
      position: sticky;
      top: 0;
      z-index: 20;
    }
    .header-inner { display: flex; align-items: center; justify-content: space-between; gap: 1rem; padding: .85rem 0; }
    .brand { display: inline-flex; align-items: center; gap: .6rem; color: var(--text); font-weight: 800; font-size: 1.05rem; }
    .brand img { width: 32px; height: 32px; border-radius: 8px; }
    .brand:hover { text-decoration: none; }
    nav.main { display: flex; align-items: center; gap: 1.1rem; font-size: .86rem; font-weight: 600; }
    nav.main a { color: var(--muted); }
    nav.main a:hover, nav.main a.active { color: var(--primary); text-decoration: none; }
    nav.main .cta {
      padding: .45rem .85rem;
      border-radius: 8px;
      background: var(--primary);
      color: #fff !important;
    }

    main { padding: 2rem 0 3.5rem; }
    .crumbs { color: var(--muted); font-size: .8rem; margin-bottom: 1.4rem; display: flex; flex-wrap: wrap; gap: .35rem; }
    .crumbs a { color: var(--muted); }
    .crumbs span { color: var(--text); }

    .article { max-width: 780px; margin: 0 auto; }
    .cat {
      display: inline-flex;
      padding: .25rem .7rem;
      border-radius: 99px;
      background: var(--primary-soft);
      color: var(--primary-strong);
      font-size: .74rem;
      font-weight: 700;
      letter-spacing: .02em;
      margin-bottom: .9rem;
    }
    h1 { font-size: clamp(1.9rem, 4vw, 2.7rem); line-height: 1.15; letter-spacing: -.035em; margin: 0 0 .8rem; }
    .dek { font-size: clamp(1.02rem, 1.6vw, 1.16rem); color: var(--muted); margin: 0 0 1.1rem; }
    .byline { color: var(--muted); font-size: .8rem; padding-bottom: 1.2rem; border-bottom: 1px solid var(--border); margin-bottom: 1.4rem; }

    .takeaways {
      background: var(--primary-soft);
      border: 1px solid color-mix(in srgb, var(--primary) 22%, transparent);
      border-radius: 14px;
      padding: 1.1rem 1.25rem;
      margin: 1.4rem 0 1.8rem;
    }
    .takeaways h2 { margin: 0 0 .55rem; font-size: .95rem; letter-spacing: 0; }
    .takeaways ul { margin: 0; padding-left: 1.15rem; }
    .takeaways li { margin: .35rem 0; font-size: .9rem; }

    .body { font-family: var(--serif); font-size: 1.06rem; line-height: 1.75; }
    .body h2 {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: clamp(1.2rem, 2.2vw, 1.42rem);
      letter-spacing: -.02em;
      margin: 2rem 0 .6rem;
    }
    .body p { margin: 0 0 1rem; }

    .data-box {
      border: 1px solid var(--border);
      background: var(--card);
      border-radius: 14px;
      padding: 1.2rem 1.3rem;
      margin: 2rem 0;
    }
    .data-box h2 { margin: 0 0 .9rem; font-size: 1rem; }
    .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .8rem; }
    .stat { background: var(--bg); border: 1px solid var(--border); border-radius: 10px; padding: .7rem .8rem; }
    .stat strong { display: block; font-size: 1.35rem; letter-spacing: -.03em; }
    .stat span { color: var(--muted); font-size: .76rem; }
    .chips { display: flex; flex-wrap: wrap; gap: .4rem; margin-top: .9rem; }
    .chip { padding: .22rem .6rem; border-radius: 99px; border: 1px solid var(--border); color: var(--muted); font-size: .74rem; }
    .data-note { margin: .9rem 0 0; color: var(--muted); font-size: .75rem; }

    .job-cta {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: .8rem;
      border: 1px solid var(--border);
      background: var(--card);
      border-radius: 14px;
      padding: 1rem 1.2rem;
      margin: 2rem 0;
    }
    .job-cta strong { display: block; font-size: .95rem; }
    .job-cta span { color: var(--muted); font-size: .82rem; }
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 0 1.1rem;
      border-radius: 9px;
      background: var(--primary);
      color: #fff;
      font-size: .86rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .btn:hover { background: var(--primary-strong); text-decoration: none; }
    .btn-ghost { background: var(--card); color: var(--text); border: 1px solid var(--border); }
    .btn-ghost:hover { border-color: var(--primary); color: var(--primary); background: var(--card); }

    .sources { margin: 2rem 0 1rem; padding-top: 1.2rem; border-top: 1px solid var(--border); }
    .sources h2 { font-size: 1rem; margin: 0 0 .6rem; }
    .sources ol { margin: 0; padding-left: 1.2rem; color: var(--muted); font-size: .85rem; }
    .sources li { margin: .35rem 0; }
    .method { color: var(--muted); font-size: .78rem; padding: 1rem; background: var(--card); border: 1px solid var(--border); border-radius: 10px; }

    .related { max-width: 780px; margin: 2.6rem auto 0; }
    .related h2 { font-size: 1.1rem; margin: 0 0 .8rem; }
    .related-list { display: grid; gap: .7rem; }
    .related-item { border: 1px solid var(--border); background: var(--card); border-radius: 11px; padding: .85rem 1rem; }
    .related-item a { color: var(--text); font-weight: 650; }
    .related-item span { display: block; color: var(--muted); font-size: .75rem; margin-top: .2rem; }

    /* index */
    .news-hero { max-width: 820px; margin: 0 auto 2rem; }
    .news-hero h1 { margin-bottom: .6rem; }
    .news-hero p { color: var(--muted); margin: 0; }
    .news-filters { display: flex; flex-wrap: wrap; gap: .45rem; margin: 1.2rem 0 1.6rem; }
    .news-filters a { padding: .3rem .7rem; border-radius: 99px; border: 1px solid var(--border); color: var(--muted); font-size: .78rem; }
    .news-filters a.active { background: var(--primary); border-color: var(--primary); color: #fff; }
    .card-list { display: grid; gap: 1rem; max-width: 860px; margin: 0 auto; }
    .news-card { border: 1px solid var(--border); background: var(--card); border-radius: 14px; padding: 1.2rem 1.3rem; }
    .news-card h2 { margin: .35rem 0 .45rem; font-size: clamp(1.15rem, 2vw, 1.35rem); line-height: 1.3; letter-spacing: -.02em; }
    .news-card h2 a { color: var(--text); }
    .news-card p { color: var(--muted); margin: 0 0 .7rem; font-size: .9rem; }
    .news-card .meta { color: var(--muted); font-size: .76rem; display: flex; gap: .6rem; flex-wrap: wrap; }
    .pager { display: flex; align-items: center; justify-content: center; gap: .5rem; margin: 2rem 0 0; }
    .pager a, .pager span { padding: .4rem .75rem; border: 1px solid var(--border); border-radius: 7px; font-size: .8rem; color: var(--muted); }
    .pager .current { background: var(--primary); border-color: var(--primary); color: #fff; }
    .method-section { max-width: 860px; margin: 3rem auto 0; border-top: 1px solid var(--border); padding-top: 1.4rem; }
    .method-section h2 { font-size: 1.05rem; }
    .method-section p { color: var(--muted); font-size: .88rem; }

    footer.site { border-top: 1px solid var(--border); padding: 1.4rem 0 2rem; color: var(--muted); font-size: .78rem; }
    footer.site .wrap { display: flex; flex-wrap: wrap; justify-content: space-between; gap: .8rem; }
    footer.site a { color: var(--muted); }
    @media (max-width: 640px) {
      nav.main { gap: .7rem; }
      nav.main a.hide-sm { display: none; }
      .article { padding: 0; }
    }
  `;
}

function layout({ title, description, canonical, bodyHtml, jsonLd, ogType = "website", publishedAt = "" }) {
  return `<!doctype html>
<html lang="vi">
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
  <meta name="google-adsense-account" content="${ADSENSE_CLIENT}">
  <meta property="og:type" content="${esc(ogType)}">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(description)}">
  <meta property="og:url" content="${esc(canonical)}">
  <meta property="og:image" content="${SITE_URL}/og-image.jpg">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:site_name" content="Good Jobs">
  <meta property="og:locale" content="vi_VN">
  ${publishedAt ? `<meta property="article:published_time" content="${esc(publishedAt)}">` : ""}
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${esc(title)}">
  <meta name="twitter:description" content="${esc(description)}">
  <meta name="twitter:image" content="${SITE_URL}/og-image.jpg">
  <link rel="icon" type="image/svg+xml" href="/favicon.svg">
  <link rel="alternate" type="application/rss+xml" title="Good Jobs — Tin tức" href="${SITE_URL}/news/rss.xml">
  <script src="/consent.js" defer></script>
  ${jsonLd ? `<script type="application/ld+json">${safeJsonLd(jsonLd)}</script>` : ""}
  <style>${newsStyles()}</style>
</head>
<body>
  <header class="site">
    <div class="wrap header-inner">
      <a class="brand" href="/"><img src="/favicon.svg" alt="Good Jobs" width="32" height="32">Good Jobs</a>
      <nav class="main" aria-label="Điều hướng">
        <a href="/">Việc làm</a>
        <a href="/news" class="active">Tin tức</a>
        <a href="/post-job/" class="hide-sm">Đăng tin</a>
        <a href="/?kw=AI+Engineer" class="cta">Tìm việc</a>
      </nav>
    </div>
  </header>
  <main>${bodyHtml}</main>
  <footer class="site">
    <div class="wrap">
      <span>Good Jobs — tổng hợp việc làm và phân tích thị trường lao động Việt Nam.</span>
      <span>
        <a href="/news">Tin tức</a> ·
        <a href="/news/rss.xml">RSS</a> ·
        <a href="/contact/">Liên hệ</a> ·
        <a href="/privacy/">Quyền riêng tư</a> ·
        <a href="/terms/">Điều khoản</a>
      </span>
    </div>
  </footer>
</body>
</html>`;
}

function articleJsonLd(article, sources) {
  const url = `${SITE_URL}/news/${article.slug}`;
  return {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "NewsArticle",
        "@id": `${url}#article`,
        headline: article.title,
        description: article.dek,
        datePublished: article.published_at,
        dateModified: article.updated_at || article.published_at,
        inLanguage: "vi",
        articleSection: article.category,
        keywords: parseArray(article.tags).join(", "),
        author: { "@type": "Organization", name: article.author || "Ban Biên Tập Good Jobs", url: SITE_URL },
        publisher: {
          "@type": "Organization",
          name: "Good Jobs",
          url: SITE_URL,
          logo: { "@type": "ImageObject", url: `${SITE_URL}/og-image.jpg` },
        },
        mainEntityOfPage: { "@type": "WebPage", "@id": url },
        image: [`${SITE_URL}/og-image.jpg`],
        isAccessibleForFree: true,
        citation: sources.map((s) => ({ "@type": "CreativeWork", name: s.title, url: s.url, publisher: s.publisher })),
      },
      {
        "@type": "BreadcrumbList",
        "@id": `${url}#breadcrumb`,
        itemListElement: [
          { "@type": "ListItem", position: 1, name: "Trang chủ", item: `${SITE_URL}/` },
          { "@type": "ListItem", position: 2, name: "Tin tức", item: `${SITE_URL}/news` },
          { "@type": "ListItem", position: 3, name: article.title, item: url },
        ],
      },
    ],
  };
}

function dataBox(signals, publishedAt) {
  const total = Number(signals.total) || 0;
  const recent = Number(signals.recent) || 0;
  const delta = signals.deltaPct;
  const skills = Array.isArray(signals.topSkills) ? signals.topSkills.slice(0, 6) : [];
  if (total === 0 && recent === 0 && skills.length === 0) return "";
  return `
    <aside class="data-box" aria-label="Dữ liệu thị trường">
      <h2>Dữ liệu thị trường</h2>
      <div class="stats">
        <div class="stat"><strong>${formatNumber(total)}</strong><span>việc làm đang lưu trên Good Jobs</span></div>
        <div class="stat"><strong>${formatNumber(recent)}</strong><span>tin tuyển dụng mới trong 7 ngày</span></div>
        ${typeof delta === "number" ? `<div class="stat"><strong>${delta >= 0 ? "+" : ""}${delta}%</strong><span>so với 7 ngày trước đó</span></div>` : ""}
      </div>
      ${skills.length ? `<div class="chips">${skills.map((s) => `<span class="chip">${esc(s.skill)} · ${formatNumber(s.count)}</span>`).join("")}</div>` : ""}
      <p class="data-note">Nguồn: dữ liệu tổng hợp từ Good Jobs, cập nhật ${esc(formatViDate(publishedAt))}.</p>
    </aside>`;
}

export function renderArticlePage(article, related, allSources) {
  const url = `${SITE_URL}/news/${article.slug}`;
  const takeaways = parseArray(article.takeaways);
  const sources = allSources.length ? allSources : parseArray(article.sources);
  const signals = parseObject(article.signals);
  const tags = parseArray(article.tags);
  const minutes = readingMinutes(article.body_text);

  const body = `
    <div class="wrap">
      <nav class="crumbs" aria-label="Breadcrumb">
        <a href="/">Trang chủ</a> › <a href="/news">Tin tức</a> › <span>${esc(article.category)}</span>
      </nav>
      <article class="article">
        <span class="cat">${esc(article.category)}</span>
        <h1>${esc(article.title)}</h1>
        <p class="dek">${esc(article.dek)}</p>
        <div class="byline">${esc(article.author || "Ban Biên Tập Good Jobs")} · ${esc(formatViDate(article.published_at))} · ${minutes} phút đọc</div>

        ${takeaways.length ? `<aside class="takeaways"><h2>Điểm chính</h2><ul>${takeaways.map((t) => `<li>${esc(t)}</li>`).join("")}</ul></aside>` : ""}

        <div class="body">${article.body_html}</div>

        ${dataBox(signals, article.published_at)}

        <div class="job-cta">
          <div>
            <strong>Đang tìm việc trong lĩnh vực này?</strong>
            <span>Xem các tin tuyển dụng mới nhất trên Good Jobs.</span>
          </div>
          <a class="btn" href="/?kw=${encodeURIComponent(article.search_keyword || article.category)}">Xem việc làm →</a>
        </div>

        ${sources.length ? `<section class="sources"><h2>Nguồn tham khảo</h2><ol>${sources.map((s) => `<li><a href="${esc(s.url)}" target="_blank" rel="noopener noreferrer nofollow">${esc(s.title)}</a>${s.publisher ? ` — ${esc(s.publisher)}` : ""}</li>`).join("")}</ol></section>` : ""}

        <p class="method">Bài viết do Ban Biên Tập Good Jobs tổng hợp từ nguồn công khai và phân tích dữ liệu việc làm của Good Jobs. Số liệu có thể thay đổi theo thời điểm; nội dung không phải lời khuyên đầu tư hay bảo đảm việc làm. Xem <a href="/news#phuong-phap">phương pháp và tiêu chí biên tập</a>.${tags.length ? ` Thẻ: ${tags.map((t) => `<span class="chip">${esc(t)}</span>`).join(" ")}` : ""}</p>
      </article>

      ${related.length ? `<section class="related"><h2>Bài viết liên quan</h2><div class="related-list">${related.map((r) => `<div class="related-item"><a href="/news/${esc(r.slug)}">${esc(r.title)}</a><span>${esc(r.category)} · ${esc(formatViDate(r.published_at))}</span></div>`).join("")}</div></section>` : ""}
    </div>`;

  return layout({
    title: `${article.title} — Good Jobs`,
    description: article.meta_description || article.dek,
    canonical: url,
    bodyHtml: body,
    jsonLd: articleJsonLd(article, sources),
    ogType: "article",
    publishedAt: article.published_at,
  });
}

export function renderNewsIndex({ articles, page, totalPages, categories, activeCategory }) {
  const cards = articles.length
    ? articles
        .map(
          (a) => `<article class="news-card">
            <span class="cat">${esc(a.category)}</span>
            <h2><a href="/news/${esc(a.slug)}">${esc(a.title)}</a></h2>
            <p>${esc(a.dek)}</p>
            <div class="meta"><span>${esc(formatViDate(a.published_at))}</span><span>·</span><span>${readingMinutes(a.body_text)} phút đọc</span></div>
          </article>`,
        )
        .join("")
    : `<div class="news-card"><p>Chưa có bài viết nào. Vui lòng quay lại sau.</p></div>`;

  const filters = [
    `<a href="/news" class="${!activeCategory ? "active" : ""}">Tất cả</a>`,
    ...categories.map(
      (c) => `<a href="/news?category=${encodeURIComponent(c.category)}" class="${activeCategory === c.category ? "active" : ""}">${esc(c.category)} <span>${c.count}</span></a>`,
    ),
  ].join("");

  const pager = [];
  if (page > 1) pager.push(`<a href="/news?page=${page - 1}${activeCategory ? `&category=${encodeURIComponent(activeCategory)}` : ""}">← Trước</a>`);
  pager.push(`<span class="current">Trang ${page}/${totalPages}</span>`);
  if (page < totalPages) pager.push(`<a href="/news?page=${page + 1}${activeCategory ? `&category=${encodeURIComponent(activeCategory)}` : ""}">Sau →</a>`);

  const jsonLd = {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "CollectionPage",
        "@id": `${SITE_URL}/news#webpage`,
        url: `${SITE_URL}/news`,
        name: "Tin tức & phân tích thị trường việc làm — Good Jobs",
        description: "Tin tức, dữ liệu và phân tích thị trường tuyển dụng Việt Nam từ Good Jobs.",
        inLanguage: "vi",
        isPartOf: { "@type": "WebSite", "@id": `${SITE_URL}/#website` },
      },
      {
        "@type": "ItemList",
        itemListElement: articles.map((a, i) => ({
          "@type": "ListItem",
          position: i + 1,
          url: `${SITE_URL}/news/${a.slug}`,
          name: a.title,
        })),
      },
      {
        "@type": "BreadcrumbList",
        itemListElement: [
          { "@type": "ListItem", position: 1, name: "Trang chủ", item: `${SITE_URL}/` },
          { "@type": "ListItem", position: 2, name: "Tin tức", item: `${SITE_URL}/news` },
        ],
      },
    ],
  };

  const body = `
    <div class="wrap">
      <section class="news-hero">
        <h1>Tin tức & phân tích thị trường việc làm</h1>
        <p>Dữ liệu tuyển dụng từ Good Jobs, đặt trong bối cảnh tin tức công nghệ, kinh doanh và thị trường lao động Việt Nam.</p>
        <div class="news-filters">${filters}</div>
      </section>
      <div class="card-list">${cards}</div>
      ${totalPages > 1 ? `<nav class="pager">${pager.join("")}</nav>` : ""}
      <section class="method-section" id="phuong-phap">
        <h2>Phương pháp và tiêu chí biên tập</h2>
        <p>Mỗi bài viết được tổng hợp từ các nguồn tin công khai (RSS, Google News) và phân tích dựa trên dữ liệu việc làm do Good Jobs thu thập từ các trang tuyển dụng. Chúng tôi không sao chép toàn văn nguồn; số liệu thị trường chỉ lấy từ kho dữ liệu của Good Jobs và được ghi rõ trong bài. Bài viết qua kiểm tra chất lượng tự động (độ dài, số nguồn, số liệu, trùng lặp) trước khi xuất bản.</p>
        <p>Phát hiện sai sót hoặc muốn góp ý? Liên hệ <a href="/contact/">Ban Biên Tập Good Jobs</a>.</p>
      </section>
    </div>`;

  return layout({
    title: "Tin tức & phân tích thị trường việc làm — Good Jobs",
    description: "Tin tức, dữ liệu và phân tích thị trường tuyển dụng Việt Nam: kỹ năng, lương, AI và xu hướng nghề nghiệp.",
    canonical: `${SITE_URL}/news`,
    bodyHtml: body,
    jsonLd,
  });
}

export function renderRss(articles) {
  const items = articles
    .map(
      (a) => `<item>
        <title><![CDATA[${a.title}]]></title>
        <link>${SITE_URL}/news/${a.slug}</link>
        <guid isPermaLink="true">${SITE_URL}/news/${a.slug}</guid>
        <pubDate>${new Date(a.published_at).toUTCString()}</pubDate>
        <description><![CDATA[${a.dek}]]></description>
        <category><![CDATA[${a.category}]]></category>
      </item>`,
    )
    .join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Good Jobs — Tin tức thị trường việc làm</title>
    <link>${SITE_URL}/news</link>
    <description>Tin tức và phân tích thị trường tuyển dụng Việt Nam từ Good Jobs.</description>
    <language>vi</language>
    <lastBuildDate>${new Date().toUTCString()}</lastBuildDate>
    <atom:link href="${SITE_URL}/news/rss.xml" rel="self" type="application/rss+xml"/>
    ${items}
  </channel>
</rss>`;
}

export function renderNewsSitemap(articles) {
  const urls = [
    `<url><loc>${SITE_URL}/news</loc><lastmod>${new Date().toISOString().slice(0, 10)}</lastmod><changefreq>daily</changefreq><priority>0.8</priority></url>`,
    ...articles.map(
      (a) => `<url><loc>${SITE_URL}/news/${a.slug}</loc><lastmod>${String(a.published_at).slice(0, 10)}</lastmod><changefreq>weekly</changefreq><priority>0.7</priority></url>`,
    ),
  ].join("\n  ");
  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  ${urls}
</urlset>`;
}

export function renderNewsNotFound() {
  const body = `
    <div class="wrap" style="text-align:center;padding:4rem 0;">
      <h1>Không tìm thấy bài viết</h1>
      <p style="color:var(--muted)">Bài viết có thể đã bị gỡ hoặc đường dẫn không đúng.</p>
      <p><a class="btn" href="/news">Xem tất cả tin tức</a></p>
    </div>`;
  return layout({
    title: "Không tìm thấy bài viết — Good Jobs",
    description: "Bài viết không tồn tại hoặc đã bị gỡ.",
    canonical: `${SITE_URL}/news`,
    bodyHtml: body,
  });
}

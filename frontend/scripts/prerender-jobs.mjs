/**
 * Build-time prerender for crawlable job pages.
 *
 * Runs after `vite build` and writes static HTML into dist/jobs:
 *   /jobs/<category>/         category landing pages
 *   /jobs/<location>/         location landing pages
 *   /jobs/job/<slug>/         one page per recent job (JobPosting JSON-LD)
 *
 * The API is the live Good Jobs backend. If it is unreachable the build still
 * succeeds: category/location pages are generated with a "no snapshot" state,
 * and the static sitemap is left intact.
 */
import fs from 'node:fs/promises';
import path from 'node:path';

const BASE = 'https://goodjobs.io.vn';
const API = (process.env.VITE_API_URL || 'https://api.goodjobs.io.vn').replace(/\/+$/, '');
const DIST = path.resolve('dist');
const TODAY = new Date().toISOString().slice(0, 10);
const OG = `${BASE}/og-image.jpg`;

const CATEGORIES = [
  { key: 'data-ai', label: 'Data & AI', seed: 'Data Scientist', blurb: 'data scientists, data analysts, data engineers, machine learning and AI engineers' },
  { key: 'it-software', label: 'IT & Software', seed: 'Software Engineer', blurb: 'backend, frontend, fullstack, mobile, QA, DevOps, cloud and security roles' },
  { key: 'product-design', label: 'Product & Design', seed: 'Product Manager', blurb: 'product managers, product owners, UI/UX and product designers' },
  { key: 'marketing', label: 'Marketing', seed: 'Marketing Executive', blurb: 'growth, content, performance, brand and communications roles' },
  { key: 'finance-banking', label: 'Finance & Banking', seed: 'Financial Analyst', blurb: 'finance, accounting, audit, banking, risk and credit roles' },
  { key: 'operations-sales', label: 'Operations & Sales', seed: 'Operations', blurb: 'operations, sales, customer success, supply chain and business development' },
];
const CAT_RE = {
  'data-ai': /data scientist|data analyst|data engineer|machine learning|ml engineer|ai engineer|business intelligence|analytics|bi analyst/i,
  'it-software': /engineer|developer|software|devops|programmer|frontend|front-end|backend|back-end|fullstack|full-stack|mobile|android|ios\b|\bqa\b|tester|cloud|security|architect|blockchain/i,
  'product-design': /product manager|product owner|\bux\b|\bui\b|ui\/ux|designer|graphic/i,
  'marketing': /marketing|growth|content|seo|brand|communications|social media|public relations|\bpr\b/i,
  'finance-banking': /finance|financial|accounting|accountant|audit|banking|risk|credit|investment/i,
  'operations-sales': /operations|sales|customer success|supply chain|logistics|procurement|business development|account manager/i,
};
const LOCATIONS = [
  { key: 'ho-chi-minh-city', label: 'Ho Chi Minh City', match: /hồ chí minh|ho chi minh|hcmc|tphcm|sai gon|saigon/i },
  { key: 'ha-noi', label: 'Hanoi', match: /hà nội|ha noi|hanoi/i },
  { key: 'da-nang', label: 'Da Nang', match: /đà nẵng|da nang/i },
  { key: 'remote', label: 'Remote', match: /remote|work from home|anywhere/i, remote: true },
  { key: 'singapore', label: 'Singapore', match: /singapore/i },
  { key: 'london', label: 'London', match: /london|\buk\b|united kingdom/i },
  { key: 'new-york', label: 'New York', match: /new york|\bnyc\b|united states|\busa\b/i },
];

const esc = (s = '') => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const decode = (s = '') => String(s)
  .replace(/<[^>]*>/g, ' ')
  .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&lt;/gi, '<').replace(/&gt;/gi, '>')
  .replace(/&quot;/gi, '"').replace(/&#39;/g, "'").replace(/&#x27;/gi, "'")
  .replace(/\s+/g, ' ').trim();
const slugify = (s = '') => String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/gi, 'd')
  .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 70);
const hash = (s = '') => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return (h >>> 0).toString(16).padStart(8, '0').slice(0, 8); };
const trunc = (s, n) => (s.length > n ? s.slice(0, n - 1).trimEnd() + '…' : s);
const plusDays = (iso, days) => new Date(new Date(iso + 'T00:00:00Z').getTime() + days * 86400000).toISOString().slice(0, 10);

function parseSkills(v) {
  if (Array.isArray(v)) return v.map(String).map(s => s.trim()).filter(Boolean).slice(0, 10);
  if (typeof v === 'string' && v.trim()) {
    try { const a = JSON.parse(v.replace(/'/g, '"')); if (Array.isArray(a)) return a.map(String).map(s => s.trim()).filter(Boolean).slice(0, 10); } catch {}
    return v.split(/[,;|]/).map(s => s.trim().replace(/^\[|\]$/g, '').replace(/^\s*['"]|['"]\s*$/g, '')).filter(Boolean).slice(0, 10);
  }
  return [];
}
function dateFor(j) {
  if (/^\d{4}-\d{2}-\d{2}/.test(j.posted_date || '')) return j.posted_date.slice(0, 10);
  const ts = parseFloat(j.posted_ts);
  if (ts > 1e9) { try { return new Date(ts * 1000).toISOString().slice(0, 10); } catch {} }
  return TODAY;
}
function categoryFor(title, skills) {
  const t = (title + ' ' + skills.join(' ')).toLowerCase();
  return CATEGORIES.find(c => CAT_RE[c.key].test(t)) || null;
}
function locFor(location) {
  return LOCATIONS.find(l => l.match.test(location)) || null;
}
function countryFor(location) {
  if (/singapore/i.test(location)) return 'SG';
  if (/london|united kingdom|\buk\b/i.test(location)) return 'GB';
  if (/new york|united states|\busa\b|\bnyc\b/i.test(location)) return 'US';
  return 'VN';
}
function page({ title, description, canonical, h1, intro, body, jsonld, extraHead = '', lang = 'en' }) {
  const ld = jsonld ? `<script type="application/ld+json">${JSON.stringify(jsonld).replace(/</g, '\\u003c')}</script>` : '';
  return `<!DOCTYPE html>
<html lang="${lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${esc(title)}</title>
<meta name="description" content="${esc(description)}">
<link rel="canonical" href="${canonical}">
<meta name="robots" content="index,follow,max-image-preview:large">
<meta property="og:type" content="website">
<meta property="og:url" content="${canonical}">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(description)}">
<meta property="og:image" content="${OG}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="Good Jobs — find your next job in Vietnam and beyond">
<meta property="og:site_name" content="Good Jobs">
<meta property="og:locale" content="en_US">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${esc(title)}">
<meta name="twitter:description" content="${esc(description)}">
<meta name="twitter:image" content="${OG}">
<meta name="theme-color" content="#2563eb">
<script src="/consent.js" defer></script>
${extraHead}
${ld}
<style>
:root{--primary:#2563eb;--bg:#f8fafc;--card:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#475569;--radius:14px}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.65;padding:2rem 1rem 4rem}
.container{max-width:980px;margin:0 auto}
header{display:flex;align-items:center;justify-content:space-between;gap:1rem;margin-bottom:2rem;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:.85rem;text-decoration:none;color:inherit}
.logo{width:44px;height:44px;border-radius:12px;background:linear-gradient(135deg,var(--primary),#1d4ed8);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:17px;flex:0 0 auto}
.brand b{font-size:1.15rem;letter-spacing:-.02em}
.brand span{display:block;font-size:.78rem;color:var(--muted)}
.header-actions{display:flex;gap:.75rem;flex-wrap:wrap}
.header-actions a{font-size:.85rem;font-weight:600;text-decoration:none;padding:.55rem .9rem;border-radius:8px}
.header-actions .ghost{color:var(--text);border:1px solid var(--border);background:var(--card)}
.header-actions .solid{color:#fff;background:var(--primary)}
.crumbs{font-size:.8rem;color:var(--muted);margin:0 0 1rem}
.crumbs a{color:var(--muted);text-decoration:none}
.crumbs a:hover{color:var(--primary)}
.hero{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:1.7rem;margin-bottom:1.25rem}
.eyebrow{font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;color:var(--primary);font-weight:700}
h1{font-size:clamp(1.8rem,4vw,2.5rem);line-height:1.12;letter-spacing:-.03em;margin:.45rem 0 .7rem}
h2{font-size:1.2rem;letter-spacing:-.015em;margin:1.6rem 0 .7rem}
.lead{color:var(--muted);max-width:70ch}
.jobs{display:grid;gap:.7rem;margin:.9rem 0}
.job-card{display:block;background:var(--card);border:1px solid var(--border);border-radius:12px;padding:1rem 1.1rem;text-decoration:none;color:inherit;transition:border-color .15s,transform .15s}
.job-card:hover{border-color:var(--primary);transform:translateY(-1px)}
.job-card b{display:block;font-size:1rem;margin-bottom:.15rem}
.job-meta{font-size:.82rem;color:var(--muted);display:flex;gap:.75rem;flex-wrap:wrap}
.skills{display:flex;gap:.35rem;flex-wrap:wrap;margin-top:.5rem}
.skills span{font-size:.72rem;border:1px solid var(--border);border-radius:999px;padding:.2rem .55rem;color:var(--muted)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:.5rem;background:var(--primary);color:#fff;text-decoration:none;font-weight:700;border-radius:9px;padding:.8rem 1.15rem;margin-top:1rem}
.btn.secondary{background:var(--card);color:var(--text);border:1px solid var(--border)}
.notice{font-size:.82rem;color:var(--muted);margin-top:1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.7rem}
.chips{display:flex;flex-wrap:wrap;gap:.5rem;margin:.7rem 0}
.chips a{font-size:.85rem;text-decoration:none;color:var(--text);background:var(--card);border:1px solid var(--border);border-radius:999px;padding:.45rem .85rem}
.chips a:hover{border-color:var(--primary);color:var(--primary)}
.desc p{margin:0 0 .8rem}
.desc ul{margin:.4rem 0 .8rem 1.2rem}
footer{margin-top:2.5rem;padding-top:1.25rem;border-top:1px solid var(--border);color:var(--muted);font-size:.85rem;display:flex;justify-content:space-between;gap:1rem;flex-wrap:wrap}
footer a{color:var(--muted);text-decoration:none;margin-right:1rem}
footer a:hover{color:var(--primary)}
@media(max-width:560px){.hero{padding:1.25rem}.job-card{padding:.85rem}}
</style>
</head>
<body>
<div class="container">
<header>
<a class="brand" href="${lang === 'vi' ? '/vi/' : '/'}"><div class="logo" aria-hidden="true">GJ</div><div><b>Good Jobs</b><span>${lang === 'vi' ? 'Tìm một lần. Ứng tuyển thông minh hơn.' : 'Search once. Apply smarter.'}</span></div></a>
<div class="header-actions">${lang === 'vi' ? '<a class="ghost" href="/">English</a><a class="solid" href="/post-job/">Đăng tin</a>' : '<a class="ghost" href="/vi/">Tiếng Việt</a><a class="ghost" href="/">Search jobs</a><a class="solid" href="/post-job/">Post a Job</a>'}</div>
</header>
<main>
${body}
</main>
<footer>${lang === 'vi' ? '<div><a href="/vi/">Trang chủ</a><a href="/vi/viec-lam/">Việc làm</a><a href="/jobs/ho-chi-minh-city/">Hồ Chí Minh</a><a href="/post-job/">Đăng tin</a><a href="/contact/">Liên hệ</a></div>' : '<div><a href="/">Home</a><a href="/jobs/">Jobs</a><a href="/post-job/">Post a Job</a><a href="/privacy/">Privacy</a><a href="/terms/">Terms</a><a href="/contact/">Contact</a></div>'}<div>© ${new Date().getFullYear()} Good Jobs</div></footer>
</div>
</body>
</html>`;
}

const crumbs = items => `<p class="crumbs">${items.map((it, i) => (it.url ? `<a href="${it.url}">${esc(it.name)}</a>` : esc(it.name)) + (i < items.length - 1 ? ' › ' : '')).join('')}</p>`;
const jobCard = j => `<a class="job-card" href="/jobs/job/${j.slug}/">
  <b>${esc(j.title)}</b>
  <div class="job-meta"><span>${esc(j.company)}</span><span>${esc(j.location || 'Vietnam')}</span><span>${esc(j.source)}</span><span>${esc(j.posted || 'Today')}</span></div>
  ${j.skills.length ? `<div class="skills">${j.skills.slice(0, 5).map(s => `<span>${esc(s)}</span>`).join('')}</div>` : ''}
</a>`;
const categoryLinks = (active) => `<div class="chips">${CATEGORIES.filter(c => c.key !== active).map(c => `<a href="/jobs/${c.key}/">${esc(c.label)}</a>`).join('')}</div>`;
const locationLinks = (active) => `<div class="chips">${LOCATIONS.filter(l => l.key !== active).map(l => `<a href="/jobs/${l.key}/">${esc(l.label)}</a>`).join('')}</div>`;

let raw = [];
for (let attempt = 1; attempt <= 3; attempt++) {
  try {
    const res = await fetch(`${API}/recent-jobs?n=300`, { headers: { 'user-agent': 'GoodJobsSEOBuilder/1.0' } });
    if (res.ok) { raw = await res.json(); break; }
    console.warn(`[prerender] API ${res.status} (attempt ${attempt}/3)`);
  } catch (e) {
    console.warn(`[prerender] API unavailable (attempt ${attempt}/3):`, e.message);
  }
  if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
}
if (!raw.length) console.warn('[prerender] no jobs fetched; generating empty category pages');

const seen = new Set();
const jobs = raw.map(j => {
  const title = String(j.title || '').trim();
  const link = String(j.link || '').trim();
  if (!title || !link || seen.has(link)) return null;
  seen.add(link);
  const company = (String(j.company || '').trim() && String(j.company).trim() !== 'N/A') ? String(j.company).trim() : String(j.source || 'Good Jobs');
  const location = String(j.location || '').trim() || 'Vietnam';
  const description = decode(j.description || '');
  const summary = decode(j.summary_description || '') || trunc(description, 180);
  const skills = parseSkills(j.skills);
  const date = dateFor(j);
  const cat = categoryFor(title, skills);
  const loc = locFor(location);
  return {
    title, link, company, location, description, summary, skills, date,
    source: String(j.source || 'Good Jobs'),
    posted: String(j.posted || 'Today'),
    slug: `${slugify(title + '-' + company) || 'job'}-${hash(link)}`,
    category: cat ? cat.key : null,
    categoryLabel: cat ? cat.label : null,
    loc: loc ? loc.key : null,
    locLabel: loc ? loc.label : null,
    remote: !!(loc && loc.remote) || /remote|work from home/i.test(location),
    country: countryFor(location),
  };
}).filter(Boolean);

const dirs = new Set();
async function writePage(rel, html) {
  const file = path.join(DIST, rel, 'index.html');
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, html);
  dirs.add(rel);
}

// category pages
for (const c of CATEGORIES) {
  const items = jobs.filter(j => j.category === c.key).slice(0, 30);
  const url = `${BASE}/jobs/${c.key}/`;
  const body = `${crumbs([{ name: 'Home', url: '/' }, { name: 'Jobs', url: '/jobs/' }, { name: c.label }])}
<div class="hero"><p class="eyebrow">Job category</p><h1>${esc(c.label)} jobs in Vietnam</h1>
<p class="lead">Recent ${esc(c.blurb)} from Good Jobs. This page is refreshed every day; open a role for the full description and a direct link to the original posting.</p>
<a class="btn" href="/?kw=${encodeURIComponent(c.seed)}&amp;loc=Ho%20Chi%20Minh%20City">Search live</a></div>
<h2>Latest ${esc(c.label)} roles</h2>
<div class="jobs">${items.length ? items.map(jobCard).join('') : '<p class="lead">No matching roles were captured in today\'s snapshot. Use the live search or check back tomorrow.</p>'}</div>
<h2>Other categories</h2>${categoryLinks(c.key)}
<h2>Browse by location</h2>${locationLinks()}`;
  const ld = { '@context': 'https://schema.org', '@graph': [
    { '@type': 'Organization', '@id': BASE + '/#organization', name: 'Good Jobs', url: BASE + '/', logo: BASE + '/favicon.svg' },
    { '@type': 'CollectionPage', '@id': url + '#webpage', url, name: `${c.label} jobs in Vietnam`, description: `Recent ${c.label.toLowerCase()} jobs aggregated by Good Jobs.`, isPartOf: { '@id': BASE + '/#website' }, inLanguage: 'en' },
    { '@type': 'BreadcrumbList', '@id': url + '#breadcrumb', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Home', item: BASE + '/' },
      { '@type': 'ListItem', position: 2, name: 'Jobs', item: BASE + '/jobs/' },
      { '@type': 'ListItem', position: 3, name: c.label, item: url },
    ] },
    ...(items.length ? [{ '@type': 'ItemList', '@id': url + '#jobs', itemListElement: items.map((j, i) => ({ '@type': 'ListItem', position: i + 1, name: j.title, url: `${BASE}/jobs/job/${j.slug}/` })) }] : []),
  ] };
  await writePage(`jobs/${c.key}`, page({
    title: `${c.label} Jobs in Vietnam — Good Jobs`,
    description: trunc(`Browse recent ${c.label.toLowerCase()} jobs in Vietnam — ${c.blurb}. Aggregated from LinkedIn, ITviec, TopCV, VietnamWorks, CareerViet and more.`, 155),
    canonical: url, h1: `${c.label} jobs in Vietnam`, body, jsonld: ld,
  }));
}

// location pages
for (const l of LOCATIONS) {
  const items = l.remote ? jobs.filter(j => j.remote) : jobs.filter(j => j.loc === l.key);
  const list = items.slice(0, 30);
  const url = `${BASE}/jobs/${l.key}/`;
  const body = `${crumbs([{ name: 'Home', url: '/' }, { name: 'Jobs', url: '/jobs/' }, { name: l.label }])}
<div class="hero"><p class="eyebrow">Job location</p><h1>Jobs in ${esc(l.label)}</h1>
<p class="lead">Recent roles in ${esc(l.label)} from 21 job boards, refreshed daily. Open a role for the description, skills and the original application link.</p>
<a class="btn" href="/?kw=Software%20Engineer&amp;loc=${encodeURIComponent(l.label)}">Search live in ${esc(l.label)}</a></div>
<h2>Latest jobs in ${esc(l.label)}</h2>
<div class="jobs">${list.length ? list.map(jobCard).join('') : '<p class="lead">No matching roles were captured in today\'s snapshot. Use the live search or check back tomorrow.</p>'}</div>
<h2>Browse by category</h2>${categoryLinks()}
<h2>Other locations</h2>${locationLinks(l.key)}`;
  const ld = { '@context': 'https://schema.org', '@graph': [
    { '@type': 'Organization', '@id': BASE + '/#organization', name: 'Good Jobs', url: BASE + '/', logo: BASE + '/favicon.svg' },
    { '@type': 'CollectionPage', '@id': url + '#webpage', url, name: `Jobs in ${l.label}`, description: `Recent jobs in ${l.label} aggregated by Good Jobs.`, isPartOf: { '@id': BASE + '/#website' }, inLanguage: 'en' },
    { '@type': 'BreadcrumbList', '@id': url + '#breadcrumb', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Home', item: BASE + '/' },
      { '@type': 'ListItem', position: 2, name: 'Jobs', item: BASE + '/jobs/' },
      { '@type': 'ListItem', position: 3, name: l.label, item: url },
    ] },
    ...(list.length ? [{ '@type': 'ItemList', '@id': url + '#jobs', itemListElement: list.map((j, i) => ({ '@type': 'ListItem', position: i + 1, name: j.title, url: `${BASE}/jobs/job/${j.slug}/` })) }] : []),
  ] };
  await writePage(`jobs/${l.key}`, page({
    title: `Jobs in ${l.label} — Good Jobs`,
    description: trunc(`Browse recent jobs in ${l.label} — engineering, data, product, marketing, finance and operations roles aggregated from 21 job boards.`, 155),
    canonical: url, h1: `Jobs in ${l.label}`, body,
    jsonld: ld,
  }));
}

// job detail pages
for (const j of jobs) {
  const url = `${BASE}/jobs/job/${j.slug}/`;
  const paragraphs = (j.description || j.summary || 'No description was provided by the source. Open the original posting for details.').split(/\n+/).map(p => p.trim()).filter(Boolean);
  const descHtml = paragraphs.map(p => `<p>${esc(p)}</p>`).join('');
  const ldDesc = paragraphs.map(p => `<p>${esc(p)}</p>`).join('');
  const catUrl = j.category ? `${BASE}/jobs/${j.category}/` : null;
  const catCrumb = j.category ? [{ name: j.categoryLabel, url: catUrl }] : [];
  const body = `${crumbs([{ name: 'Home', url: '/' }, { name: 'Jobs', url: '/jobs/' }, ...catCrumb, { name: j.title }])}
<div class="hero">
<p class="eyebrow">${esc(j.source)} · ${esc(j.posted || 'Today')}</p>
<h1>${esc(j.title)}</h1>
<p class="lead"><strong>${esc(j.company)}</strong> · ${esc(j.location || 'Vietnam')}</p>
${j.skills.length ? `<div class="skills">${j.skills.map(s => `<span>${esc(s)}</span>`).join('')}</div>` : ''}
<a class="btn" href="${esc(j.link)}" target="_blank" rel="noopener noreferrer nofollow">Apply on ${esc(j.source)} ↗</a>
<a class="btn secondary" href="/jobs/">Browse all jobs</a>
<p class="notice">Good Jobs aggregates this listing. The original posting controls the details, requirements and application process. Posted ${esc(j.date)}.</p>
</div>
<h2>Job description</h2>
<div class="desc">${descHtml}</div>
${catUrl ? `<h2>More ${esc(j.categoryLabel)} jobs</h2><div class="jobs">${jobs.filter(x => x.category === j.category && x.slug !== j.slug).slice(0, 6).map(jobCard).join('')}</div>` : ''}
<h2>Browse</h2>${categoryLinks()}<h2>Locations</h2>${locationLinks()}`;
  const ld = { '@context': 'https://schema.org', '@graph': [
    { '@type': 'JobPosting', '@id': url + '#jobposting',
      title: j.title,
      description: ldDesc,
      datePosted: j.date,
      validThrough: `${plusDays(j.date, 14)}T23:59:59${j.country === 'VN' ? '+07:00' : '+00:00'}`,
      hiringOrganization: { '@type': 'Organization', name: j.company },
      directApply: false,
      url,
      sameAs: j.link,
      identifier: { '@type': 'PropertyValue', name: j.source, value: j.link },
      ...(j.remote ? { jobLocationType: 'TELECOMMUTE' } : { jobLocation: { '@type': 'Place', address: { '@type': 'PostalAddress', addressLocality: j.location, addressCountry: j.country } } }),
    },
    { '@type': 'BreadcrumbList', '@id': url + '#breadcrumb', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Home', item: BASE + '/' },
      { '@type': 'ListItem', position: 2, name: 'Jobs', item: BASE + '/jobs/' },
      ...(j.category ? [{ '@type': 'ListItem', position: 3, name: j.categoryLabel, item: catUrl }] : []),
      { '@type': 'ListItem', position: j.category ? 4 : 3, name: j.title, item: url },
    ] },
    { '@type': 'WebPage', '@id': url + '#webpage', url, name: j.title, description: j.summary, inLanguage: 'en', isPartOf: { '@id': BASE + '/#website' } },
  ] };
  await writePage(`jobs/job/${j.slug}`, page({
    title: trunc(`${j.title} at ${j.company} — Good Jobs`, 65),
    description: trunc(`${j.title} at ${j.company}${j.location ? ' in ' + j.location : ''}. ${j.summary}`, 155),
    canonical: url, h1: j.title, body, jsonld: ld,
  }));
}


const viJobCard = j => `<a class="job-card" href="/jobs/job/${j.slug}/"><b>${esc(j.title)}</b><div class="job-meta"><span>${esc(j.company)}</span><span>${esc(j.location || 'Hồ Chí Minh')}</span><span>${esc(j.source)}</span><span>${esc(j.posted || 'Hôm nay')}</span></div>${j.skills.length ? `<div class="skills">${j.skills.slice(0, 5).map(x => `<span>${esc(x)}</span>`).join('')}</div>` : ''}</a>`;
const viCatLabels = { 'data-ai': 'Data & AI', 'it-software': 'IT & Phần mềm', 'product-design': 'Sản phẩm & Thiết kế', 'marketing': 'Marketing', 'finance-banking': 'Tài chính & Ngân hàng', 'operations-sales': 'Vận hành & Kinh doanh' };

// Vietnamese homepage and jobs directory (English remains the default)
const viAll = jobs.slice(0, 18);
const viHomeUrl = `${BASE}/vi/`;
const viDirUrl = `${BASE}/vi/viec-lam/`;
const viCatCards = CATEGORIES.map(c => `<a class="job-card" href="/jobs/${c.key}/"><b>${esc(viCatLabels[c.key] || c.label)}</b><span class="job-meta">${esc(c.label)} jobs</span></a>`).join('');
const viLocChips = `<div class="chips"><a href="/jobs/ho-chi-minh-city/">TP. Hồ Chí Minh</a><a href="/jobs/ha-noi/">Hà Nội</a><a href="/jobs/da-nang/">Đà Nẵng</a><a href="/jobs/remote/">Remote</a></div>`;
const viHomeBody = `${crumbs([{ name: 'Trang chủ' }])}
<div class="hero"><p class="eyebrow">Website tìm việc tổng hợp</p>
<h1>Tìm việc làm tại Việt Nam</h1>
<p class="lead">Good Jobs tổng hợp việc làm mới từ 21 trang tuyển dụng — LinkedIn, ITviec, TopCV, VietnamWorks, CareerViet, TopDev, JobsGo, CareerLink, RemoteOK và nhiều nguồn khác. Một lần tìm kiếm, bạn thấy nhiều cơ hội hơn, kèm kỹ năng và mức độ phù hợp.</p>
<a class="btn" href="/?kw=Software%20Engineer&amp;loc=Ho%20Chi%20Minh%20City">Tìm việc ngay</a>
<a class="btn secondary" href="/vi/viec-lam/">Xem tất cả việc làm</a>
<a class="btn secondary" href="/">English version</a></div>

<h2>Việc làm mới hôm nay</h2>
<div class="jobs">${viAll.length ? viAll.map(viJobCard).join('') : '<p class="lead">Hôm nay chưa có tin trong bản tổng hợp. Dùng ô tìm kiếm để lấy kết quả trực tiếp từ các trang tuyển dụng.</p>'}</div>

<h2>Tìm việc theo ngành</h2>
<div class="chips">${CATEGORIES.map(c => `<a href="/jobs/${c.key}/">${esc(viCatLabels[c.key] || c.label)}</a>`).join('')}</div>

<h2>Tìm việc theo khu vực</h2>
${viLocChips}

<h2>Vì sao nên dùng Good Jobs?</h2>
<p><strong>Một chỗ thay vì mười trang:</strong> bạn không cần mở từng trang tuyển dụng. Nhập vị trí, kỹ năng hoặc dán CV, Good Jobs tổng hợp kết quả và dẫn về trang gốc để ứng tuyển.</p>
<p><strong>Cập nhật hàng ngày:</strong> tin mới trong ngày được ưu tiên, tin trùng theo link được loại tự động, tin cũ được dọn sau 14 ngày.</p>
<p><strong>Tìm theo CV hoặc kỹ năng:</strong> AI đọc nội dung bạn dán vào, gợi ý từ khóa phù hợp và chấm điểm mức độ khớp — hữu ích khi bạn chưa biết nên tìm chức danh nào.</p>
<p><strong>Có việc làm remote:</strong> chọn địa điểm "Remote" để xem các vị trí làm từ xa trong nước và quốc tế.</p>

<h2>Cách tìm việc hiệu quả</h2>
<ul>
<li>Dùng chức danh tiếng Anh cho IT, dữ liệu và công ty đa quốc gia: "Data Engineer", "Backend Engineer".</li>
<li>Thêm cấp bậc: "Senior", "Junior", "Intern".</li>
<li>Nhập địa điểm cụ thể: "Hồ Chí Minh", "Hà Nội", "Đà Nẵng" hoặc "Remote".</li>
<li>Dán CV hoặc danh sách kỹ năng để AI gợi ý vị trí gần nhất.</li>
<li>Kiểm tra mô tả, mức lương và hạn nộp trên trang gốc trước khi ứng tuyển.</li>
</ul>

<h2>Câu hỏi thường gặp</h2>
<p><strong>Good Jobs có phải trang tuyển dụng không?</strong> Không. Good Jobs tổng hợp tin từ các trang tuyển dụng và dẫn về trang gốc; chúng tôi không phải nhà tuyển dụng và không thu phí ứng viên.</p>
<p><strong>Good Jobs có miễn phí không?</strong> Có. Tìm kiếm và ứng tuyển miễn phí cho ứng viên. Nhà tuyển dụng có thể đăng tin qua trang <a href="/post-job/">Post a Job</a>.</p>
<p><strong>Tìm việc làm ở Hồ Chí Minh ở đâu?</strong> Mở <a href="/jobs/ho-chi-minh-city/">trang việc làm Hồ Chí Minh</a> hoặc nhập "Hồ Chí Minh" vào ô địa điểm.</p>
<p><strong>Có việc làm remote không?</strong> Có. Chọn "Remote" ở ô địa điểm để xem các vị trí làm từ xa.</p>`;
const viHomeLd = { '@context': 'https://schema.org', '@graph': [
  { '@type': 'Organization', '@id': BASE + '/#organization', name: 'Good Jobs', url: BASE + '/', logo: BASE + '/favicon.svg' },
  { '@type': 'CollectionPage', '@id': viHomeUrl + '#webpage', url: viHomeUrl, name: 'Tìm việc làm tại Việt Nam', description: 'Website tổng hợp việc làm tại Việt Nam từ 21 trang tuyển dụng.', inLanguage: 'vi', isPartOf: { '@id': BASE + '/#website' } },
  { '@type': 'BreadcrumbList', '@id': viHomeUrl + '#breadcrumb', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Trang chủ', item: viHomeUrl }] },
  ...(viAll.length ? [{ '@type': 'ItemList', '@id': viHomeUrl + '#jobs', itemListElement: viAll.map((j, i) => ({ '@type': 'ListItem', position: i + 1, name: j.title, url: `${BASE}/jobs/job/${j.slug}/` })) }] : []),
] };
await writePage('vi', page({
  title: 'Tìm Việc Làm Tại Việt Nam | Good Jobs',
  description: trunc('Good Jobs — website tổng hợp việc làm tại Việt Nam từ LinkedIn, ITviec, TopCV, VietnamWorks, CareerViet... Cập nhật hàng ngày, tìm theo CV, kỹ năng, remote.', 155),
  canonical: viHomeUrl, h1: 'Tìm việc làm tại Việt Nam', body: viHomeBody, jsonld: viHomeLd, lang: 'vi',
  extraHead: `<link rel="alternate" hreflang="vi" href="${viHomeUrl}"><link rel="alternate" hreflang="en" href="${BASE}/"><link rel="alternate" hreflang="x-default" href="${BASE}/">`,
}));

const viDirBody = `${crumbs([{ name: 'Trang chủ', url: '/vi/' }, { name: 'Việc làm' }])}
<div class="hero"><p class="eyebrow">Danh mục việc làm</p>
<h1>Việc làm tại Việt Nam</h1>
<p class="lead">Chọn ngành hoặc khu vực để xem việc làm mới. Mỗi tin dẫn về trang gốc để bạn ứng tuyển trực tiếp; Good Jobs không thu phí ứng viên.</p>
<a class="btn" href="/?kw=Software%20Engineer&amp;loc=Ho%20Chi%20Minh%20City">Tìm việc ngay</a></div>

<h2>Theo ngành</h2>
<div class="grid">${viCatCards}</div>

<h2>Theo khu vực</h2>
${viLocChips}

<h2>Việc làm mới</h2>
<div class="jobs">${viAll.length ? viAll.map(viJobCard).join('') : '<p class="lead">Hôm nay chưa có tin trong bản tổng hợp. Dùng ô tìm kiếm để lấy kết quả trực tiếp.</p>'}</div>`;
const viDirLd = { '@context': 'https://schema.org', '@graph': [
  { '@type': 'Organization', '@id': BASE + '/#organization', name: 'Good Jobs', url: BASE + '/', logo: BASE + '/favicon.svg' },
  { '@type': 'CollectionPage', '@id': viDirUrl + '#webpage', url: viDirUrl, name: 'Việc làm tại Việt Nam', description: 'Danh mục việc làm theo ngành và khu vực tại Việt Nam.', inLanguage: 'vi', isPartOf: { '@id': BASE + '/#website' } },
  { '@type': 'BreadcrumbList', '@id': viDirUrl + '#breadcrumb', itemListElement: [
    { '@type': 'ListItem', position: 1, name: 'Trang chủ', item: viHomeUrl },
    { '@type': 'ListItem', position: 2, name: 'Việc làm', item: viDirUrl },
  ] },
] };
await writePage('vi/viec-lam', page({
  title: 'Việc Làm Tại Việt Nam — Theo Ngành & Khu Vực | Good Jobs',
  description: trunc('Danh mục việc làm tại Việt Nam theo ngành và khu vực: IT, Data, Marketing, Tài chính, Hồ Chí Minh, Hà Nội, Đà Nẵng và Remote.', 155),
  canonical: viDirUrl, h1: 'Việc làm tại Việt Nam', body: viDirBody, jsonld: viDirLd, lang: 'vi',
  extraHead: `<link rel="alternate" hreflang="vi" href="${viDirUrl}"><link rel="alternate" hreflang="en" href="${BASE}/jobs/"><link rel="alternate" hreflang="x-default" href="${BASE}/jobs/">`,
}));

// extend sitemap
try {
  const sitemapPath = path.join(DIST, 'sitemap.xml');
  let xml = await fs.readFile(sitemapPath, 'utf8').catch(() => '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n</urlset>');
  const existing = new Set([...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map(m => m[1]));
  const extra = [];
  for (const c of CATEGORIES) extra.push({ loc: `${BASE}/jobs/${c.key}/`, freq: 'weekly', pri: '0.8' });
  for (const l of LOCATIONS) extra.push({ loc: `${BASE}/jobs/${l.key}/`, freq: 'weekly', pri: '0.8' });
  extra.push({ loc: `${BASE}/vi/`, freq: 'daily', pri: '0.9' });
  extra.push({ loc: `${BASE}/vi/viec-lam/`, freq: 'daily', pri: '0.8' });
  for (const j of jobs) extra.push({ loc: `${BASE}/jobs/job/${j.slug}/`, freq: 'daily', pri: '0.6' });
  for (const e of extra) {
    if (existing.has(e.loc)) continue;
    xml = xml.replace('</urlset>', `  <url><loc>${e.loc}</loc><lastmod>${TODAY}</lastmod><changefreq>${e.freq}</changefreq><priority>${e.pri}</priority></url>\n</urlset>`);
    existing.add(e.loc);
  }
  await fs.writeFile(sitemapPath, xml);
} catch (e) { console.warn('[prerender] sitemap update skipped:', e.message); }

console.log(`[prerender] ${jobs.length} jobs, ${CATEGORIES.length} category pages, ${LOCATIONS.length} location pages`);

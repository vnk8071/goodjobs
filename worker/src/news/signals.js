/** Collapse the D1 job store into a compact set of market signals for prompts. */

const CATEGORY_RULES = [
  ["AI & ML", /\b(ai|ml|machine learning|deep learning|llm|nlp|computer vision|data scientist|prompt engineer)\b/i],
  ["Data", /\b(data engineer|data analyst|business intelligence|bi |etl|analytics|database)\b/i],
  ["Software", /\b(software|backend|frontend|fullstack|full-stack|developer|engineer|programmer|mobile|ios|android|devops|cloud|sre|qa|tester)\b/i],
  ["Sản phẩm & BA", /\b(product manager|product owner|business analyst|ba)\b/i],
  ["Thiết kế", /\b(designer|ux|ui|graphic)\b/i],
  ["Marketing", /\b(marketing|seo|content|brand|social media|truyền thông)\b/i],
  ["Kinh doanh & CS", /\b(sales|account manager|customer success|kinh doanh|bán hàng)\b/i],
  ["Tài chính & Nhân sự", /\b(accountant|finance|hr|recruit|talent|kế toán|nhân sự)\b/i],
];

function countCategories(titles) {
  const counts = new Map();
  for (const title of titles) {
    for (const [name, re] of CATEGORY_RULES) {
      if (re.test(title)) {
        counts.set(name, (counts.get(name) || 0) + 1);
        break;
      }
    }
  }
  return [...counts.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);
}

async function rows(env, sql, ...bind) {
  try {
    const result = await env.DB.prepare(sql).bind(...bind).all();
    return result.results || [];
  } catch {
    return [];
  }
}

async function first(env, sql, ...bind) {
  try {
    return await env.DB.prepare(sql).bind(...bind).first();
  } catch {
    return null;
  }
}

/**
 * @returns {Promise<{
 *   total:number, recent:number, previous:number, deltaPct:number|null,
 *   topSkills:{skill:string,count:number}[],
 *   topCompanies:{company:string,count:number}[],
 *   topLocations:{location:string,count:number}[],
 *   topSources:{source:string,count:number}[],
 *   categories:{name:string,count:number}[],
 *   windowDays:number, generatedAt:string
 * }>}
 */
export async function collectSignals(env) {
  const now = Math.floor(Date.now() / 1000);
  const weekAgo = now - 7 * 86400;
  const twoWeeksAgo = weekAgo - 7 * 86400;

  const [totalRow, recentRow, previousRow] = await Promise.all([
    first(env, "SELECT COUNT(*) AS n FROM jobs"),
    first(env, "SELECT COUNT(*) AS n FROM jobs WHERE posted_ts >= ?", weekAgo),
    first(env, "SELECT COUNT(*) AS n FROM jobs WHERE posted_ts >= ? AND posted_ts < ?", twoWeeksAgo, weekAgo),
  ]);

  const total = Number(totalRow?.n) || 0;
  const recent = Number(recentRow?.n) || 0;
  const previous = Number(previousRow?.n) || 0;

  const [skillRows, companyRows, locationRows, sourceRows, titleRows] = await Promise.all([
    rows(
      env,
      "SELECT value AS skill, COUNT(*) AS n FROM jobs, json_each(jobs.skills) WHERE jobs.posted_ts >= ? GROUP BY value ORDER BY n DESC LIMIT 12",
      weekAgo,
    ),
    rows(
      env,
      "SELECT company, COUNT(*) AS n FROM jobs WHERE posted_ts >= ? AND company NOT IN ('', 'N/A', 'n/a', 'Unknown') GROUP BY company ORDER BY n DESC LIMIT 8",
      weekAgo,
    ),
    rows(
      env,
      "SELECT location, COUNT(*) AS n FROM jobs WHERE posted_ts >= ? AND location <> '' GROUP BY location ORDER BY n DESC LIMIT 6",
      weekAgo,
    ),
    rows(
      env,
      "SELECT source, COUNT(*) AS n FROM jobs WHERE posted_ts >= ? AND source <> '' GROUP BY source ORDER BY n DESC LIMIT 8",
      weekAgo,
    ),
    rows(env, "SELECT title FROM jobs WHERE posted_ts >= ? LIMIT 4000", weekAgo),
  ]);

  const topSkills = skillRows
    .map((r) => ({ skill: String(r.skill || "").trim(), count: Number(r.n) || 0 }))
    .filter((r) => r.skill)
    .slice(0, 10);
  const topCompanies = companyRows.map((r) => ({ company: String(r.company || "").trim(), count: Number(r.n) || 0 }));
  const topLocations = locationRows.map((r) => ({ location: String(r.location || "").trim(), count: Number(r.n) || 0 }));
  const topSources = sourceRows.map((r) => ({ source: String(r.source || "").trim(), count: Number(r.n) || 0 }));

  return {
    total,
    recent,
    previous,
    deltaPct: previous > 0 ? Math.round(((recent - previous) / previous) * 100) : null,
    topSkills,
    topCompanies,
    topLocations,
    topSources,
    categories: countCategories(titleRows.map((r) => String(r.title || ""))),
    windowDays: 7,
    generatedAt: new Date().toISOString(),
  };
}

/** Every number a generated article may legitimately cite. */
export function signalNumbers(signals) {
  const values = new Set();
  for (const n of [signals.total, signals.recent, signals.previous]) {
    if (Number(n) > 0) values.add(Number(n));
  }
  if (typeof signals.deltaPct === "number") values.add(Math.abs(signals.deltaPct));
  for (const s of signals.topSkills || []) values.add(s.count);
  for (const c of signals.topCompanies || []) values.add(c.count);
  for (const c of signals.categories || []) values.add(c.count);
  for (const l of signals.topLocations || []) values.add(l.count);
  return [...values].filter((n) => n >= 3);
}

/** Vietnamese + plain formatting variants of a number. */
export function numberVariants(n) {
  const plain = String(n);
  const vi = Number(n).toLocaleString("vi-VN");
  return [...new Set([plain, vi, vi.replace(/\./g, ",")])];
}

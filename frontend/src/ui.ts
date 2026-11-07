import type { Job } from "./types";
import { t, getLang } from "./i18n";
import { fetchJobDetail, fetchJobDetails, ensureJobPage } from "./api";

type StatusType = "loading" | "success" | "error" | "info";

// ─── Apply Tracker ────────────────────────────────────────────────────────────

const _APPLIED_KEY = "goodjobs:applied";

function _getApplied(): Set<string> {
  try {
    return new Set(JSON.parse(localStorage.getItem(_APPLIED_KEY) ?? "[]") as string[]);
  } catch {
    return new Set();
  }
}

function _saveApplied(set: Set<string>): void {
  try {
    localStorage.setItem(_APPLIED_KEY, JSON.stringify([...set]));
  } catch { /* storage quota exceeded — silently ignore */ }
}

export function isApplied(link: string): boolean {
  return _getApplied().has(link);
}

export function markApplied(link: string): void {
  const set = _getApplied();
  set.add(link);
  _saveApplied(set);
}

export function unmarkApplied(link: string): void {
  const set = _getApplied();
  set.delete(link);
  _saveApplied(set);
}

// Pending-apply keys in sessionStorage (tab-scoped, survives page reload).
const _PENDING_LINK_KEY  = "goodjobs:pending_apply_link";
const _PENDING_TITLE_KEY = "goodjobs:pending_apply_title";

/** Called by the Apply button click handler BEFORE opening the external link. */
export function setPendingApply(link: string, title: string): void {
  try {
    sessionStorage.setItem(_PENDING_LINK_KEY, link);
    sessionStorage.setItem(_PENDING_TITLE_KEY, title);
  } catch { /* private browsing quota — ignore */ }
}

function _clearPendingApply(): void {
  try {
    sessionStorage.removeItem(_PENDING_LINK_KEY);
    sessionStorage.removeItem(_PENDING_TITLE_KEY);
  } catch { /* ignore */ }
}

/** Called from main.ts visibilitychange. Shows the prompt if a pending apply exists.
 *  Reads from sessionStorage so it survives a page reload on the same tab. */
export function applyTrackerHandleReturn(): void {
  let link = "";
  let title = "";
  try {
    link  = sessionStorage.getItem(_PENDING_LINK_KEY)  ?? "";
    title = sessionStorage.getItem(_PENDING_TITLE_KEY) ?? "";
  } catch { /* ignore */ }
  if (!link) return;
  _clearPendingApply();
  // Already marked — don't re-prompt
  if (isApplied(link)) return;
  _showApplyToast(link, title);
}

// ─── Apply Toast ──────────────────────────────────────────────────────────────

let _applyToast: HTMLDivElement | null = null;
let _toastTimeout: ReturnType<typeof setTimeout> | null = null;

export function initApplyToast(): void {
  if (_applyToast) return;
  const el = document.createElement("div");
  el.id = "applyToast";
  el.className = "apply-toast hidden";
  el.innerHTML = `
    <div class="apply-toast__icon">📋</div>
    <div class="apply-toast__body">
      <div class="apply-toast__q">${t("toast.question")}</div>
      <div class="apply-toast__title" id="applyToastTitle"></div>
    </div>
    <div class="apply-toast__actions">
      <button class="apply-toast__yes" id="applyToastYes">${t("toast.yes")}</button>
      <button class="apply-toast__no"  id="applyToastNo">${t("toast.no")}</button>
    </div>
  `;
  document.body.appendChild(el);
  _applyToast = el;

  el.querySelector("#applyToastYes")!.addEventListener("click", () => {
    const link = el.dataset.link ?? "";
    if (link) {
      markApplied(link);
      _refreshApplyUI(link);
    }
    _hideApplyToast();
  });
  el.querySelector("#applyToastNo")!.addEventListener("click", () => {
    _hideApplyToast();
  });
}

function _refreshApplyToastLanguage(): void {
  if (!_applyToast) return;
  const q = _applyToast.querySelector<HTMLElement>(".apply-toast__q");
  const yes = _applyToast.querySelector<HTMLElement>("#applyToastYes");
  const no = _applyToast.querySelector<HTMLElement>("#applyToastNo");
  if (q) q.textContent = t("toast.question");
  if (yes) yes.textContent = t("toast.yes");
  if (no) no.textContent = t("toast.no");
}

function _showApplyToast(link: string, title: string): void {
  if (!_applyToast) return;
  if (_toastTimeout) { clearTimeout(_toastTimeout); _toastTimeout = null; }
  _applyToast.dataset.link = link;
  const titleEl = _applyToast.querySelector<HTMLElement>("#applyToastTitle");
  if (titleEl) titleEl.textContent = title;
  _applyToast.classList.remove("hidden");
  // Auto-dismiss after 12 seconds
  _toastTimeout = setTimeout(_hideApplyToast, 12000);
}

function _hideApplyToast(): void {
  _applyToast?.classList.add("hidden");
  if (_toastTimeout) { clearTimeout(_toastTimeout); _toastTimeout = null; }
}

/** Refresh the apply button state for a given link in the table and modal. */
function _refreshApplyUI(link: string): void {
  const applied = isApplied(link);
  // Update all apply buttons in the table for this link
  document.querySelectorAll<HTMLButtonElement>(`[data-apply-link="${CSS.escape(link)}"]`).forEach(btn => {
    _styleApplyBtn(btn, applied);
  });
  // Update modal if it's open for this job
  const modalApplyBtn = document.getElementById("jobModalApplyBtn") as HTMLAnchorElement | null;
  if (modalApplyBtn && modalApplyBtn.href === link) {
    _styleModalApplyBtn(modalApplyBtn, applied);
  }
  // Re-render applied badge row indicator
  document.querySelectorAll<HTMLTableRowElement>(`tr[data-job-link="${CSS.escape(link)}"]`).forEach(tr => {
    if (applied) tr.classList.add("applied-row");
    else tr.classList.remove("applied-row");
  });
}

function _styleApplyBtn(btn: HTMLButtonElement, applied: boolean): void {
  btn.classList.toggle("apply-btn--applied", applied);
  btn.textContent = applied ? t("apply.applied") : t("apply.cta");
  btn.title = applied ? t("apply.titleUnmark") : t("apply.titleApply");
}

function _styleModalApplyBtn(btn: HTMLAnchorElement, applied: boolean): void {
  btn.classList.toggle("modal-apply-btn--applied", applied);
  btn.textContent = applied ? t("apply.applied") : t("apply.cta");
}

// Generic role nouns and level words that carry no domain signal.
const _ROLE_WORDS = new Set([
  "engineer", "developer", "dev", "specialist", "analyst", "consultant",
  "architect", "manager", "officer", "executive", "lead", "head",
  "coordinator", "administrator", "admin", "technician", "expert",
  "programmer", "coder", "designer", "researcher", "scientist",
  "senior", "sr", "junior", "jr", "intern", "internship",
  "fresher", "staff", "principal", "mid", "entry", "associate",
]);

// Expand common abbreviations to their full forms for matching job titles.
const _ABBR_EXPAND: Record<string, string[]> = {
  "ai": ["ai", "artificial intelligence"],
  "ml": ["ml", "machine learning"],
  "nlp": ["nlp", "natural language"],
  "cv": ["computer vision"],
  "ui": ["ui", "user interface"],
  "ux": ["ux", "user experience"],
  "be": ["backend"],
  "fe": ["frontend"],
  "fs": ["fullstack", "full stack"],
  "de": ["data"],
  "da": ["data"],
  "ds": ["data"],
  "ba": ["business"],
  "qa": ["qa", "quality"],
  "qc": ["qc", "quality"],
};

function _expandDomainWords(words: string[]): string[] {
  const result: string[] = [];
  for (const w of words) {
    result.push(w);
    const extras = _ABBR_EXPAND[w];
    if (extras) result.push(...extras);
  }
  return [...new Set(result)];
}

/**
 * Wrap domain words from the search keyword in <mark> tags within a job title.
 * Skips generic role/level words so only meaningful domain terms are highlighted.
 * Uses word-boundary matching to avoid partial matches (e.g. "ai" inside "aircraft").
 */
function highlightDomain(title: string, keyword: string): string {
  if (!keyword) return esc(title);
  const base = keyword.toLowerCase().split(/\s+/).filter(w => w.length >= 2 && !_ROLE_WORDS.has(w));
  const domainWords = _expandDomainWords(base).filter(w => w.length >= 2);
  if (domainWords.length === 0) return esc(title);
  const escaped = domainWords.map(w => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const pattern = new RegExp(`\\b(${escaped.join("|")})\\b`, "gi");
  return esc(title).replace(pattern, "<mark>$1</mark>");
}

/** Strip HTML tags and truncate plain text to max characters for table display. */
function truncate(text: string, max: number): string {
  const plain = text.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  if (plain.length <= max) return plain;
  return plain.slice(0, max).replace(/\s+\S*$/, "") + "…";
}

const statusEl      = document.getElementById("status")       as HTMLDivElement;
const resultsEl     = document.getElementById("results")      as HTMLDivElement;
const jobsBody      = document.getElementById("jobsBody")     as HTMLTableSectionElement;
const jobCountEl    = document.getElementById("jobCount")     as HTMLSpanElement;
// Related jobs section removed (feature gated for later release).
const filterBar    = document.getElementById("filterBar")   as HTMLDivElement;
const titleFilter  = document.getElementById("titleFilter") as HTMLInputElement;
const resultsPager       = document.getElementById("resultsPager")       as HTMLDivElement | null;
const resultsPrevBtn     = document.getElementById("resultsPrev")        as HTMLButtonElement | null;
const resultsNextBtn     = document.getElementById("resultsNext")        as HTMLButtonElement | null;
const resultsPageInfo    = document.getElementById("resultsPageInfo")    as HTMLSpanElement | null;
const resultsPagerShowing = document.getElementById("resultsPagerShowing") as HTMLDivElement | null;
const resultsPageSizeEl   = document.getElementById("resultsPageSize")    as HTMLSelectElement | null;

/** Search results are paginated; only this many rows render at once. */
let resultsPageSize = Math.max(1, Number(resultsPageSizeEl?.value) || 20);
let _resultsPage = 1;

titleFilter.addEventListener("input", () => _applyFilter(true));

resultsPageSizeEl?.addEventListener("change", () => {
  resultsPageSize = Math.max(1, Number(resultsPageSizeEl.value) || 20);
  _resultsPage = 1;
  _applyFilter();
});

resultsPrevBtn?.addEventListener("click", () => {
  if (_resultsPage > 1) {
    _resultsPage -= 1;
    _applyFilter();
    resultsEl.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

resultsNextBtn?.addEventListener("click", () => {
  _resultsPage += 1;
  _applyFilter();
  resultsEl.scrollIntoView({ behavior: "smooth", block: "start" });
});

// Job detail modal elements
const jobModal         = document.getElementById("jobModal")         as HTMLDivElement;
const jobModalTitle    = document.getElementById("jobModalTitle")    as HTMLHeadingElement;
const jobModalCompany  = document.getElementById("jobModalCompany")  as HTMLSpanElement;
const jobModalLocation = document.getElementById("jobModalLocation") as HTMLSpanElement;
const jobModalPosted   = document.getElementById("jobModalPosted")   as HTMLSpanElement;
const jobModalSource   = document.getElementById("jobModalSource")   as HTMLSpanElement;
const jobModalDesc     = document.getElementById("jobModalDesc")     as HTMLDivElement;
const jobModalClose    = document.getElementById("jobModalClose")    as HTMLButtonElement;
const jobModalShareBtn = document.getElementById("jobModalShareBtn") as HTMLButtonElement;
const jobModalSkills   = document.getElementById("jobModalSkills")   as HTMLDivElement;
const jobModalSummary  = document.getElementById("jobModalSummary")  as HTMLDivElement;
const jobModalBody     = document.querySelector(".job-modal-body")    as HTMLDivElement;

const resultsShareBtn = document.getElementById("resultsShareBtn") as HTMLButtonElement;

let _searchKeyword = "";
let _searchLocation: string | undefined;
let _fromCache = false;

export function setSearchContext(keyword: string, location?: string): void {
  _searchKeyword = keyword;
  _searchLocation = location;
  _fromCache = false;
}

export function setFromCache(val: boolean): void {
  _fromCache = val;
}

/** Vector similarity scores only mean something for CV/skills-paste searches —
 * hide the Score column when searching by a plain job title. */
export function setScoreColumnVisible(visible: boolean): void {
  jobsBody.closest("table")?.classList.toggle("hide-score", !visible);
}

async function _copyToClipboard(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // fall back below
  }

  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-1000px";
    ta.style.left = "-1000px";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    ta.remove();
    return ok;
  } catch {
    return false;
  }
}

function _copyWithFeedback(btn: HTMLButtonElement, text: string): void {
  const original = btn.textContent ?? "";
  void _copyToClipboard(text).then((ok) => {
    btn.textContent = ok ? t("copy.copied") : t("copy.failed");
    btn.disabled = true;
    window.setTimeout(() => {
      btn.textContent = original;
      btn.disabled = false;
    }, 1000);
  });
}

/** Format an ISO date (YYYY-MM-DD) as "Sep 5", adding the year only if it isn't the current one. */
function formatPostedDate(dateStr?: string, ts?: number): string {
  let d: Date | null = null;
  if (dateStr) {
    d = new Date(`${dateStr}T00:00:00`);
    if (isNaN(d.getTime())) d = null;
  }
  if (!d && typeof ts === "number" && ts > 0) d = new Date(ts * 1000);
  if (!d) return "—";
  const opts: Intl.DateTimeFormatOptions = { month: "short", day: "numeric" };
  if (d.getFullYear() !== new Date().getFullYear()) opts.year = "numeric";
  return d.toLocaleDateString(getLang() === "vi" ? "vi-VN" : "en-US", opts);
}

const _INVALID_POSTED = /^(?:-|—|none|null|n\/a|unknown|not specified)?$/i;

/** Relative label from a Unix timestamp, matching the backend's wording. */
function relativePosted(ts?: number): string {
  if (!ts || ts <= 0) return "";
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

/** Never show "-"/"None": fall back to the post timestamp, date or scraped date. */
function displayPosted(job: Job): string {
  const raw = String(job.posted ?? "").trim();
  if (raw && !_INVALID_POSTED.test(raw)) return raw;
  const relative = relativePosted(job.posted_ts);
  if (relative) return relative;
  const date = formatPostedDate(job.posted_date, job.posted_ts);
  if (date !== "—") return date;
  const scraped = formatPostedDate(undefined, job._scraped_ts);
  return scraped !== "—" ? scraped : "Recently";
}

/** Post-date cell value with posted_date → posted_ts → scrape time fallback. */
function displayPostDate(job: Job): string {
  const date = formatPostedDate(job.posted_date, job.posted_ts);
  if (date !== "—") return date;
  const scraped = formatPostedDate(undefined, job._scraped_ts);
  return scraped !== "—" ? scraped : "Recently";
}

/** Render skill tags as HTML spans, collapsing extras into a "+N" pill. */
function renderSkillTags(skills: string[] | undefined, max = 10): string {
  if (!skills || skills.length === 0) return "";
  const shown = skills.slice(0, max);
  const extra = skills.length - shown.length;
  const tags = shown.map(s => `<span class="skill-tag">${esc(s)}</span>`).join("");
  if (extra > 0) {
    const remaining = skills.slice(max).map(s => esc(s));
    return tags + `<span class="skill-tag skill-tag-more" data-extra='${JSON.stringify(remaining)}'>+${extra}</span>`;
  }
  return tags;
}

const _AVATAR_COLORS = [
  ["#dbeafe","#1d4ed8"], ["#dcfce7","#15803d"], ["#fef9c3","#854d0e"],
  ["#ede9fe","#5b21b6"], ["#ffedd5","#c2410c"], ["#fee2e2","#b91c1c"],
  ["#cffafe","#0e7490"], ["#f0fdf4","#166534"], ["#fdf4ff","#86198f"],
];

/** Return [bg, fg] color pair for a company letter avatar, deterministic per company name. */
function _avatarStyle(company: string): [string, string] {
  let hash = 0;
  for (let i = 0; i < company.length; i++) hash = (hash * 31 + company.charCodeAt(i)) & 0xffff;
  const [bg, fg] = _AVATAR_COLORS[hash % _AVATAR_COLORS.length];
  return [bg, fg];
}

const _SOURCE_DOMAINS: Record<string, string> = {
  "linkedin":     "linkedin.com",
  "itviec":       "itviec.com",
  "topcv":        "topcv.vn",
  "vietnamworks": "vietnamworks.com",
  "topdev":       "topdev.vn",
  "indeed":       "indeed.com",
  "careerviet":   "careerviet.vn",
  "jobsgo":       "jobsgo.vn",
  "careerlink":   "careerlink.vn",
  "glints":       "glints.com",
  "viecoi":       "viecoi.vn",
  "techjobs":     "techjobs.vn",
  "xomdata":      "xomdata.com",
  "dice":         "dice.com",
  "usajobs":      "usajobs.gov",
  "glassdoor":    "glassdoor.com",
  "remoteok":     "remoteok.com",
  "weworkremotely": "weworkremotely.com",
  "google":       "google.com",
};

/** Some sources store "N/A"/"Unknown" as the company; don't show that to users. */
function displayCompany(company?: string | null): string {
  const value = (company ?? "").trim();
  if (!value || /^(n\/?a|unknown|none|null|-+)$/i.test(value)) return "Company not specified";
  return value;
}

/** Render a company logo img or letter-avatar span, with Clearbit and scraped-logo fallbacks. */
function companyLogoHtml(company: string, source: string, logo?: string): string {
  const initial = (company.trim()[0] || "?").toUpperCase();
  const [bg, fg] = _avatarStyle(company);
  const fallback = `Object.assign(document.createElement('span'),{className:'company-avatar',style:'background:${bg};color:${fg}',textContent:'${initial}'})`;

  if (logo) {
    return `<img class="company-logo" src="${esc(logo)}" alt="${esc(company)}" width="20" height="20" loading="lazy" decoding="async" onerror="this.replaceWith(${fallback})">`;
  }

  // Direct submissions rarely have a company logo of their own (often
  // confidential/recruiter postings) — brand them with the GoodJobs mark
  // instead of a random letter-avatar or a Clearbit lookup that would just
  // guess at an unrelated domain.
  if (source.toLowerCase() === "direct") {
    return `<img class="company-logo" src="/favicon.svg" alt="GoodJobs" width="20" height="20" loading="lazy" decoding="async" onerror="this.replaceWith(${fallback})">`;
  }

  const domain = _SOURCE_DOMAINS[source.toLowerCase()];
  if (domain) {
    return `<img class="company-logo" src="https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=64" alt="${esc(source)}" width="20" height="20" loading="lazy" decoding="async" onerror="this.replaceWith(${fallback})">`;
  }

  return `<span class="company-avatar" style="background:${bg};color:${fg}">${initial}</span>`;
}

/** Short display labels for source badges — keeps the UI compact. */
const _SOURCE_LABEL: Record<string, string> = {
  "VietnamWorks": "VNWorks",
  "CareerViet":   "CViet",
  "CareerLink":   "CLink",
};
function sourceLabel(source: string): string {
  return _SOURCE_LABEL[source] ?? source;
}

/** Render the job board's own favicon/logo instead of a text badge.
 *  Falls back to the text badge if the image cannot be loaded. */
function sourceLogoHtml(source: string): string {
  const key = source.toLowerCase();
  const label = sourceLabel(source);
  const domain = _SOURCE_DOMAINS[key] ?? (source.includes(".") ? source : "");

  if (key === "direct" || key === "goodjobs") {
    return `<span class="source-logo-wrap" data-source-label="${esc(label)}" title="${esc(label)}">
      <img class="source-logo" src="/favicon.svg" alt="${esc(label)}" width="18" height="18" loading="lazy" decoding="async"
        onerror="this.style.display='none';this.parentElement.classList.add('source-logo-wrap--failed')">
    </span>`;
  }

  if (domain) {
    const favicon = `https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=64`;
    return `<span class="source-logo-wrap" data-source-label="${esc(label)}" title="${esc(label)}">
      <img class="source-logo" src="${favicon}" alt="${esc(label)}" width="18" height="18" loading="lazy" decoding="async"
        onerror="this.style.display='none';this.parentElement.classList.add('source-logo-wrap--failed')">
    </span>`;
  }

  return `<span class="badge badge-${esc(key)}">${esc(label)}</span>`;
}

/** Cached full jobs, loaded lazily for the current page only. */
const _fullJobCache = new Map<string, Job>();

function cacheFullJob(full: Job): void {
  if (!full.link) return;
  if (_fullJobCache.size > 500) {
    const first = _fullJobCache.keys().next().value;
    if (first) _fullJobCache.delete(first);
  }
  _fullJobCache.set(full.link, full);
}

function applyFullJobToModal(full: Job): void {
  if (jobModal.classList.contains("hidden")) return;
  if (full.description) jobModalDesc.innerHTML = full.description;
  jobModalSkills.innerHTML = renderSkillTags(full.skills, 30);
  jobModalCompany.innerHTML = `<div class="company-cell">${companyLogoHtml(full.company, full.source, full.logo)}<span>${esc(displayCompany(full.company))}</span></div>`;
}

/**
 * Load full descriptions only for the jobs on the current page.
 * Called after rendering a page, so opening a modal is instant.
 */
export async function prefetchJobDetails(jobs: Job[]): Promise<void> {
  const items = jobs
    .filter((job) => job._compact && job.link && job._cache_key)
    .map((job) => ({ link: job.link, key: job._cache_key }));
  if (items.length === 0) return;

  const fullJobs = await fetchJobDetails(items);
  for (const full of fullJobs) cacheFullJob(full);
  if (_openModalLink) {
    const openFull = _fullJobCache.get(_openModalLink);
    if (openFull) applyFullJobToModal(openFull);
  }
}

/** Open the job detail modal, populating all fields from the given job. */
function openJobModal(job: Job): void {
  const cachedFull = job.link ? _fullJobCache.get(job.link) : undefined;
  if (cachedFull) {
    job.description = cachedFull.description || job.description;
    job.skills = cachedFull.skills ?? job.skills;
    job.company = cachedFull.company || job.company;
    job.logo = cachedFull.logo || job.logo;
  }

  _openModalLink               = job.link;
  _openModalCacheKey           = job._cache_key || "";
  jobModalTitle.textContent    = job.title;
  jobModalCompany.innerHTML    = `<div class="company-cell">${companyLogoHtml(job.company, job.source, job.logo)}<span>${esc(displayCompany(job.company))}</span></div>`;
  jobModalLocation.textContent = job.location;
  jobModalPosted.textContent   = displayPosted(job);
  jobModalSource.innerHTML     = `${sourceLogoHtml(job.source)}<span class="modal-source-name">${esc(sourceLabel(job.source))}</span>`;
  const isEnriching = !_enrichmentDone && (
    (_linkedinEnriching && job.source === "LinkedIn") ||
    (_topcvEnriching    && job.source === "TopCV"));
  if (!job.skills?.length && isEnriching) {
    jobModalSkills.innerHTML = `<span class="desc-loading">${t("modal.findingSkills")}</span>`;
  } else {
    // Modal can show more skills than the table.
    jobModalSkills.innerHTML = renderSkillTags(job.skills, 30);
  }
  // Summaries are disabled in the backend; never show an AI summary box.
  jobModalSummary.textContent = "";
  jobModalSummary.classList.add("hidden");

  const modalApplyBtn = document.getElementById("jobModalApplyBtn") as HTMLAnchorElement | null;
  if (modalApplyBtn && job.link) {
    const applied = isApplied(job.link);
    modalApplyBtn.href = job.link;
    _styleModalApplyBtn(modalApplyBtn, applied);
    modalApplyBtn.onclick = (e) => {
      e.preventDefault();
      setPendingApply(job.link, job.title);
      window.open(job.link, "_blank", "noopener");
    };
  }

  jobModal.classList.remove("hidden");
  document.body.style.overflow = "hidden";
  const needsFull = Boolean(job._compact && job.link && !cachedFull);
  const descHtml = (!job.description && isEnriching)
    ? `<span class="desc-loading">${t("modal.findingDesc")}</span>`
    : (job.description || "") +
      (needsFull ? `<div class="desc-loading">${t("modal.loadingFullDesc")}</div>` : "");
  requestAnimationFrame(() => {
    jobModalBody.scrollTop = 0;
    jobModalDesc.innerHTML = descHtml;
  });

  // Homepage stream rows carry only a short description snippet. If the current
  // page's prefetch hasn't arrived yet, fetch this one job as a fallback.
  const modalLink = job.link;
  if (needsFull && modalLink) {
    void fetchJobDetail(modalLink, job._cache_key).then((full) => {
      if (!full) {
        jobModalDesc.querySelector(".desc-loading")?.remove();
        return;
      }
      cacheFullJob(full);
      if (_openModalLink === modalLink) applyFullJobToModal(full);
    });
  }
}

/** Close the job detail modal and restore body scroll. */
function closeJobModal(): void {
  jobModal.classList.add("hidden");
  document.body.style.overflow = "";
  _openModalLink = "";
  _openModalCacheKey = "";
}

jobModalClose.addEventListener("click", closeJobModal);
jobModal.addEventListener("click", (e) => {
  if (e.target === jobModal) closeJobModal();
});

// Expand +N skill pill on click
jobModalSkills.addEventListener("click", (e) => {
  const pill = (e.target as HTMLElement).closest(".skill-tag-more") as HTMLElement | null;
  if (!pill) return;
  const extra: string[] = JSON.parse(pill.dataset.extra || "[]");
  const newTags = extra.map(s => `<span class="skill-tag">${s}</span>`).join("");
  pill.outerHTML = newTags;
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && !jobModal.classList.contains("hidden")) closeJobModal();
});

/** Show a plain-text status message with the given severity type. */
export function setStatus(msg: string, type: StatusType): void {
  statusEl.textContent = msg;
  statusEl.className = `status status-${type}`;
  statusEl.classList.remove("hidden");
}

/** Show an HTML status message with the given severity type. */
export function setStatusHtml(html: string, type: StatusType): void {
  statusEl.innerHTML = html;
  statusEl.className = `status status-${type}`;
  statusEl.classList.remove("hidden");
}

/** Hide the status bar. */
export function clearStatus(): void {
  statusEl.classList.add("hidden");
}

const progressBar     = document.getElementById("progressBar")     as HTMLDivElement;
const progressCount   = document.getElementById("progressCount")   as HTMLSpanElement;
const progressPills   = document.getElementById("progressPills")   as HTMLDivElement;
const progressTimer   = document.getElementById("progressTimer")   as HTMLSpanElement;
const queueBanner     = document.getElementById("queueBanner")     as HTMLDivElement;

const SITES = ["LinkedIn", "ITViec", "TopCV", "VietnamWorks", "TopDev", "Indeed", "CareerViet"];

let _timerInterval: ReturnType<typeof setInterval> | null = null;
let _linkedinEnriching = false;
let _topcvEnriching    = false;
// False while a search stream is open. Once it closes, no more descriptions can
// arrive, so rows still missing one show "—" instead of a never-ending spinner.
let _enrichmentDone    = true;
let _openModalLink = "";
let _openModalCacheKey = "";

const _PROGRESS_MESSAGES: Record<"en" | "vi", string[]> = {
  en: [
    "Vibing…", "Scouting…", "Scoring…", "Hunting…", "Crawling…", "Fetching…",
    "Searching…", "Browsing…", "Scanning…", "Digging…", "Grinding…", "Hustling…",
    "Lurking…", "Sniffing…", "Stalking…", "Probing…", "Yapping…", "Cooking…",
    "Rizzling…", "Slaying…", "Leveling up…", "No-lifing…", "Sweating…",
    "Speedrunning…", "Farming…", "Touching grass…", "Manifesting…", "Crunching…",
    "Summoning…", "Plotting…",
  ],
  vi: [
    "Đang vibe…", "Đang dò la…", "Đang chấm điểm…", "Đang săn việc…", "Đang bò…",
    "Đang lấy data…", "Đang tìm…", "Đang lượn…", "Đang quét…", "Đang đào…",
    "Đang cày…", "Đang cố gắng…", "Đang rình…", "Đang đánh hơi…", "Đang theo dõi…",
    "Đang dò xét…", "Đang lảm nhảm…", "Đang nấu…", "Đang gáy…", "Đang cháy…",
    "Đang lên level…", "Đang cắm mặt…", "Đang đổ mồ hôi…", "Đang speedrun…",
    "Đang farm…", "Đang hít thở…", "Đang ước…", "Đang nghiền…", "Đang triệu hồi…",
    "Đang lên kế hoạch…",
  ],
};

/** Show the progress bar and start the rotating status message animation. */
export function showProgress(): void {
  progressPills.innerHTML = "";
  progressCount.textContent = t("progress.searching");
  progressBar.classList.remove("hidden");

  const messages = _PROGRESS_MESSAGES[getLang()];
  let msgIndex = Math.floor(Math.random() * messages.length);
  progressTimer.textContent = messages[msgIndex];

  if (_timerInterval) clearInterval(_timerInterval);
  _timerInterval = setInterval(() => {
    const msgs = _PROGRESS_MESSAGES[getLang()];
    msgIndex = (msgIndex + 1) % msgs.length;
    progressTimer.textContent = msgs[msgIndex];
  }, 3000);
}

/** Update the progress bar job count with a per-source breakdown. */
export function updateProgressCount(jobs: Job[]): void {
  if (jobs.length === 0) {
    progressCount.textContent = t("progress.searching");
    return;
  }
  const bySource = new Map<string, number>();
  for (const j of jobs) bySource.set(j.source, (bySource.get(j.source) ?? 0) + 1);
  const breakdown = [...bySource.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([src, n]) => `${src} ${n}`)
    .join(" · ");
  progressCount.textContent = t("progress.foundBreakdown", { count: jobs.length, breakdown });
}

/** Set whether LinkedIn Phase 2 description enrichment is in progress. */
export function setLinkedInEnriching(value: boolean): void {
  _linkedinEnriching = value;
}

/** Mark whether the search stream has finished, re-rendering rows so placeholders settle. */
export function setEnrichmentDone(value: boolean): void {
  _enrichmentDone = value;
  if (!value) return;
  setLinkedInEnriching(false);
  setTopCVEnriching(false);
  if (_allJobs.length > 0) _applyFilter();
  if (jobModalDesc.querySelector(".desc-loading")) {
    jobModalDesc.innerHTML = '<span class="no-skills">—</span>';
  }
  if (jobModalSkills.querySelector(".desc-loading")) {
    jobModalSkills.innerHTML = '<span class="no-skills">—</span>';
  }
}

/** Set whether TopCV Phase 2 detail enrichment is in progress. */
export function setTopCVEnriching(value: boolean): void {
  _topcvEnriching = value;
}

/** Replace or add a progress pill for a site that is currently fetching descriptions. */
export function markSiteLoading(site: string, count: number): void {
  const label = SITES.find((s) => s.toLowerCase() === site.toLowerCase()) ?? site;
  // Remove existing pill for this site (e.g. the "done" pill from Phase 1)
  const existing = progressPills.querySelector<HTMLElement>(`[data-site="${label}"]`);
  if (existing) existing.remove();
  const pill = document.createElement("span");
  pill.className = "progress-pill progress-pill-loading";
  pill.dataset.site = label;
  pill.textContent = t("progress.siteLoading", { label });
  pill.title = t("progress.siteLoadingTitle", { count });
  progressPills.appendChild(pill);
}

/** Replace or add a completed progress pill for a site showing its result count. */
export function markSiteDone(site: string, count: number): void {
  if (count === 0) return; // only show sites that found results
  const label = SITES.find((s) => s.toLowerCase() === site.toLowerCase()) ?? site;
  const existing = progressPills.querySelector<HTMLElement>(`[data-site="${label}"]`);
  if (existing) existing.remove();
  const pill = document.createElement("span");
  pill.className = "progress-pill progress-pill-done";
  pill.dataset.site = label;
  pill.textContent = `${label} ${count}`;
  pill.title = `${count} jobs`;
  progressPills.appendChild(pill);
}

/** Show the queue banner indicating the user's position in the scrape queue. */
export function showQueuedMessage(position: number): void {
  queueBanner.textContent = t("queue.waiting", { position });
  queueBanner.classList.remove("hidden");
}

/** Hide and clear the queue banner. */
export function clearQueuedMessage(): void {
  queueBanner.classList.add("hidden");
  queueBanner.textContent = "";
}

/** Hide the progress bar, stop the timer, and clear all pills and queue banners. */
export function hideProgress(): void {
  if (_timerInterval) {
    clearInterval(_timerInterval);
    _timerInterval = null;
  }
  progressBar.classList.add("hidden");
  progressPills.innerHTML = "";
  clearQueuedMessage();
}

/** Render all jobs into the results table and make it visible. */
export function showResults(jobs: Job[]): void {
  renderTable(jobs);
  resultsEl.classList.remove("hidden");
}

/** Clear and hide the results table and reset all filter state. */
export function hideResults(): void {
  jobsBody.innerHTML = "";
  jobCountEl.textContent = "";
  resultsEl.classList.add("hidden");
  filterBar.classList.add("hidden");
  // Reset filter state
  _allJobs = [];
  _activeSources.clear();
  _activeSkills.clear();
  titleFilter.value = "";
  _resultsPage = 1;
  resultsPager?.classList.add("hidden");
}

/** Re-render all language-dependent dynamic UI after an EN/VI toggle (static
 * markup is handled separately via data-i18n attributes in main.ts). */
export function refreshUILanguage(): void {
  _refreshApplyToastLanguage();
  if (_allJobs.length > 0) {
    _rebuildFilterBar();
    _applyFilter();
  }
  if (_openModalLink) {
    const btn = document.getElementById("jobModalApplyBtn") as HTMLAnchorElement | null;
    if (btn) _styleModalApplyBtn(btn, isApplied(_openModalLink));
  }
}

export function openJobByLink(link: string): boolean {
  const job = _allJobs.find((j) => j.link === link);
  if (!job) return false;
  openJobModal(job);
  return true;
}

/** Open a shared job link directly, without needing a matching search. */
export async function openSharedJob(link: string): Promise<boolean> {
  const full = await fetchJobDetail(link);
  if (!full) return false;
  openJobModal(full);
  return true;
}

let _allJobs: Job[] = [];
let _activeSources = new Set<string>();
let _activeSkills = new Set<string>();

/** Re-render the table rows based on the current _activeSources filter set, title words, and active skills.
 *  Results are paginated (resultsPageSize per page) so a big search does not render hundreds of rows at once. */
function _applyFilter(resetPage = false): void {
  if (resetPage) _resultsPage = 1;
  const words = titleFilter.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const visible = _allJobs.filter((j) => {
    if (_activeSources.size > 0 && !_activeSources.has(j.source)) return false;
    if (words.length > 0) {
      const title = j.title.toLowerCase();
      if (!words.every((w) => title.includes(w))) return false;
    }
    if (_activeSkills.size > 0) {
      const jobSkills = (j.skills ?? []).map(s => s.toLowerCase());
      if (![..._activeSkills].every(s => jobSkills.includes(s))) return false;
    }
    return true;
  });

  const totalPages = Math.max(1, Math.ceil(visible.length / resultsPageSize));
  _resultsPage = Math.min(Math.max(1, _resultsPage), totalPages);
  const start = (_resultsPage - 1) * resultsPageSize;
  const pageJobs = visible.slice(start, start + resultsPageSize);

  // Direct submissions (source "Direct") are merged into the same results
  // table as scraped jobs when searching — they only get a separate table
  // on the default homepage "Recent Jobs" feed (see main.ts).
  jobsBody.innerHTML = "";
  if (visible.length === 0) {
    jobsBody.innerHTML =
      `<tr><td colspan="10" class="empty">${t("empty.noMatch")}</td></tr>`;
  } else {
    pageJobs.forEach((job, i) => jobsBody.appendChild(buildRow(job, start + i + 1)));
  }

  const total = _allJobs.length;
  const shown = visible.length;
  jobCountEl.textContent =
    shown === total
      ? t("results.countAll", { total })
      : t("results.countFiltered", { shown, total });

  _renderResultsPager(shown, totalPages, start);
}

/** Update the search-results pager (hidden when everything fits on one page). */
function _renderResultsPager(total: number, totalPages: number, start: number): void {
  if (!resultsPager) return;
  const show = total > resultsPageSize;
  resultsPager.classList.toggle("hidden", !show);
  if (!show) return;
  if (resultsPrevBtn) resultsPrevBtn.disabled = _resultsPage <= 1;
  if (resultsNextBtn) resultsNextBtn.disabled = _resultsPage >= totalPages;
  if (resultsPageInfo) {
    resultsPageInfo.textContent = t("pager.pageInfo", { page: _resultsPage, pages: totalPages });
  }
  if (resultsPagerShowing) {
    resultsPagerShowing.textContent = t("pager.showing", {
      from: start + 1,
      to: Math.min(start + resultsPageSize, total),
      total,
    });
  }
}

/** Rebuild the source filter pill bar from current _allJobs. */
function _rebuildFilterBar(): void {
  const label = filterBar.querySelector(".filter-label");
  filterBar.innerHTML = "";
  if (label) filterBar.appendChild(label);
  filterBar.appendChild(titleFilter);

  const sources = [...new Set(_allJobs.map((j) => j.source))].sort();

  if (_allJobs.length === 0) {
    filterBar.classList.add("hidden");
    return;
  }

  filterBar.classList.remove("hidden");

  if (sources.length <= 1) return;

  sources.forEach((source) => {
    const count = _allJobs.filter((j) => j.source === source).length;
    const btn = document.createElement("span");
    // Start inactive; activate on click (like skill pills)
    const isActive = _activeSources.has(source);
    btn.className = `badge badge-${source.toLowerCase()} badge-filter${isActive ? " active" : ""}`;
    btn.textContent = `${sourceLabel(source)} ${count}`;
    btn.dataset.source = source;

    btn.addEventListener("click", () => {
      if (_activeSources.has(source)) {
        _activeSources.delete(source);
        btn.classList.remove("active");
      } else {
        _activeSources.add(source);
        btn.classList.add("active");
      }
      _applyFilter(true);
    });

    filterBar.appendChild(btn);
  });

  // Skill filter row
  const sourceKeys = new Set(sources.map(s => s.toLowerCase()));
  const skillFreq = new Map<string, number>();
  const skillDisplay = new Map<string, string>();
  for (const j of _allJobs) {
    for (const s of (j.skills ?? [])) {
      const key = s.toLowerCase();
      // Don't treat job sources (LinkedIn/TopCV/...) as skills.
      if (sourceKeys.has(key)) continue;
      skillFreq.set(key, (skillFreq.get(key) ?? 0) + 1);
      if (!skillDisplay.has(key)) skillDisplay.set(key, s);
    }
  }
  const topSkills = [...skillFreq.entries()]
    .filter(([, count]) => count >= 2)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([skill]) => skill);

  if (topSkills.length > 0) {
    const skillRow = document.createElement("div");
    skillRow.className = "skill-filter-row";
    topSkills.forEach((skill) => {
      const pill = document.createElement("span");
      const isActive = _activeSkills.has(skill);
      pill.className = `skill-tag skill-filter-pill${isActive ? " active" : ""}`;
      pill.textContent = `${skillDisplay.get(skill) ?? skill} ×${skillFreq.get(skill)}`;
      pill.addEventListener("click", () => {
        if (_activeSkills.has(skill)) {
          _activeSkills.delete(skill);
          pill.classList.remove("active");
        } else {
          _activeSkills.add(skill);
          pill.classList.add("active");
        }
        _applyFilter(true);
      });
      skillRow.appendChild(pill);
    });
    filterBar.appendChild(skillRow);
  }
}

/**
 * Insert a batch of new jobs into the table, keeping rows sorted newest-first.
 * Jobs with the same link are updated in-place (e.g. LinkedIn enrichment).
 * Returns the merged sorted job list.
 */
export function appendJobs(existing: Job[], incoming: Job[]): Job[] {
  const jobKey = (j: Job): string => {
    // Primary identity is the canonical link (used for in-place enrichment).
    if (j.link) return j.link;
    // Fallback for rare cases where a scraper returns an empty link.
    // This prevents clobbering multiple jobs into one Map entry.
    const t = (j.title || "").trim().toLowerCase();
    const c = (j.company || "").trim().toLowerCase();
    const s = (j.source || "").trim().toLowerCase();
    const l = (j.location || "").trim().toLowerCase();
    const ts = typeof j.posted_ts === "number" ? j.posted_ts : 0;
    return `nolink:${s}:${c}:${t}:${l}:${ts}`;
  };

  // Merge incoming into existing: update jobs with same link (e.g. LinkedIn
  // description enrichment pass), append truly new ones.
  const byLink = new Map(existing.map(j => [jobKey(j), j]));
  for (const j of incoming) {
    const key = jobKey(j);
    const prev = byLink.get(key);
    // Preserve existing semantic score if the incoming update doesn't include it
    // (common for phase-2 enrichment updates).
    if (prev && typeof prev._vector_score === "number" && typeof j._vector_score !== "number") {
      (j as Job)._vector_score = prev._vector_score;
    }
    byLink.set(key, j); // overwrite with enriched version if same link
  }
  const merged = [...byLink.values()].sort((a, b) => {
    const sa = a._vector_score;
    const sb = b._vector_score;
    // If we have vector scores, rank by relevance first.
    if (typeof sa === "number" || typeof sb === "number") {
      const da = typeof sa === "number" ? sa : -Infinity;
      const db = typeof sb === "number" ? sb : -Infinity;
      if (db !== da) return db - da;
    }
    const ta = a.posted_ts ?? 0;
    const tb = b.posted_ts ?? 0;
    return tb - ta; // larger timestamp = newer = first
  });

  _allJobs = merged;
  _rebuildFilterBar();
  _applyFilter();

  // If the modal is open for a job that just received enriched data, update it live
  if (_openModalLink) {
    // Modal tracks by real link; refresh only when that specific link was enriched.
    const openJob = byLink.get(_openModalLink);
    if (openJob && incoming.some(j => j.link === _openModalLink)) {
      // This job was part of the incoming batch — refresh modal fields
      if (openJob.description) {
        jobModalDesc.innerHTML = openJob.description;
      }
      // Modal can show more skills than the table.
      jobModalSkills.innerHTML = renderSkillTags(openJob.skills, 30);
    }
  }

  resultsEl.classList.remove("hidden");
  return merged;
}

/** Replace the full job list and re-render the table and filter bar. */
function renderTable(jobs: Job[]): void {
  _allJobs = jobs;
  _rebuildFilterBar();
  _applyFilter();
}

/**
 * Replace the entire displayed job list with a newly-sorted set.
 * Used by the `rescore` SSE event when local embedding scores arrive
 * after the initial `cached` event has already been rendered.
 */
export function replaceJobs(jobs: Job[]): Job[] {
  _allJobs = jobs;
  _rebuildFilterBar();
  _applyFilter();
  return jobs;
}

/** Build a table row element for a single job, wiring up the modal click handler.
 * `seventhColumn` picks what the 7th column shows: "skills" (search results) or
 * "date" (homepage Recent Jobs / Posted Directly tables). */
export function buildRow(job: Job, num: number, seventhColumn: "skills" | "date" = "skills"): HTMLTableRowElement {
  const tr = document.createElement("tr");
  if (job.level_match) tr.classList.add("level-match");
  if (!_fromCache) {
    const _baseDomain = _searchKeyword.toLowerCase().split(/\s+/).filter(w => w.length >= 2 && !_ROLE_WORDS.has(w));
    const _domainWords = _expandDomainWords(_baseDomain).filter(w => w.length >= 2);
    const titleLower = job.title.toLowerCase();
    if (_domainWords.length > 0 && _domainWords.some(w => new RegExp(`\\b${w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`).test(titleLower))) tr.classList.add("domain-match");
  }
  const seventhCellHtml = seventhColumn === "date"
    ? `<td class="posted-date-cell">${esc(displayPostDate(job))}</td>`
    : (() => {
        const skillsHtml = renderSkillTags(job.skills, 6);
        return `<td class="skills-cell">${skillsHtml ? `<div class="skills-clamp">${skillsHtml}</div>` : '<span class="no-skills">—</span>'}</td>`;
      })();
  const descText = truncate(job.description ?? "", 200);
  // Job listings stream in before their descriptions are fetched (Phase 2 enrichment).
  // Show a shimmer placeholder while the stream is open; after it closes, the
  // description is not coming (e.g. beyond the per-site enrich limit).
  const descCell = descText
    ? esc(descText)
    : _enrichmentDone
      ? '<span class="no-skills">—</span>'
      : `<span class="desc-loading">${t("table.loadingDesc")}</span>`;
  const score = typeof job._vector_score === "number" ? job._vector_score.toFixed(3) : "";
  const scorePill = score ? `<span class="title-score" title="Vector similarity score">${esc(score)}</span>` : "";
  const levelBadge = job.level_match ? `<span class="level-badge">✓ Match</span>` : "";
  const applied = job.link ? isApplied(job.link) : false;
  if (applied) tr.classList.add("applied-row");
  if (job.link) tr.dataset.jobLink = job.link;
  tr.innerHTML = `
    <td class="num">${num}</td>
    <td class="title" data-company="${esc(job.company)}">
      <div class="title-main">${_fromCache ? esc(job.title) : highlightDomain(job.title, _searchKeyword)}${levelBadge}</div>
      <div class="title-meta">
        ${companyLogoHtml(job.company, job.source, job.logo)}
        <span class="title-company">${esc(displayCompany(job.company))}</span>
        ${scorePill}
      </div>
    </td>
    <td><div class="company-cell">${companyLogoHtml(job.company, job.source, job.logo)}<span>${esc(displayCompany(job.company))}</span></div></td>
    <td>${esc(job.location)}</td>
    <td class="posted">${esc(displayPosted(job))}</td>
    <td class="score" title="Vector similarity score">${esc(score)}</td>
    ${seventhCellHtml}
    <td class="desc">${descCell}</td>
    <td class="source-cell">${sourceLogoHtml(job.source)}</td>
    <td class="apply-cell">
      ${job.link
        ? `<button class="apply-btn${applied ? " apply-btn--applied" : ""}" data-apply-link="${esc(job.link)}" title="${applied ? t("apply.titleUnmark") : t("apply.titleApply")}" onclick="event.stopPropagation()">${applied ? t("apply.applied") : t("apply.cta")}</button>`
        : `<span class="view-link" style="opacity:0.45;cursor:default">—</span>`}
    </td>
  `;

  // Wire up Apply button: open job externally, set pending-apply state
  if (job.link) {
    const applyBtn = tr.querySelector<HTMLButtonElement>(".apply-btn");
    applyBtn?.addEventListener("click", (e) => {
      e.stopPropagation();
      if (isApplied(job.link)) {
        // Toggle off
        unmarkApplied(job.link);
        _refreshApplyUI(job.link);
      } else {
        setPendingApply(job.link, job.title);
        window.open(job.link, "_blank", "noopener");
      }
    });
  }

  tr.addEventListener("click", () => openJobModal(job));
  return tr;
}

// (showRelated/hideRelated removed)

// ─── Intent box ───────────────────────────────────────────────────────────────

const _intentBox = document.getElementById("intentBox") as HTMLDivElement;

export function showIntentBox(keyword: string, inputType: "job_title" | "cv_or_skills" | "not_job", reasoning: string): void {
  if (inputType === "not_job") {
    _intentBox.innerHTML = `
      <div class="intent-box__header">
        <svg width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>
        </svg>
        ${t("intent.notJob")}
      </div>
    `;
  } else {
    const label = inputType === "cv_or_skills" ? t("intent.resumeSkills") : t("intent.jobTitle");
    _intentBox.innerHTML = `
      <div class="intent-box__header">
        <svg width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>
        </svg>
        ${t("intent.detectedPrefix")} <span style="font-weight:400;color:#374151;">${label}</span>
        ${t("intent.searchingWith")} <span class="intent-box__keyword">${esc(keyword)}</span>
      </div>
      ${reasoning ? `<div class="intent-box__reasoning">${esc(reasoning)}</div>` : ""}
    `;
  }
  _intentBox.classList.remove("hidden");
}

export function setIntentAlternatives(
  titles: string[],
  onPick: (title: string) => void,
): void {
  const uniq = [...new Set(titles.map(t => t.trim()).filter(Boolean))];
  // Avoid bloating the UI.
  const top = uniq.slice(0, 6);
  if (top.length === 0) return;

  const row = document.createElement("div");
  row.className = "intent-box__alts";
  row.innerHTML = `<span class="intent-box__alts-label">${t("intent.suggestions")}</span>`;

  for (const t of top) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "intent-box__alt";
    btn.textContent = t;
    btn.addEventListener("click", () => onPick(t));
    row.appendChild(btn);
  }

  _intentBox.appendChild(row);
}

export function hideIntentBox(): void {
  _intentBox.classList.add("hidden");
  _intentBox.innerHTML = "";
}

// ─── Query suggestion banner ──────────────────────────────────────────────────

let _suggestionBanner: HTMLDivElement | null = null;

function _getOrCreateSuggestionBanner(): HTMLDivElement {
  if (_suggestionBanner) return _suggestionBanner;
  const banner = document.createElement("div");
  banner.id = "suggestionBanner";
  banner.className = "suggestion-banner hidden";
  // Insert right after the status bar so it appears prominently
  statusEl.insertAdjacentElement("afterend", banner);
  _suggestionBanner = banner;
  return banner;
}

/**
 * Show a "Did you mean?" banner with optional accept/dismiss actions.
 * @param corrected  The AI-corrected query text.
 * @param onAccept   Called when the user clicks the corrected suggestion.
 * @param onDismiss  Called when the user dismisses the banner.
 */
export function showSuggestionBanner(
  corrected: string,
  onAccept: (corrected: string) => void,
  onDismiss: () => void,
  label = t("suggestion.didYouMean"),
): void {
  const banner = _getOrCreateSuggestionBanner();
  banner.innerHTML = `
    <span class="suggestion-banner__text">${label}</span>
    <button class="suggestion-banner__accept" type="button">${corrected}</button>
    <button class="suggestion-banner__dismiss" type="button" aria-label="Dismiss">✕</button>
  `;
  banner.classList.remove("hidden");

  banner.querySelector<HTMLButtonElement>(".suggestion-banner__accept")!
    .addEventListener("click", () => {
      hideSuggestionBanner();
      onAccept(corrected);
    });

  banner.querySelector<HTMLButtonElement>(".suggestion-banner__dismiss")!
    .addEventListener("click", () => {
      hideSuggestionBanner();
      onDismiss();
    });
}

/** Hide and clear the suggestion banner. */
export function hideSuggestionBanner(): void {
  _suggestionBanner?.classList.add("hidden");
  if (_suggestionBanner) _suggestionBanner.innerHTML = "";
}

resultsShareBtn?.addEventListener("click", () => {
  const url = new URL(window.location.origin + window.location.pathname);
  if (_searchKeyword) url.searchParams.set("kw", _searchKeyword);
  else url.searchParams.delete("kw");
  if (_searchLocation) url.searchParams.set("loc", _searchLocation);
  else url.searchParams.delete("loc");
  url.searchParams.delete("job");
  _copyWithFeedback(resultsShareBtn, url.toString());
});

jobModalShareBtn?.addEventListener("click", () => {
  if (!_openModalLink) return;
  const link = _openModalLink;
  const fallback = new URL(window.location.origin + window.location.pathname);
  if (_searchKeyword) fallback.searchParams.set("kw", _searchKeyword);
  else fallback.searchParams.delete("kw");
  if (_searchLocation) fallback.searchParams.set("loc", _searchLocation);
  else fallback.searchParams.delete("loc");
  fallback.searchParams.set("job", link);

  void ensureJobPage(link, _openModalCacheKey || undefined).then((pageUrl) => {
    _copyWithFeedback(jobModalShareBtn, pageUrl ?? fallback.toString());
  });
});

/** Escape a string for safe insertion into HTML attribute or text contexts. */
function esc(str: string): string {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

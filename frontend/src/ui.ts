import type { Job } from "./types";

type StatusType = "loading" | "success" | "error";

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

titleFilter.addEventListener("input", () => _applyFilter());

// Job detail modal elements
const jobModal         = document.getElementById("jobModal")         as HTMLDivElement;
const jobModalTitle    = document.getElementById("jobModalTitle")    as HTMLHeadingElement;
const jobModalCompany  = document.getElementById("jobModalCompany")  as HTMLSpanElement;
const jobModalLocation = document.getElementById("jobModalLocation") as HTMLSpanElement;
const jobModalPosted   = document.getElementById("jobModalPosted")   as HTMLSpanElement;
const jobModalSource   = document.getElementById("jobModalSource")   as HTMLSpanElement;
const jobModalDesc     = document.getElementById("jobModalDesc")     as HTMLDivElement;
const jobModalLink     = document.getElementById("jobModalLink")     as HTMLAnchorElement;
const jobModalShareBtn = document.getElementById("jobModalShareBtn") as HTMLButtonElement;
const jobModalClose    = document.getElementById("jobModalClose")    as HTMLButtonElement;
const jobModalSkills   = document.getElementById("jobModalSkills")   as HTMLDivElement;
const jobModalSummary  = document.getElementById("jobModalSummary")  as HTMLDivElement;
const jobModalBody     = document.querySelector(".job-modal-body")    as HTMLDivElement;

const resultsShareBtn = document.getElementById("resultsShareBtn") as HTMLButtonElement;

let _searchKeyword = "";
let _searchLocation: string | undefined;

export function setSearchContext(keyword: string, location?: string): void {
  _searchKeyword = keyword;
  _searchLocation = location;
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
    btn.textContent = ok ? "Đã sao chép" : "Không thể chia sẻ";
    btn.disabled = true;
    window.setTimeout(() => {
      btn.textContent = original;
      btn.disabled = false;
    }, 1000);
  });
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
};

/** Render a company logo img or letter-avatar span, with Clearbit and scraped-logo fallbacks. */
function companyLogoHtml(company: string, source: string, logo?: string): string {
  const initial = (company.trim()[0] || "?").toUpperCase();
  const [bg, fg] = _avatarStyle(company);
  const fallback = `Object.assign(document.createElement('span'),{className:'company-avatar',style:'background:${bg};color:${fg}',textContent:'${initial}'})`;

  if (logo) {
    return `<img class="company-logo" src="${esc(logo)}" alt="${esc(company)}" onerror="this.replaceWith(${fallback})">`;
  }

  const domain = _SOURCE_DOMAINS[source.toLowerCase()];
  if (domain) {
    return `<img class="company-logo" src="https://logo.clearbit.com/${domain}" alt="${esc(source)}" onerror="this.replaceWith(${fallback})">`;
  }

  return `<span class="company-avatar" style="background:${bg};color:${fg}">${initial}</span>`;
}

/** Open the job detail modal, populating all fields from the given job. */
function openJobModal(job: Job): void {
  _openModalLink               = job.link;
  jobModalTitle.textContent    = job.title;
  jobModalCompany.innerHTML    = `<div class="company-cell">${companyLogoHtml(job.company, job.source, job.logo)}<span>${esc(job.company)}</span></div>`;
  jobModalLocation.textContent = job.location;
  jobModalPosted.textContent   = job.posted || "N/A";
  jobModalSource.innerHTML     = `<span class="badge badge-${job.source.toLowerCase()}">${esc(job.source)}</span>`;
  const isEnriching =
    (_linkedinEnriching && job.source === "LinkedIn") ||
    (_topcvEnriching    && job.source === "TopCV");
  if (!job.skills?.length && isEnriching) {
    jobModalSkills.innerHTML = '<span class="desc-loading">Đang tìm kỹ năng…</span>';
  } else {
    jobModalSkills.innerHTML = renderSkillTags(job.skills);
  }
  const isMobile = window.innerWidth <= 640;
  const summaryText = job.summary_description || (isMobile ? truncate(job.description ?? "", 300) : "");
  if (summaryText) {
    jobModalSummary.textContent = summaryText;
    jobModalSummary.classList.remove("hidden");
  } else {
    jobModalSummary.textContent = "";
    jobModalSummary.classList.add("hidden");
  }
  jobModalLink.href            = job.link;
  jobModal.classList.remove("hidden");
  document.body.style.overflow = "hidden";
  const descHtml = (!job.description && isEnriching)
    ? '<span class="desc-loading">Đang tìm mô tả…</span>'
    : (job.description || "");
  requestAnimationFrame(() => {
    jobModalBody.scrollTop = 0;
    jobModalDesc.innerHTML = descHtml;
  });
}

/** Close the job detail modal and restore body scroll. */
function closeJobModal(): void {
  jobModal.classList.add("hidden");
  document.body.style.overflow = "";
  _openModalLink = "";
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
let _openModalLink = "";

const _PROGRESS_MESSAGES = [
  "Vibing…",
  "Scouting…",
  "Scoring…",
  "Hunting…",
  "Crawling…",
  "Fetching…",
  "Searching…",
  "Browsing…",
  "Scanning…",
  "Digging…",
  "Grinding…",
  "Hustling…",
  "Lurking…",
  "Sniffing…",
  "Stalking…",
  "Probing…",
  "Yapping…",
  "Cooking…",
  "Rizzling…",
  "Slaying…",
  "Leveling up…",
  "No-lifing…",
  "Sweating…",
  "Speedrunning…",
  "Farming…",
  "Touching grass…",
  "Manifesting…",
  "Crunching…",
  "Summoning…",
  "Plotting…",
];

/** Show the progress bar and start the rotating status message animation. */
export function showProgress(): void {
  progressPills.innerHTML = "";
  progressCount.textContent = "Searching…";
  progressBar.classList.remove("hidden");

  let msgIndex = Math.floor(Math.random() * _PROGRESS_MESSAGES.length);
  progressTimer.textContent = _PROGRESS_MESSAGES[msgIndex];

  if (_timerInterval) clearInterval(_timerInterval);
  _timerInterval = setInterval(() => {
    msgIndex = (msgIndex + 1) % _PROGRESS_MESSAGES.length;
    progressTimer.textContent = _PROGRESS_MESSAGES[msgIndex];
  }, 3000);
}

/** Update the progress bar job count with a per-source breakdown. */
export function updateProgressCount(jobs: Job[]): void {
  if (jobs.length === 0) {
    progressCount.textContent = "Searching…";
    return;
  }
  const bySource = new Map<string, number>();
  for (const j of jobs) bySource.set(j.source, (bySource.get(j.source) ?? 0) + 1);
  const breakdown = [...bySource.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([src, n]) => `${src} ${n}`)
    .join(" · ");
  progressCount.textContent = `Found ${jobs.length} — ${breakdown}`;
}

/** Set whether LinkedIn Phase 2 description enrichment is in progress. */
export function setLinkedInEnriching(value: boolean): void {
  _linkedinEnriching = value;
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
  pill.textContent = `${label} đang tải mô tả…`;
  pill.title = `Đang tải mô tả cho ${count} việc làm`;
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
  pill.title = `${count} việc làm`;
  progressPills.appendChild(pill);
}

/** Show the queue banner indicating the user's position in the scrape queue. */
export function showQueuedMessage(position: number): void {
  queueBanner.textContent = `⏳ Máy chủ đang bận — bạn đang ở vị trí #${position} trong hàng đợi. Vui lòng chờ…`;
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
  _hiddenSources.clear();
  _activeSkills.clear();
  titleFilter.value = "";
}

export function openJobByLink(link: string): boolean {
  const job = _allJobs.find((j) => j.link === link);
  if (!job) return false;
  openJobModal(job);
  return true;
}

let _allJobs: Job[] = [];
let _hiddenSources = new Set<string>();
let _activeSkills = new Set<string>();

/** Re-render the table rows based on the current _hiddenSources filter set, title words, and active skills. */
function _applyFilter(): void {
  const words = titleFilter.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const visible = _allJobs.filter((j) => {
    if (_hiddenSources.has(j.source)) return false;
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

  jobsBody.innerHTML = "";
  if (visible.length === 0) {
    jobsBody.innerHTML =
      '<tr><td colspan="9" class="empty">Không có việc làm nào phù hợp với bộ lọc đã chọn.</td></tr>';
  } else {
    visible.forEach((job, i) => jobsBody.appendChild(buildRow(job, i + 1)));
  }

  const total = _allJobs.length;
  const shown = visible.length;
  jobCountEl.textContent =
    shown === total
      ? `${total} kết quả`
      : `${shown} trong ${total} kết quả`;
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
    // Start active (visible); only faded if already in _hiddenSources
    const isHidden = _hiddenSources.has(source);
    btn.className = `badge badge-${source.toLowerCase()} badge-filter${isHidden ? "" : " active"}`;
    btn.textContent = `${source} ${count}`;
    btn.dataset.source = source;

    btn.addEventListener("click", () => {
      if (_hiddenSources.has(source)) {
        // Currently hidden → show it
        _hiddenSources.delete(source);
        btn.classList.add("active");
      } else {
        // Currently visible → hide it
        _hiddenSources.add(source);
        btn.classList.remove("active");
      }
      _applyFilter();
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
        _applyFilter();
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
  // Merge incoming into existing: update jobs with same link (e.g. LinkedIn
  // description enrichment pass), append truly new ones.
  const byLink = new Map(existing.map(j => [j.link, j]));
  for (const j of incoming) {
    byLink.set(j.link, j); // overwrite with enriched version if same link
  }
  const merged = [...byLink.values()].sort((a, b) => {
    const ta = a.posted_ts ?? 0;
    const tb = b.posted_ts ?? 0;
    return tb - ta; // larger timestamp = newer = first
  });

  _allJobs = merged;
  _rebuildFilterBar();
  _applyFilter();

  // If the modal is open for a job that just received enriched data, update it live
  if (_openModalLink) {
    const openJob = byLink.get(_openModalLink);
    if (openJob && incoming.some(j => j.link === _openModalLink)) {
      // This job was part of the incoming batch — refresh modal fields
      if (openJob.description) {
        jobModalDesc.innerHTML = openJob.description;
      }
      jobModalSkills.innerHTML = renderSkillTags(openJob.skills);
      const isMobile = window.innerWidth <= 640;
      const refreshSummary = openJob.summary_description || (isMobile ? truncate(openJob.description ?? "", 300) : "");
      if (refreshSummary) {
        jobModalSummary.textContent = refreshSummary;
        jobModalSummary.classList.remove("hidden");
      }
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

/** Build a table row element for a single job, wiring up the modal click handler. */
function buildRow(job: Job, num: number): HTMLTableRowElement {
  const tr = document.createElement("tr");
  const skillsHtml = renderSkillTags(job.skills);
  const descText = job.summary_description ? job.summary_description : truncate(job.description ?? "", 200);
  tr.innerHTML = `
    <td class="num">${num}</td>
    <td class="title" data-company="${esc(job.company)}">${esc(job.title)}</td>
    <td><div class="company-cell">${companyLogoHtml(job.company, job.source, job.logo)}<span>${esc(job.company)}</span></div></td>
    <td>${esc(job.location)}</td>
    <td class="posted">${esc(job.posted ?? "")}</td>
    <td class="skills-cell">${skillsHtml || '<span class="no-skills">—</span>'}</td>
    <td class="desc">${esc(descText)}</td>
    <td><span class="badge badge-${job.source.toLowerCase()}">${esc(job.source)}</span></td>
    <td><a href="${esc(job.link)}" target="_blank" rel="noopener" class="view-link" onclick="event.stopPropagation()">View ↗</a></td>
  `;
  tr.addEventListener("click", () => openJobModal(job));
  return tr;
}

// (showRelated/hideRelated removed)

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
): void {
  const banner = _getOrCreateSuggestionBanner();
  banner.innerHTML = `
    <span class="suggestion-banner__text">Ý bạn là: </span>
    <button class="suggestion-banner__accept" type="button">${corrected}</button>
    <button class="suggestion-banner__dismiss" type="button" aria-label="Bỏ qua">✕</button>
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
  const url = new URL(window.location.origin + window.location.pathname);
  if (_searchKeyword) url.searchParams.set("kw", _searchKeyword);
  else url.searchParams.delete("kw");
  if (_searchLocation) url.searchParams.set("loc", _searchLocation);
  else url.searchParams.delete("loc");
  url.searchParams.set("job", _openModalLink);
  _copyWithFeedback(jobModalShareBtn, url.toString());
});

/** Escape a string for safe insertion into HTML attribute or text contexts. */
function esc(str: string): string {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

import { scrapeJobsStream, scrapeLinkedInFallback, classifyInput, normalizeCity, streamRecentJobs } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching, setEnrichmentDone, setSearchContext, setScoreColumnVisible, setFromCache, openJobByLink, hideSuggestionBanner, showIntentBox, hideIntentBox, setIntentAlternatives, replaceJobs, initApplyToast, applyTrackerHandleReturn, buildRow, refreshUILanguage } from "./ui";
import { initLang, getLang, setLang, t, applyStaticTranslations } from "./i18n";
import { initChat, refreshChatLanguage } from "./chat";
import { CATEGORY_IDS, jobMatchesCategory, type CategoryFilter } from "./categories";
import type { Job } from "./types";

// Initialise apply tracker toast (injected into DOM once)
initApplyToast();

// Subtle scroll reveal for static sections (reference: template fade-up).
(() => {
  const elements = document.querySelectorAll<HTMLElement>("[data-reveal]");
  if (elements.length === 0) return;
  if (
    !("IntersectionObserver" in window) ||
    window.matchMedia("(prefers-reduced-motion: reduce)").matches
  ) {
    elements.forEach((element) => element.classList.add("is-revealed"));
    return;
  }
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        entry.target.classList.add("is-revealed");
        observer.unobserve(entry.target);
      }
    },
    { threshold: 0.08, rootMargin: "0px 0px -40px 0px" },
  );
  elements.forEach((element) => observer.observe(element));
})();

// EN/VI toggle — same single page, no separate /vi/ URL.
initLang();
applyStaticTranslations();
initChat();
const langToggleBtn = document.getElementById("langToggle") as HTMLButtonElement | null;
function syncLangToggleLabel(): void {
  if (langToggleBtn) langToggleBtn.textContent = getLang() === "en" ? "VI" : "EN";
}
syncLangToggleLabel();
langToggleBtn?.addEventListener("click", () => {
  setLang(getLang() === "en" ? "vi" : "en");
  applyStaticTranslations();
  syncLangToggleLabel();
  refreshUILanguage();
  refreshChatLanguage();
  syncSuggestionsToggle();
  if (homeJobs().length > 0 && recentJobsSection && !recentJobsSection.classList.contains("hidden")) {
    renderHomeJobs();
  }
});

const recentJobsSection  = document.getElementById("recentJobsSection")  as HTMLElement | null;
const recentJobsBody     = document.getElementById("recentJobsBody")     as HTMLTableSectionElement | null;
// Direct submissions (source "Direct") get their own featured table on the
// default homepage feed too, shown above the scraped "Recent Jobs" table.
const directRecentSection = document.getElementById("directRecentSection") as HTMLElement | null;
const directRecentBody    = document.getElementById("directRecentBody")    as HTMLTableSectionElement | null;

const homeCategoryBar   = document.getElementById("homeCategoryBar") as HTMLElement | null;
const homeCategorySelect = document.getElementById("homeCategorySelect") as HTMLSelectElement | null;
const homeLocationSelect = document.getElementById("homeLocationSelect") as HTMLSelectElement | null;
const recentPager        = document.getElementById("recentPager") as HTMLElement | null;
const recentPageSizeEl   = document.getElementById("recentPageSize") as HTMLSelectElement | null;
const recentPrevBtn      = document.getElementById("recentPrev") as HTMLButtonElement | null;
const recentNextBtn      = document.getElementById("recentNext") as HTMLButtonElement | null;
const recentPageInfoEl   = document.getElementById("recentPageInfo") as HTMLElement | null;
const recentPagerShowing = document.getElementById("recentPagerShowing") as HTMLElement | null;

type LocationFilter = "all" | "hcm" | "hanoi" | "danang" | "remote" | "other";
const LOCATION_FILTERS: Exclude<LocationFilter, "all">[] = [
  "hcm", "hanoi", "danang", "remote", "other",
];
const LOCATION_KEYS: Record<Exclude<LocationFilter, "all">, string> = {
  hcm: "location.hcm",
  hanoi: "location.hanoi",
  danang: "location.danang",
  remote: "location.remote",
  other: "location.other",
};

function normalizeLocation(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase();
}

function jobLocationBucket(job: Job): Exclude<LocationFilter, "all"> {
  const location = normalizeLocation(job.location ?? "");
  if (/(ho chi minh|hcm|sai gon|saigon|thu duc|binh thanh|phu nhuan|quan 1|quan 3|quan 7|tan binh|go vap|binh tan|nha be|can gio|cu chi|hoc mon)/.test(location)) {
    return "hcm";
  }
  if (/(ha noi|hanoi|hoan kiem|ba dinh|dong da|cau giay|thanh xuan|ha dong|nam tu liem|bac tu liem|tay ho|long bien|hoang mai)/.test(location)) {
    return "hanoi";
  }
  if (/(da nang|danang|hoi an)/.test(location)) {
    return "danang";
  }
  if (/(remote|work from home|wfh|tu xa)/.test(location)) {
    return "remote";
  }
  return "other";
}

function jobMatchesLocation(job: Job, filter: LocationFilter): boolean {
  return filter === "all" || jobLocationBucket(job) === filter;
}

let homeDirectJobs: Job[] = [];
let homeScrapedJobs: Job[] = [];
let homeCategory: CategoryFilter = "all";
let homeLocation: LocationFilter = "all";
let homeVisible = true;
let homePage = 1;
let homePageSize = Number(recentPageSizeEl?.value) || 10;

function homeJobs(): Job[] {
  return [...homeDirectJobs, ...homeScrapedJobs];
}

const byDate = (a: Job, b: Job): number => (b.posted_ts ?? 0) - (a.posted_ts ?? 0);

/**
 * One job per source in rotation, newest-first within each source.
 * Prevents a single board (e.g. Glints) from filling the first page.
 */
function interleaveBySource(jobs: Job[]): Job[] {
  const bySource = new Map<string, Job[]>();
  for (const job of jobs) {
    const source = job.source || "Other";
    const bucket = bySource.get(source);
    if (bucket) bucket.push(job);
    else bySource.set(source, [job]);
  }

  const buckets = [...bySource.values()]
    .map((bucket) => bucket.sort(byDate))
    .sort((a, b) => (b[0]?.posted_ts ?? 0) - (a[0]?.posted_ts ?? 0));

  const indices = new Array(buckets.length).fill(0);
  const result: Job[] = [];
  let added = true;
  while (added) {
    added = false;
    for (let i = 0; i < buckets.length; i++) {
      const job = buckets[i][indices[i]++];
      if (!job) continue;
      result.push(job);
      added = true;
    }
  }
  return result;
}

/** Refresh the category/location dropdown labels and keep the active values. */
function updateHomeFilters(): void {
  if (!homeCategoryBar || !homeCategorySelect || !homeLocationSelect) return;
  const jobs = homeJobs();
  if (jobs.length === 0) {
    homeCategoryBar.classList.add("hidden");
    return;
  }
  homeCategoryBar.classList.remove("hidden");

  const selectedCategory = homeCategory;
  homeCategorySelect.innerHTML = "";
  const addCategory = (value: CategoryFilter, label: string) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    homeCategorySelect.appendChild(option);
  };
  addCategory("all", t("category.all"));
  for (const id of CATEGORY_IDS) {
    addCategory(id, t(`category.${id}`));
  }
  homeCategorySelect.value = selectedCategory;

  const selectedLocation = homeLocation;
  homeLocationSelect.innerHTML = "";
  const addLocation = (value: LocationFilter, label: string) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    homeLocationSelect.appendChild(option);
  };
  addLocation("all", t("location.all"));
  for (const id of LOCATION_FILTERS) {
    addLocation(id, t(LOCATION_KEYS[id]));
  }
  homeLocationSelect.value = selectedLocation;
}

/** Render the homepage "Posted Directly" and "Recent Jobs" tables for the active filters. */
function renderHomeJobs(): void {
  if (!homeVisible) return;
  if (!recentJobsBody || !directRecentBody) return;

  const directJobs = homeDirectJobs.filter(
    (job) => jobMatchesCategory(job, homeCategory) && jobMatchesLocation(job, homeLocation),
  ).sort(byDate);
  const scrapedJobs = interleaveBySource(
    homeScrapedJobs.filter(
      (job) => jobMatchesCategory(job, homeCategory) && jobMatchesLocation(job, homeLocation),
    ),
  );

  directRecentBody.innerHTML = "";
  if (directJobs.length > 0) {
    const frag = document.createDocumentFragment();
    directJobs.forEach((job, i) => frag.appendChild(buildRow(job, i + 1, "date")));
    directRecentBody.appendChild(frag);
    directRecentSection?.classList.remove("hidden");
  } else {
    directRecentSection?.classList.add("hidden");
  }

  recentJobsBody.innerHTML = "";
  const totalPages = Math.max(1, Math.ceil(scrapedJobs.length / homePageSize));
  homePage = Math.min(Math.max(1, homePage), totalPages);
  const start = (homePage - 1) * homePageSize;
  const pageJobs = scrapedJobs.slice(start, start + homePageSize);

  if (pageJobs.length > 0) {
    const frag = document.createDocumentFragment();
    pageJobs.forEach((job, i) => frag.appendChild(buildRow(job, start + i + 1, "date")));
    recentJobsBody.appendChild(frag);
    recentJobsSection?.classList.remove("hidden");
  } else if ((homeCategory !== "all" || homeLocation !== "all") && homeJobs().length > 0) {
    const row = document.createElement("tr");
    row.innerHTML = `<td colspan="10" class="empty">${t("category.empty")}</td>`;
    recentJobsBody.appendChild(row);
    recentJobsSection?.classList.remove("hidden");
  } else {
    recentJobsSection?.classList.add("hidden");
  }

  if (recentPager) {
    updateHomePager(scrapedJobs, totalPages, start);
  }

  updateHomeFilters();
}

/** Update only the pager text/state (used while the stream appends jobs). */
function updateHomePager(scrapedJobs: Job[], totalPages: number, start: number): void {
  if (!recentPager) return;
  const showPager = scrapedJobs.length > homePageSize;
  recentPager.classList.toggle("hidden", !showPager);
  if (!showPager) return;
  if (recentPageInfoEl) {
    recentPageInfoEl.textContent = t("pager.pageInfo", { page: homePage, pages: totalPages });
  }
  if (recentPagerShowing) {
    recentPagerShowing.textContent = t("pager.showing", {
      from: start + 1,
      to: Math.min(start + homePageSize, scrapedJobs.length),
      total: scrapedJobs.length,
    });
  }
  if (recentPrevBtn) recentPrevBtn.disabled = homePage <= 1;
  if (recentNextBtn) recentNextBtn.disabled = homePage >= totalPages;
}

/** While streaming, only refresh the pager — the visible rows stay stable. */
function refreshHomePager(): void {
  if (!homeVisible) return;
  const scrapedJobs = homeScrapedJobs.filter(
    (job) => jobMatchesCategory(job, homeCategory) && jobMatchesLocation(job, homeLocation),
  );
  const totalPages = Math.max(1, Math.ceil(scrapedJobs.length / homePageSize));
  const start = (homePage - 1) * homePageSize;
  updateHomePager(scrapedJobs, totalPages, start);
}

recentPageSizeEl?.addEventListener("change", () => {
  homePageSize = Number(recentPageSizeEl.value) || 10;
  homePage = 1;
  renderHomeJobs();
});

recentPrevBtn?.addEventListener("click", () => {
  if (homePage <= 1) return;
  homePage -= 1;
  renderHomeJobs();
  recentJobsSection?.scrollIntoView({ behavior: "smooth", block: "start" });
});

recentNextBtn?.addEventListener("click", () => {
  homePage += 1;
  renderHomeJobs();
  recentJobsSection?.scrollIntoView({ behavior: "smooth", block: "start" });
});

homeCategorySelect?.addEventListener("change", () => {
  homeCategory = homeCategorySelect.value as CategoryFilter;
  homePage = 1;
  renderHomeJobs();
});

homeLocationSelect?.addEventListener("change", () => {
  homeLocation = homeLocationSelect.value as LocationFilter;
  homePage = 1;
  renderHomeJobs();
});

function hideRecentJobs(): void {
  homeVisible = false;
  recentJobsSection?.classList.add("hidden");
  directRecentSection?.classList.add("hidden");
  homeCategoryBar?.classList.add("hidden");
}
function showRecentJobs(): void {
  homeVisible = true;
  homePage = 1;
  // Re-render so the category filter and row numbering are applied again.
  if (homeJobs().length > 0) renderHomeJobs();
}

// Stream all cached jobs onto the homepage. Jobs are appended in the order
// the backend sends them (most recently fetched cache first), so the visible
// rows stay stable while the rest of the feed loads in the background.
(async () => {
  if (!recentJobsBody || !recentJobsSection) return;

  let firstRender = false;
  let knownSourceCount = 0;
  const sourceCount = () => new Set(homeScrapedJobs.map((job) => job.source)).size;
  try {
    await streamRecentJobs(
      (batch) => {
        for (const job of batch) {
          if (job.source === "Direct") homeDirectJobs.push(job);
          else homeScrapedJobs.push(job);
        }
        if (!firstRender && homeScrapedJobs.length >= homePageSize) {
          firstRender = true;
          knownSourceCount = sourceCount();
          renderHomeJobs();
        } else if (firstRender && sourceCount() > knownSourceCount) {
          // Re-interleave when a new source arrives so one board can't
          // dominate the first page. This happens only a handful of times.
          knownSourceCount = sourceCount();
          renderHomeJobs();
        } else if (firstRender) {
          refreshHomePager();
        }
      },
      () => {
        firstRender = true;
        renderHomeJobs();
      },
    );
  } catch {
    // silently ignore — recent jobs is non-critical
    if (homeJobs().length === 0) {
      recentJobsBody.innerHTML = "";
      recentJobsSection.classList.add("hidden");
    }
  }
})();

let currentJobs: Job[] = [];

const fetchBtn        = document.getElementById("fetchBtn")        as HTMLButtonElement;
const keywordEl       = document.getElementById("keyword")         as HTMLTextAreaElement;
const locationInput   = document.getElementById("locationInput")   as HTMLInputElement;

/** Return the resolved location string from the free-text input. */
function getLocation(): string {
  return locationInput.value.trim() || "Ho Chi Minh City";
}

const homeLink = document.getElementById("homeLink") as HTMLAnchorElement;
homeLink.addEventListener("click", (e) => {
  e.preventDefault();
  abortController?.abort();
  abortController = null;
  // Reset deep-link URL (/ ?kw=&loc=&job=...) back to the homepage.
  history.replaceState({}, "", window.location.pathname);
  hideResults();
  hideProgress();
  clearStatus();
  setLinkedInEnriching(false);
  setTopCVEnriching(false);
  setEnrichmentDone(true);
  hideIntentBox();
  showRecentJobs();
  (window as any)._slideshowShow?.();
  currentJobs = [];
  fetchBtn.disabled = false;
  keywordEl.value = "";
  keywordEl.style.height = "auto";
  suggestionChips.forEach(c => c.classList.remove("active"));
  window.scrollTo({ top: 0, behavior: "smooth" });
});

const suggestionChips = document.querySelectorAll<HTMLElement>(".suggestion-chip");
suggestionChips.forEach((chip) => {
  chip.addEventListener("click", () => {
    keywordEl.value = chip.dataset.kw ?? chip.textContent ?? "";
    suggestionChips.forEach(c => c.classList.remove("active"));
    chip.classList.add("active");
    keywordEl.focus();
  });
});

const suggestionList = document.getElementById("suggestionList") as HTMLElement | null;
const suggestionsToggle = document.getElementById("suggestionsToggle") as HTMLButtonElement | null;

function syncSuggestionsToggle(): void {
  const expanded = suggestionList?.classList.contains("suggestions--expanded") ?? false;
  suggestionsToggle?.classList.toggle("is-expanded", expanded);
  suggestionsToggle?.setAttribute("aria-expanded", String(expanded));
  suggestionsToggle?.setAttribute("aria-label", t(expanded ? "search.showFewer" : "search.showMore"));
}

suggestionsToggle?.addEventListener("click", () => {
  suggestionList?.classList.toggle("suggestions--expanded");
  syncSuggestionsToggle();
});
syncSuggestionsToggle();

keywordEl.addEventListener("input", () => {
  // Highlight matching suggestion chip.
  const val = keywordEl.value.trim().toLowerCase();
  suggestionChips.forEach(c => {
    c.classList.toggle("active", (c.dataset.kw ?? "").toLowerCase() === val);
  });
  // Auto-expand height.
  keywordEl.style.height = "auto";
  keywordEl.style.height = `${keywordEl.scrollHeight}px`;
});

keywordEl.addEventListener("keydown", (e) => {
  // Enter without Shift submits; Shift+Enter inserts a newline.
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    fetchBtn.click();
  }
});

const weeklyStatsEl = document.getElementById("weeklyStats") as HTMLElement;

(async () => {
  try {
    // Prefer remote API base when configured; fall back to Vite proxy in local dev.
    const apiBase = import.meta.env.VITE_API_URL ?? "";
    const res = await fetch(`${apiBase}/stats`);
    if (res.ok) {
      const data = await res.json() as { jobs_this_week: number };
      if (data.jobs_this_week > 0) {
        const display = data.jobs_this_week.toLocaleString("en-US");
        weeklyStatsEl.innerHTML = `<strong>${display}</strong><span>${t("stats.jobsSuffix")}</span>`;
        weeklyStatsEl.classList.remove("hidden");
      }
    }
  } catch {
    // silently ignore — stats are non-critical
  }
})();

let abortController: AbortController | null = null;

let _pendingSharedJobLink: string | null = null;

/** Run a search with the given keyword and location. Extracted so it can be
 *  triggered both from the button click and from accepting an AI suggestion.
 *  `rawInput` — original free-form CV/skills text; used for vector search when set.
 *  `estimatedLevel` — AI-inferred experience level from CV ("junior"|"middle"|"senior"). */
async function runSearch(keyword: string, location: string | undefined, sharedJobLink: string | null, rawInput = "", replaceInput = false, estimatedLevel = "", intent = ""): Promise<void> {
  const fromCvOrSkills = rawInput.length > 0;
  abortController?.abort();
  abortController = new AbortController();

  hideSuggestionBanner();
  // Intent box is shown for all search types; only hide on explicit reset.
  setSearchContext(keyword, location);
  setScoreColumnVisible(fromCvOrSkills);
  // Only replace input when explicitly requested (e.g. clicking an alternative keyword).
  if (replaceInput) {
    keywordEl.value = keyword;
    keywordEl.style.height = "auto";
    keywordEl.style.height = `${keywordEl.scrollHeight}px`;
  }

  // Keep the URL clean while browsing — params are only used for sharing via the share button.
  // Store kw/loc in history state so the back button still restores the previous search.
  history.pushState({ kw: keyword, loc: location ?? "" }, "", window.location.pathname);

  fetchBtn.disabled = true;
  currentJobs = [];
  hideResults();
  hideRecentJobs();
  (window as any)._slideshowHide?.();
  // related jobs feature is disabled for now
  hideProgress();
  clearStatus();
  showProgress();

  let _isCacheHit = false;
  let _isFuzzyCache = false;
  let _descStatusPending = false;
  setEnrichmentDone(false);

  try {
    await scrapeJobsStream(
      { keyword, location, ...(rawInput ? { raw_input: rawInput.slice(0, 2000) } : {}), ...(estimatedLevel ? { estimated_level: estimatedLevel } : {}), ...(intent ? { intent } : {}) },
      (batch) => {
        if (location) {
          for (const j of batch) {
            if (!j.location) j.location = location;
          }
        }
        // Hide vector-matched jobs that have no description — they add no value to the user.
        batch = batch.filter(j => typeof j._vector_score !== "number" || !!j.description?.trim());
        if (batch.some(j => j.source === "LinkedIn" && !j.description)) {
          setLinkedInEnriching(true);
        }
        if (batch.some(j => j.source === "TopCV" && !j.description)) {
          setTopCVEnriching(true);
        }
        currentJobs = appendJobs(currentJobs, batch);
        updateProgressCount(currentJobs);

        if (sharedJobLink) {
          openJobByLink(sharedJobLink);
        }
      },
      () => {
        hideProgress();
        const count = currentJobs.length;
        if (count === 0) {
          setStatus(t("status.noMatch"), "error");
          return;
        }

        if (sharedJobLink && !openJobByLink(sharedJobLink)) {
          setStatus(t("status.sharedNotFound"), "error");
          return;
        }

        _descStatusPending = !(_isCacheHit || _isFuzzyCache);
        if (_isCacheHit || _isFuzzyCache) {
          setStatus(t("status.foundWeek", { count }), "success");
        } else if (fromCvOrSkills) {
          setStatus(t("status.foundProfile", { count }), "success");
        } else {
          setStatus(t("status.foundLoading", { count }), "success");
        }
      },
      abortController.signal,
      (site, count) => { markSiteDone(site, count); },
      (site, count) => {
        if (site === "LinkedIn") setLinkedInEnriching(true);
        if (site === "TopCV")    setTopCVEnriching(true);
        void count;
      },
      (position) => showQueuedMessage(position),
      () => { clearQueuedMessage(); setFromCache(false); },
      () => {
        setLinkedInEnriching(false);
        const count = currentJobs.length;
        setStatus(t("status.foundWeek", { count }), "success");
      },
      () => {
        setTopCVEnriching(false);
        const count = currentJobs.length;
        setStatus(t("status.foundWeek", { count }), "success");
      },
      (_fetchedTs, fuzzy) => {
        if (!fuzzy) _isCacheHit = true;
        // Only skip highlighting for warmup keyword cache (fuzzy hits where all jobs already match).
        // For user-specific searches, keep highlighting even when served from cache.
        if (fuzzy) { _isFuzzyCache = true; setFromCache(true); }
      },
      // onVectorResults (related jobs) is disabled for now
      () => {},
      // onRescore: silently re-sort the table after local embedding scores arrive
      (rescored) => {
        currentJobs = replaceJobs(rescored);
      },
    );
    setEnrichmentDone(true);
    if (_descStatusPending && currentJobs.length > 0) {
      setStatus(t("status.foundWeek", { count: currentJobs.length }), "success");
    }
  } catch (err) {
    hideProgress();
    if ((err as Error).name === "AbortError") return;
    setEnrichmentDone(true);
    currentJobs = [];
    const isNetworkDown = err instanceof TypeError && err.message.toLowerCase().includes("fetch");
    if (isNetworkDown) {
      setStatus(t("status.serverBusyFallback"), "error");
      try {
        const fallbackJobs = await scrapeLinkedInFallback(
          keyword,
          location ?? "Vietnam",
          abortController?.signal,
        );
        if (fallbackJobs.length > 0) {
          currentJobs = appendJobs([], fallbackJobs);
          const count = fallbackJobs.length;
          setStatus(t("status.linkedinOnly", { count }), "error");
        } else {
          setStatus(t("status.serverBusy"), "error");
        }
      } catch {
        setStatus(t("status.serverBusy"), "error");
      }
    } else {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("searched too many times") || msg.includes("Too Many") || msg.includes("429")) {
        setStatus(t("status.rateLimited"), "error");
      } else {
        setStatus(t("status.errorPrefix", { msg }), "error");
      }
    }
  } finally {
    fetchBtn.disabled = false;
  }
}

fetchBtn.addEventListener("click", async () => {
  const sharedJobLink = _pendingSharedJobLink;
  _pendingSharedJobLink = null;

  const rawInput = keywordEl.value.trim();
  if (!rawInput) {
    setStatus(t("status.enterKeyword"), "error");
    return;
  }

  const rawLocation = getLocation();
  fetchBtn.disabled = true;
  const cityTimeout = new Promise<null>(resolve => setTimeout(() => resolve(null), 5000));
  const normalized = await Promise.race([
    normalizeCity(rawLocation, abortController?.signal ?? undefined),
    cityTimeout,
  ]);
  fetchBtn.disabled = false;
  const location = normalized?.city?.trim() || rawLocation || undefined;

  // Warmup chips are curated — skip AI classification entirely.
  const isWarmupKeyword = [...suggestionChips].some(
    c => (c.dataset.kw ?? "").toLowerCase() === rawInput.toLowerCase(),
  );
  if (isWarmupKeyword) {
    hideIntentBox();
    void runSearch(rawInput, location, sharedJobLink, "", false, "", "warmup_job");
    return;
  }

  // Classify all non-warmup input via AI (job title vs CV/skills).
  setStatus(t("status.analyzing"), "info");
  fetchBtn.disabled = true;
  const classifyTimeout = new Promise<null>(resolve => setTimeout(() => resolve(null), 5000));
  const classified = await Promise.race([
    classifyInput(rawInput, abortController?.signal ?? undefined),
    classifyTimeout,
  ]);
  fetchBtn.disabled = false;
  clearStatus();

  // If classify failed (null = timeout or network error), skip AI checks and search directly.
  if (classified === null) {
    hideIntentBox();
    void runSearch(rawInput.trim().slice(0, 60), location, sharedJobLink);
    return;
  }

  if (classified.input_type === "not_job") {
    showIntentBox("", "not_job", classified.reasoning ?? "");
    const suggestions = classified.alternatives?.length
      ? classified.alternatives
      : ["AI Engineer", "Business Analyst", "Marketing Executive", "Data Analyst", "Software Engineer"];
    setIntentAlternatives(suggestions, (picked) => {
      fetchBtn.disabled = true;
      void runSearch(picked, location, sharedJobLink, "", true);
    });
    return;
  }

  const isJobTitle = classified.is_job_title;
  const extractedKeyword = classified.keyword || rawInput.trim().slice(0, 60);
  const inputType = classified.input_type;
  const reasoning = classified.reasoning ?? "";

showIntentBox(extractedKeyword, inputType, reasoning);

  if (inputType === "cv_or_skills" && (classified?.alternatives?.length ?? 0) > 0) {
    setIntentAlternatives(classified!.alternatives!, (picked) => {
      // Clicking an alternative switches to a job-title search.
      // Disable immediately to prevent double-submits before runSearch starts.
      fetchBtn.disabled = true;
      void runSearch(picked, location, sharedJobLink, "", true);
    });
  }

  // For CV/skills: pass raw input so vector search uses the full text.
  const rawForVector = isJobTitle ? "" : rawInput;
  const levelHint = classified.estimated_level ?? "";
  void runSearch(extractedKeyword, location, sharedJobLink, rawForVector, false, levelHint, inputType);

});

// Deep-link support: /?kw=...&loc=... and optional &job=... to auto-open modal.
// The URL params are read once to seed the search, then cleared from the address bar
// by runSearch(). Use the "Share results" button to get a shareable link.
(() => {
  const url = new URL(window.location.href);
  const kw = (url.searchParams.get("kw") ?? "").trim();
  const loc = (url.searchParams.get("loc") ?? "").trim();
  _pendingSharedJobLink = (url.searchParams.get("job") ?? "").trim() || null;

  if (!kw && !_pendingSharedJobLink) return;

  if (kw) {
    keywordEl.value = kw;
  }
  if (loc) {
    locationInput.value = loc;
  }

  // Kick off the search after the initial DOM is ready.
  queueMicrotask(() => fetchBtn.click());
})();

document.addEventListener("visibilitychange", () => {
  // Apply tracker: prompt the user when they return after clicking Apply
  if (document.visibilityState === "visible") {
    applyTrackerHandleReturn();
  }
  // Unblock the search button if a scrape was in progress when the tab was hidden
  if (document.visibilityState === "visible" && fetchBtn.disabled) {
    abortController?.abort();
    abortController = null;
    fetchBtn.disabled = false;
    hideProgress();
    setEnrichmentDone(true);
  }
});

// Browser back/forward navigation: restore the search from history state.
window.addEventListener("popstate", (event) => {
  const state = event.state as { kw?: string; loc?: string } | null;
  const kw = (state?.kw ?? "").trim();
  const loc = (state?.loc ?? "").trim();

  if (!kw) {
    // Navigated back to the homepage — reset the UI.
    abortController?.abort();
    abortController = null;
    hideResults();
    hideProgress();
    clearStatus();
    setLinkedInEnriching(false);
    setTopCVEnriching(false);
    setEnrichmentDone(true);
    hideIntentBox();
    (window as any)._slideshowShow?.();
    showRecentJobs();
    currentJobs = [];
    fetchBtn.disabled = false;
    keywordEl.value = "";
    keywordEl.style.height = "auto";
    return;
  }

  // Restore keyword and location into the inputs, then re-run the search.
  keywordEl.value = kw;
  if (loc) {
    locationInput.value = loc;
  }
  void runSearch(kw, loc || getLocation(), null);
});

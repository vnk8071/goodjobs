import { scrapeJobsStream, scrapeLinkedInFallback, classifyInput, normalizeCity, API_BASE } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching, setEnrichmentDone, setSearchContext, setScoreColumnVisible, setFromCache, openJobByLink, hideSuggestionBanner, showIntentBox, hideIntentBox, setIntentAlternatives, replaceJobs, initApplyToast, applyTrackerHandleReturn, buildRow, refreshUILanguage } from "./ui";
import { initLang, getLang, setLang, t, applyStaticTranslations } from "./i18n";
import { CATEGORY_LABELS, getJobCategories, jobMatchesCategory, type CategoryFilter } from "./categories";
import type { Job } from "./types";

// Initialise apply tracker toast (injected into DOM once)
initApplyToast();

// EN/VI toggle — same single page, no separate /vi/ URL.
initLang();
applyStaticTranslations();
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
const homeCategoryChips = document.querySelectorAll<HTMLButtonElement>("#homeCategoryBar .category-chip");

let homeDirectJobs: Job[] = [];
let homeScrapedJobs: Job[] = [];
let homeCategory: CategoryFilter = "all";
let homeVisible = true;

function homeJobs(): Job[] {
  return [...homeDirectJobs, ...homeScrapedJobs];
}

/** Refresh chip labels, counts, and active state from the loaded homepage jobs. */
function updateHomeCategoryBar(): void {
  if (!homeCategoryBar) return;
  const jobs = homeJobs();
  if (jobs.length === 0) {
    homeCategoryBar.classList.add("hidden");
    return;
  }
  homeCategoryBar.classList.remove("hidden");

  const counts: Record<CategoryFilter, number> = {
    all: jobs.length, ai: 0, it: 0, data: 0, product: 0,
    business: 0, design: 0, marketing: 0, hr: 0,
  };
  for (const job of jobs) {
    for (const id of getJobCategories(job)) counts[id]++;
  }

  homeCategoryChips.forEach((chip) => {
    const category = (chip.dataset.category ?? "all") as CategoryFilter;
    const count = counts[category] ?? 0;
    const label = category === "all" ? t("category.all") : CATEGORY_LABELS[category];

    chip.replaceChildren(document.createTextNode(`${label} `));
    const countEl = document.createElement("span");
    countEl.className = "category-count";
    countEl.textContent = String(count);
    chip.appendChild(countEl);

    const active = homeCategory === category;
    chip.classList.toggle("active", active);
    chip.setAttribute("aria-pressed", String(active));
    chip.disabled = category !== "all" && count === 0;
  });
}

/** Render the homepage "Posted Directly" and "Recent Jobs" tables for the active category. */
function renderHomeJobs(): void {
  if (!homeVisible) return;
  if (!recentJobsBody || !directRecentBody) return;

  const directJobs = homeDirectJobs.filter((job) => jobMatchesCategory(job, homeCategory));
  const scrapedJobs = homeScrapedJobs.filter((job) => jobMatchesCategory(job, homeCategory));

  directRecentBody.innerHTML = "";
  if (directJobs.length > 0) {
    directJobs.forEach((job, i) => directRecentBody.appendChild(buildRow(job, i + 1, "date")));
    directRecentSection?.classList.remove("hidden");
  } else {
    directRecentSection?.classList.add("hidden");
  }

  recentJobsBody.innerHTML = "";
  if (scrapedJobs.length > 0) {
    scrapedJobs.forEach((job, i) => recentJobsBody.appendChild(buildRow(job, i + 1, "date")));
    recentJobsSection?.classList.remove("hidden");
  } else if (homeCategory !== "all" && homeJobs().length > 0) {
    const row = document.createElement("tr");
    row.innerHTML = `<td colspan="10" class="empty">${t("category.empty")}</td>`;
    recentJobsBody.appendChild(row);
    recentJobsSection?.classList.remove("hidden");
  } else {
    recentJobsSection?.classList.add("hidden");
  }

  updateHomeCategoryBar();
}

homeCategoryChips.forEach((chip) => {
  chip.addEventListener("click", () => {
    const category = (chip.dataset.category ?? "all") as CategoryFilter;
    // Clicking the active category toggles back to "All".
    homeCategory = homeCategory === category ? "all" : category;
    renderHomeJobs();
  });
});

function hideRecentJobs(): void {
  homeVisible = false;
  recentJobsSection?.classList.add("hidden");
  directRecentSection?.classList.add("hidden");
  homeCategoryBar?.classList.add("hidden");
}
function showRecentJobs(): void {
  homeVisible = true;
  // Re-render so the category filter and row numbering are applied again.
  if (homeJobs().length > 0) renderHomeJobs();
}

// Load and render recent jobs on the homepage: up to 20 direct jobs + recent scraped jobs
(async () => {
  if (!recentJobsBody || !recentJobsSection) return;
  try {
    const [directRes, mixedRes] = await Promise.all([
      fetch(`${API_BASE}/recent-jobs?n=20&source=Direct`),
      fetch(`${API_BASE}/recent-jobs?n=20`),
    ]);

    if (directRes.ok) {
      homeDirectJobs = await directRes.json() as Job[];
    }

    if (mixedRes.ok) {
      const jobs = await mixedRes.json() as Job[];
      homeScrapedJobs = jobs.filter((j) => j.source !== "Direct");
    }

    renderHomeJobs();
  } catch {
    // silently ignore — recent jobs is non-critical
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
        const rounded = Math.floor(data.jobs_this_week / 100) * 100;
        const display = rounded > 0 ? rounded.toLocaleString("en-US") : data.jobs_this_week;
        weeklyStatsEl.innerHTML = `<strong>${display}+</strong> ${t("stats.jobsSuffix")}`;
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

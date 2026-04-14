import { scrapeJobsStream, scrapeLinkedInFallback, classifyInput } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching, setSearchContext, setFromCache, openJobByLink, hideSuggestionBanner, showIntentBox, hideIntentBox, setIntentAlternatives, replaceJobs, initApplyToast, applyTrackerHandleReturn } from "./ui";
import type { Job } from "./types";

// Initialise apply tracker toast (injected into DOM once)
initApplyToast();

let currentJobs: Job[] = [];

const fetchBtn        = document.getElementById("fetchBtn")        as HTMLButtonElement;
const keywordEl       = document.getElementById("keyword")         as HTMLTextAreaElement;
const locationSelect  = document.getElementById("locationSelect")  as HTMLSelectElement;
const locationCustom  = document.getElementById("locationCustom")  as HTMLInputElement;

locationSelect.addEventListener("change", () => {
  if (locationSelect.value === "_custom") {
    locationCustom.classList.remove("hidden");
    locationCustom.focus();
  } else {
    locationCustom.classList.add("hidden");
  }
});

/** Return the resolved location string from the dropdown or custom text input. */
function getLocation(): string {
  if (locationSelect.value === "_custom") {
    return locationCustom.value.trim() || "Ho Chi Minh City";
  }
  return locationSelect.value;
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
  hideIntentBox();
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
        const display = rounded > 0 ? rounded.toLocaleString("vi-VN") : data.jobs_this_week;
        weeklyStatsEl.innerHTML = `<strong>${display}+</strong> việc làm mới tuần này`;
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
  (window as any)._slideshowHide?.();
  // related jobs feature is disabled for now
  hideProgress();
  clearStatus();
  showProgress();

  let _isCacheHit = false;
  let _isFuzzyCache = false;

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
          setStatus("Không tìm thấy việc làm phù hợp trong tuần qua. Thử từ khóa khác.", "error");
          return;
        }

        if (sharedJobLink && !openJobByLink(sharedJobLink)) {
          setStatus("Không tìm thấy job từ link chia sẻ (có thể đã hết hạn).", "error");
          return;
        }

        if (_isCacheHit || _isFuzzyCache) {
          setStatus(`Tìm thấy ${count} việc làm trong tuần qua.`, "success");
        } else if (fromCvOrSkills) {
          setStatus(`Tìm thấy ${count} việc làm phù hợp với hồ sơ — đang tải mô tả…`, "success");
        } else {
          setStatus(`Tìm thấy ${count} việc làm — đang tải mô tả…`, "success");
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
        setStatus(`Tìm thấy ${count} việc làm trong tuần qua.`, "success");
      },
      () => {
        setTopCVEnriching(false);
        const count = currentJobs.length;
        setStatus(`Tìm thấy ${count} việc làm trong tuần qua.`, "success");
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
  } catch (err) {
    hideProgress();
    if ((err as Error).name === "AbortError") return;
    currentJobs = [];
    const isNetworkDown = err instanceof TypeError && err.message.toLowerCase().includes("fetch");
    if (isNetworkDown) {
      setStatus("Máy chủ đang bận — đang thử tìm kiếm trực tiếp từ LinkedIn…", "error");
      try {
        const fallbackJobs = await scrapeLinkedInFallback(
          keyword,
          location ?? "Vietnam",
          abortController?.signal,
        );
        if (fallbackJobs.length > 0) {
          currentJobs = appendJobs([], fallbackJobs);
          const count = fallbackJobs.length;
          setStatus(
            `Máy chủ bận — chỉ hiển thị ${count} kết quả từ LinkedIn.`,
            "error",
          );
        } else {
          setStatus("Máy chủ đang bận. Vui lòng thử lại sau.", "error");
        }
      } catch {
        setStatus("Máy chủ đang bận. Vui lòng thử lại sau.", "error");
      }
    } else {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("quá nhiều lần") || msg.includes("Too Many") || msg.includes("429")) {
        setStatus("Bạn đã tìm kiếm quá nhiều lần. Vui lòng chờ 1 phút rồi thử lại.", "error");
      } else {
        setStatus(`Lỗi: ${msg}`, "error");
      }
    }
  } finally {
    fetchBtn.disabled = false;
  }
}

fetchBtn.addEventListener("click", async () => {
  const sharedJobLink = _pendingSharedJobLink;
  _pendingSharedJobLink = null;

  if (locationSelect.value === "_custom" && !locationCustom.value.trim()) {
    setStatus("Vui lòng nhập địa điểm.", "error");
    locationCustom.classList.remove("hidden");
    locationCustom.focus();
    return;
  }

  const rawInput = keywordEl.value.trim();
  if (!rawInput) {
    setStatus("Vui lòng nhập tên công việc hoặc dán kỹ năng/CV.", "error");
    return;
  }

  const location = getLocation() || undefined;

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
  setStatus("Đang phân tích…", "info");
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
// by runSearch(). Use the "Chia sẻ kết quả" button to get a shareable link.
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
    const option = [...locationSelect.options].find((o) => o.value === loc);
    if (option) {
      locationSelect.value = loc;
      locationCustom.classList.add("hidden");
    } else {
      locationSelect.value = "_custom";
      locationCustom.classList.remove("hidden");
      locationCustom.value = loc;
    }
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
    hideIntentBox();
    (window as any)._slideshowShow?.();
    currentJobs = [];
    fetchBtn.disabled = false;
    keywordEl.value = "";
    keywordEl.style.height = "auto";
    return;
  }

  // Restore keyword and location into the inputs, then re-run the search.
  keywordEl.value = kw;
  if (loc) {
    const option = [...locationSelect.options].find((o) => o.value === loc);
    if (option) {
      locationSelect.value = loc;
      locationCustom.classList.add("hidden");
    } else {
      locationSelect.value = "_custom";
      locationCustom.classList.remove("hidden");
      locationCustom.value = loc;
    }
  }
  void runSearch(kw, loc || getLocation(), null);
});

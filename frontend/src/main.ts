import { scrapeJobsStream, scrapeLinkedInFallback, suggestQuery } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching, setSearchContext, openJobByLink, showSuggestionBanner, hideSuggestionBanner } from "./ui";
import type { Job } from "./types";

let currentJobs: Job[] = [];

const fetchBtn        = document.getElementById("fetchBtn")        as HTMLButtonElement;
const keywordEl       = document.getElementById("keyword")         as HTMLInputElement;
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
  currentJobs = [];
  fetchBtn.disabled = false;
  keywordEl.value = "";
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
  const val = keywordEl.value.trim().toLowerCase();
  suggestionChips.forEach(c => {
    c.classList.toggle("active", (c.dataset.kw ?? "").toLowerCase() === val);
  });
});


keywordEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter") fetchBtn.click();
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
 *  triggered both from the button click and from accepting an AI suggestion. */
async function runSearch(keyword: string, location: string | undefined, sharedJobLink: string | null): Promise<void> {
  abortController?.abort();
  abortController = new AbortController();

  hideSuggestionBanner();
  setSearchContext(keyword, location);
  keywordEl.value = keyword;

  fetchBtn.disabled = true;
  currentJobs = [];
  hideResults();
  // related jobs feature is disabled for now
  hideProgress();
  clearStatus();
  showProgress();

  let _isCacheHit = false;

  try {
    await scrapeJobsStream(
      { keyword, location },
      (batch) => {
        if (location) {
          for (const j of batch) {
            if (!j.location) j.location = location;
          }
        }
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

        if (_isCacheHit) {
          setStatus(`Tìm thấy ${count} việc làm trong tuần qua.`, "success");
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
      () => clearQueuedMessage(),
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
      },
      // onVectorResults (related jobs) is disabled for now
      () => {},
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

  const keyword = keywordEl.value.trim();
  if (!keyword) {
    setStatus("Vui lòng nhập tên công việc.", "error");
    return;
  }
  const isChip = [...suggestionChips].some(c => (c.dataset.kw ?? "").toLowerCase() === keyword.toLowerCase());
  if (!isChip && keyword.split(/\s+/).length < 2) {
    setStatus("Vui lòng nhập cụ thể hơn — ít nhất 2 từ (ví dụ: \"AI Engineer\").", "error");
    return;
  }

  // Predefined chips are already curated; skip AI typo/suggestion round-trip.
  if (isChip) {
    void runSearch(keyword, getLocation(), sharedJobLink);
    return;
  }

  const location = getLocation() || undefined;

  // Fire AI suggestion check in parallel — race with 2s timeout so it never blocks.
  const suggestionTimeout = new Promise<null>(resolve => setTimeout(() => resolve(null), 2000));
  const suggestionPromise = suggestQuery(keyword, location);
  const suggestion = await Promise.race([suggestionPromise, suggestionTimeout]);

  // Auto-apply pure spelling corrections (corrected ≈ original in word count).
  const effectiveKeyword = (suggestion?.changed && suggestion.corrected)
    ? suggestion.corrected
    : keyword;

  // Start the actual search with the (possibly corrected) keyword.
  void runSearch(effectiveKeyword, location, sharedJobLink);

  // If the AI corrected the query, show a dismissible "Did you mean?" banner
  // so the user can revert to their original search.
  if (suggestion?.changed) {
    // Show banner after a short delay so progress bar is visible first.
    setTimeout(() => {
      showSuggestionBanner(
        keyword,           // original — offer to revert
        (original) => void runSearch(original, location, null),
        () => { /* dismissed — keep corrected search running */ },
      );
    }, 500);
  }

});

// Deep-link support: /?kw=...&loc=... and optional &job=... to auto-open modal.
(() => {
  const url = new URL(window.location.href);
  const kw = (url.searchParams.get("kw") ?? "").trim();
  const loc = (url.searchParams.get("loc") ?? "").trim();
  _pendingSharedJobLink = (url.searchParams.get("job") ?? "").trim() || null;

  if (kw || loc || _pendingSharedJobLink) {
    history.replaceState({}, "", window.location.pathname);
  }

  if (!kw) return;

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

  // Kick off the search after the initial DOM is ready.
  queueMicrotask(() => fetchBtn.click());
})();

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && fetchBtn.disabled) {
    abortController?.abort();
    abortController = null;
    fetchBtn.disabled = false;
    hideProgress();
  }
});

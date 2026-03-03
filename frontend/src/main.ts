import { scrapeJobsStream, scrapeLinkedInFallback } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching } from "./ui";
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

let abortController: AbortController | null = null;

fetchBtn.addEventListener("click", async () => {
  const keyword = keywordEl.value.trim();
  if (!keyword) {
    setStatus("Please enter a job title.", "error");
    return;
  }

  abortController?.abort();
  abortController = new AbortController();

  const location = getLocation() || undefined;

  fetchBtn.disabled = true;
  currentJobs = [];
  hideResults();
  hideProgress();
  clearStatus();
  showProgress();

  let _isCacheHit = false;

  try {
    await scrapeJobsStream(
      { keyword, location },
      (batch) => {
        if (batch.some(j => j.source === "LinkedIn" && !j.description)) {
          setLinkedInEnriching(true);
        }
        if (batch.some(j => j.source === "TopCV" && !j.description)) {
          setTopCVEnriching(true);
        }
        currentJobs = appendJobs(currentJobs, batch);
        updateProgressCount(currentJobs);
      },
      () => {
        hideProgress();
        const count = currentJobs.length;
        if (count === 0) {
          setStatus("No matching jobs found in the last week. Try a different keyword.", "error");
        } else if (_isCacheHit) {
          setStatus(`Found ${count} job${count !== 1 ? "s" : ""} in the last week.`, "success");
        } else {
          setStatus(`Found ${count} job${count !== 1 ? "s" : ""} — fetching descriptions…`, "success");
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
        setStatus(`Found ${count} job${count !== 1 ? "s" : ""} in the last week.`, "success");
      },
      () => {
        setTopCVEnriching(false);
        const count = currentJobs.length;
        setStatus(`Found ${count} job${count !== 1 ? "s" : ""} in the last week.`, "success");
      },
      (_fetchedTs, fuzzy) => {
        if (!fuzzy) _isCacheHit = true;
      },
    );
  } catch (err) {
    hideProgress();
    if ((err as Error).name === "AbortError") return;
    currentJobs = [];
    const isNetworkDown = err instanceof TypeError && err.message.toLowerCase().includes("fetch");
    if (isNetworkDown) {
      setStatus("Server is busy — trying LinkedIn directly…", "error");
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
            `Server busy — showing ${count} LinkedIn result${count !== 1 ? "s" : ""} only.`,
            "error",
          );
        } else {
          setStatus("Server is busy. Please try again in a moment.", "error");
        }
      } catch {
        setStatus("Server is busy. Please try again in a moment.", "error");
      }
    } else {
      const msg = err instanceof Error ? err.message : String(err);
      setStatus(`Error: ${msg}`, "error");
    }
  } finally {
    fetchBtn.disabled = false;
  }
});

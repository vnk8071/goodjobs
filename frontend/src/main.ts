import { scrapeJobsStream, scrapeLinkedInFallback } from "./api";
import { setStatus, clearStatus, appendJobs, hideResults, showProgress, updateProgressCount, markSiteDone, hideProgress, showQueuedMessage, clearQueuedMessage, setLinkedInEnriching, setTopCVEnriching, showRelated, hideRelated } from "./ui";
import type { Job } from "./types";

let currentJobs: Job[] = [];

const fetchBtn        = document.getElementById("fetchBtn")        as HTMLButtonElement;
const keywordEl       = document.getElementById("keyword")         as HTMLInputElement;
const locationSelect  = document.getElementById("locationSelect")  as HTMLSelectElement;
const locationCustom  = document.getElementById("locationCustom")  as HTMLInputElement;
const aboutSection    = document.getElementById("aboutSection")    as HTMLElement;

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
  hideResults();
  clearStatus();
  currentJobs = [];
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

const weeklyStatsEl = document.getElementById("weeklyStats") as HTMLDivElement;

(async () => {
  try {
    const res = await fetch("/stats");
    if (res.ok) {
      const data = await res.json() as { jobs_this_week: number };
      if (data.jobs_this_week > 0) {
        const rounded = Math.floor(data.jobs_this_week / 100) * 100;
        const display = rounded > 0 ? rounded.toLocaleString("vi-VN") : data.jobs_this_week;
        weeklyStatsEl.innerHTML = `<strong>${display}+</strong> việc làm được đăng trong tuần này`;
        weeklyStatsEl.classList.remove("hidden");
      }
    }
  } catch {
    // silently ignore — stats are non-critical
  }
})();

let abortController: AbortController | null = null;

fetchBtn.addEventListener("click", async () => {
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

  abortController?.abort();
  abortController = new AbortController();

  const location = getLocation() || undefined;

  fetchBtn.disabled = true;
  currentJobs = [];
  aboutSection.classList.add("hidden");
  hideResults();
  hideRelated();
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
      },
      () => {
        hideProgress();
        const count = currentJobs.length;
        if (count === 0) {
          setStatus("Không tìm thấy việc làm phù hợp trong tuần qua. Thử từ khóa khác.", "error");
        } else if (_isCacheHit) {
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
      (relatedJobs) => {
        if (location) {
          for (const j of relatedJobs) {
            if (!j.location) j.location = location;
          }
        }
        showRelated(relatedJobs);
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
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && fetchBtn.disabled) {
    abortController?.abort();
    abortController = null;
    fetchBtn.disabled = false;
    hideProgress();
  }
});

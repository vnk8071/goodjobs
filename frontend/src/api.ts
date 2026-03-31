import type { Job, ScrapeRequest } from "./types";

const _LINKEDIN_BASE =
  "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search";

const _CORS_PROXIES = [
  (u: string) => `https://api.allorigins.win/raw?url=${u}`,
  (u: string) => `https://api.codetabs.com/v1/proxy?quest=${u}`,
  (u: string) => `https://thingproxy.freeboard.io/fetch/${decodeURIComponent(u)}`,
];

/** Parse LinkedIn job listing HTML into a Job array using the DOM parser. */
function _parseLinkedInHtml(html: string): Job[] {
  const doc = new DOMParser().parseFromString(html, "text/html");
  const jobs: Job[] = [];

  doc.querySelectorAll("li").forEach((li) => {
    const titleEl   = li.querySelector(".base-search-card__title, h3");
    const companyEl = li.querySelector(".base-search-card__subtitle, h4");
    const locEl     = li.querySelector(".job-search-card__location");
    const timeEl    = li.querySelector("time");
    const linkEl    = li.querySelector("a.base-card__full-link, a[href*='/jobs/view/']");

    if (!titleEl || !linkEl) return;

    const href = (linkEl as HTMLAnchorElement).href.split("?")[0];
    if (!href || !href.includes("linkedin.com")) return;

    jobs.push({
      title:       titleEl.textContent?.trim() ?? "",
      company:     companyEl?.textContent?.trim() ?? "Unknown",
      location:    locEl?.textContent?.trim() ?? "",
      link:        href,
      source:      "LinkedIn",
      posted:      timeEl?.textContent?.trim() ?? "",
      posted_date: timeEl?.getAttribute("datetime")?.slice(0, 10) ?? "",
      posted_ts:   timeEl?.getAttribute("datetime")
                     ? new Date(timeEl.getAttribute("datetime")!).getTime() / 1000
                     : 0,
    });
  });

  return jobs;
}

/**
 * Client-side LinkedIn fallback used when the backend is unreachable.
 * Tries LinkedIn's public guest API directly, then falls back through _CORS_PROXIES in order.
 * Throws if all proxies fail.
 */
export async function scrapeLinkedInFallback(
  keyword: string,
  location: string,
  signal?: AbortSignal,
): Promise<Job[]> {
  const target = `${_LINKEDIN_BASE}?keywords=${encodeURIComponent(keyword)}&location=${encodeURIComponent(location)}&start=0`;
  const encoded = encodeURIComponent(target);

  for (const proxyFn of _CORS_PROXIES) {
    try {
      const resp = await fetch(proxyFn(encoded), { signal });
      if (!resp.ok) continue;
      const html = await resp.text();
      const jobs = _parseLinkedInHtml(html);
      if (jobs.length > 0) return jobs;
    } catch {
      // try next proxy
    }
  }

  throw new Error("All LinkedIn fallback proxies failed");
}

const API_BASE = import.meta.env.VITE_API_URL ?? "";

/**
 * Streams job results from the backend via SSE (VPS) or plain JSON (Lambda).
 * Calls `onBatch` each time a scraper finishes with its jobs.
 * Calls `onSiteDone(site, count)` when a single scraper completes.
 * Calls `onDone` when all scrapers have completed.
 *
 * Supports two response formats:
 *  - text/event-stream (VPS FastAPI): incremental SSE batches per scraper
 *  - application/json  (Lambda):      single JSON array returned at once
 */
export async function scrapeJobsStream(
  req: ScrapeRequest,
  onBatch: (jobs: Job[]) => void,
  onDone: () => void,
  signal?: AbortSignal,
  onSiteDone?: (site: string, count: number) => void,
  onSiteLoading?: (site: string, count: number) => void,
  onQueued?: (position: number) => void,
  onStarted?: () => void,
  onLinkedInDone?: () => void,
  onTopCVDone?: () => void,
  onCached?: (fetchedTs: number, fuzzy: boolean) => void,
  onVectorResults?: (jobs: Job[]) => void,
): Promise<void> {
  const resp = await fetch(`${API_BASE}/scrape-stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
    signal,
  });

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail ?? "Failed to start stream");
  }

  const contentType = resp.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const jobs = (await resp.json()) as Job[];
    if (jobs.length > 0) {
      onBatch(jobs);
      if (onSiteDone) {
        const counts = new Map<string, number>();
        for (const j of jobs) counts.set(j.source, (counts.get(j.source) ?? 0) + 1);
        counts.forEach((cnt, site) => onSiteDone(site, cnt));
      }
    }
    onDone();
    return;
  }

  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let _doneFired = false;
  let eventType = "message";
  const siteCounts = new Map<string, number>();

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const line of lines) {
      if (line.startsWith("event:")) {
        eventType = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        const payload = line.slice(5).trim();
        if (eventType === "done") {
          if (onSiteDone) {
            siteCounts.forEach((count, site) => onSiteDone!(site, count));
          }
          onDone();
          _doneFired = true;
          eventType = "message";
          continue; // keep reading for Phase 2 enrichment
        }
        if (eventType === "linkedin-done") {
          if (onSiteDone) onSiteDone("LinkedIn", siteCounts.get("LinkedIn") ?? 0);
          if (onLinkedInDone) onLinkedInDone();
          eventType = "message";
          continue;
        }
        if (eventType === "topcv-done") {
          if (onSiteDone) onSiteDone("TopCV", siteCounts.get("TopCV") ?? 0);
          if (onTopCVDone) onTopCVDone();
          eventType = "message";
          continue;
        }
        if (eventType === "itviec-done") {
          if (onSiteDone) onSiteDone("ITViec", siteCounts.get("ITViec") ?? 0);
          eventType = "message";
          continue;
        }
        if (eventType === "queued") {
          try {
            const { position } = JSON.parse(payload) as { position: number };
            if (onQueued) onQueued(position);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "started") {
          if (onStarted) onStarted();
          eventType = "message";
          continue;
        }
        if (eventType === "cached") {
          try {
            const { jobs, fetched_ts, fuzzy } = JSON.parse(payload) as { jobs: Job[]; fetched_ts: number; fuzzy: boolean };
            if (jobs.length > 0) {
              for (const j of jobs) siteCounts.set(j.source, (siteCounts.get(j.source) ?? 0) + 1);
              const BATCH = 5;
              for (let i = 0; i < jobs.length; i += BATCH) {
                onBatch(jobs.slice(i, i + BATCH));
                if (i + BATCH < jobs.length) await new Promise(r => setTimeout(r, 80));
              }
            }
            if (onCached) onCached(fetched_ts, fuzzy ?? false);
          } catch (e) { console.error("[cached] parse error:", e, "payload length:", payload.length); }
          eventType = "message";
          continue;
        }
        if (eventType === "linkedin-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("LinkedIn", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "topcv-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("TopCV", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "itviec-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("ITViec", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "topdev-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("TopDev", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "topdev-done") {
          if (onSiteDone) onSiteDone("TopDev", siteCounts.get("TopDev") ?? 0);
          eventType = "message";
          continue;
        }
        if (eventType === "jobsgo-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("JobsGo", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "jobsgo-done") {
          if (onSiteDone) onSiteDone("JobsGo", siteCounts.get("JobsGo") ?? 0);
          eventType = "message";
          continue;
        }
        if (eventType === "careerlink-enriching") {
          try {
            const { count } = JSON.parse(payload) as { count: number };
            if (onSiteLoading) onSiteLoading("CareerLink", count);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        if (eventType === "careerlink-done") {
          if (onSiteDone) onSiteDone("CareerLink", siteCounts.get("CareerLink") ?? 0);
          eventType = "message";
          continue;
        }
        if (eventType === "vector-results") {
          try {
            const { jobs } = JSON.parse(payload) as { jobs: Job[]; count: number };
            if (jobs.length > 0 && onVectorResults) onVectorResults(jobs);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }
        try {
          const jobs = JSON.parse(payload) as Job[];
          if (jobs.length > 0) {
            const site = jobs[0].source;
            siteCounts.set(site, (siteCounts.get(site) ?? 0) + jobs.length);
            onBatch(jobs);
          }
        } catch { /* ignore malformed lines */ }
        eventType = "message";
      }
    }
  }

  if (!_doneFired) onDone();
}

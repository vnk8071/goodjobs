import type { Job, ScrapeRequest, QuerySuggestion } from "./types";

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
 * Pre-verify user query with AI spell-check and intent mapping.
 * Returns a suggestion with corrected text and best matching cached keyword.
 * Resolves to null on network error or timeout so the search can proceed unblocked.
 */
export async function suggestQuery(
  keyword: string,
  location?: string,
  signal?: AbortSignal,
): Promise<QuerySuggestion | null> {
  try {
    const resp = await fetch(`${API_BASE}/suggest-query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keyword, location }),
      signal,
    });
    if (!resp.ok) return null;
    return (await resp.json()) as QuerySuggestion;
  } catch {
    return null;
  }
}

export interface ClassifyResult {
  input_type: "job_title" | "cv_or_skills";
  keyword: string;
  alternatives?: string[];
  reasoning: string;
  is_job_title: boolean;
}

export async function classifyInput(
  rawText: string,
  signal?: AbortSignal,
): Promise<ClassifyResult | null> {
  try {
    const resp = await fetch(`${API_BASE}/classify-input`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keyword: rawText }),
      signal,
    });
    if (!resp.ok) return null;
    return (await resp.json()) as ClassifyResult;
  } catch {
    return null;
  }
}

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
        // Map backend site keys to display names for enriching/done events
        const _SITE_DISPLAY: Record<string, string> = {
          linkedin:   "LinkedIn",
          topcv:      "TopCV",
          itviec:     "ITViec",
          topdev:     "TopDev",
          jobsgo:     "JobsGo",
          careerlink: "CareerLink",
        };

        // Special lifecycle events
        if (eventType === "done") {
          if (onSiteDone) siteCounts.forEach((count, site) => onSiteDone!(site, count));
          onDone();
          _doneFired = true;
          eventType = "message";
          continue; // keep reading for Phase 2 enrichment
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
        if (eventType === "vector-results") {
          try {
            const { jobs } = JSON.parse(payload) as { jobs: Job[]; count: number };
            if (jobs.length > 0 && onVectorResults) onVectorResults(jobs);
          } catch { /* ignore */ }
          eventType = "message";
          continue;
        }

        // Per-site enriching / done events
        const enrichingMatch = eventType.match(/^(.+)-enriching$/);
        if (enrichingMatch) {
          const display = _SITE_DISPLAY[enrichingMatch[1]];
          if (display) {
            try {
              const { count } = JSON.parse(payload) as { count: number };
              if (onSiteLoading) onSiteLoading(display, count);
            } catch { /* ignore */ }
          }
          eventType = "message";
          continue;
        }
        const doneMatch = eventType.match(/^(.+)-done$/);
        if (doneMatch) {
          const display = _SITE_DISPLAY[doneMatch[1]];
          if (display) {
            if (onSiteDone) onSiteDone(display, siteCounts.get(display) ?? 0);
            if (display === "LinkedIn" && onLinkedInDone) onLinkedInDone();
            if (display === "TopCV" && onTopCVDone) onTopCVDone();
          }
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

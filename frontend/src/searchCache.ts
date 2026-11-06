import type { Job } from "./types";

/**
 * Search-result cache (IndexedDB).
 *
 * A search can return hundreds of jobs with descriptions, so localStorage is
 * not suitable (5 MB quota, synchronous). We keep the last few keyword×location
 * result sets for a short TTL; a fresh cache skips `/scrape-stream` entirely.
 */

const DB_NAME = "goodjobs-searches";
const STORE = "searches";
const VERSION = 1;
const MAX_QUERIES = 12;
const MAX_JOBS = 1500;
const MAX_BYTES = 3_000_000;

export const SEARCH_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

type SearchRecord = {
  key: string;
  keyword: string;
  location: string;
  fetchedAt: number;
  jobs: Job[];
};

function openDb(): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    if (typeof indexedDB === "undefined") {
      resolve(null);
      return;
    }
    try {
      const request = indexedDB.open(DB_NAME, VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(STORE)) {
          const store = db.createObjectStore(STORE, { keyPath: "key" });
          store.createIndex("fetchedAt", "fetchedAt");
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

/** Stable key for a keyword × location × level combination. */
export function searchCacheKey(keyword: string, location?: string, level = ""): string {
  const kw = (keyword || "").trim().toLowerCase().replace(/\s+/g, " ");
  const loc = (location || "").trim().toLowerCase().replace(/\s+/g, " ");
  const lvl = (level || "").trim().toLowerCase();
  return `v1|${kw}|${loc}|${lvl}`;
}

export function isSearchCacheFresh(fetchedAt: number): boolean {
  return Date.now() - fetchedAt < SEARCH_CACHE_TTL_MS;
}

export async function readSearchCache(key: string): Promise<{ jobs: Job[]; fetchedAt: number } | null> {
  const db = await openDb();
  if (!db) return null;
  try {
    const record = await new Promise<SearchRecord | null>((resolve) => {
      const tx = db.transaction(STORE, "readonly");
      const req = tx.objectStore(STORE).get(key);
      req.onsuccess = () => resolve((req.result as SearchRecord) ?? null);
      req.onerror = () => resolve(null);
    });
    if (!record || !Array.isArray(record.jobs) || record.jobs.length === 0) return null;
    return { jobs: record.jobs, fetchedAt: Number(record.fetchedAt) || 0 };
  } catch {
    return null;
  } finally {
    db.close();
  }
}

export async function writeSearchCache(
  key: string,
  keyword: string,
  location: string | undefined,
  jobs: Job[],
): Promise<void> {
  if (!Array.isArray(jobs) || jobs.length === 0 || jobs.length > MAX_JOBS) return;
  let approximateBytes = 0;
  try {
    approximateBytes = JSON.stringify(jobs).length;
  } catch {
    return;
  }
  if (approximateBytes > MAX_BYTES) return;

  const db = await openDb();
  if (!db) return;
  try {
    const record: SearchRecord = {
      key,
      keyword,
      location: location || "",
      fetchedAt: Date.now(),
      jobs,
    };
    await new Promise<void>((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).put(record);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
      tx.onabort = () => resolve();
    });

    // Keep only the most recent MAX_QUERIES result sets.
    const all = await new Promise<SearchRecord[]>((resolve) => {
      const tx = db.transaction(STORE, "readonly");
      const req = tx.objectStore(STORE).getAll();
      req.onsuccess = () => resolve((req.result as SearchRecord[]) || []);
      req.onerror = () => resolve([]);
    });
    if (all.length > MAX_QUERIES) {
      const stale = all
        .sort((a, b) => (b.fetchedAt || 0) - (a.fetchedAt || 0))
        .slice(MAX_QUERIES);
      if (stale.length > 0) {
        await new Promise<void>((resolve) => {
          const tx = db.transaction(STORE, "readwrite");
          const store = tx.objectStore(STORE);
          for (const record of stale) store.delete(record.key);
          tx.oncomplete = () => resolve();
          tx.onerror = () => resolve();
          tx.onabort = () => resolve();
        });
      }
    }
  } catch {
    // quota/private mode — ignore
  } finally {
    db.close();
  }
}

export async function clearSearchCache(): Promise<void> {
  const db = await openDb();
  if (!db) return;
  try {
    await new Promise<void>((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  } catch {
    // ignore
  } finally {
    db.close();
  }
}

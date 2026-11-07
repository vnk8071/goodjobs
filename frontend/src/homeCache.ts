import type { Job } from "./types";

/**
 * Homepage feed cache (IndexedDB).
 *
 * The feed is ~3.4 MB / 3,177 jobs, which does not fit safely in localStorage
 * (5 MB quota, UTF-16 strings, synchronous writes). IndexedDB stores the same
 * objects without blocking the main thread and with a much larger quota.
 */

const DB_NAME = "goodjobs-cache";
const STORE = "kv";
const KEY = "home-feed";
const VERSION = 2;
const MAX_JOBS = 5000;
export const HOME_CACHE_TTL_MS = 3 * 60 * 60 * 1000; // 3 hours

type CacheRecord = {
  key: string;
  version: number;
  fetchedAt: number;
  /** Number of unique jobs the server reported when this snapshot was written. */
  expectedCount: number;
  jobs: Job[];
};

function openDb(): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    if (typeof indexedDB === "undefined") {
      resolve(null);
      return;
    }
    try {
      const request = indexedDB.open(DB_NAME, 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: "key" });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

/** Read the cached homepage feed, or null when missing/unavailable. */
export async function readHomeFeed(): Promise<{ jobs: Job[]; fetchedAt: number; expectedCount: number } | null> {
  const db = await openDb();
  if (!db) return null;
  try {
    const record = await new Promise<CacheRecord | null>((resolve) => {
      const tx = db.transaction(STORE, "readonly");
      const req = tx.objectStore(STORE).get(KEY);
      req.onsuccess = () => resolve((req.result as CacheRecord) ?? null);
      req.onerror = () => resolve(null);
    });
    if (!record || record.version !== VERSION || !Array.isArray(record.jobs) || record.jobs.length === 0) {
      return null;
    }
    const expected = Number(record.expectedCount) || 0;
    // Reject partial snapshots written by an interrupted stream.
    if (expected > 0 && record.jobs.length < expected * 0.98) return null;
    return {
      jobs: record.jobs,
      fetchedAt: Number(record.fetchedAt) || 0,
      expectedCount: expected,
    };
  } catch {
    return null;
  } finally {
    db.close();
  }
}

export function isHomeCacheFresh(fetchedAt: number): boolean {
  return Date.now() - fetchedAt < HOME_CACHE_TTL_MS;
}

/** Persist the homepage feed for the next visit. Best-effort only. */
export async function writeHomeFeed(jobs: Job[], expectedCount = jobs.length): Promise<void> {
  if (!Array.isArray(jobs) || jobs.length === 0) return;
  const db = await openDb();
  if (!db) return;
  try {
    const payload: CacheRecord = {
      key: KEY,
      version: VERSION,
      fetchedAt: Date.now(),
      expectedCount: Math.max(Number(expectedCount) || 0, jobs.length),
      jobs: jobs.slice(0, MAX_JOBS),
    };
    await new Promise<void>((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).put(payload);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
      tx.onabort = () => resolve();
    });
  } catch {
    // quota/private mode — ignore
  } finally {
    db.close();
  }
}

/** Remove the cached feed (useful after a data-shape change). */
export async function clearHomeFeed(): Promise<void> {
  const db = await openDb();
  if (!db) return;
  try {
    await new Promise<void>((resolve) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).delete(KEY);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  } catch {
    // ignore
  } finally {
    db.close();
  }
}

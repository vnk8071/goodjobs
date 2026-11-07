export interface Job {
  title: string;
  company: string;
  location: string;
  link: string;
  source:
    | "LinkedIn"
    | "ITViec"
    | "TopCV"
    | "VietnamWorks"
    | "TopDev"
    | "Indeed"
    | "CareerViet"
    | "JobsGo"
    | "CareerLink"
    | "TechJobs"
    | "XomData"
    | "Google"
    | "Direct"
    | (string & {});  // TechJobs jobs carry their apply domain as source
  posted?: string;
  posted_date?: string;   // ISO date YYYY-MM-DD for sorting
  posted_ts?: number;     // Unix timestamp for precise sort (newer = larger)
  _scraped_ts?: number;   // cache fetch time; fallback when the source has no posted date
  description?: string;
  summary_description?: string;
  skills?: string[];
  logo?: string;
  _vector_score?: number;
  _from_vector?: boolean;
  _compact?: boolean;     // true for homepage stream rows; full details are fetched on modal open
  _cache_key?: string;    // Redis key for the row's cache entry (fast detail lookup)
  level_match?: boolean;   // true when title contains the requested level word (e.g. "junior")
}

export interface ScrapeRequest {
  keyword: string;
  location?: string;
  raw_input?: string;  // Free-form CV/skills text; used for vector search
}

export interface QuerySuggestion {
  corrected: string;
  changed: boolean;
  suggested_cache_keyword: string | null;
  reasoning: string;
}

export interface ChatHistoryMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatJob {
  title: string;
  company: string;
  location: string;
  source: string;
  link: string;
  posted: string;
  skills: string[];
  score: number;
}

export interface ChatResponse {
  answer: string;
  jobs: ChatJob[];
  count: number;
}

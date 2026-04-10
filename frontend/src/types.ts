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
    | "CareerLink";
  posted?: string;
  posted_date?: string;   // ISO date YYYY-MM-DD for sorting
  posted_ts?: number;     // Unix timestamp for precise sort (newer = larger)
  description?: string;
  summary_description?: string;
  skills?: string[];
  logo?: string;
}

export interface ScrapeRequest {
  keyword: string;
  location?: string;
}

export interface QuerySuggestion {
  corrected: string;
  changed: boolean;
  suggested_cache_keyword: string | null;
  reasoning: string;
}

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  link TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  company TEXT NOT NULL DEFAULT '',
  location TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  posted TEXT NOT NULL DEFAULT '',
  posted_ts REAL NOT NULL DEFAULT 0,
  posted_date TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  skills TEXT NOT NULL DEFAULT '[]',
  logo TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_posted_ts ON jobs(posted_ts DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_link ON jobs(link);

-- News desk (Vietnamese) -----------------------------------------------------
CREATE TABLE IF NOT EXISTS news_articles (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  dek TEXT NOT NULL DEFAULT '',
  meta_description TEXT NOT NULL DEFAULT '',
  body_html TEXT NOT NULL,
  body_text TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT '',
  tags TEXT NOT NULL DEFAULT '[]',
  sources TEXT NOT NULL DEFAULT '[]',
  signals TEXT NOT NULL DEFAULT '{}',
  takeaways TEXT NOT NULL DEFAULT '[]',
  search_keyword TEXT NOT NULL DEFAULT '',
  linkedin_text TEXT NOT NULL DEFAULT '',
  linkedin_status TEXT NOT NULL DEFAULT '',
  linkedin_url TEXT NOT NULL DEFAULT '',
  lang TEXT NOT NULL DEFAULT 'vi',
  author TEXT NOT NULL DEFAULT 'Ban Biên Tập Good Jobs',
  status TEXT NOT NULL DEFAULT 'published',
  content_hash TEXT NOT NULL DEFAULT '',
  published_at TEXT NOT NULL DEFAULT (datetime('now')),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_news_status_pub ON news_articles(status, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_articles(category);

CREATE TABLE IF NOT EXISTS news_seen (
  source_key TEXT PRIMARY KEY,
  title TEXT NOT NULL DEFAULT '',
  publisher TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  seen_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS news_runs (
  id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  generated INTEGER NOT NULL DEFAULT 0,
  published INTEGER NOT NULL DEFAULT 0,
  skipped INTEGER NOT NULL DEFAULT 0,
  notes TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL DEFAULT (datetime('now')),
  finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_runs_date ON news_runs(run_date);

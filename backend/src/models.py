from pydantic import BaseModel


class Job(BaseModel):
    title: str
    company: str
    location: str
    link: str
    source: str
    posted: str = ""
    posted_date: str = ""   # ISO date string YYYY-MM-DD for sorting
    posted_ts: float = 0.0  # Unix timestamp for precise sort (newer = larger)
    description: str = ""
    # Eight scrapers (itviec.py, viecoi.py, vietnamworks.py, topcv.py, linkedin.py,
    # careerlink.py, jobsgo.py, glints.py) deliberately write `None` here as a
    # "not yet summarized — background_summarizer.py will fill this in" sentinel,
    # distinct from "" ("no description to summarize", set by topdev.py/indeed.py/
    # glints.py's listing-only branch). A bare `str` type rejects that `None` on
    # response-model validation, which is exactly what broke `POST /scrape`: the
    # cache-hit fast path returns cached jobs through `response_model=list[Job]`,
    # and any cached job still carrying the `None` sentinel raised an uncaught
    # ResponseValidationError, surfacing as a bare 500. `/recent-jobs` and `/stats`
    # never hit this because neither route declares a response_model, so they pass
    # the same dicts straight through. `get_jobs_without_summary()`
    # (background_summarizer.py) already treats `None` and `""` identically as
    # "needs summarizing", so widening the type here doesn't change any behavior
    # other than letting the existing convention actually validate.
    summary_description: str | None = ""  # None = not yet summarized; "" = no summary to write

    skills: list[str] = []
    logo: str = ""          # Company logo URL (if available from source)


class ScrapeRequest(BaseModel):
    keyword: str
    location: str = "Ho Chi Minh City"
    country: str = "VN"   # "VN" | "US" | "UK" | "SG" — selects VN vs. global scraper registry
    raw_input: str = ""   # Free-form CV/skills text; used for vector search when set
    estimated_level: str = ""  # "junior" | "middle" | "senior" | "" — AI-inferred from CV
    intent: str = ""      # "job_title" | "cv_or_skills" | "not_job" — AI-classified on frontend

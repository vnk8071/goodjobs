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
    summary_description: str = ""  # NEW: LLM-generated summary
    skills: list[str] = []
    logo: str = ""          # Company logo URL (if available from source)


class ScrapeRequest(BaseModel):
    keyword: str
    location: str = "Ho Chi Minh City"
    raw_input: str = ""   # Free-form CV/skills text; used for vector search when set
    estimated_level: str = ""  # "junior" | "middle" | "senior" | "" — AI-inferred from CV
    intent: str = ""      # "job_title" | "cv_or_skills" | "not_job" — AI-classified on frontend

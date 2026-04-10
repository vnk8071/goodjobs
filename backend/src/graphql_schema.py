import asyncio
from typing import List, Optional

import strawberry

from src.cache import cache_get, cache_fuzzy_get
from src.matching import title_matches, extract_skills, posted_ts


@strawberry.type
class Job:
    title: str
    company: str
    location: str
    posted: str
    posted_ts: float
    link: str
    description: Optional[str] = None
    source: str
    skills: List[str]


def _job_to_gql(job: dict) -> Job:
    """Convert a raw job dict to GraphQL Job type."""
    return Job(
        title=job.get("title", ""),
        company=job.get("company", ""),
        location=job.get("location", ""),
        posted=job.get("posted", ""),
        posted_ts=job.get("posted_ts", 0.0),
        link=job.get("link", ""),
        description=job.get("description"),
        source=job.get("source", ""),
        skills=job.get("skills", []),
    )


@strawberry.type
class Query:
    @strawberry.field
    async def jobs(self, keyword: str, location: str, limit: int = 50) -> List[Job]:
        """Search for jobs by keyword and location from cache."""
        result = await cache_get(keyword, location)
        if not result:
            result = await cache_fuzzy_get(keyword, location)
        if not result:
            return []

        jobs, _fetched_ts = result[0] if isinstance(result, tuple) and len(result) == 3 else result
        if not jobs:
            return []

        filtered = [j for j in jobs if title_matches(j.get("title", ""), keyword)]
        filtered.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        return [_job_to_gql(j) for j in filtered[:limit]]

    @strawberry.field
    def health(self) -> str:
        return "good jobs service is running"


schema = strawberry.Schema(query=Query)

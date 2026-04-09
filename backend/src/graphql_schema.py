import strawberry
from typing import List, Optional
from datetime import datetime
from src.models import Job as JobModel


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


@strawberry.type
class Query:
    @strawberry.field
    def jobs(self, keyword: str, location: str, limit: int = 50) -> List[Job]:
        """
        Search for jobs by keyword and location.
        This is a simplified version - in a real implementation,
        this would call the existing scrape logic.
        """
        # For now, returning empty list as placeholder
        # In a full implementation, this would integrate with existing scrapers
        return []

    @strawberry.field
    def health(self) -> str:
        return "good jobs service is running"


schema = strawberry.Schema(query=Query)

import os
import random
import re
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

# Pool of realistic desktop Chrome User-Agents across OS/versions.
# new_session() picks one at random so each HTTP request has a different fingerprint.
_USER_AGENTS = [
    # Windows Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # macOS Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # macOS Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Windows Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Linux Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

HEADERS = {
    "User-Agent": _USER_AGENTS[0],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def new_session() -> requests.Session:
    """Return a fresh requests.Session with a randomly rotated User-Agent.

    Call this per-request (or per-scrape-call) instead of reusing a global
    session — prevents sites from fingerprinting a long-lived connection.
    """
    s = requests.Session()
    headers = {**HEADERS, "User-Agent": random.choice(_USER_AGENTS)}
    s.headers.update(headers)
    return s


CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--hide-scrollbars",
    "--mute-audio",
    "--no-first-run",
    "--safebrowsing-disable-auto-update",
    *(["--no-zygote", "--single-process"] if sys.platform == "linux" else []),
]

DESC_MAX_CHARS    = 10000
RECENT_DAYS       = 8
MAX_CONCURRENT    = 6
REDIS_URL         = os.getenv("REDIS_URL", "redis://redis:6379")

# Summarizer configuration
SUMMARIZER_MAX_LENGTH = int(os.getenv("SUMMARIZER_MAX_LENGTH", "300"))
SUMMARIZER_MIN_LENGTH = int(os.getenv("SUMMARIZER_MIN_LENGTH", "50"))

# Cloudflare AI configuration
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")
CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4/accounts"
CLOUDFLARE_MODEL = "@cf/qwen/qwen3-30b-a3b-fp8"

SYNONYMS: list[set[str]] = [
    {"engineer", "developer", "dev", "programmer"},
    {"ai", "ml", "machine learning", "artificial intelligence"},
    {"fullstack", "full stack", "full-stack"},
    {"frontend", "front end", "front-end"},
    {"backend", "back end", "back-end"},
]

SKILL_PATTERNS: list[tuple[str, list[str]]] = [
    ("Python",       ["python"]),
    ("Java",         ["\\bjava\\b"]),
    ("JavaScript",   ["javascript", "js"]),
    ("TypeScript",   ["typescript", "\\bts\\b"]),
    ("C++",          ["c\\+\\+", "cpp"]),
    ("C#",           ["c#", "csharp"]),
    ("Go",           ["\\bgolang\\b", "\\bgo\\b"]),
    ("Rust",         ["\\brust\\b"]),
    ("Kotlin",       ["kotlin"]),
    ("Swift",        ["\\bswift\\b"]),
    ("PHP",          ["\\bphp\\b"]),
    ("Ruby",         ["\\bruby\\b"]),
    ("Scala",        ["\\bscala\\b"]),
    ("R",            ["\\blanguage r\\b", "\\bprogramming r\\b", "\\br studio\\b", "\\brstudio\\b"]),
    ("Dart",         ["\\bdart\\b"]),
    ("Machine Learning", ["machine learning", "\\bml\\b"]),
    ("Deep Learning",    ["deep learning"]),
    ("LLM",             ["\\bllm\\b", "large language model"]),
    ("NLP",             ["\\bnlp\\b", "natural language processing"]),
    ("Computer Vision",  ["computer vision", "\\bcv\\b"]),
    ("Reinforcement Learning", ["reinforcement learning", "\\brl\\b"]),
    ("PyTorch",      ["pytorch", "torch"]),
    ("TensorFlow",   ["tensorflow"]),
    ("Keras",        ["\\bkeras\\b"]),
    ("scikit-learn", ["scikit-learn", "sklearn"]),
    ("Hugging Face", ["hugging face", "huggingface", "transformers"]),
    ("LangChain",    ["langchain"]),
    ("OpenCV",       ["opencv"]),
    ("YOLO",         ["\\byolo\\b"]),
    ("RAG",          ["\\brag\\b", "retrieval.augmented"]),
    ("SQL",          ["\\bsql\\b"]),
    ("PostgreSQL",   ["postgres", "postgresql"]),
    ("MySQL",        ["\\bmysql\\b"]),
    ("MongoDB",      ["mongodb", "\\bmongo\\b"]),
    ("Redis",        ["\\bredis\\b"]),
    ("Elasticsearch",["elasticsearch", "\\bes\\b"]),
    ("Kafka",        ["\\bkafka\\b"]),
    ("Spark",        ["\\bspark\\b", "pyspark"]),
    ("Airflow",      ["airflow"]),
    ("dbt",          ["\\bdbt\\b"]),
    ("Pandas",       ["\\bpandas\\b"]),
    ("NumPy",        ["\\bnumpy\\b"]),
    ("React",        ["\\breact\\b", "reactjs"]),
    ("Vue",          ["\\bvue\\b", "vuejs"]),
    ("Angular",      ["\\bangular\\b"]),
    ("Next.js",      ["next\\.js", "nextjs"]),
    ("Node.js",      ["node\\.js", "nodejs", "\\bnode\\b"]),
    ("Django",       ["\\bdjango\\b"]),
    ("FastAPI",      ["fastapi"]),
    ("Flask",        ["\\bflask\\b"]),
    ("Spring",       ["\\bspring\\b"]),
    ("GraphQL",      ["graphql"]),
    ("REST API",     ["rest api", "restful"]),
    ("Flutter",      ["\\bflutter\\b"]),
    ("React Native", ["react native"]),
    ("Android",      ["\\bandroid\\b"]),
    ("iOS",          ["\\bios\\b"]),
    ("AWS",          ["\\baws\\b", "amazon web services"]),
    ("GCP",          ["\\bgcp\\b", "google cloud"]),
    ("Azure",        ["\\bazure\\b"]),
    ("Docker",       ["\\bdocker\\b"]),
    ("Kubernetes",   ["\\bkubernetes\\b", "\\bk8s\\b"]),
    ("Terraform",    ["terraform"]),
    ("CI/CD",        ["ci/cd", "cicd", "github actions", "jenkins", "gitlab ci"]),
    ("Linux",        ["\\blinux\\b"]),
    ("Git",          ["\\bgit\\b"]),
    ("Excel",        ["\\bexcel\\b", "microsoft excel"]),
    ("Power BI",     ["power bi", "powerbi"]),
    ("Tableau",      ["\\btableau\\b"]),
    ("Looker",       ["\\blooker\\b"]),
    ("Google Analytics", ["google analytics", "\\bga4\\b"]),
    ("Metabase",     ["metabase"]),
    ("Statistics",   ["\\bstatistics\\b", "statistical analysis"]),
    ("A/B Testing",  ["a/b test", "ab test", "split test"]),
    ("BPMN",         ["\\bbpmn\\b", "business process"]),
    ("UML",          ["\\buml\\b"]),
    ("Agile",        ["\\bagile\\b", "\\bscrum\\b", "\\bkanban\\b"]),
    ("JIRA",         ["\\bjira\\b"]),
    ("Confluence",   ["confluence"]),
    ("Figma",        ["\\bfigma\\b"]),
    ("ServiceNow",   ["servicenow"]),
    ("Active Directory", ["active directory", "\\bad\\b"]),
    ("Windows",      ["\\bwindows\\b"]),
    ("Office 365",   ["office 365", "microsoft 365", "\\bo365\\b", "\\bm365\\b"]),
    ("Networking",   ["\\bnetworking\\b", "tcp/ip", "\\bvpn\\b", "\\bdns\\b", "\\bdhcp\\b"]),
    ("ITIL",         ["\\bitil\\b"]),
    ("Ticketing",    ["ticketing system", "help desk", "helpdesk", "service desk"]),
    ("Roadmap",      ["\\broadmap\\b", "product roadmap"]),
    ("OKR",          ["\\bokr\\b", "\\bokrs\\b"]),
    ("KPI",          ["\\bkpi\\b", "\\bkpis\\b"]),
    ("Scrum",        ["\\bscrum\\b", "scrum master"]),
    ("Stakeholder",  ["stakeholder"]),
    ("User Story",   ["user stor", "user journey", "acceptance criteria"]),
    ("Wireframe",    ["wireframe", "\\bmockup\\b", "prototype"]),
    ("PRD",          ["\\bprd\\b", "product requirement"]),
    ("Backlog",      ["\\bbacklog\\b", "sprint planning"]),
    ("MS Project",   ["ms project", "microsoft project"]),
    ("Trello",       ["\\btrello\\b"]),
    ("Notion",       ["\\bnotion\\b"]),
    ("Miro",         ["\\bmiro\\b"]),
]

COMPILED_SKILLS: list[tuple[str, list[re.Pattern]]] = [
    (name, [re.compile(p, re.IGNORECASE) for p in pats])
    for name, pats in SKILL_PATTERNS
]

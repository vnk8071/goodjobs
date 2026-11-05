import type { Job } from "./types";

/**
 * Homepage category filter.
 *
 * The category groups are derived from the backend warmup keywords
 * (`backend/src/warmup.py`): AI Engineer, Product Manager, Business Analyst,
 * Backend/Frontend/Fullstack/Data/DevOps/QA/Cloud Engineer, Mobile Developer,
 * Data Scientist, UI/UX Designer, Marketing Executive — plus HR, which is a
 * common search on Good Jobs.
 *
 * A job can belong to more than one category (e.g. "Machine Learning Engineer"
 * is both AI and IT), so category counts are not mutually exclusive.
 */

export const CATEGORY_IDS = [
  "ai",
  "it",
  "data",
  "product",
  "business",
  "design",
  "marketing",
  "hr",
] as const;

export type CategoryId = (typeof CATEGORY_IDS)[number];
export type CategoryFilter = "all" | CategoryId;

export const CATEGORY_LABELS: Record<CategoryId, string> = {
  ai: "AI",
  it: "IT",
  data: "Data",
  product: "Product",
  business: "Business",
  design: "Design",
  marketing: "Marketing",
  hr: "HR",
};

const CATEGORY_TERMS: Record<CategoryId, string[]> = {
  ai: [
    "ai",
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "ml",
    "nlp",
    "natural language processing",
    "computer vision",
    "llm",
    "large language model",
    "generative ai",
    "gen ai",
    "prompt engineer",
    "mlops",
    "ai engineer",
    "ai researcher",
    "ai scientist",
    "data scientist",
    "data science",
    "chatbot",
    "tensorflow",
    "pytorch",
    "hugging face",
    "openai",
    "trí tuệ nhân tạo",
    "học máy",
    "học sâu",
    "khoa học dữ liệu",
    "xử lý ngôn ngữ tự nhiên",
    "thị giác máy tính",
  ],
  it: [
    "it",
    "information technology",
    "software",
    "software engineer",
    "programmer",
    "frontend",
    "front end",
    "backend",
    "back end",
    "fullstack",
    "full stack",
    "devops",
    "sre",
    "site reliability",
    "qa",
    "qc",
    "tester",
    "test engineer",
    "automation test",
    "manual test",
    "data engineer",
    "data analyst",
    "cloud engineer",
    "cloud architect",
    "cloud infrastructure",
    "aws",
    "azure",
    "gcp",
    "information security",
    "cybersecurity",
    "network engineer",
    "network administrator",
    "sysadmin",
    "system admin",
    "system administrator",
    "mobile developer",
    "mobile engineer",
    "ios",
    "android",
    "flutter",
    "react native",
    "web developer",
    "api",
    "database",
    "dba",
    "java",
    "python",
    "javascript",
    "typescript",
    "php",
    "golang",
    "rust",
    "react",
    "angular",
    "vue",
    "node",
    "spring",
    "laravel",
    "kubernetes",
    "docker",
    "linux",
    "embedded",
    "firmware",
    "game developer",
    "solution architect",
    "technical architect",
    "scrum master",
    "lập trình viên",
    "phát triển phần mềm",
    "kỹ sư phần mềm",
    "công nghệ thông tin",
    "kiểm thử",
    "quản trị hệ thống",
    "an ninh mạng",
    "kỹ sư dữ liệu",
    "phân tích dữ liệu",
    "điện toán đám mây",
    "phát triển web",
  ],
  data: [
    "data engineer",
    "data analyst",
    "data scientist",
    "data science",
    "data analytics",
    "business intelligence",
    "power bi",
    "big data",
    "etl",
    "data warehouse",
    "data platform",
    "data architect",
    "analytics engineer",
    "database analyst",
    "kỹ sư dữ liệu",
    "phân tích dữ liệu",
    "khoa học dữ liệu",
    "kho dữ liệu",
    "dữ liệu lớn",
  ],
  product: [
    "product manager",
    "product owner",
    "product lead",
    "product director",
    "product specialist",
    "product analyst",
    "head of product",
    "vp product",
    "chief product officer",
    "quản lý sản phẩm",
    "chủ sở hữu sản phẩm",
    "giám đốc sản phẩm",
  ],
  business: [
    "business analyst",
    "business analysis",
    "business development",
    "business consultant",
    "business intelligence",
    "phân tích nghiệp vụ",
    "phát triển kinh doanh",
    "tư vấn kinh doanh",
  ],
  design: [
    "ui/ux",
    "ui ux",
    "ui",
    "ux",
    "product designer",
    "graphic designer",
    "visual designer",
    "interaction designer",
    "web designer",
    "ux researcher",
    "design lead",
    "design manager",
    "creative designer",
    "motion designer",
    "designer",
    "thiết kế",
    "thiết kế đồ họa",
    "thiết kế giao diện",
    "trải nghiệm người dùng",
    "mỹ thuật",
  ],
  marketing: [
    "marketing",
    "marketing executive",
    "marketing manager",
    "digital marketing",
    "content marketing",
    "brand marketing",
    "trade marketing",
    "product marketing",
    "growth marketing",
    "seo",
    "sem",
    "social media",
    "content creator",
    "copywriter",
    "brand",
    "branding",
    "communications",
    "public relations",
    "pr",
    "advertising",
    "media",
    "campaign",
    "performance marketing",
    "crm",
    "email marketing",
    "affiliate",
    "influencer",
    "event",
    "creative",
    "tiếp thị",
    "truyền thông",
    "quảng cáo",
    "thương hiệu",
    "nội dung",
    "sáng tạo",
    "quan hệ công chúng",
    "chiến dịch",
  ],
  hr: [
    "hr",
    "human resources",
    "human resource",
    "recruitment",
    "recruiter",
    "talent acquisition",
    "talent",
    "people operations",
    "people partner",
    "people & culture",
    "hrbp",
    "hr business partner",
    "compensation",
    "benefits",
    "c&b",
    "payroll",
    "learning and development",
    "l&d",
    "training",
    "organizational development",
    "employee relations",
    "headhunter",
    "nhân sự",
    "tuyển dụng",
    "tài năng",
    "lương thưởng",
    "phúc lợi",
    "đào tạo",
    "quan hệ lao động",
    "văn hóa doanh nghiệp",
  ],
};

/** Lowercase and strip Vietnamese diacritics so "Nhân sự" matches "nhan su". */
function normalizeText(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase();
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Compile one word-boundary regex per category at module load. */
const CATEGORY_PATTERNS: Record<CategoryId, RegExp> = Object.fromEntries(
  CATEGORY_IDS.map((id) => {
    const alternatives = CATEGORY_TERMS[id]
      .map(normalizeText)
      .filter(Boolean)
      .sort((a, b) => b.length - a.length)
      .map(escapeRegex)
      .join("|");
    return [
      id,
      new RegExp(`(?:^|[^a-z0-9])(?:${alternatives})(?=[^a-z0-9]|$)`),
    ];
  }),
) as Record<CategoryId, RegExp>;

/**
 * Return every category a job belongs to.
 *
 * Title and skills are checked first because they carry the strongest signal.
 * Descriptions are only used when title/skills produced no match, which avoids
 * tagging every engineering job as "marketing" just because the company blurb
 * mentions marketing.
 */
export function getJobCategories(job: Job): Set<CategoryId> {
  const found = new Set<CategoryId>();
  const title = normalizeText(job.title ?? "");
  const skills = normalizeText(
    Array.isArray(job.skills) ? job.skills.join(" ") : String(job.skills ?? ""),
  );

  for (const id of CATEGORY_IDS) {
    if (CATEGORY_PATTERNS[id].test(title) || CATEGORY_PATTERNS[id].test(skills)) {
      found.add(id);
    }
  }

  if (found.size === 0) {
    const description = normalizeText(
      [job.summary_description, job.description].filter(Boolean).join(" "),
    );
    if (description) {
      for (const id of CATEGORY_IDS) {
        if (CATEGORY_PATTERNS[id].test(description)) found.add(id);
      }
    }
  }

  return found;
}

/** True when a job matches the selected category ("all" always matches). */
export function jobMatchesCategory(job: Job, category: CategoryFilter): boolean {
  if (category === "all") return true;
  return getJobCategories(job).has(category);
}

/** Editorial configuration for the Good Jobs news desk (Vietnamese). */

export const NEWS_AUTHOR = "Ban Biên Tập Good Jobs";
export const DEFAULT_ARTICLES_PER_RUN = 2;
export const MAX_ARTICLES_PER_RUN = 3;

/** Category for "share the original article" posts (Điểm tin). */
export const SHARE_CATEGORY = "Điểm tin";

/** Established Vietnamese outlets allowed as the source of a share post (host → display name). */
export const SHARE_PUBLISHERS = {
  "vnexpress.net": "VnExpress", "tuoitre.vn": "Tuổi Trẻ", "thanhnien.vn": "Thanh Niên",
  "dantri.com.vn": "Dân trí", "vietnamnet.vn": "VietNamNet", "laodong.vn": "Lao Động",
  "nld.com.vn": "Người Lao Động", "vneconomy.vn": "VnEconomy", "cafef.vn": "CafeF",
  "cafebiz.vn": "CafeBiz", "znews.vn": "Znews", "vtv.vn": "VTV", "vov.vn": "VOV",
  "htv.vn": "HTV", "tienphong.vn": "Tiền Phong", "plo.vn": "Pháp Luật TP.HCM",
  "baochinhphu.vn": "Báo Chính phủ", "nhandan.vn": "Nhân Dân", "vietnamplus.vn": "VietnamPlus",
  "sggp.org.vn": "Sài Gòn Giải Phóng", "kinhtedothi.vn": "Kinh tế & Đô thị", "genk.vn": "GenK",
  "doanhnhansaigon.vn": "Doanh Nhân Sài Gòn",
  "tinnhanhchungkhoan.vn": "Đầu tư Chứng khoán", "baodautu.vn": "Báo Đầu tư",
  "vnbusiness.vn": "VnBusiness", "ictnews.vn": "ICTnews", "kenh14.vn": "Kenh14",
};

/**
 * Each topic is a recurring editorial angle. `query` is used with Google News
 * RSS (last 7 days); `searchKeyword` powers the "related jobs" CTA.
 */
export const TOPICS = [
  {
    slug: "thi-truong-viec-lam",
    bingQuery: "việc làm tuyển dụng",
    name: "Thị trường việc làm",
    query: '"việc làm" OR "tuyển dụng" OR "lao động"',
    searchKeyword: "tuyển dụng",
    keywords: ["tuyển dụng", "việc làm", "lao động", "nhân sự", "thất nghiệp"],
  },
  {
    slug: "ai-va-nghe-nghiep",
    bingQuery: "AI việc làm",
    name: "AI & nghề nghiệp",
    query: '"trí tuệ nhân tạo" OR "AI" "việc làm"',
    searchKeyword: "AI Engineer",
    keywords: ["ai", "trí tuệ nhân tạo", "tự động hóa", "machine learning", "llm"],
  },
  {
    slug: "ky-nang-nghe-nghiep",
    bingQuery: "kỹ năng nghề nghiệp",
    name: "Kỹ năng & nghề nghiệp",
    query: '"kỹ năng" OR "nghề nghiệp" OR "phát triển sự nghiệp"',
    searchKeyword: "kỹ năng mềm",
    keywords: ["kỹ năng", "nghề nghiệp", "sự nghiệp", "học tập", "đào tạo"],
  },
  {
    slug: "luong-phuc-loi",
    bingQuery: "lương thưởng phúc lợi",
    name: "Lương & phúc lợi",
    query: '"lương" OR "thưởng Tết" OR "phúc lợi"',
    searchKeyword: "lương",
    keywords: ["lương", "thưởng", "phúc lợi", "đãi ngộ", "thu nhập"],
  },
  {
    slug: "cong-nghe-tuyen-dung",
    bingQuery: "tuyển dụng nhân sự công nghệ",
    name: "Công nghệ & tuyển dụng",
    query: '"tuyển dụng" OR "nhân sự" OR "HR tech"',
    searchKeyword: "tuyển dụng",
    keywords: ["công nghệ", "tuyển dụng", "hr", "nhân sự", "ats"],
  },
  {
    slug: "nghe-nghiep-cong-nghe",
    bingQuery: "lập trình viên IT",
    name: "Nghề nghiệp công nghệ",
    query: '"lập trình viên" OR "phần mềm" OR "IT"',
    searchKeyword: "Software Engineer",
    keywords: ["it", "lập trình", "phần mềm", "công nghệ", "developer"],
  },
];

/** Curated RSS feeds that respond from Cloudflare Workers. */
export const CURATED_FEEDS = [
  {
    name: "VietnamNet Công nghệ",
    url: "https://vietnamnet.vn/cong-nghe.rss",
    topics: ["ai-va-nghe-nghiep", "cong-nghe-tuyen-dung", "nghe-nghiep-cong-nghe"],
  },
  {
    name: "VietnamNet Kinh doanh",
    url: "https://vietnamnet.vn/kinh-doanh.rss",
    topics: ["thi-truong-viec-lam", "luong-phuc-loi", "ky-nang-nghe-nghiep"],
  },
  {
    name: "Thanh Niên Kinh tế",
    url: "https://thanhnien.vn/rss/kinh-te.rss",
    topics: ["thi-truong-viec-lam", "luong-phuc-loi", "ky-nang-nghe-nghiep"],
  },
  {
    name: "Thanh Niên Công nghệ",
    url: "https://thanhnien.vn/rss/cong-nghe.rss",
    topics: ["ai-va-nghe-nghiep", "cong-nghe-tuyen-dung", "nghe-nghiep-cong-nghe"],
  },
  {
    name: "Dân trí Kinh doanh",
    url: "https://dantri.com.vn/rss/kinh-doanh.rss",
    topics: ["thi-truong-viec-lam", "luong-phuc-loi"],
  },
  {
    name: "Tuổi Trẻ Kinh doanh",
    url: "https://tuoitre.vn/rss/kinh-doanh.rss",
    topics: ["thi-truong-viec-lam", "luong-phuc-loi"],
  },
  {
    name: "Tuổi Trẻ Nhịp sống số",
    url: "https://tuoitre.vn/rss/nhip-song-so.rss",
    topics: ["ai-va-nghe-nghiep", "cong-nghe-tuyen-dung", "nghe-nghiep-cong-nghe"],
  },
];

/** Bing News RSS is a search fallback when Google News rate-limits Workers. */
export const BING_NEWS_URL = "https://www.bing.com/news/search?format=RSS&setmkt=vi-VN&q=";

/** Every candidate must mention the job market (accent-insensitive match on title + summary). */
export const JOB_MARKET_TERMS = [
  "việc làm", "tuyển dụng", "lao động", "nhân sự", "nhân lực", "tiền lương", "mức lương",
  "tăng lương", "lương tối thiểu", "lương thưởng", "bảng lương", "trả lương",
  "thu nhập", "phúc lợi", "nghề nghiệp", "ứng viên", "thất nghiệp", "tìm việc",
  "tuyển người", "sa thải", "cắt giảm nhân sự", "kỹ năng nghề", "thị trường lao động",
];

/** Hosts that publish listings/spam rather than news. */
export const BLOCKED_HOSTS = ["quasa.io"];

export const DEFAULT_MODELS = [
  "@cf/meta/llama-4-scout-17b-16e-instruct",
  "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
];

/** Pick `count` topics, rotating by day so the desk covers every angle. */
export function pickTopics(dateKey, count = DEFAULT_ARTICLES_PER_RUN) {
  const day = Math.floor(new Date(`${dateKey}T00:00:00Z`).getTime() / 86400000) || 0;
  const start = ((day % TOPICS.length) + TOPICS.length) % TOPICS.length;
  const picked = [];
  for (let i = 0; i < Math.min(count, TOPICS.length); i += 1) {
    picked.push(TOPICS[(start + i) % TOPICS.length]);
  }
  return picked;
}

export function topicBySlug(slug) {
  return TOPICS.find((t) => t.slug === slug) || null;
}

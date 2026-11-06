/**
 * Lightweight in-place EN/VI toggle for the single-page app.
 *
 * There is no separate /vi/ URL — the language is a client-side preference
 * (localStorage) applied to the same page via `data-i18n*` attributes and
 * the `t()` helper used throughout main.ts/ui.ts for dynamic strings.
 *
 * Search keywords (suggestion chips, the keyword textbox value) are
 * intentionally NOT translated: English job titles return more/better
 * matches regardless of UI language (see the "Use English Keywords" slide).
 */

export type Lang = "en" | "vi";

const LANG_KEY = "goodjobs:lang";

type Dict = Record<string, string>;

export const STRINGS: Record<Lang, Dict> = {
  en: {
    "header.subtitle": "Search recent jobs in Vietnam and beyond",
    "header.postJob": "Post",
    "header.news": "News",
    "header.githubAria": "View source on GitHub",
    "header.themeAria": "Toggle dark/light theme",
    "header.themeTitle": "Toggle theme",
    "header.langToggleAria": "Switch to Vietnamese",

    "hero.title": "Job Search for Vietnam and Beyond",
    "hero.desc": "Good Jobs aggregates the latest listings from top Vietnam and global job boards into a single page, refreshed daily so you never miss an opportunity.",
    "hero.badge": "21 sources · one query",

    "searchIntro.badgeNew": "New",
    "searchIntro.badgeSources": "21 sources",
    "searchIntro.title": "Find your next role.",
    "searchIntro.desc": "One search across LinkedIn, ITviec, TopCV, VietnamWorks, Glints and more — refreshed daily.",
    "brand.tagline": "A self-hosted job aggregator across 21 sources: one query, real-time results and recommendations that understand the role.",

    "category.label": "Browse by category",
    "category.all": "All",
    "category.aria": "Filter jobs by category",
    "category.empty": "No jobs in this category right now.",
    "category.ai": "Artificial Intelligence",
    "category.it": "Information Technology",
    "category.data": "Data",
    "category.product": "Product",
    "category.business": "Business",
    "category.design": "Design",
    "category.marketing": "Marketing",
    "category.hr": "Human Resources",

    "homeFilter.aria": "Filter jobs",
    "location.label": "Location",
    "location.all": "All locations",
    "location.hcm": "Ho Chi Minh City",
    "location.hanoi": "Ha Noi",
    "location.danang": "Da Nang",
    "location.remote": "Remote",
    "location.other": "Other",
    "source.label": "Source",
    "source.all": "All sources",

    "chat.launch": "Ask Good Jobs AI",
    "chat.title": "Good Jobs AI",
    "chat.subtitle": "Ask about jobs, skills, and companies",
    "chat.welcome": "Hi! I can help you find jobs from Good Jobs' cache. Ask me about a role, location, or skill.",
    "chat.placeholder": "Ask about jobs…",
    "chat.send": "Send",
    "chat.close": "Close chat",
    "chat.reset": "New chat",
    "chat.thinking": "Searching jobs…",
    "chat.error": "The assistant is busy. Please try again.",
    "chat.rateLimited": "You're asking too fast. Please wait a minute.",
    "chat.suggestions": "Try asking:",
    "chat.viewJob": "View job ↗",
    "chat.note": "AI can make mistakes. Answers are based on cached jobs.",
    "chat.suggestion1": "AI jobs in Ho Chi Minh City",
    "chat.suggestion2": "Marketing jobs",
    "chat.suggestion3": "Remote jobs",
    "chat.suggestion4": "Skills for a Data Engineer",

    "pager.perPage": "Jobs per page",
    "pager.prev": "Prev",
    "pager.next": "Next",
    "pager.pageInfo": "Page {page} of {pages}",
    "pager.showing": "Showing {from}–{to} of {total}",

    "slide1.title": "Multi-Source Search",
    "slide1.desc": "Results from LinkedIn, ITviec, TopCV, VietnamWorks, CareerLink, TopDev, JobsGo, and CareerViet at once.",
    "slide2.title": "Fresh Data",
    "slide2.desc": "Listings refresh automatically every day, showing only posts from the last 7 days.",
    "slide3.title": "Filter by Skill",
    "slide3.desc": "See required skills, job descriptions, and company info directly on the page.",
    "slide4.title": "Job Summaries",
    "slide4.desc": "AI summarizes long job descriptions instantly, so you can grasp the key requirements in seconds.",
    "slide5.title": "Search by CV or Skills",
    "slide5.desc": "Paste your CV or list your skills in the search box — AI analyzes it and finds the best-matching jobs.",
    "slide6.title": "Use English Keywords",
    "slide6.desc": "The IT industry and most multinational companies use English job titles. For example, \"AI Engineer\" or \"Data Scientist\" returns more results than Vietnamese.",
    "slide7.title": "Filter by Level",
    "slide7.desc": "Add \"Senior\", \"Junior\", or \"Intern\" to the job title to narrow results by years of experience.",
    "slide8.title": "Find Remote Jobs (Work from Home)",
    "slide8.desc": "Set the location to \"Remote\" to see positions that don't require office attendance.",
    "slide9.title": "Quick Share",
    "slide9.desc": "Share a link to your search results or to a specific job with friends. When they open the link, Good Jobs reloads the results and opens the right job's popup.",
    "slide10.title": "Match Score",
    "slide10.desc": "The \"Match Score\" column shows how well a job's requirements match your skills (AI-powered). Use it to prioritize applying to the highest-scoring positions.",
    "slide11.title": "Enter Skills/CV",
    "slide11.desc": "Paste your CV or list your skills — AI analyzes it and finds the best-matching jobs. For example: \"2 years of experience with React, Node.js, PostgreSQL. Worked at a fintech startup, familiar with CI/CD and Docker…\"",

    "search.labelKeyword": "Job Title / Skills / CV",
    "search.placeholderKeyword": "Job title, skills, or CV…",
    "search.labelLocation": "Location",
    "search.placeholderLocation": "City or Remote…",
    "search.button": "Search Jobs",
    "search.showMore": "Show more suggestions",
    "search.showFewer": "Show fewer suggestions",

    "results.title": "Results",
    "results.share": "Share Results",
    "filter.label": "Filter:",
    "filter.placeholder": "Keyword in title…",

    "table.num": "#",
    "table.title": "Title",
    "table.company": "Company",
    "table.location": "Location",
    "table.posted": "Posted",
    "table.score": "Score",
    "table.skills": "Skills",
    "table.description": "Description",
    "table.source": "Source",
    "table.apply": "Apply",
    "table.postDate": "Post Date",

    "direct.title": "📌 Posted Directly to Good Jobs",
    "recent.title": "Recent Jobs",
    "recent.loading": "Loading jobs…",

    "about.title": "Good Jobs — Smart Job Aggregation",
    "about.desc": "Good Jobs is an AI-powered job aggregator (Cloudflare Workers AI) that automatically syncs thousands of listings from LinkedIn, ITviec, TopCV, and Vietnam's largest job boards, plus LinkedIn, Indeed, RemoteOK, We Work Remotely, and Glassdoor for global roles. Unlike traditional keyword search, Good Jobs' Semantic Vector Search understands the context of your skills and background, surfacing cross-industry opportunities with a high-precision Matching Score.",

    "modal.close": "Close",
    "modal.shareLink": "Share Link",
    "modal.apply": "Apply ↗",

    "footer.privacy": "Privacy Policy",
    "footer.terms": "Terms of Service",
    "footer.postJob": "Post a Job",
    "footer.news": "News",
    "footer.contact": "Contact",
    "footer.contactEmail": "Contact · khoivn@goodjobs.io.vn",
    "footer.tagline": "Search recent jobs across Vietnam and global job boards, refreshed daily.",
    "footer.explore": "Explore",
    "footer.categories": "Categories",
    "footer.company": "Company",
    "footer.resources": "Resources",
    "footer.home": "Home",
    "footer.recentJobs": "Recent Jobs",
    "footer.about": "About",
    "footer.aiJobs": "AI Jobs",
    "footer.itJobs": "IT Jobs",
    "footer.marketingJobs": "Marketing Jobs",
    "footer.hrJobs": "HR Jobs",
    "footer.github": "GitHub",
    "footer.linkedin": "LinkedIn",
    "footer.email": "Email",
    "footer.githubAria": "View source on GitHub",
    "footer.linkedinAria": "Good Jobs on LinkedIn",
    "footer.emailAria": "Email Good Jobs",
    "footer.copyright": "© 2026 Good Jobs | Automated job search system | Data refreshed daily",
    "footer.disclaimer": "Good Jobs is an independent search tool. We aggregate publicly available job listings to help job seekers find opportunities more efficiently.",

    "status.enterKeyword": "Please enter a job title or paste your skills/CV.",
    "status.analyzing": "Analyzing…",
    "status.noMatch": "No matching jobs found in the past week. Try a different keyword.",
    "status.sharedNotFound": "Couldn't find the job from the shared link (it may have expired).",
    "status.foundWeek": "Found {count} jobs from the past week.",
    "status.foundProfile": "Found {count} jobs matching your profile — loading descriptions…",
    "status.foundLoading": "Found {count} jobs — loading descriptions…",
    "status.serverBusyFallback": "Server is busy — trying a direct LinkedIn search…",
    "status.linkedinOnly": "Server busy — showing only {count} LinkedIn results.",
    "status.serverBusy": "Server is busy. Please try again later.",
    "status.rateLimited": "You've searched too many times. Please wait 1 minute and try again.",
    "status.errorPrefix": "Error: {msg}",
    "stats.jobsSuffix": "jobs · 14 days",

    "toast.question": "Have you applied yet?",
    "toast.yes": "✓ Applied",
    "toast.no": "✕ Not yet",
    "copy.copied": "Copied",
    "copy.failed": "Couldn't share",
    "apply.applied": "✓ Applied",
    "apply.cta": "Apply ↗",
    "apply.titleUnmark": "You've marked this as applied. Click to unmark.",
    "apply.titleApply": "Apply to this position",
    "empty.noMatch": "No jobs match the selected filters.",
    "results.countAll": "{total} results",
    "results.countFiltered": "{shown} of {total} results",
    "modal.findingSkills": "Finding skills…",
    "modal.findingDesc": "Finding description…",
    "modal.loadingFullDesc": "Loading full description…",
    "table.loadingDesc": "Loading description…",
    "intent.notJob": "This doesn't look like a job title — did you mean:",
    "intent.detectedPrefix": "AI detected:",
    "intent.resumeSkills": "Resume / skills",
    "intent.jobTitle": "Job title",
    "intent.searchingWith": "→ searching with keyword",
    "intent.suggestions": "Suggestions:",
    "queue.waiting": "⏳ Server is busy — you're at position #{position} in the queue. Please wait…",
    "suggestion.didYouMean": "Did you mean: ",
    "progress.searching": "Searching…",
    "progress.foundBreakdown": "Found {count} — {breakdown}",
    "progress.siteLoading": "{label} loading description…",
    "progress.siteLoadingTitle": "Loading description for {count} jobs",
  },
  vi: {
    "header.subtitle": "Tìm việc mới tại Việt Nam và nhiều nơi khác",
    "header.postJob": "Đăng tin",
    "header.news": "Tin tức",
    "header.githubAria": "Xem mã nguồn trên GitHub",
    "header.themeAria": "Chuyển giao diện sáng/tối",
    "header.themeTitle": "Đổi giao diện",
    "header.langToggleAria": "Chuyển sang tiếng Anh",

    "hero.title": "Tìm Việc Tại Việt Nam Và Nhiều Nơi Khác",
    "hero.desc": "Good Jobs tổng hợp tin tuyển dụng mới nhất từ các trang việc làm lớn tại Việt Nam và quốc tế vào một trang duy nhất, cập nhật hàng ngày để bạn không bỏ lỡ cơ hội nào.",
    "hero.badge": "21 nguồn · một lần tìm",

    "searchIntro.badgeNew": "Mới",
    "searchIntro.badgeSources": "21 nguồn",
    "searchIntro.title": "Tìm vai trò tiếp theo của bạn.",
    "searchIntro.desc": "Một lần tìm trên LinkedIn, ITviec, TopCV, VietnamWorks, Glints và nhiều nguồn khác — cập nhật mỗi ngày.",
    "brand.tagline": "Công cụ tổng hợp việc làm tự vận hành từ 21 nguồn: một lần tìm, kết quả thời gian thực và gợi ý hiểu đúng vai trò.",

    "category.label": "Lọc theo ngành",
    "category.all": "Tất cả",
    "category.aria": "Lọc việc làm theo ngành",
    "category.empty": "Hiện chưa có việc làm trong ngành này.",
    "category.ai": "Trí tuệ nhân tạo",
    "category.it": "Công nghệ thông tin",
    "category.data": "Dữ liệu",
    "category.product": "Sản phẩm",
    "category.business": "Kinh doanh",
    "category.design": "Thiết kế",
    "category.marketing": "Tiếp thị",
    "category.hr": "Nhân sự",

    "homeFilter.aria": "Lọc việc làm",
    "location.label": "Địa điểm",
    "location.all": "Tất cả địa điểm",
    "location.hcm": "TP. Hồ Chí Minh",
    "location.hanoi": "Hà Nội",
    "location.danang": "Đà Nẵng",
    "location.remote": "Remote",
    "location.other": "Khác",
    "source.label": "Nguồn",
    "source.all": "Tất cả nguồn",

    "chat.launch": "Hỏi Good Jobs AI",
    "chat.title": "Good Jobs AI",
    "chat.subtitle": "Hỏi về việc làm, kỹ năng và công ty",
    "chat.welcome": "Xin chào! Mình có thể giúp bạn tìm việc từ dữ liệu của Good Jobs. Hỏi về vị trí, địa điểm hoặc kỹ năng nhé.",
    "chat.placeholder": "Hỏi về việc làm…",
    "chat.send": "Gửi",
    "chat.close": "Đóng khung chat",
    "chat.reset": "Cuộc trò chuyện mới",
    "chat.thinking": "Đang tìm việc…",
    "chat.error": "Trợ lý đang bận. Vui lòng thử lại.",
    "chat.rateLimited": "Bạn hỏi hơi nhanh. Vui lòng đợi một phút.",
    "chat.suggestions": "Thử hỏi:",
    "chat.viewJob": "Xem việc ↗",
    "chat.note": "AI có thể nhầm lẫn. Câu trả lời dựa trên dữ liệu việc làm đã lưu.",
    "chat.suggestion1": "Việc AI ở Hồ Chí Minh",
    "chat.suggestion2": "Việc Marketing",
    "chat.suggestion3": "Việc Remote",
    "chat.suggestion4": "Kỹ năng cho Data Engineer",

    "pager.perPage": "Số việc mỗi trang",
    "pager.prev": "Trước",
    "pager.next": "Sau",
    "pager.pageInfo": "Trang {page}/{pages}",
    "pager.showing": "Hiển thị {from}–{to} trên {total}",

    "slide1.title": "Tìm Kiếm Đa Nguồn",
    "slide1.desc": "Kết quả từ LinkedIn, ITviec, TopCV, VietnamWorks, CareerLink, TopDev, JobsGo và CareerViet trong một lần tìm.",
    "slide2.title": "Dữ Liệu Mới",
    "slide2.desc": "Tin tuyển dụng tự động làm mới mỗi ngày, chỉ hiển thị tin trong 7 ngày gần nhất.",
    "slide3.title": "Lọc Theo Kỹ Năng",
    "slide3.desc": "Xem kỹ năng yêu cầu, mô tả công việc và thông tin công ty ngay trên trang.",
    "slide4.title": "Tóm Tắt Công Việc",
    "slide4.desc": "AI tóm tắt mô tả công việc dài ngay lập tức, giúp bạn nắm yêu cầu chính trong vài giây.",
    "slide5.title": "Tìm Kiếm Bằng CV Hoặc Kỹ Năng",
    "slide5.desc": "Dán CV hoặc liệt kê kỹ năng vào ô tìm kiếm — AI sẽ phân tích và tìm công việc phù hợp nhất.",
    "slide6.title": "Dùng Từ Khóa Tiếng Anh",
    "slide6.desc": "Ngành IT và hầu hết công ty đa quốc gia dùng chức danh tiếng Anh. Ví dụ: \"AI Engineer\" hoặc \"Data Scientist\" sẽ cho nhiều kết quả hơn so với tên tiếng Việt.",
    "slide7.title": "Lọc Theo Cấp Bậc",
    "slide7.desc": "Thêm \"Senior\", \"Junior\" hoặc \"Intern\" vào chức danh để thu hẹp kết quả theo số năm kinh nghiệm.",
    "slide8.title": "Tìm Việc Remote (Làm Từ Xa)",
    "slide8.desc": "Chọn địa điểm \"Remote\" để xem các vị trí không cần lên văn phòng.",
    "slide9.title": "Chia Sẻ Nhanh",
    "slide9.desc": "Chia sẻ link kết quả tìm kiếm hoặc một công việc cụ thể với bạn bè. Khi họ mở link, Good Jobs sẽ tải lại kết quả và mở đúng popup công việc đó.",
    "slide10.title": "Điểm Phù Hợp",
    "slide10.desc": "Cột \"Điểm Phù Hợp\" cho biết yêu cầu công việc khớp với kỹ năng của bạn đến đâu (dùng AI). Hãy ưu tiên ứng tuyển các vị trí có điểm cao nhất.",
    "slide11.title": "Nhập Kỹ Năng/CV",
    "slide11.desc": "Dán CV hoặc liệt kê kỹ năng — AI sẽ phân tích và tìm công việc phù hợp nhất. Ví dụ: \"2 năm kinh nghiệm với React, Node.js, PostgreSQL. Từng làm tại một startup fintech, quen với CI/CD và Docker…\"",

    "search.labelKeyword": "Chức Danh / Kỹ Năng / CV",
    "search.placeholderKeyword": "Nhập chức danh, kỹ năng hoặc dán CV…",
    "search.labelLocation": "Địa Điểm",
    "search.placeholderLocation": "Nhập tên thành phố, hoặc \"Remote\" (ví dụ: Ho Chi Minh City, London, Remote)",
    "search.button": "Tìm Việc",
    "search.showMore": "Hiện thêm gợi ý",
    "search.showFewer": "Thu gọn gợi ý",

    "results.title": "Kết Quả",
    "results.share": "Chia Sẻ Kết Quả",
    "filter.label": "Lọc:",
    "filter.placeholder": "Từ khóa trong tiêu đề…",

    "table.num": "#",
    "table.title": "Tiêu Đề",
    "table.company": "Công Ty",
    "table.location": "Địa Điểm",
    "table.posted": "Đăng",
    "table.score": "Điểm",
    "table.skills": "Kỹ Năng",
    "table.description": "Mô Tả",
    "table.source": "Nguồn",
    "table.apply": "Ứng Tuyển",
    "table.postDate": "Ngày Đăng",

    "direct.title": "📌 Đăng Trực Tiếp Trên Good Jobs",
    "recent.title": "Việc Làm Gần Đây",
    "recent.loading": "Đang tải việc làm…",

    "about.title": "Good Jobs — Tổng Hợp Việc Làm Thông Minh",
    "about.desc": "Good Jobs là công cụ tổng hợp việc làm dùng AI (Cloudflare Workers AI), tự động đồng bộ hàng nghìn tin tuyển dụng từ LinkedIn, ITviec, TopCV và các trang việc làm lớn nhất tại Việt Nam, cùng LinkedIn, Indeed, RemoteOK, We Work Remotely và Glassdoor cho các vị trí quốc tế. Khác với tìm kiếm từ khóa thông thường, Semantic Vector Search của Good Jobs hiểu bối cảnh kỹ năng và kinh nghiệm của bạn, giúp tìm ra cơ hội liên ngành với Điểm Phù Hợp có độ chính xác cao.",

    "modal.close": "Đóng",
    "modal.shareLink": "Chia Sẻ Link",
    "modal.apply": "Ứng Tuyển ↗",

    "footer.privacy": "Chính Sách Bảo Mật",
    "footer.terms": "Điều Khoản Dịch Vụ",
    "footer.postJob": "Đăng Tin Tuyển Dụng",
    "footer.news": "Tin tức",
    "footer.contact": "Liên Hệ",
    "footer.contactEmail": "Liên hệ · khoivn@goodjobs.io.vn",
    "footer.tagline": "Tìm việc mới từ các trang tuyển dụng Việt Nam và quốc tế, cập nhật mỗi ngày.",
    "footer.explore": "Khám phá",
    "footer.categories": "Ngành nghề",
    "footer.company": "Công ty",
    "footer.resources": "Tài nguyên",
    "footer.home": "Trang chủ",
    "footer.recentJobs": "Việc làm gần đây",
    "footer.about": "Giới thiệu",
    "footer.aiJobs": "Việc AI",
    "footer.itJobs": "Việc IT",
    "footer.marketingJobs": "Việc Marketing",
    "footer.hrJobs": "Việc HR",
    "footer.github": "GitHub",
    "footer.linkedin": "LinkedIn",
    "footer.email": "Email",
    "footer.githubAria": "Xem mã nguồn trên GitHub",
    "footer.linkedinAria": "Good Jobs trên LinkedIn",
    "footer.emailAria": "Gửi email cho Good Jobs",
    "footer.copyright": "© 2026 Good Jobs | Hệ thống tìm việc tự động | Dữ liệu cập nhật hàng ngày",
    "footer.disclaimer": "Good Jobs là công cụ tìm kiếm độc lập. Chúng tôi tổng hợp các tin tuyển dụng công khai để giúp người tìm việc tiếp cận cơ hội hiệu quả hơn.",

    "status.enterKeyword": "Vui lòng nhập chức danh hoặc dán kỹ năng/CV của bạn.",
    "status.analyzing": "Đang phân tích…",
    "status.noMatch": "Không tìm thấy công việc phù hợp trong tuần qua. Hãy thử từ khóa khác.",
    "status.sharedNotFound": "Không tìm thấy công việc từ link chia sẻ (có thể đã hết hạn).",
    "status.foundWeek": "Tìm thấy {count} việc làm trong tuần qua.",
    "status.foundProfile": "Tìm thấy {count} việc làm phù hợp với hồ sơ của bạn — đang tải mô tả…",
    "status.foundLoading": "Tìm thấy {count} việc làm — đang tải mô tả…",
    "status.serverBusyFallback": "Hệ thống đang bận — đang thử tìm trực tiếp trên LinkedIn…",
    "status.linkedinOnly": "Hệ thống đang bận — chỉ hiển thị {count} kết quả từ LinkedIn.",
    "status.serverBusy": "Hệ thống đang bận. Vui lòng thử lại sau.",
    "status.rateLimited": "Bạn đã tìm kiếm quá nhiều lần. Vui lòng đợi 1 phút rồi thử lại.",
    "status.errorPrefix": "Lỗi: {msg}",
    "stats.jobsSuffix": "việc · 14 ngày",

    "toast.question": "Bạn đã ứng tuyển chưa?",
    "toast.yes": "✓ Đã ứng tuyển",
    "toast.no": "✕ Chưa",
    "copy.copied": "Đã sao chép",
    "copy.failed": "Không thể chia sẻ",
    "apply.applied": "✓ Đã ứng tuyển",
    "apply.cta": "Ứng Tuyển ↗",
    "apply.titleUnmark": "Bạn đã đánh dấu ứng tuyển. Bấm để bỏ đánh dấu.",
    "apply.titleApply": "Ứng tuyển vị trí này",
    "empty.noMatch": "Không có việc làm phù hợp với bộ lọc đã chọn.",
    "results.countAll": "{total} kết quả",
    "results.countFiltered": "{shown}/{total} kết quả",
    "modal.findingSkills": "Đang tìm kỹ năng…",
    "modal.findingDesc": "Đang tìm mô tả…",
    "modal.loadingFullDesc": "Đang tải mô tả đầy đủ…",
    "table.loadingDesc": "Đang tải mô tả…",
    "intent.notJob": "Có vẻ đây không phải chức danh — có phải bạn muốn tìm:",
    "intent.detectedPrefix": "AI phát hiện:",
    "intent.resumeSkills": "CV / kỹ năng",
    "intent.jobTitle": "Chức danh",
    "intent.searchingWith": "→ đang tìm với từ khóa",
    "intent.suggestions": "Gợi ý:",
    "queue.waiting": "⏳ Hệ thống đang bận — bạn đang ở vị trí #{position} trong hàng chờ. Vui lòng đợi…",
    "suggestion.didYouMean": "Có phải ý bạn là: ",
    "progress.searching": "Đang tìm…",
    "progress.foundBreakdown": "Tìm thấy {count} — {breakdown}",
    "progress.siteLoading": "{label} đang tải mô tả…",
    "progress.siteLoadingTitle": "Đang tải mô tả cho {count} việc làm",
  },
};

let _lang: Lang = "en";

/** Read the stored language preference (default "en") and apply `lang` to <html>. */
export function initLang(): Lang {
  try {
    const stored = localStorage.getItem(LANG_KEY);
    if (stored === "vi" || stored === "en") _lang = stored;
  } catch { /* private browsing — fall back to default */ }
  document.documentElement.lang = _lang;
  return _lang;
}

export function getLang(): Lang {
  return _lang;
}

export function setLang(lang: Lang): void {
  _lang = lang;
  try { localStorage.setItem(LANG_KEY, lang); } catch { /* ignore */ }
  document.documentElement.lang = lang;
}

/** Translate `key`, substituting `{name}` placeholders from `vars`. */
export function t(key: string, vars?: Record<string, string | number>): string {
  let str = STRINGS[_lang][key] ?? STRINGS.en[key] ?? key;
  if (vars) {
    for (const [k, v] of Object.entries(vars)) {
      str = str.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
    }
  }
  return str;
}

/** Apply translations to every element carrying a data-i18n* attribute. */
export function applyStaticTranslations(root: ParentNode = document): void {
  root.querySelectorAll<HTMLElement>("[data-i18n]").forEach(el => {
    el.textContent = t(el.dataset.i18n!);
  });
  root.querySelectorAll<HTMLElement>("[data-i18n-placeholder]").forEach(el => {
    (el as HTMLInputElement | HTMLTextAreaElement).placeholder = t(el.dataset.i18nPlaceholder!);
  });
  root.querySelectorAll<HTMLElement>("[data-i18n-title]").forEach(el => {
    el.title = t(el.dataset.i18nTitle!);
  });
  root.querySelectorAll<HTMLElement>("[data-i18n-aria-label]").forEach(el => {
    el.setAttribute("aria-label", t(el.dataset.i18nAriaLabel!));
  });
}

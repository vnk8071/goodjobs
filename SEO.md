# SEO — goodjobs.io.vn

Audit and implementation date: **2026-11-04**  
Target: <https://goodjobs.io.vn/> · Repo: <https://github.com/vnk8071/goodjobs>

## Result

| Check | Before | After |
| --- | --- | --- |
| Lighthouse SEO (mobile, lab) | 100 | **100** |
| Lighthouse Accessibility | 94 | **100** |
| Lighthouse Performance | 98 | **96–98** |
| Lighthouse Best Practices | 73 | **77** |
| LCP / CLS / TBT | 1.3 s / 0.051 / 0 ms | **1.6 s / 0.051 / 10 ms** |
| Canonical | missing | present on all indexable pages |
| Sitemap | 404 | **valid, 6 URLs** |
| llms.txt | 404 | **published** |
| Structured data | Organization only | Organization, WebSite + SearchAction, WebPage, ContactPage, BreadcrumbList, CollectionPage + ItemList |
| Social preview image | SVG (not supported by many platforms) | **1200×630 JPG** |
| Indexable pages | 5 HTML pages (admin public) | **6 indexable pages + admin noindex** |

Lighthouse Best Practices is capped by Google AdSense third-party cookies and the ad iframe; that is a consent/revenue decision, not a markup defect. Console errors from the old Clearbit logo lookup were fixed.

## Vietnamese version (single page, client-side toggle)

- **2026-11-05:** removed the entire `/jobs/` directory (static jobs directory page, per-category and per-location sub-pages, per-job detail pages) and the separate `/vi/` + `/vi/viec-lam/` Vietnamese pages. There is now exactly one indexable app page, `/`.
- The header has an `EN`/`VI` toggle (`frontend/src/i18n.ts`) that swaps every static and dynamic UI string in place via `localStorage` — no navigation, no second URL, no `hreflang` pair to maintain.
- Search keywords (suggestion chips, the keyword box) are intentionally never translated — English job titles return more/better matches regardless of UI language.
- The consent banner is localised; GTM and AdSense only load after consent.

**Indexing status (checked 2026-11-04):** Google already knows the domain (`site:goodjobs.io.vn` returns the homepage, terms and contact), so this is a ranking problem, not an indexing block. The old `/blog/` URLs visible in Google's index now return 404; either restore that content or let Google drop them.

## What was implemented

### Technical
- `robots.txt` now declares `Sitemap: https://goodjobs.io.vn/sitemap.xml` and disallows `/admin/`. Cloudflare's managed AI-crawler policy is still prepended by the edge; the origin file is appended.
- `sitemap.xml` with home, `/post-job/`, `/contact/`, `/privacy/`, `/terms/` (admin excluded).
- `llms.txt` with a one-paragraph summary, page index, source list and contact.
- Self-canonicals and `meta robots` on every page; `/admin/` is `noindex,nofollow`.
- `og-image.jpg` (1200×630) and a complete Open Graph/Twitter card set (`og:image:width/height/alt`).
- Replaced `logo.clearbit.com` company logos with Google's favicon service and added `width`/`height`/`loading="lazy"` to logo images, removing a recurring console/network error.

### On-page
- Unique, descriptive titles and 120–155-character descriptions on all public pages.
- One page-level H1 per page. The four legal/utility pages previously used the site name as their H1; the header is now non-heading text and the page title is the H1.
- Expanded thin pages: `/contact/` now explains support topics and response times; `/post-job/` explains the review process, what to include and why post.
- Consistent contact email (`khoivn@goodjobs.io.vn`).

### Structured data
- Home: `Organization`, `WebSite` with `SearchAction` (`?kw={search_term_string}`), `WebPage`.
- `/contact/`: `ContactPage`.
- Other pages: `WebPage` + `BreadcrumbList`; all graph nodes share the same `Organization`/`WebSite` IDs.

### Accessibility / UX
- Added a `<main>` landmark on every page.
- Removed `aria-label`s that did not contain the visible link text (`Post a Job`, homepage link).
- Darkened the muted label/table-header grey (#999 → #5f6368) and the primary blue (#3b82f6 → #2563eb), and removed footer text opacity — contrast is now 100.
- Fixed the app at 390 px and 1440 px: no horizontal scroll.

## Remaining recommendations

1. **Submit the sitemap** in Google Search Console and Bing Webmaster Tools. This cannot be done from the repo.
2. **Best Practices / consent:** AdSense sets third-party cookies before consent. If consent is required in your markets, add a CMP and gate the ad script; that should lift Best Practices toward 100 without changing the content.
3. **Speed Index** varies 3.9–4.5 s because of GTM/AdSense; consider loading ads after the first interaction or below the fold. LCP is already 1.3–1.6 s.
4. **Search Console monitoring:** track index coverage for `/`; queries around "jobs in Vietnam", "tìm việc làm ở Hồ Chí Minh", "remote jobs Vietnam"; compare with the pages that are actually indexed. Since the Vietnamese UI is now a client-side toggle on the same URL, Google will index the English-default render — there is no separate Vietnamese URL to rank for Vietnamese-language queries.

## Verification

- `curl -I https://goodjobs.io.vn/sitemap.xml` → 200 `application/xml`
- `curl -I https://goodjobs.io.vn/llms.txt` → 200 `text/plain`
- `curl -I https://goodjobs.io.vn/og-image.jpg` → 200 `image/jpeg`
- `curl https://goodjobs.io.vn/robots.txt` → contains the origin `Sitemap:` line after the Cloudflare-managed block
- Lighthouse mobile: Performance 96, Accessibility 100, SEO 100, Best Practices 77
- Live crawl: one H1 on every public page, canonical on every page, `/admin/` noindex, no horizontal overflow at 390 px/1440 px

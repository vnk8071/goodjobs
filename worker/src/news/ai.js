/** Workers AI prompt + response normalisation for the news desk. */

import { DEFAULT_MODELS } from "./config.js";
import { formatNumber, parseJsonLoose } from "./util.js";

export const SYSTEM_PROMPT = `Bạn là biên tập viên phân tích thị trường việc làm của Good Jobs (https://goodjobs.io.vn), viết cho người tìm việc và nhà tuyển dụng Việt Nam.

QUY TẮC BẮT BUỘC:
1. Viết bằng tiếng Việt tự nhiên, giọng phân tích, điềm tĩnh; không giật tít, không cảm thán, không hứa hẹn.
2. Chỉ dùng số liệu có trong phần "DỮ LIỆU GOOD JOBS". Không bịa số liệu, mức lương, tên công ty hay trích dẫn mới.
3. Không sao chép câu chữ từ nguồn. Chỉ tổng hợp ý và bình luận; khi nhắc một tin, ghi tên nguồn trong ngoặc, ví dụ (VnExpress).
4. Không nhắc rằng bạn là AI hay mô hình ngôn ngữ. Không dùng cụm "trong bài viết này".
5. Bài ngắn để đọc lướt: 350-500 từ, 3-4 mục H2, mỗi mục 2 đoạn 60-90 từ. Không viết dài dòng.
6. Mục đầu tiên phải phân tích dữ liệu Good Jobs với số liệu cụ thể.
7. Có khuyến nghị thực tế cho người tìm việc và/hoặc nhà tuyển dụng.
8. Trả về DUY NHẤT một JSON hợp lệ. Không markdown, không giải thích ngoài JSON.
9. Tiêu đề cụ thể, dài 40-90 ký tự, nêu góc phân tích hoặc con số; không dùng tiêu đề chung chung như "Thị trường việc làm tăng trưởng".
10. JSON sẽ dài khoảng 3.000-5.000 ký tự; viết cô đọng, mỗi câu có thông tin.`;

function signalsBlock(signals) {
  const lines = [];
  lines.push(`- Tổng việc làm đang lưu: ${formatNumber(signals.total)}`);
  lines.push(`- Việc làm mới trong ${signals.windowDays} ngày: ${formatNumber(signals.recent)}`);
  lines.push(`- ${signals.windowDays} ngày trước đó: ${formatNumber(signals.previous)}`);
  if (typeof signals.deltaPct === "number") {
    lines.push(`- Thay đổi so với tuần trước: ${signals.deltaPct >= 0 ? "+" : ""}${signals.deltaPct}%`);
  }
  if (signals.topSkills?.length) {
    lines.push(`- Kỹ năng được yêu cầu nhiều: ${signals.topSkills.map((s) => `${s.skill} (${s.count})`).join(", ")}`);
  }
  if (signals.categories?.length) {
    lines.push(`- Nhóm nghề nổi bật: ${signals.categories.map((c) => `${c.name} (${c.count})`).join(", ")}`);
  }
  if (signals.topLocations?.length) {
    lines.push(`- Địa điểm: ${signals.topLocations.map((l) => `${l.location} (${l.count})`).join(", ")}`);
  }
  if (signals.topCompanies?.length) {
    lines.push(`- Công ty tuyển nhiều: ${signals.topCompanies.map((c) => `${c.company} (${c.count})`).join(", ")}`);
  }
  if (signals.topSources?.length) {
    lines.push(`- Nguồn dữ liệu: ${signals.topSources.map((s) => `${s.source} (${s.count})`).join(", ")}`);
  }
  return lines.join("\n");
}

function candidatesBlock(candidates) {
  return candidates
    .map((c, i) => {
      const date = c.publishedAt ? c.publishedAt.slice(0, 10) : "";
      const summary = c.summary ? ` — ${c.summary.slice(0, 180)}` : "";
      return `[${i + 1}] ${c.title} — ${c.publisher} — ${date}${summary}`;
    })
    .join("\n");
}

export function buildPrompt(topic, signals, candidates, feedback = "") {
  return `CHỦ ĐỀ: ${topic.name}
TỪ KHÓA TÌM VIỆC GỢI Ý: ${topic.searchKeyword}
NGÀY: ${new Date().toISOString().slice(0, 10)}

DỮ LIỆU GOOD JOBS (chỉ được dùng các số liệu này):
${signalsBlock(signals)}

TIN THAM KHẢO (chỉ để tham khảo, KHÔNG chép câu chữ; nguồn trong ngoặc):
${candidatesBlock(candidates)}

SCHEMA JSON BẮT BUỘC:
{
  "title": "tiêu đề 40-90 ký tự, cụ thể, có số liệu hoặc góc phân tích",
  "dek": "sapo 100-220 ký tự, nêu điểm chính",
  "meta_description": "120-158 ký tự cho SEO",
  "tags": ["3-6 thẻ ngắn"],
  "sections": [
    { "heading": "tiêu đề mục", "paragraphs": ["đoạn 1", "đoạn 2"] }
  ],
  "takeaways": ["3-5 ý chính, mỗi ý 1 câu"],
  "sources": [1, 2],
  "search_keyword": "từ khóa tìm việc phù hợp trên Good Jobs",
  "linkedin_text": "500-900 ký tự: 1 câu quan sát từ dữ liệu, 3 bullet, kết bằng 'Chi tiết:', tối đa 5 hashtag"
}

YÊU CẦU THÊM:
- sources: chọn 2-4 số của những tin thực sự được nhắc trong bài; không chọn tin không dùng.
- Phần đầu bài phải có số liệu cụ thể từ DỮ LIỆU GOOD JOBS.
- Độ dài bắt buộc: 350-500 từ. Mỗi mục 2 đoạn, mỗi đoạn 60-90 từ.
- Viết ngắn, câu rõ, ưu tiên số liệu và ý chính; không lan man.
- Không thêm URL vào linkedin_text.
- Không dùng markdown trong bất kỳ trường nào.
${feedback ? `\nLẦN TRƯỚC CHƯA ĐẠT, HÃY SỬA:\n${feedback}\n` : ""}`;
}

function extractText(result) {
  if (!result) return "";
  if (typeof result === "string") return result;
  if (typeof result.response === "string") return result.response;
  if (typeof result.result?.response === "string") return result.result.response;
  if (typeof result.choices?.[0]?.message?.content === "string") return result.choices[0].message.content;
  if (typeof result.choices?.[0]?.text === "string") return result.choices[0].text;
  return "";
}

function asString(value, max = 4000) {
  return String(value ?? "").replace(/\s+/g, " ").trim().slice(0, max);
}

function normalizeDraft(raw) {
  const sections = Array.isArray(raw.sections)
    ? raw.sections
        .map((section) => ({
          heading: asString(section?.heading, 160),
          paragraphs: Array.isArray(section?.paragraphs)
            ? section.paragraphs.map((p) => asString(p, 4000)).filter(Boolean)
            : [],
        }))
        .filter((section) => section.heading && section.paragraphs.length > 0)
    : [];

  return {
    title: asString(raw.title, 140),
    dek: asString(raw.dek, 300),
    metaDescription: asString(raw.meta_description, 200),
    tags: Array.isArray(raw.tags) ? raw.tags.map((t) => asString(t, 40)).filter(Boolean).slice(0, 6) : [],
    sections,
    takeaways: Array.isArray(raw.takeaways) ? raw.takeaways.map((t) => asString(t, 220)).filter(Boolean).slice(0, 5) : [],
    sources: Array.isArray(raw.sources)
      ? raw.sources.map((n) => Number(n)).filter((n) => Number.isInteger(n) && n > 0).slice(0, 5)
      : [],
    searchKeyword: asString(raw.search_keyword, 60),
    linkedinText: String(raw.linkedin_text ?? "").trim().slice(0, 2800),
  };
}

/**
 * Ask Workers AI for one article. Tries the configured model first, then the
 * fallback model, with one repair attempt per model.
 */
export async function generateDraft(env, { topic, signals, candidates, feedback = "" }) {
  const models = env.NEWS_MODEL
    ? [env.NEWS_MODEL, ...DEFAULT_MODELS.filter((m) => m !== env.NEWS_MODEL)]
    : DEFAULT_MODELS;

  const attempts = [];
  for (const model of models) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const result = await env.AI.run(model, {
          messages: [
            { role: "system", content: SYSTEM_PROMPT },
            {
              role: "user",
              content: buildPrompt(
                topic,
                signals,
                candidates,
                attempt === 0 ? feedback : `${feedback}\nTrả về JSON hợp lệ, không thêm chữ nào ngoài JSON.`,
              ),
            },
          ],
          max_tokens: 3000,
          temperature: attempt === 0 ? 0.55 : 0.3,
          response_format: { type: "json_object" },
        });
        const text = extractText(result);
        const finish = result?.choices?.[0]?.finish_reason || result?.result?.choices?.[0]?.finish_reason || "";
        const parsed = parseJsonLoose(text);
        if (parsed) return { draft: normalizeDraft(parsed), model, attempts };
        attempts.push(`${model}${finish === "length" ? " (truncated)" : ""}: invalid JSON ${text.slice(0, 120)}`);
        if (finish === "length") {
          feedback = `${feedback}\nBài trước bị cắt vì quá dài. Viết khoảng 650-800 từ, JSON phải đóng đầy đủ.`;
        }
      } catch (error) {
        attempts.push(`${model}: ${String(error?.message || error).slice(0, 160)}`);
      }
    }
  }
  throw new Error(`AI generation failed: ${attempts.slice(-4).join(" | ")}`);
}

/**
 * Expansion pass: keep the structure/sources but lengthen each section.
 * More reliable than asking the model to start over when the draft is short.
 */
export async function expandDraft(env, { draft, feedback = "" }) {
  const models = env.NEWS_MODEL
    ? [env.NEWS_MODEL, ...DEFAULT_MODELS.filter((m) => m !== env.NEWS_MODEL)]
    : DEFAULT_MODELS;

  const current = JSON.stringify({
    title: draft.title,
    dek: draft.dek,
    meta_description: draft.metaDescription,
    tags: draft.tags,
    sections: draft.sections,
    takeaways: draft.takeaways,
    sources: draft.sources,
    search_keyword: draft.searchKeyword,
    linkedin_text: draft.linkedinText,
  });

  const prompt = `Mở rộng bài viết dưới đây thành 350-500 từ tiếng Việt (bản tin ngắn, dễ đọc lướt).
QUY TẮC:
- Giữ nguyên tiêu đề, sapo, nguồn và mọi số liệu.
- Giữ đúng 3-4 mục H2 hiện có, không thêm mục mới.
- Mỗi mục có 2 đoạn, mỗi đoạn 60-90 từ.
- Meta description dài 120-158 ký tự.
- Trả về DUY NHẤT JSON đúng schema như bài hiện tại, không markdown, không giải thích ngoài JSON.
${feedback ? `- Sửa thêm: ${feedback}` : ""}

BÀI HIỆN TẠI:
${current}`;

  const attempts = [];
  for (const model of models) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const result = await env.AI.run(model, {
          messages: [
            { role: "system", content: SYSTEM_PROMPT },
            { role: "user", content: attempt === 0 ? prompt : `${prompt}\nChỉ trả JSON hợp lệ.` },
          ],
          max_tokens: 3000,
          temperature: attempt === 0 ? 0.5 : 0.3,
          response_format: { type: "json_object" },
        });
        const text = extractText(result);
        const parsed = parseJsonLoose(text);
        if (parsed) return { draft: normalizeDraft(parsed), model, attempts };
        attempts.push(`${model}: invalid JSON ${text.slice(0, 100)}`);
      } catch (error) {
        attempts.push(`${model}: ${String(error?.message || error).slice(0, 140)}`);
      }
    }
  }
  throw new Error(`AI expansion failed: ${attempts.slice(-3).join(" | ")}`);
}

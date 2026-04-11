# src/summarizer.py
import json
import time

import requests

from src.constants import (
    CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_ACCOUNT_IDS,
    CLOUDFLARE_API_BASE,
    CLOUDFLARE_API_TOKEN,
    CLOUDFLARE_API_TOKENS,
    CLOUDFLARE_MODEL,
    SUMMARIZER_MAX_LENGTH,
    SUMMARIZER_MIN_LENGTH,
)
from src.logger import log_app


# Shared singleton instance (lazy initialized)
_summarizer_instance: "SummarizerService | None" = None


def get_summarizer() -> "SummarizerService":
    """Get or create the shared SummarizerService singleton."""
    global _summarizer_instance
    if _summarizer_instance is None:
        _summarizer_instance = SummarizerService()
    return _summarizer_instance


class SummarizerService:
    """Job description summarization via Cloudflare AI batch API."""

    _MAX_INPUT_CHARS = 2000
    _RETRY_DELAY = 1.0
    _POLL_TIMEOUT = 300  # 5 minutes max wait for batch results
    _POLL_INTERVAL = 30  # seconds between polls

    # Model generation config — lower values = more concise/deterministic
    _TEMPERATURE = 0.1
    _TOP_P = 0.3
    _TOP_K = 10

    # Combined prompt: produces both Vietnamese summary and English skill list in one call.
    # Output format: JSON object {"summary": "...", "skills": ["..."]}
    _SYSTEM_PROMPT = (
        "Bạn là chuyên gia phân tích tin tuyển dụng. Với mỗi tin, hãy trả về JSON object gồm 2 trường:\n"
        '1. "summary": tóm tắt bằng TIẾNG VIỆT, CHỈ tập trung vào YÊU CẦU ứng viên (kinh nghiệm, kỹ năng, tech stack, yêu cầu công việc). '
        "KHÔNG QUÁ {max_len} kí tự. Plain text, 1 dòng, không markdown.\n"
        '2. "skills": mảng JSON tối đa 20 tên kỹ năng/công nghệ TIẾNG ANH ngắn gọn. '
        "Dùng tên chuẩn, KHÔNG viết tắt hay số nhiều: "
        '"Machine Learning" (không phải "ML"), "LLM" (không phải "LLMs"), '
        '"Deep Learning" (không phải "DL"), "Natural Language Processing" → dùng "NLP", '
        '"Artificial Intelligence" → dùng "AI". '
        'Ví dụ: ["Python", "Machine Learning", "LLM", "Docker", "AWS"].\n'
        "Chỉ trả về JSON object, không giải thích thêm."
    )

    def __init__(
        self,
        account_id: str | None = None,
        api_token: str | None = None,
        max_length: int | None = None,
    ):
        self.account_id = CLOUDFLARE_ACCOUNT_ID if account_id is None else account_id
        self.api_token = CLOUDFLARE_API_TOKEN if api_token is None else api_token
        self.max_length = max_length or SUMMARIZER_MAX_LENGTH
        self._base_url = f"{CLOUDFLARE_API_BASE}/{self.account_id}/ai"
        self._run_url = f"{self._base_url}/run/{CLOUDFLARE_MODEL}"
        self._headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "x-session-affinity": "summarize-jobs",
        }
        log_app(f"[summarizer] Cloudflare batch API initialized (account={self.account_id[:10] if self.account_id else 'NOT SET'}...)")

    def _iter_ai_creds(self) -> list[tuple[str, str]]:
        accounts_all = CLOUDFLARE_ACCOUNT_IDS or ([] if not self.account_id else [self.account_id])
        tokens_all = CLOUDFLARE_API_TOKENS or ([] if not self.api_token else [self.api_token])

        accounts = accounts_all[1:] if len(accounts_all) > 1 else accounts_all
        tokens = tokens_all[1:] if len(tokens_all) > 1 else tokens_all

        if not accounts or not tokens:
            accounts = accounts_all
            tokens = tokens_all
        if not accounts or not tokens:
            return []
        if len(tokens) == 1 and len(accounts) > 1:
            tokens = tokens * len(accounts)
        if len(accounts) == 1 and len(tokens) > 1:
            accounts = accounts * len(tokens)
        n = min(len(accounts), len(tokens))
        return list(zip(accounts[:n], tokens[:n]))

    def _is_quota_error(self, response: requests.Response) -> bool:
        if response.status_code == 429:
            return True
        try:
            data = response.json()
        except Exception:
            return False
        errors = data.get("errors")
        if isinstance(errors, list):
            for e in errors:
                if isinstance(e, dict) and e.get("code") == 4006:
                    return True
        return False

    def summarize(self, description: str) -> str:
        """Summarize single description using batch API."""
        if not description or not description.strip():
            return ""
        if len(description) < SUMMARIZER_MIN_LENGTH:
            return description[: self.max_length]

        truncated = self._truncate_input(description)
        results = self.batch_summarize([truncated])
        return results[0] if results else self._truncate_output(truncated)

    def batch_summarize(self, descriptions: list[str]) -> list[str]:
        """Summarize multiple descriptions. Returns list of Vietnamese summaries."""
        return [summary for summary, _ in self.batch_analyze(descriptions)]

    def batch_analyze(self, descriptions: list[str]) -> list[tuple[str, list[str]]]:
        """Analyze multiple descriptions in one API call.

        Returns list of (summary, skills) tuples in same order as inputs.
        summary: Vietnamese plain-text summary string.
        skills: list of English skill name strings.
        """
        if not descriptions:
            return []

        results: list[tuple[str, list[str]]] = []
        pending: list[tuple[int, str]] = []

        for i, desc in enumerate(descriptions):
            if not desc or not desc.strip():
                results.append(("", []))
            elif len(desc) < SUMMARIZER_MIN_LENGTH:
                results.append((desc[: self.max_length], []))
            else:
                truncated = self._truncate_input(desc)
                pending.append((i, truncated))
                results.append(("", []))  # Placeholder

        if not pending:
            return results

        try:
            raw_responses = self._batch_call([p[1] for p in pending])
            if len(raw_responses) != len(pending):
                log_app(f"[summarizer] response count mismatch: expected {len(pending)}, got {len(raw_responses)}", "WARNING")
            for (i, _), raw in zip(pending, raw_responses):
                summary, skills = self._parse_combined(raw)
                results[i] = (summary[: self.max_length] if summary else "", skills)
        except Exception as e:
            log_app(f"[summarizer] batch API failed: {e}, {len(pending)} jobs will be retried next cycle", "WARNING")

        return results

    def _parse_combined(self, content: str) -> tuple[str, list[str]]:
        """Parse combined JSON response into (summary, skills)."""
        try:
            cleaned = content.strip().strip("`").lstrip("json").strip()
            obj = json.loads(cleaned)
            summary = obj.get("summary", "") or ""
            skills_raw = obj.get("skills", [])
            skills = [s for s in skills_raw if isinstance(s, str)][:20]
            return summary.replace("\n", " ").replace("\r", " ").strip(), skills
        except Exception:
            # Fallback: treat entire content as summary, no skills
            fallback = content.strip().replace("\n", " ").replace("\r", " ")
            return fallback, []

    def _truncate_input(self, text: str, max_chars: int = 2000) -> str:
        """Truncate at sentence boundary."""
        if len(text) <= max_chars:
            return text
        truncated = text[:max_chars]
        for end in (".", "!", "?", "\n"):
            pos = truncated.rfind(end)
            if pos > max_chars // 2:
                return truncated[: pos + 1].strip()
        return truncated.strip()

    def _truncate_output(self, text: str) -> str:
        """Truncate at word boundary."""
        if len(text) <= self.max_length:
            return text
        return text[: self.max_length].rsplit(" ", 1)[0] + "..."

    def _batch_call(self, descriptions: list[str]) -> list[str]:
        """Make Cloudflare batch API call and poll for results. Returns raw response strings."""
        creds = self._iter_ai_creds()
        if not creds:
            raise ValueError("Cloudflare credentials not configured")

        system_prompt = self._SYSTEM_PROMPT.format(max_len=self.max_length)
        requests_payload = []
        for idx, desc in enumerate(descriptions):
            requests_payload.append({
                "external_reference": str(idx),
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Phân tích tin tuyển dụng:\n{desc}"},
                ],
                "max_tokens": 400,
                "temperature": self._TEMPERATURE,
                "top_p": self._TOP_P,
                "top_k": self._TOP_K,
                "chat_template_kwargs": {"enable_thinking": False},
            })

        log_app(f"[summarizer] batch API request: {len(requests_payload)} requests")

        last_err = None
        request_id = None
        run_url = None
        headers = None
        for idx, (account_id, api_token) in enumerate(creds):
            run_url = f"{CLOUDFLARE_API_BASE}/{account_id}/ai/run/{CLOUDFLARE_MODEL}"
            headers = {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
                "x-session-affinity": "summarize-jobs",
            }

            response = requests.post(
                f"{run_url}?queueRequest=true",
                headers=headers,
                json={"requests": requests_payload},
                timeout=30,
            )

            if response.status_code in (200, 202):
                data = response.json()
                if data.get("success"):
                    request_id = data.get("result", {}).get("request_id")
                    if request_id:
                        break
                    last_err = RuntimeError("No request_id in batch response")
                else:
                    last_err = RuntimeError(f"Batch API failed: {data}")
            else:
                if idx < len(creds) - 1 and self._is_quota_error(response):
                    log_app(f"[summarizer] quota-limited, trying next CF account ({response.status_code})", "WARN")
                    continue
                last_err = RuntimeError(f"Batch API error: {response.status_code} {response.text}")

            # Non-quota errors should stop immediately.
            break

        if not request_id:
            if last_err is None:
                last_err = RuntimeError("Batch API failed without request_id")
            raise last_err

        log_app(f"[summarizer] batch submitted: {len(descriptions)} prompts, request_id={request_id}")

        start_time = time.time()

        while time.time() - start_time < self._POLL_TIMEOUT:
            poll_response = requests.post(
                f"{run_url}?queueRequest=true",
                headers=headers,
                json={"request_id": request_id},
                timeout=30,
            )

            if poll_response.status_code != 200:
                log_app(f"[summarizer] poll error {poll_response.status_code}: {poll_response.text[:200]}")
                time.sleep(self._POLL_INTERVAL)
                continue

            poll_data = poll_response.json()
            result_data = poll_data.get("result", {})

            responses = result_data.get("responses", [])
            if responses:
                log_app(f"[summarizer] batch completed: {len(responses)} responses, request_id={request_id}")

                ordered: dict[int, str] = {}
                for r in responses:
                    ref = r.get("external_reference")
                    try:
                        idx = int(ref) if ref is not None else None
                    except (ValueError, TypeError):
                        idx = None
                    try:
                        message = r.get("result", {}).get("choices", [{}])[0].get("message", {})
                        content = message.get("content", "") or message.get("reasoning_content", "")
                        content = content.strip() if content else ""
                    except (IndexError, KeyError, AttributeError) as e:
                        log_app(f"[summarizer] failed to parse response ref={ref}: {e}", "WARNING")
                        content = ""
                    if idx is not None:
                        ordered[idx] = content
                    else:
                        log_app(f"[summarizer] response missing external_reference, falling back to positional", "WARNING")
                        ordered = {i: "" for i in range(len(descriptions))}
                        for i, r2 in enumerate(responses):
                            try:
                                m = r2.get("result", {}).get("choices", [{}])[0].get("message", {})
                                c = m.get("content", "") or m.get("reasoning_content", "")
                                ordered[i] = c.strip() if c else ""
                            except Exception:
                                ordered[i] = ""
                        break
                results = [ordered.get(i, "") for i in range(len(descriptions))]
                return results

            result_status = result_data.get("status", "")

            if result_status in ("queued", "202", "running"):
                time.sleep(self._POLL_INTERVAL)
                continue

            if poll_data.get("success"):
                log_app(f"[summarizer] batch processing (no responses yet), request_id={request_id}")
                time.sleep(self._POLL_INTERVAL)
                continue

            log_app(f"[summarizer] poll error: {poll_data}", "WARNING")
            time.sleep(self._POLL_INTERVAL)
            continue

        raise RuntimeError(f"Batch request timed out after {self._POLL_TIMEOUT}s")

    def _summarize_with_retry(self, descriptions: list[str]) -> list[str]:
        """Call batch API with retry."""
        try:
            return self._batch_call(descriptions)
        except Exception as e:
            log_app(f"[summarizer] batch API failed: {e}, retrying in {self._RETRY_DELAY}s...", "WARNING")
            time.sleep(self._RETRY_DELAY)
            try:
                return self._batch_call(descriptions)
            except Exception as e2:
                log_app(f"[summarizer] batch API failed after retry: {e2}", "WARNING")
                return [self._truncate_output(d[:2000]) for d in descriptions]

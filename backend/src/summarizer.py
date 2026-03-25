# src/summarizer.py
import time
import requests

from langdetect import detect as lang_detect

from src.constants import (
    CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN,
    CLOUDFLARE_API_BASE,
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

    # System prompt templates — {max_len} is filled from SUMMARIZER_MAX_LENGTH
    _SYSTEM_PROMPT_VI = "Bạn là chuyên gia viết tóm tắt công việc. Viết bản tóm tắt KHÔNG QUÁ {max_len} kí tự, cùng ngôn ngữ với bản gốc. Gồm: trách nhiệm chính, yêu cầu quan trọng, quyền lợi nổi bật. KHÔNG giải thích, KHÔNG phân tích, chỉ đưa ra kết quả. Không dùng markdown, chỉ plain text. Viết trên 1 dòng duy nhất, không xuống dòng."
    _SYSTEM_PROMPT_EN = "You are a job description summarization expert. Write a summary NO MORE THAN {max_len} characters, in the same language as input. Cover: main responsibilities, key requirements, top benefits. No explanation, no analysis, just give the summary result. No markdown, plain text only. Write on a single line, no newlines."

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
            "x-session-affinity": "summarize-jobs",  # Prompt caching
        }
        log_app(f"[summarizer] Cloudflare batch API initialized (account={self.account_id[:10] if self.account_id else 'NOT SET'}...)")

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
        """Summarize multiple descriptions using Cloudflare batch API.

        Returns list of summaries in same order as inputs.
        """
        if not descriptions:
            return []

        # Pre-process: handle empty/short descriptions
        results: list[str] = []
        pending: list[tuple[int, str]] = []  # (index, truncated_description)

        for i, desc in enumerate(descriptions):
            if not desc or not desc.strip():
                results.append("")
            elif len(desc) < SUMMARIZER_MIN_LENGTH:
                results.append(desc[: self.max_length])
            else:
                truncated = self._truncate_input(desc)
                pending.append((i, truncated))
                results.append("")  # Placeholder

        if not pending:
            return results

        # Batch API call
        try:
            summaries = self._batch_call([p[1] for p in pending])
            for (i, _), summary in zip(pending, summaries):
                results[i] = summary
        except Exception as e:
            log_app(f"[summarizer] batch API failed: {e}, {len(pending)} jobs will be retried next cycle", "WARNING")
            # Leave results as "" so they get retried on next summarization run

        return results

    def _detect_language(self, text: str) -> str:
        """Detect 'vi', 'en', or 'unknown'."""
        try:
            detected = lang_detect(text)
            lang = detected if detected in ("vi", "en") else "unknown"
            preview = text[:120].strip().replace('\n', ' ').replace('\r', '')
            log_app(f"[summarizer] langdetect: '{preview}...' → {lang}")
            return lang
        except Exception as e:
            log_app(f"[summarizer] langdetect failed: {e}", "WARNING")
            return "unknown"

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
        """Make Cloudflare batch API call and poll for results."""
        if not self.account_id or not self.api_token:
            raise ValueError("Cloudflare credentials not configured")

        # Build requests array using messages format
        requests_payload = []
        for desc in descriptions:
            lang = self._detect_language(desc)
            system_prompt = (self._SYSTEM_PROMPT_VI if lang == "vi" else self._SYSTEM_PROMPT_EN).format(max_len=self.max_length)
            requests_payload.append({
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Tóm tắt tin tuyển dụng:\n{desc}" if lang == "vi" else f"Summarize this job description:\n{desc}"},
                ],
                "max_tokens": 350,
                "temperature": self._TEMPERATURE,
                "top_p": self._TOP_P,
                "top_k": self._TOP_K,
                "chat_template_kwargs": {"enable_thinking": False},
            })

        log_app(f"[summarizer] batch API request: {len(requests_payload)} requests")

        # Submit batch job with queueRequest=true for async processing
        response = requests.post(
            f"{self._run_url}?queueRequest=true",
            headers=self._headers,
            json={"requests": requests_payload},
            timeout=30,
        )

        # 200 = completed, 202 = queued (initial response is success)
        if response.status_code not in (200, 202):
            log_app(f"[summarizer] batch API error {response.status_code}: {response.text[:500]}", "ERROR")
            raise RuntimeError(f"Batch API error: {response.status_code} {response.text}")

        data = response.json()
        if not data.get("success"):
            log_app(f"[summarizer] batch API success=false: {data}", "ERROR")
            raise RuntimeError(f"Batch API failed: {data}")

        request_id = data.get("result", {}).get("request_id")
        if not request_id:
            raise RuntimeError("No request_id in batch response")

        log_app(f"[summarizer] batch submitted: {len(descriptions)} prompts, request_id={request_id}")

        # Poll for results by posting to the same endpoint with request_id
        start_time = time.time()

        while time.time() - start_time < self._POLL_TIMEOUT:
            poll_response = requests.post(
                f"{self._run_url}?queueRequest=true",
                headers=self._headers,
                json={"request_id": request_id},
                timeout=30,
            )

            if poll_response.status_code != 200:
                log_app(f"[summarizer] poll error {poll_response.status_code}: {poll_response.text[:200]}")
                time.sleep(self._POLL_INTERVAL)
                continue

            poll_data = poll_response.json()
            result_data = poll_data.get("result", {})

            # Primary check: if responses exist, we're done
            responses = result_data.get("responses", [])
            if responses:
                log_app(f"[summarizer] batch completed: {len(responses)} responses, request_id={request_id}")

                results = []
                for i, r in enumerate(responses):
                    try:
                        message = r.get("result", {}).get("choices", [{}])[0].get("message", {})
                        content = message.get("content", "")
                        if not content:
                            content = message.get("reasoning_content", "")
                        if content:
                            content = content.strip().replace('\n', ' ').replace('\r', ' ')
                        else:
                            content = ""
                        results.append(content[: self.max_length] if content else "")
                    except (IndexError, KeyError, AttributeError) as e:
                        log_app(f"[summarizer] failed to parse response[{i}]: {e}", "WARNING")
                        results.append("")
                return results

            # Check status field
            result_status = result_data.get("status", "")

            if result_status in ("queued", "202", "running"):
                log_app(f"[summarizer] batch {result_status} (status={result_status}), request_id={request_id}")
                time.sleep(self._POLL_INTERVAL)
                continue

            # Success but no responses yet - keep waiting
            if poll_data.get("success"):
                log_app(f"[summarizer] batch processing (no responses yet), request_id={request_id}")
                time.sleep(self._POLL_INTERVAL)
                continue

            # Error case
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
                # Fallback: truncate descriptions
                return [self._truncate_output(d[:2000]) for d in descriptions]

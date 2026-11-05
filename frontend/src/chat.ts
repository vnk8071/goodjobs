import { askJobs } from "./api";
import { getLang, t } from "./i18n";
import type { ChatHistoryMessage, ChatJob } from "./types";

/**
 * Floating "Good Jobs AI" chat window.
 *
 * The backend retrieves relevant cached jobs and asks Cloudflare Workers AI for
 * a grounded answer. Job cards returned with the answer link to the real
 * listings, so the UI never has to trust AI-generated links.
 */

interface UIMessage {
  role: "user" | "assistant";
  content: string;
  jobs?: ChatJob[];
  welcome?: boolean;
  error?: boolean;
}

const STORAGE_KEY = "goodjobs:chat-v1";
const MAX_MESSAGES = 20;

let launcher: HTMLButtonElement | null = null;
let panel: HTMLDivElement | null = null;
let messagesEl: HTMLDivElement | null = null;
let suggestionsEl: HTMLDivElement | null = null;
let inputEl: HTMLTextAreaElement | null = null;
let sendBtn: HTMLButtonElement | null = null;
let typingEl: HTMLDivElement | null = null;

let messages: UIMessage[] = [];
let busy = false;
let abortController: AbortController | null = null;

const SUGGESTION_KEYS = [
  "chat.suggestion1",
  "chat.suggestion2",
  "chat.suggestion3",
  "chat.suggestion4",
];

const ICONS = {
  bot: `<svg viewBox="0 0 24 24" width="26" height="26" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 6V3.4"/><circle cx="12" cy="2.6" r="1" fill="currentColor" stroke="none"/><rect x="4" y="6" width="16" height="12.5" rx="4.2"/><circle cx="9" cy="12" r="1.35" fill="currentColor" stroke="none"/><circle cx="15" cy="12" r="1.35" fill="currentColor" stroke="none"/><path d="M9.6 15.4h4.8"/><path d="M2 11v3M22 11v3"/><path d="M8.5 18.5l-1.4 2.6 3.7-2.2"/></svg>`,
  close: `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>`,
  reset: `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 12a8 8 0 1 0 2.6-5.9"/><path d="M4 4v4.5h4.5"/></svg>`,
  send: `<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 12h14M13 6l6 6-6 6"/></svg>`,
};

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className = "",
  html = "",
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (html) node.innerHTML = html;
  return node;
}

function loadMessages(): UIMessage[] {
  try {
    const raw = JSON.parse(sessionStorage.getItem(STORAGE_KEY) || "[]") as UIMessage[];
    if (!Array.isArray(raw)) return [];
    return raw
      .filter((m) => (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
      .slice(-MAX_MESSAGES);
  } catch {
    return [];
  }
}

function saveMessages(): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(messages.slice(-MAX_MESSAGES)));
  } catch {
    /* private browsing quota — ignore */
  }
}

function scrollToBottom(): void {
  if (!messagesEl) return;
  requestAnimationFrame(() => {
    messagesEl!.scrollTop = messagesEl!.scrollHeight;
  });
}

function jobCard(job: ChatJob): HTMLAnchorElement {
  const card = el("a", "chat-job") as HTMLAnchorElement;
  card.href = job.link;
  card.target = "_blank";
  card.rel = "noopener noreferrer";

  const title = el("div", "chat-job__title");
  title.textContent = job.title || "—";
  card.appendChild(title);

  const meta = el("div", "chat-job__meta");
  meta.textContent = [job.company, job.location, job.source].filter(Boolean).join(" · ");
  card.appendChild(meta);

  if (job.skills?.length) {
    const skills = el("div", "chat-job__skills");
    for (const skill of job.skills.slice(0, 5)) {
      const pill = el("span", "chat-job__skill");
      pill.textContent = skill;
      skills.appendChild(pill);
    }
    card.appendChild(skills);
  }

  return card;
}

function renderMessage(message: UIMessage): void {
  if (!messagesEl) return;
  const row = el("div", `chat-msg chat-msg--${message.role}`);
  const bubble = el("div", "chat-bubble");
  if (message.error) bubble.classList.add("chat-bubble--error");

  const text = el("div", "chat-text");
  text.textContent = message.content;
  bubble.appendChild(text);

  if (message.jobs?.length) {
    const jobs = el("div", "chat-jobs");
    for (const job of message.jobs) {
      if (job.link) jobs.appendChild(jobCard(job));
    }
    if (jobs.children.length) bubble.appendChild(jobs);
  }

  row.appendChild(bubble);
  messagesEl.appendChild(row);
}

function renderMessages(): void {
  if (!messagesEl) return;
  messagesEl.innerHTML = "";
  for (const message of messages) renderMessage(message);
  scrollToBottom();
}

function renderSuggestions(): void {
  if (!suggestionsEl) return;
  suggestionsEl.innerHTML = "";
  const hasUserMessage = messages.some((m) => m.role === "user");
  if (hasUserMessage) {
    suggestionsEl.classList.add("hidden");
    return;
  }
  suggestionsEl.classList.remove("hidden");
  const label = el("span", "chat-suggestions__label");
  label.textContent = t("chat.suggestions");
  suggestionsEl.appendChild(label);
  for (const key of SUGGESTION_KEYS) {
    const text = t(key);
    const chip = el("button", "chat-suggestion") as HTMLButtonElement;
    chip.type = "button";
    chip.textContent = text;
    chip.addEventListener("click", () => sendMessage(text));
    suggestionsEl.appendChild(chip);
  }
}

function setTyping(show: boolean): void {
  if (!typingEl) return;
  typingEl.classList.toggle("hidden", !show);
  if (show) {
    if (!typingEl.parentElement) messagesEl?.appendChild(typingEl);
    scrollToBottom();
  }
}

function setBusy(value: boolean): void {
  busy = value;
  if (inputEl) inputEl.disabled = value;
  if (sendBtn) sendBtn.disabled = value;
}

function addMessage(message: UIMessage): void {
  messages.push(message);
  if (messages.length > MAX_MESSAGES) messages = messages.slice(-MAX_MESSAGES);
  renderMessages();
  renderSuggestions();
  saveMessages();
}

async function sendMessage(rawText?: string): Promise<void> {
  const text = (rawText ?? inputEl?.value ?? "").trim();
  if (!text || busy) return;

  if (inputEl) {
    inputEl.value = "";
    inputEl.style.height = "auto";
  }

  addMessage({ role: "user", content: text });
  setBusy(true);
  setTyping(true);

  const history: ChatHistoryMessage[] = messages
    .slice(0, -1)
    .slice(-6)
    .map((m) => ({ role: m.role, content: m.content }));

  abortController = new AbortController();
  try {
    const response = await askJobs(text, history, getLang(), abortController.signal);
    addMessage({
      role: "assistant",
      content: response.answer,
      jobs: response.jobs,
    });
  } catch (error) {
    if ((error as Error).name === "AbortError") return;
    const status = (error as Error & { status?: number }).status;
    addMessage({
      role: "assistant",
      content: status === 429 ? t("chat.rateLimited") : t("chat.error"),
      error: true,
    });
  } finally {
    abortController = null;
    setTyping(false);
    setBusy(false);
    scrollToBottom();
  }
}

function resetChat(): void {
  abortController?.abort();
  abortController = null;
  messages = [{ role: "assistant", content: t("chat.welcome"), welcome: true }];
  renderMessages();
  renderSuggestions();
  saveMessages();
  setBusy(false);
  setTyping(false);
  inputEl?.focus();
}

function setOpen(open: boolean): void {
  if (!panel || !launcher) return;
  panel.classList.toggle("hidden", !open);
  panel.setAttribute("aria-hidden", String(!open));
  launcher.setAttribute("aria-expanded", String(open));
  if (open) {
    inputEl?.focus();
    scrollToBottom();
  }
}

/** Keep the launcher/panel above the cookie-consent banner when it is visible. */
function positionChat(): void {
  if (!launcher || !panel) return;
  const consent = document.getElementById("gj-consent");
  let offset = 0;
  if (consent) {
    const rect = consent.getBoundingClientRect();
    if (rect.height > 0) offset = Math.max(0, window.innerHeight - rect.top);
  }
  const bottom = offset > 0 ? Math.round(offset + 12) : 20;
  const root = document.documentElement;
  root.style.setProperty("--chat-launcher-bottom", `${bottom}px`);
  root.style.setProperty("--chat-panel-bottom", `${bottom + 68}px`);
}

function build(): void {
  if (document.getElementById("chatLauncher")) return;

  launcher = el("button", "chat-launcher") as HTMLButtonElement;
  launcher.id = "chatLauncher";
  launcher.type = "button";
  launcher.setAttribute("aria-expanded", "false");
  launcher.innerHTML = `${ICONS.bot}<span class="chat-launcher__dot" aria-hidden="true"></span>`;
  launcher.addEventListener("click", () => setOpen(panel?.classList.contains("hidden") ?? true));

  panel = el("div", "chat-panel hidden") as HTMLDivElement;
  panel.id = "chatPanel";
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-modal", "false");
  panel.setAttribute("aria-labelledby", "chatTitle");
  panel.setAttribute("aria-hidden", "true");

  const header = el("div", "chat-header");
  const headerIcon = el("div", "chat-header__icon", ICONS.bot);
  const headerText = el("div", "chat-header__text");
  const title = el("strong");
  title.id = "chatTitle";
  const subtitle = el("span");
  headerText.append(title, subtitle);

  const resetBtn = el("button", "chat-icon-btn") as HTMLButtonElement;
  resetBtn.type = "button";
  resetBtn.innerHTML = ICONS.reset;
  resetBtn.addEventListener("click", resetChat);

  const closeBtn = el("button", "chat-icon-btn") as HTMLButtonElement;
  closeBtn.type = "button";
  closeBtn.innerHTML = ICONS.close;
  closeBtn.addEventListener("click", () => setOpen(false));

  header.append(headerIcon, headerText, resetBtn, closeBtn);

  messagesEl = el("div", "chat-messages") as HTMLDivElement;
  typingEl = el("div", "chat-typing hidden");
  typingEl.innerHTML = "<span></span><span></span><span></span>";

  suggestionsEl = el("div", "chat-suggestions") as HTMLDivElement;

  const form = el("form", "chat-form") as HTMLFormElement;
  inputEl = el("textarea", "chat-input") as HTMLTextAreaElement;
  inputEl.rows = 1;
  inputEl.spellcheck = false;
  inputEl.addEventListener("input", () => {
    if (!inputEl) return;
    inputEl.style.height = "auto";
    inputEl.style.height = `${Math.min(inputEl.scrollHeight, 120)}px`;
  });
  inputEl.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void sendMessage();
    }
  });

  sendBtn = el("button", "chat-send") as HTMLButtonElement;
  sendBtn.type = "submit";
  sendBtn.innerHTML = ICONS.send;

  form.append(inputEl, sendBtn);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    void sendMessage();
  });

  const note = el("div", "chat-note");
  note.dataset.chatNote = "true";

  panel.append(header, messagesEl, suggestionsEl, form, note);
  document.body.append(launcher, panel);

  positionChat();
  window.addEventListener("resize", positionChat);
  window.setTimeout(positionChat, 300);
  if (document.getElementById("gj-consent")) {
    const observer = new MutationObserver(() => {
      if (!document.getElementById("gj-consent")) {
        positionChat();
        observer.disconnect();
      }
    });
    observer.observe(document.body, { childList: true });
  }

  // Keep translated labels in one place so refreshChatLanguage() can update them.
  const labelNodes = { title, subtitle, resetBtn, closeBtn, note, inputEl };
  (panel as unknown as { _chatLabels?: typeof labelNodes })._chatLabels = labelNodes;

  messages = loadMessages();
  if (messages.length === 0) {
    messages = [{ role: "assistant", content: t("chat.welcome"), welcome: true }];
  }

  applyLanguage();
  renderMessages();
  renderSuggestions();

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && panel && !panel.classList.contains("hidden")) {
      setOpen(false);
    }
  });
}

function applyLanguage(): void {
  if (!panel || !launcher || !inputEl || !sendBtn) return;
  const labels = (panel as unknown as {
    _chatLabels?: {
      title: HTMLElement;
      subtitle: HTMLElement;
      resetBtn: HTMLButtonElement;
      closeBtn: HTMLButtonElement;
      note: HTMLElement;
      inputEl: HTMLTextAreaElement;
    };
  })._chatLabels;
  if (!labels) return;

  labels.title.textContent = t("chat.title");
  labels.subtitle.textContent = t("chat.subtitle");
  labels.resetBtn.title = t("chat.reset");
  labels.resetBtn.setAttribute("aria-label", t("chat.reset"));
  labels.closeBtn.title = t("chat.close");
  labels.closeBtn.setAttribute("aria-label", t("chat.close"));
  labels.note.textContent = t("chat.note");
  labels.inputEl.placeholder = t("chat.placeholder");
  labels.inputEl.setAttribute("aria-label", t("chat.placeholder"));
  sendBtn.setAttribute("aria-label", t("chat.send"));
  launcher.setAttribute("aria-label", t("chat.launch"));
  launcher.title = t("chat.launch");

  // Translate the welcome bubble until the user starts a conversation.
  if (!messages.some((m) => m.role === "user")) {
    for (const message of messages) {
      if (message.welcome) message.content = t("chat.welcome");
    }
  }
}

/** Initialise the floating chat window. Safe to call more than once. */
export function initChat(): void {
  build();
}

/** Refresh labels after an EN/VI toggle. */
export function refreshChatLanguage(): void {
  applyLanguage();
  renderMessages();
  renderSuggestions();
}

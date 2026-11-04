/* Good Jobs consent gate: analytics and ads load only after explicit consent. */
(function () {
  'use strict';
  var GA_ID = 'G-KZ325XVPVW';
  var ADS_CLIENT = 'ca-pub-1563090184756838';
  var KEY = 'gj-consent';
  var loaded = false;

  function read() {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  }
  function write(v) {
    try { localStorage.setItem(KEY, v); } catch (e) {}
  }
  function inject(src, opts) {
    var s = document.createElement('script');
    s.async = true;
    s.src = src;
    if (opts) Object.keys(opts).forEach(function (k) { s.setAttribute(k, opts[k]); });
    document.head.appendChild(s);
    return s;
  }
  function loadTags() {
    if (loaded) return;
    loaded = true;
    window.dataLayer = window.dataLayer || [];
    window.gtag = window.gtag || function () { window.dataLayer.push(arguments); };
    inject('https://www.googletagmanager.com/gtag/js?id=' + GA_ID);
    window.gtag('js', new Date());
    window.gtag('config', GA_ID, { anonymize_ip: true });
    inject('https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=' + ADS_CLIENT, { crossorigin: 'anonymous' });
    try { (window.adsbygoogle = window.adsbygoogle || []).push({}); } catch (e) {}
  }

  function banner() {
    if (document.getElementById('gj-consent')) return;
    var isVi = document.documentElement.lang === 'vi';
    var copy = isVi
      ? '<p><strong>Cookie &amp; quyền riêng tư.</strong> Chúng tôi dùng cookie phân tích và quảng cáo để hiểu lưu lượng truy cập và duy trì Good Jobs miễn phí. Bạn có thể chấp nhận tất cả hoặc từ chối cookie không cần thiết. Xem <a href="/privacy/">Chính sách quyền riêng tư</a>.</p>'
      : '<p><strong>Cookies &amp; privacy.</strong> We use analytics and advertising cookies to understand traffic and keep Good Jobs free. You can accept all or reject non-essential cookies. See our <a href="/privacy/">Privacy Policy</a>.</p>';
    var accept = isVi ? 'Chấp nhận tất cả' : 'Accept all';
    var reject = isVi ? 'Từ chối không cần thiết' : 'Reject non-essential';
    var settings = isVi ? 'Cài đặt cookie' : 'Cookie settings';
    var wrap = document.createElement('div');
    wrap.id = 'gj-consent';
    wrap.setAttribute('role', 'dialog');
    wrap.setAttribute('aria-modal', 'false');
    wrap.setAttribute('aria-label', 'Cookie consent');
    wrap.innerHTML =
      '<div class="gj-consent-inner">' +
        copy +
        '<div class="gj-consent-actions">' +
          '<button type="button" data-gj="reject">' + reject + '</button>' +
          '<button type="button" data-gj="accept">' + accept + '</button>' +
        '</div>' +
      '</div>';
    var style = document.createElement('style');
    style.textContent =
      '#gj-consent{position:fixed;left:0;right:0;bottom:0;z-index:9999;padding:0.75rem;background:transparent;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}' +
      '#gj-consent .gj-consent-inner{max-width:980px;margin:0 auto;background:#fff;color:#0f172a;border:1px solid #cbd5e1;border-radius:12px;box-shadow:0 12px 40px rgba(15,23,42,.18);padding:1rem 1.1rem;display:flex;gap:1rem;align-items:center;justify-content:space-between;flex-wrap:wrap}' +
      '#gj-consent p{margin:0;font-size:.86rem;line-height:1.5;max-width:70ch;color:#0f172a}' +
      '#gj-consent a{color:#1d4ed8;text-decoration:underline}' +
      '#gj-consent .gj-consent-actions{display:flex;gap:.5rem;flex-wrap:wrap}' +
      '#gj-consent button{font:inherit;font-size:.82rem;font-weight:700;border-radius:8px;padding:.6rem .9rem;cursor:pointer}' +
      '#gj-consent button[data-gj="reject"]{background:#fff;color:#0f172a;border:1px solid #94a3b8}' +
      '#gj-consent button[data-gj="accept"]{background:#1d4ed8;color:#fff;border:1px solid #1d4ed8}' +
      '#gj-consent button:focus-visible{outline:2px solid #0f172a;outline-offset:2px}' +
      '.gj-cookie-settings{background:none;border:0;padding:0;margin-left:1rem;font:inherit;color:inherit;text-decoration:underline;cursor:pointer}' +
      '@media (max-width:640px){#gj-consent{padding:.5rem}#gj-consent .gj-consent-inner{padding:.9rem}#gj-consent .gj-consent-actions{width:100%}#gj-consent button{flex:1}}';
    document.head.appendChild(style);
    document.body.appendChild(wrap);
    wrap.querySelector('[data-gj="accept"]').addEventListener('click', function () {
      write('granted'); loadTags(); wrap.remove();
    });
    wrap.querySelector('[data-gj="reject"]').addEventListener('click', function () {
      write('denied'); wrap.remove();
    });
  }

  function addSettingsLink() {
    var footer = document.querySelector('footer');
    if (!footer || footer.querySelector('.gj-cookie-settings')) return;
    var b = document.createElement('button');
    b.type = 'button';
    b.className = 'gj-cookie-settings';
    b.textContent = document.documentElement.lang === 'vi' ? 'Cài đặt cookie' : 'Cookie settings';
    b.addEventListener('click', function () {
      try { localStorage.removeItem(KEY); } catch (e) {}
      banner();
    });
    footer.appendChild(b);
  }

  function init() {
    if (/^\/admin(\/|$)/.test(location.pathname)) return;
    var choice = read();
    if (choice === 'granted') loadTags();
    else if (choice !== 'denied') banner();
    addSettingsLink();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();

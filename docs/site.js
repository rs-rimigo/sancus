// nav toggle on phones, copy buttons on code blocks, hide missing images
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.querySelector('.topbar button');
  const rail = document.querySelector('nav.rail');
  if (btn && rail) btn.addEventListener('click', () => { const open = rail.classList.toggle('open'); btn.setAttribute('aria-expanded', String(open)); });
  document.querySelectorAll('pre').forEach((pre) => {
    const b = document.createElement('button'); b.className = 'copy'; b.type = 'button'; b.textContent = 'Copy';
    b.addEventListener('click', async () => { try { await navigator.clipboard.writeText(pre.querySelector('code')?.innerText ?? pre.innerText); b.textContent = 'Copied'; setTimeout(() => (b.textContent = 'Copy'), 1500); } catch { b.textContent = 'Select and copy'; } });
    pre.appendChild(b);
  });
  // ?theme=light|dark forces a palette (handy for screenshots); otherwise the system setting wins
  const theme = new URLSearchParams(location.search).get('theme');
  if (theme === 'light' || theme === 'dark') document.documentElement.dataset.theme = theme;
  const fallback = (img) => {
    img.classList.add('missing');
    if (img.nextElementSibling?.classList.contains('img-fallback')) return;
    const f = document.createElement('div'); f.className = 'img-fallback'; f.textContent = `image: ${img.getAttribute('src')}`;
    img.insertAdjacentElement('afterend', f);
  };
  document.querySelectorAll('img[data-asset]').forEach((img) => {
    if (img.classList.contains('missing') || (img.complete && img.naturalWidth === 0)) fallback(img);
    img.addEventListener('error', () => fallback(img));
  });
});

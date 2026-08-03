// ZORA chart page: live CEX bid vs. DEX price line chart for one pair.
// Served fresh off disk on every request -- edit and refresh, no restart.
// Line/grid colors are read from theme.css's --variables at draw time
// (see cssVar below), so recoloring the chart is a theme.css edit, not
// a JS edit.

const PAIR_KEY = new URLSearchParams(location.search).get('pair');
const WINDOW_MS = 5 * 60 * 1000; // rolling 5-minute window
const POLL_MS = 300;
const points = []; // {t, cex: {bid, age_ms, stale}|null, dex: {price, age_ms, stale}|null}

const canvas = document.getElementById('chart');
const ctx = canvas.getContext('2d');
const titleEl = document.getElementById('pair-title');
const fCex = document.getElementById('f-cex');
const fDex = document.getElementById('f-dex');
const fSpread = document.getElementById('f-spread');
const fCexAge = document.getElementById('f-cex-age');
const fDexAge = document.getElementById('f-dex-age');

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

async function loadLabel() {
  try {
    const res = await fetch('/api/pairs');
    const pairs = await res.json();
    const found = pairs.find(p => p.key === PAIR_KEY);
    if (found) {
      document.title = found.label + ' -- ZORA';
      titleEl.textContent = found.label;
    }
  } catch (e) {
    // non-fatal -- chart still works without a label
  }
}

async function poll() {
  try {
    const res = await fetch('/api/prices?pair=' + PAIR_KEY);
    const snap = await res.json();
    const now = Date.now();
    points.push({ t: now, cex: snap.cex, dex: snap.dex });
    while (points.length && now - points[0].t > WINDOW_MS) points.shift();
    updateFields(snap);
    draw();
  } catch (e) {
    fCex.textContent = 'ERR';
  }
}

function fmt(v) {
  return v === null || v === undefined ? '--' : v.toFixed(8);
}

function setField(el, text, isStale) {
  el.textContent = text;
  el.classList.toggle('stale', !!isStale);
}

function updateFields(snap) {
  setField(fCex, snap.cex ? fmt(snap.cex.bid) : 'WAITING', snap.cex && snap.cex.stale);
  setField(fDex, snap.dex ? fmt(snap.dex.price) : 'WAITING', snap.dex && snap.dex.stale);
  setField(fCexAge, snap.cex ? snap.cex.age_ms.toFixed(0) + 'MS' : '--', snap.cex && snap.cex.stale);
  setField(fDexAge, snap.dex ? snap.dex.age_ms.toFixed(0) + 'MS' : '--', snap.dex && snap.dex.stale);

  fSpread.classList.remove('pos', 'neg');
  if (snap.cex && snap.dex && snap.dex.price !== 0) {
    const spread = (snap.cex.bid - snap.dex.price) / snap.dex.price * 100;
    fSpread.textContent = (spread >= 0 ? '+' : '') + spread.toFixed(3) + '%';
    fSpread.classList.add(spread >= 0 ? 'pos' : 'neg');
  } else {
    fSpread.textContent = '--';
  }
}

function draw() {
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  const withData = points.filter(p => p.cex || p.dex);
  if (withData.length < 2) {
    ctx.fillStyle = cssVar('--grey');
    ctx.font = '12px ' + cssVar('--font');
    ctx.fillText('WAITING FOR DATA...', 16, 24);
    return;
  }

  const margin = { l: 90, r: 16, t: 16, b: 24 };
  const plotW = w - margin.l - margin.r;
  const plotH = h - margin.t - margin.b;

  const tMin = points[0].t, tMax = points[points.length - 1].t || tMin + 1;
  let vMin = Infinity, vMax = -Infinity;
  for (const p of points) {
    if (p.cex) { vMin = Math.min(vMin, p.cex.bid); vMax = Math.max(vMax, p.cex.bid); }
    if (p.dex) { vMin = Math.min(vMin, p.dex.price); vMax = Math.max(vMax, p.dex.price); }
  }
  if (vMin === vMax) { vMin -= 1; vMax += 1; }
  const pad = (vMax - vMin) * 0.08;
  vMin -= pad; vMax += pad;

  const x = t => margin.l + (tMax === tMin ? 0 : (t - tMin) / (tMax - tMin) * plotW);
  const y = v => margin.t + plotH - (v - vMin) / (vMax - vMin) * plotH;

  // gridlines + y-axis labels
  ctx.strokeStyle = '#1a1200';
  ctx.fillStyle = cssVar('--grey');
  ctx.font = '10px ' + cssVar('--font');
  const rows = 4;
  for (let i = 0; i <= rows; i++) {
    const v = vMin + (vMax - vMin) * i / rows;
    const yy = y(v);
    ctx.beginPath(); ctx.moveTo(margin.l, yy); ctx.lineTo(w - margin.r, yy); ctx.stroke();
    ctx.fillText(v.toFixed(8), 2, yy + 3);
  }
  ctx.strokeStyle = cssVar('--amber-dim');
  ctx.strokeRect(margin.l, margin.t, plotW, plotH);

  function drawLine(key, field, color) {
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    for (const p of points) {
      const obj = p[key];
      if (!obj) { started = false; continue; }
      const xx = x(p.t), yy = y(obj[field]);
      if (!started) { ctx.moveTo(xx, yy); started = true; }
      else ctx.lineTo(xx, yy);
    }
    ctx.stroke();
  }
  drawLine('cex', 'bid', cssVar('--amber'));
  drawLine('dex', 'price', cssVar('--green'));
}

loadLabel();
setInterval(poll, POLL_MS);
poll();

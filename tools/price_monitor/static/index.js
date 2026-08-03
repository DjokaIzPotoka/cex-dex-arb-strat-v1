// ZORA index page: pair quick-launch pills + the PRICES/LIQUIDITY tables.
// Served fresh off disk on every request -- edit and refresh, no restart.

function openChart(key) {
  window.open('/chart?pair=' + key, 'zora_chart_' + key, 'width=960,height=600');
}

function fmtPrice(v) {
  return v === null || v === undefined ? '--' : v.toFixed(8);
}

function fmtAge(o) {
  return o ? o.age_ms.toFixed(0) + 'MS' : '--';
}

function fmtQty(v, dp) {
  return v === null || v === undefined
    ? '--'
    : v.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

function fmtUsd(v) {
  return v === null || v === undefined ? '--' : '$' + v.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

async function loadPairButtons() {
  const res = await fetch('/api/pairs');
  const pairs = await res.json();
  document.getElementById('pairpills').innerHTML = pairs
    .map((p, i) =>
      '<div class="pill" onclick="openChart(\'' + p.key + '\')">' + p.label + '</div>'
    )
    .join('');
}

function renderPricesRow(p, i) {
  const cex = p.cex, dex = p.dex;
  let diff = '--', diffPct = '--', diffClass = '';
  if (cex && dex && dex.price !== 0) {
    const d = cex.bid - dex.price;
    const pct = d / dex.price * 100;
    diff = (d >= 0 ? '+' : '') + d.toFixed(8);
    diffPct = (pct >= 0 ? '+' : '') + pct.toFixed(3) + '%';
    // Mirrors classify_spread in spread-detector/src/detector.rs: green
    // only once gross spread actually clears this pair's own
    // round-trip cost threshold (a genuine Opportunity), amber if
    // positive but not enough to cover costs (Interesting, not
    // tradeable), red if negative.
    diffClass = pct >= p.threshold_pct ? 'pos' : (pct >= 0 ? 'neutral' : 'neg');
  }
  return '<tr>' +
    '<td class="pair-cell">' + p.pair + '</td>' +
    '<td' + (cex && cex.stale ? ' class="stale"' : '') + '>' + (cex ? fmtPrice(cex.bid) : 'WAITING') + '</td>' +
    '<td' + (dex && dex.stale ? ' class="stale"' : '') + '>' + (dex ? fmtPrice(dex.price) : 'WAITING') + '</td>' +
    '<td class="' + diffClass + '">' + diff + '</td>' +
    '<td class="' + diffClass + '">' + diffPct + '</td>' +
    '<td class="dim">' + p.threshold_pct.toFixed(3) + '%</td>' +
    '<td class="dim">' + fmtAge(cex) + '</td>' +
    '<td class="dim">' + fmtAge(dex) + '</td>' +
  '</tr>';
}

function renderLiquidityRow(p, i, offset) {
  const dex = p.dex;
  return '<tr>' +
    '<td class="pair-cell">' + p.pair + '</td>' +
    '<td>' + (dex ? fmtUsd(dex.tvl_usd) : 'WAITING') + '</td>' +
    '<td>' + (dex ? fmtQty(dex.reserve_base, 2) + ' ' + dex.base_symbol : '--') + '</td>' +
    '<td>' + (dex ? fmtQty(dex.reserve_usdc, 2) + ' USDC' : '--') + '</td>' +
    '<td class="dim">' + (dex ? dex.slot : '--') + '</td>' +
    '<td class="dim">N/A</td>' +
  '</tr>';
}

async function poll() {
  let data;
  try {
    const res = await fetch('/api/all');
    data = await res.json();
  } catch (e) {
    return;
  }

  document.getElementById('body-prices').innerHTML = data.pairs.map(renderPricesRow).join('');
  document.getElementById('body-liquidity').innerHTML = data.pairs
    .map((p, i) => renderLiquidityRow(p, i, data.pairs.length))
    .join('');
}

loadPairButtons();
setInterval(poll, 500);
poll();

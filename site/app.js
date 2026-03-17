/* === TGA Refill Atlas — Interactive Charts === */

const COLORS = {
  tga: '#60a5fa',
  onrrp: '#22c55e',
  mmf: '#a855f7',
  reserves: '#3b82f6',
  deposits: '#f59e0b',
  bankTA: '#ef4444',
  dealer: '#ec4899',
  accent: '#60a5fa',
  muted: '#64748b',
  grid: 'rgba(255,255,255,0.06)',
  gridLight: 'rgba(0,0,0,0.06)',
  event: 'rgba(249,115,22,0.15)',
  binary: '#60a5fa',
  billSurprise: '#ef4444',
};

const CHANNEL_COLORS = {
  'reserve_drain_bn': COLORS.reserves,
  'deposit_drawdown_bn': COLORS.deposits,
  'on_rrp_runoff_bn': COLORS.onrrp,
  'bank_t&a_bn': COLORS.bankTA,
  'mmf_treasury_bn': COLORS.mmf,
  'dealer_repo_bn': COLORS.dealer,
};

const CHANNEL_LABELS = {
  'reserve_drain_bn': 'Reserve drain',
  'deposit_drawdown_bn': 'Deposit drawdown',
  'on_rrp_runoff_bn': 'ON RRP runoff',
  'bank_t&a_bn': 'Bank T&A',
  'mmf_treasury_bn': 'MMF Treasury',
  'dealer_repo_bn': 'Dealer repo',
};

const RESPONSE_LABELS = {
  'reserve_balances_weekly_wednesday': 'Reserves',
  'commercial_bank_deposits_weekly_nsa': 'Bank Deposits',
  'on_rrp_daily_total': 'ON RRP',
  'bank_treasury_and_agency_securities_weekly_nsa': 'Bank T&A',
  'mmf_treasury_holdings': 'MMF Treasury',
  'dealer_treasury_repo': 'Dealer Repo',
};

const RESPONSE_COLORS = {
  'reserve_balances_weekly_wednesday': COLORS.reserves,
  'commercial_bank_deposits_weekly_nsa': COLORS.deposits,
  'on_rrp_daily_total': COLORS.onrrp,
  'bank_treasury_and_agency_securities_weekly_nsa': COLORS.bankTA,
  'mmf_treasury_holdings': COLORS.mmf,
  'dealer_treasury_repo': COLORS.dealer,
};

function isDark() {
  return document.documentElement.dataset.theme !== 'light';
}

function plotlyLayout(title, extra = {}) {
  const dark = isDark();
  return {
    title: { text: title, font: { size: 15, color: dark ? '#e4e7ec' : '#1e293b' } },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: dark ? '#111827' : '#f8fafc',
    font: { family: 'Inter, sans-serif', color: dark ? '#8b95a8' : '#64748b', size: 12 },
    xaxis: { gridcolor: dark ? COLORS.grid : COLORS.gridLight, zeroline: false, ...extra.xaxis },
    yaxis: { gridcolor: dark ? COLORS.grid : COLORS.gridLight, zeroline: true,
             zerolinecolor: dark ? '#374151' : '#cbd5e1', ...extra.yaxis },
    margin: { l: 60, r: 20, t: 50, b: 50 },
    legend: { orientation: 'h', y: -0.18, font: { size: 11 } },
    hovermode: 'x unified',
    ...extra,
  };
}

function plotlyConfig(small = false) {
  if (small) {
    return { responsive: true, displayModeBar: false, staticPlot: false };
  }
  return {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d'],
    displaylogo: false,
  };
}

/* ====== 1. TGA Timeline with Events ====== */
async function renderTimeline() {
  const [timeline, events] = await Promise.all([
    fetch('data/timeline.json').then(r => r.json()),
    fetch('data/events.json').then(r => r.json()),
  ]);

  const shapes = events.map(e => ({
    type: 'rect', xref: 'x', yref: 'paper',
    x0: e.start_date, x1: e.end_date, y0: 0, y1: 1,
    fillcolor: COLORS.event, line: { width: 0 }, layer: 'below',
  }));

  const trace = {
    x: timeline.map(d => d.date),
    y: timeline.map(d => d.tga_bn),
    type: 'scatter', mode: 'lines',
    line: { color: COLORS.tga, width: 1.5 },
    name: 'TGA ($B)',
    hovertemplate: '%{x|%b %Y}<br>TGA: $%{y:.0f}B<extra></extra>',
  };

  Plotly.newPlot('chart-timeline', [trace], plotlyLayout(
    'Treasury General Account — Weekly Balance with Rebuild Events', {
      shapes,
      xaxis: { title: '', rangeslider: { visible: true, thickness: 0.06 } },
      yaxis: { title: 'TGA Balance ($B)' },
      margin: { l: 60, r: 20, t: 50, b: 80 },
    }
  ), plotlyConfig());
}

/* ====== 2. IRF Comparison: Binary vs Bill-Surprise ====== */
async function renderIRFComparison() {
  const data = await fetch('data/lp_comparison.json').then(r => r.json());
  const vars = Object.keys(RESPONSE_LABELS);
  const container = document.getElementById('chart-irf-grid');
  container.innerHTML = '';

  for (const v of vars) {
    const div = document.createElement('div');
    div.style.minHeight = '280px';
    container.appendChild(div);

    const traces = [];
    for (const [spec, color, dash, label] of [
      ['binary', COLORS.binary, 'solid', 'Binary shock'],
      ['bill_surprise', COLORS.billSurprise, 'dash', 'Bill surprise'],
    ]) {
      const rows = data.filter(r => r.response_var === v && r.shock_spec === spec);
      rows.sort((a, b) => a.horizon - b.horizon);
      if (!rows.length) continue;

      traces.push({
        x: rows.map(r => r.horizon), y: rows.map(r => r.beta_bn),
        type: 'scatter', mode: 'lines', name: label,
        line: { color, dash, width: 2 },
        hovertemplate: `h=%{x}<br>β=$%{y:.1f}B<extra>${label}</extra>`,
      });
      // CI band
      traces.push({
        x: [...rows.map(r => r.horizon), ...rows.map(r => r.horizon).reverse()],
        y: [...rows.map(r => r.ci_upper_bn), ...rows.map(r => r.ci_lower_bn).reverse()],
        type: 'scatter', fill: 'toself', fillcolor: color.replace(')', ',0.1)').replace('rgb', 'rgba'),
        line: { color: 'transparent' }, showlegend: false, hoverinfo: 'skip',
      });
    }

    Plotly.newPlot(div, traces, plotlyLayout(RESPONSE_LABELS[v], {
      xaxis: { title: 'Weeks', dtick: 4 },
      yaxis: { title: '' },
      shapes: [{ type: 'line', x0: 0, x1: 0, y0: 0, y1: 1, yref: 'paper',
                 line: { color: '#6b7280', width: 1, dash: 'dot' } }],
      legend: { orientation: 'h', y: 1.12, font: { size: 10 } },
      margin: { l: 50, r: 10, t: 40, b: 40 },
      height: 280,
    }), plotlyConfig(true));
  }
}

/* ====== 3. Attribution Stacked Bars (Top 20) ====== */
async function renderAttribution() {
  const data = await fetch('data/attribution_enriched.json').then(r => r.json());
  const top20 = data.sort((a, b) => b.delta_tga_bn - a.delta_tga_bn).slice(0, 20)
    .sort((a, b) => new Date(a.baseline_date) - new Date(b.baseline_date));

  const x = top20.map(d => d.baseline_date.slice(0, 7));
  const traces = [];

  for (const [key, label] of Object.entries(CHANNEL_LABELS)) {
    traces.push({
      x, y: top20.map(d => d[key] || 0),
      type: 'bar', name: label,
      marker: { color: CHANNEL_COLORS[key] },
      hovertemplate: `${label}: $%{y:.0f}B<extra></extra>`,
    });
  }

  // ΔTGA dots
  traces.push({
    x, y: top20.map(d => d.delta_tga_bn),
    type: 'scatter', mode: 'markers', name: 'ΔTGA',
    marker: { color: '#fff', size: 8, line: { color: '#000', width: 1.5 } },
    hovertemplate: 'ΔTGA: $%{y:.0f}B<extra></extra>',
  });

  Plotly.newPlot('chart-attribution', traces, plotlyLayout(
    'Attribution Decomposition — Top 20 Largest Events', {
      barmode: 'stack',
      xaxis: { title: '', tickangle: -45 },
      yaxis: { title: '$B' },
      margin: { l: 60, r: 20, t: 50, b: 80 },
    }
  ), plotlyConfig(true));
}

/* ====== 4. Era Dominant Source ====== */
async function renderEraBars() {
  const data = await fetch('data/era_summary.json').then(r => r.json());
  const eras = ['2009-13', '2014-19', '2020-22'];
  const sources = [...new Set(data.map(d => d.source))];

  const sourceColors = {
    'Reserve drain': COLORS.reserves,
    'Deposit drawdown': COLORS.deposits,
    'ON RRP runoff': COLORS.onrrp,
    'Bank T&A': COLORS.bankTA,
    'MMF Treasury': COLORS.mmf,
    'Dealer repo': COLORS.dealer,
  };

  const traces = sources.map(s => ({
    x: eras,
    y: eras.map(e => (data.find(d => d.era === e && d.source === s) || {}).count || 0),
    type: 'bar', name: s,
    marker: { color: sourceColors[s] || '#888' },
    hovertemplate: `${s}: %{y}<extra></extra>`,
  }));

  Plotly.newPlot('chart-era', traces, plotlyLayout(
    'Dominant Funding Source by Era', {
      barmode: 'stack',
      xaxis: { title: '' },
      yaxis: { title: 'Number of Events' },
    }
  ), plotlyConfig(true));
}

/* ====== 5. ON RRP Era Panel ====== */
async function renderONRRP() {
  const timeline = await fetch('data/timeline.json').then(r => r.json());
  const events = await fetch('data/events.json').then(r => r.json());
  const post2021 = timeline.filter(d => d.date >= '2021-01-01');
  const post2021Events = events.filter(e => e.start_date >= '2021-01-01');

  const shapes = post2021Events.map(e => ({
    type: 'rect', xref: 'x', yref: 'paper',
    x0: e.start_date, x1: e.end_date, y0: 0, y1: 1,
    fillcolor: COLORS.event, line: { width: 0 }, layer: 'below',
  }));

  const tgaTrace = {
    x: post2021.map(d => d.date), y: post2021.map(d => d.tga_bn),
    type: 'scatter', mode: 'lines', name: 'TGA ($B)',
    line: { color: COLORS.tga, width: 1.5 },
    hovertemplate: '%{x|%b %Y}<br>TGA: $%{y:.0f}B<extra></extra>',
  };

  const onrrpTrace = {
    x: post2021.map(d => d.date), y: post2021.map(d => d.onrrp_bn),
    type: 'scatter', mode: 'lines', name: 'ON RRP ($B)',
    line: { color: COLORS.onrrp, width: 1.5 }, yaxis: 'y2',
    hovertemplate: '%{x|%b %Y}<br>ON RRP: $%{y:.0f}B<extra></extra>',
  };

  const dark = isDark();
  Plotly.newPlot('chart-onrrp', [tgaTrace, onrrpTrace], {
    ...plotlyLayout('TGA and ON RRP During the Facility Era (2021–2026)', {
      shapes,
      yaxis: { title: 'TGA ($B)', side: 'left', gridcolor: dark ? COLORS.grid : COLORS.gridLight },
      yaxis2: { title: 'ON RRP ($B)', side: 'right', overlaying: 'y',
                gridcolor: 'transparent', showgrid: false },
      legend: { orientation: 'h', y: 1.1 },
    }),
    height: 380,
  }, plotlyConfig(true));
}

/* ====== 6. Event Scatter ====== */
async function renderEventScatter() {
  const events = await fetch('data/events.json').then(r => r.json());

  const billHeavy = events.filter(e => e.issuance_mix === 'bill-heavy');
  const mixed = events.filter(e => e.issuance_mix !== 'bill-heavy');

  const makeTrace = (data, name, color) => ({
    x: data.map(d => d.start_date),
    y: data.map(d => d.delta_tga_bn),
    text: data.map(d => `${d.event_id}<br>${d.start_date} to ${d.end_date}<br>Duration: ${d.duration_weeks}w<br>ΔTGA: $${d.delta_tga_bn}B<br>Mix: ${d.issuance_mix}${d.manual_tags ? '<br>Tags: ' + d.manual_tags : ''}`),
    type: 'scatter', mode: 'markers', name,
    marker: {
      color, size: data.map(d => Math.max(d.duration_weeks * 4 + 6, 8)),
      opacity: 0.8, line: { color: 'rgba(255,255,255,0.4)', width: 1 },
    },
    hovertemplate: '%{text}<extra></extra>',
  });

  Plotly.newPlot('chart-events', [
    makeTrace(billHeavy, 'Bill-heavy', COLORS.tga),
    makeTrace(mixed, 'Mixed', COLORS.deposits),
  ], plotlyLayout('Rebuild Events — Size Over Time (bubble = duration)', {
    xaxis: { title: '' },
    yaxis: { title: 'ΔTGA ($B)' },
  }), plotlyConfig(true));
}

/* ====== 7. Pre-trend Scorecard ====== */
async function renderPretrends() {
  const data = await fetch('data/lp_comparison.json').then(r => r.json());
  const container = document.getElementById('pretrend-cards');
  if (!container) return;
  container.innerHTML = '';

  const vars = Object.keys(RESPONSE_LABELS);
  for (const v of vars) {
    const binaryPre = data.filter(r => r.response_var === v && r.shock_spec === 'binary' && r.horizon < 0);
    const billPre = data.filter(r => r.response_var === v && r.shock_spec === 'bill_surprise' && r.horizon < 0);
    const binarySig = binaryPre.filter(r => r.significant_5pct).length;
    const billSig = billPre.filter(r => r.significant_5pct).length;

    const card = document.createElement('div');
    card.className = 'pretrend-card';
    card.innerHTML = `
      <div class="pretrend-label">${RESPONSE_LABELS[v]}</div>
      <div class="pretrend-row">
        <span class="pretrend-spec">Binary:</span>
        <span class="pretrend-count ${binarySig > 0 ? 'bad' : 'good'}">${binarySig}/4</span>
      </div>
      <div class="pretrend-row">
        <span class="pretrend-spec">Bill surprise:</span>
        <span class="pretrend-count ${billSig > 0 ? 'warn' : 'good'}">${billSig}/4</span>
      </div>
    `;
    container.appendChild(card);
  }
}

/* ====== Init ====== */
async function init() {
  await Promise.all([
    renderTimeline(),
    renderIRFComparison(),
    renderAttribution(),
    renderEraBars(),
    renderONRRP(),
    renderEventScatter(),
    renderPretrends(),
  ]);
}

// Re-render on theme change
const observer = new MutationObserver(() => init());
observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });

document.addEventListener('DOMContentLoaded', init);

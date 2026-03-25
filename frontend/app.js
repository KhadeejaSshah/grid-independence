let currentSystemId = null;
let currentSystemName = '';
let targetIndependence = 100;
let lastRecommendation = null;
let lastSystemData = null;

// UI Elements
const searchInput = document.getElementById('systemSearch');
const searchResults = document.getElementById('searchResults');
const welcomeScreen = document.getElementById('welcomeScreen');
const systemOverview = document.getElementById('systemOverview');
const analysisSection = document.getElementById('analysisSection');
const loadingOverlay = document.getElementById('loadingOverlay');
const loadingText = document.getElementById('loadingText');

// ─── DIAL ANIMATION ───
const dialFill = document.getElementById('dialFill');
const dialKnob = document.getElementById('dialKnob');
const ARC_LENGTH = 251.3; // approximate length of the SVG arc

function setDialValue(pct) {
    const ratio = pct / 100;
    const offset = ARC_LENGTH * (1 - ratio);
    dialFill.style.strokeDashoffset = offset;

    // Compute knob position along the arc (180° arc from left to right)
    const angle = Math.PI * (1 - ratio); // π to 0
    const cx = 100 + 80 * Math.cos(angle);
    const cy = 100 - 80 * Math.sin(angle);
    dialKnob.setAttribute('cx', cx);
    dialKnob.setAttribute('cy', cy);

    document.getElementById('currentTarget').textContent = pct;

    // Color shift from blue → green at higher targets
    const hue = 210 + (ratio * 40); // blue to teal
    const color = `hsl(${hue}, 80%, 55%)`;
    dialFill.style.stroke = color;
    dialKnob.style.fill = color;
    document.querySelector('.dial-center-value').style.color = color;
}

// Initialize dial at 100%
setDialValue(100);

// Dial label click
document.querySelectorAll('.dial-label').forEach(label => {
    label.addEventListener('click', () => {
        document.querySelectorAll('.dial-label').forEach(l => l.classList.remove('active'));
        label.classList.add('active');
        targetIndependence = parseInt(label.dataset.value);
        setDialValue(targetIndependence);
    });
});

// ─── SEARCH ───
let searchTimeout;
searchInput.addEventListener('input', (e) => {
    clearTimeout(searchTimeout);
    const query = e.target.value.trim();
    if (query.length < 2) { searchResults.classList.add('hidden'); return; }

    searchTimeout = setTimeout(async () => {
        try {
            const resp = await fetch(`/api/systems?search=${encodeURIComponent(query)}`);
            const data = await resp.json();
            renderSearchResults(data);
        } catch (err) { console.error("Search failed", err); }
    }, 300);
});

function renderSearchResults(systems) {
    if (!systems.length) {
        searchResults.innerHTML = '<div class="search-item">No systems found</div>';
    } else {
        searchResults.innerHTML = systems.map(s => `
            <div class="search-item" onclick="selectSystem('${s.id}', '${(s.name || '').replace(/'/g, "\\'")}')">
                <span class="name">${s.name} (${s.system_no})</span>
                <span class="meta">${s.customer_name || ''} | ${s.location || ''}</span>
            </div>
        `).join('');
    }
    searchResults.classList.remove('hidden');
}

// ─── SYSTEM SELECT ───
async function selectSystem(id, name) {
    currentSystemId = id;
    currentSystemName = name;
    searchResults.classList.add('hidden');
    searchInput.value = name;

    showLoading("Fetching System Data...");
    try {
        const resp = await fetch(`/api/system-data/${id}`);
        if (!resp.ok) throw new Error("Failed to fetch system data");
        lastSystemData = await resp.json();

        displaySystemData(lastSystemData);
        welcomeScreen.classList.add('hidden');
        systemOverview.classList.remove('hidden');
        analysisSection.classList.remove('hidden');
        document.getElementById('resultsContainer').classList.add('hidden');
    } catch (err) {
        alert("Error: " + err.message);
    } finally {
        hideLoading();
    }
}

function displaySystemData(data) {
    const specs = data.specs || {};
    const ie = data.import_export || data.summary_metrics || {};

    const deployed = specs.deployed_at ? specs.deployed_at.split(' ')[0] : 'N/A';
    document.getElementById('systemMeta').textContent = `${specs.customer || ''} | ${specs.location || ''} | Deployed: ${deployed}`;
    document.getElementById('pvCap').textContent = specs.panels_capacity_kw != null ? Number(specs.panels_capacity_kw).toFixed(2) : '-';
    document.getElementById('batCap').textContent = specs.current_battery_kwh != null ? specs.current_battery_kwh : '-';
    document.getElementById('invModel').textContent = specs.inverter_model || '-';
    document.getElementById('dailyImport').textContent = ie.daily_avg_import != null ? Number(ie.daily_avg_import).toFixed(2) : '-';
}

// ─── RUN ANALYSIS ───
document.getElementById('runAnalysis').addEventListener('click', async () => {
    if (!currentSystemId) return;

    showLoading("Generating AI Recommendation...");
    document.getElementById('resultsContainer').classList.add('hidden');

    try {
        const resp = await fetch('/api/recommend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ system_id: currentSystemId, target_independence: targetIndependence })
        });
        lastRecommendation = await resp.json();
        renderRecommendations(lastRecommendation);
    } catch (err) {
        alert("Recommendation failed: " + err.message);
    } finally {
        hideLoading();
    }
});

// ─── RENDER RECOMMENDATIONS ───
function renderRecommendations(res) {
    const container = document.getElementById('resultsContainer');
    container.classList.remove('hidden');

    // Grid Independence Bars
    const giCurrent = res.current_grid_independence || 0;
    const giProjected = res.projected_grid_independence || 0;
    animateBar('giCurrentBar', giCurrent);
    animateBar('giProjectedBar', giProjected);
    document.getElementById('giCurrentVal').textContent = `${Math.round(giCurrent)}%`;
    document.getElementById('giProjectedVal').textContent = `${Math.round(giProjected)}%`;

    // Solar Card
    const s = res.solar || {};
    setBadge('solarBadge', s.status);
    document.getElementById('solarAction').textContent = s.action || '-';
    document.getElementById('solarCurrent').textContent = fmt(s.current_kw);
    document.getElementById('solarRec').textContent = fmt(s.recommended_kw);
    document.getElementById('solarGain').textContent = s.production_gain_kwh != null ? `+${fmt(s.production_gain_kwh)} kWh/day` : '-';

    // Battery Card
    const b = res.battery || {};
    setBadge('batteryBadge', b.status);
    document.getElementById('batteryAction').textContent = b.action || '-';
    document.getElementById('batCurrent').textContent = fmt(b.current_kwh);
    document.getElementById('batRec').textContent = fmt(b.recommended_kwh);
    document.getElementById('batGain').textContent = b.backup_hours_gain != null ? `+${fmt(b.backup_hours_gain)} hrs` : '-';

    // Inverter Card
    const inv = res.inverter || {};
    setBadge('inverterBadge', inv.status);
    document.getElementById('inverterAction').textContent = inv.action || '-';
    document.getElementById('invCurrent').textContent = fmt(inv.current_kw);
    document.getElementById('invRec').textContent = fmt(inv.recommended_kw);

    // Grid Impact
    const gi = res.grid_impact || {};
    document.getElementById('impDailyBefore').textContent = `${fmt(gi.current_daily_import_kwh)} kWh`;
    document.getElementById('impDailyAfter').textContent = `${fmt(gi.projected_daily_import_kwh)} kWh`;
    document.getElementById('impNightBefore').textContent = `${fmt(gi.current_night_import_kwh)} kWh`;
    document.getElementById('impNightAfter').textContent = `${fmt(gi.projected_night_import_kwh)} kWh`;
    document.getElementById('impAnnual').textContent = gi.annual_savings_kwh != null ? `${fmt(gi.annual_savings_kwh)} kWh` : '-';

    // Summary
    document.getElementById('aiSummary').textContent = res.summary || "No summary provided.";

    container.scrollIntoView({ behavior: 'smooth' });
}

function animateBar(id, pct) {
    const el = document.getElementById(id);
    el.style.width = '0%';
    requestAnimationFrame(() => {
        requestAnimationFrame(() => { el.style.width = `${Math.min(pct, 100)}%`; });
    });
}

function setBadge(id, status) {
    const el = document.getElementById(id);
    el.textContent = status || 'N/A';
    el.className = 'status-badge';
    const s = (status || '').toLowerCase();
    if (s === 'ok') el.classList.add('ok');
    else if (s === 'increase' || s === 'upgrade') el.classList.add('increase');
    else if (s === 'repair' || s === 'replace') el.classList.add('warning');
    else el.classList.add('danger');
}

function fmt(v) {
    if (v == null || v === '') return '-';
    const n = Number(v);
    return isNaN(n) ? v : (n % 1 === 0 ? n.toString() : n.toFixed(2));
}

// ─── DOWNLOAD REPORT ───
document.getElementById('downloadReport').addEventListener('click', () => {
    if (!lastRecommendation || !lastSystemData) return;

    const specs = lastSystemData.specs || {};
    const res = lastRecommendation;
    const gi = res.grid_impact || {};
    const s = res.solar || {};
    const b = res.battery || {};
    const inv = res.inverter || {};

    const lines = [
        '═══════════════════════════════════════════════════',
        '         GRID INDEPENDENCE AI REPORT',
        '═══════════════════════════════════════════════════',
        '',
        `System: ${specs.name || currentSystemName}`,
        `Customer: ${specs.customer || '-'}`,
        `Location: ${specs.location || '-'}`,
        `Deployed: ${specs.deployed_at || '-'}`,
        `Target Independence: ${targetIndependence}%`,
        `Generated: ${new Date().toLocaleString()}`,
        '',
        '───────────────────────────────────────────────────',
        '  GRID INDEPENDENCE',
        '───────────────────────────────────────────────────',
        `  Current:   ${Math.round(res.current_grid_independence || 0)}%`,
        `  Projected: ${Math.round(res.projected_grid_independence || 0)}%`,
        '',
        '───────────────────────────────────────────────────',
        '  SOLAR PANELS',
        '───────────────────────────────────────────────────',
        `  Status:      ${s.status || '-'}`,
        `  Action:      ${s.action || '-'}`,
        `  Current:     ${fmt(s.current_kw)} kW`,
        `  Recommended: ${fmt(s.recommended_kw)} kW`,
        `  Daily Gain:  +${fmt(s.production_gain_kwh)} kWh/day`,
        '',
        '───────────────────────────────────────────────────',
        '  BATTERY',
        '───────────────────────────────────────────────────',
        `  Status:      ${b.status || '-'}`,
        `  Action:      ${b.action || '-'}`,
        `  Current:     ${fmt(b.current_kwh)} kWh`,
        `  Recommended: ${fmt(b.recommended_kwh)} kWh`,
        `  Backup Gain: +${fmt(b.backup_hours_gain)} hrs`,
        '',
        '───────────────────────────────────────────────────',
        '  INVERTER',
        '───────────────────────────────────────────────────',
        `  Status:      ${inv.status || '-'}`,
        `  Action:      ${inv.action || '-'}`,
        `  Current:     ${fmt(inv.current_kw)} kW`,
        `  Recommended: ${fmt(inv.recommended_kw)} kW`,
        '',
        '───────────────────────────────────────────────────',
        '  GRID IMPORT IMPACT',
        '───────────────────────────────────────────────────',
        `  Daily Import:  ${fmt(gi.current_daily_import_kwh)} → ${fmt(gi.projected_daily_import_kwh)} kWh`,
        `  Night Import:  ${fmt(gi.current_night_import_kwh)} → ${fmt(gi.projected_night_import_kwh)} kWh`,
        `  Annual Savings: ${fmt(gi.annual_savings_kwh)} kWh`,
        '',
        '───────────────────────────────────────────────────',
        '  SUMMARY',
        '───────────────────────────────────────────────────',
        `  ${res.summary || '-'}`,
        '',
        '═══════════════════════════════════════════════════',
    ];

    const blob = new Blob([lines.join('\n')], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `grid_independence_report_${currentSystemId.substring(0, 8)}.txt`;
    a.click();
    URL.revokeObjectURL(url);
});

// ─── HELPERS ───
function showLoading(text) { loadingText.textContent = text; loadingOverlay.classList.remove('hidden'); }
function hideLoading() { loadingOverlay.classList.add('hidden'); }

document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-container')) searchResults.classList.add('hidden');
});

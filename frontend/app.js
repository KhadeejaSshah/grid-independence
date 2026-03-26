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
        // Re-render with cached recommendation data for the new tier
        if (lastRecommendation) {
            renderRecommendations(lastRecommendation);
        }
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
    console.log("Displaying system data (v3 - CSV priority)...", data);
    const specs = data.specs || {};

    const ie = data.import_export || data.summary_metrics || {};

    const deployed = specs.deployed_at ? specs.deployed_at.split(' ')[0] : 'N/A';
    document.getElementById('systemMeta').textContent = `${specs.customer || ''} | ${specs.location || ''} | Deployed: ${deployed}`;

    // Prioritize CSV summary_metrics over Postgres specs for display
    const currentPV = specs.panels_capacity_kw || null;
    const currentBat = ie.current_battery != null ? ie.current_battery : specs.current_battery_kwh;

    document.getElementById('pvCap').textContent = currentPV != null ? Number(currentPV).toFixed(2) : '-';
    document.getElementById('batCap').textContent = currentBat != null ? Number(currentBat).toFixed(2) : '-';
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

    // Pick the tier matching the selected target independence
    const tierKey = `tier_${targetIndependence}`;
    const tier = (res.tiers && res.tiers[tierKey]) || {};
    const tierSolar = tier.solar || {};
    const tierBatt = tier.battery || {};
    const tierInv = tier.inverter || {};
    const tierGrid = tier.grid_impact || {};

    // Grid Independence Bars — use tier-specific projected value
    const giCurrent = res.current_grid_independence || 0;
    const giProjected = tier.projected_grid_independence != null
        ? tier.projected_grid_independence
        : (res.projected_grid_independence || 0);
    animateBar('giCurrentBar', giCurrent);
    animateBar('giProjectedBar', giProjected);
    document.getElementById('giCurrentVal').textContent = `${Math.round(giCurrent)}%`;
    document.getElementById('giProjectedVal').textContent = `${Math.round(giProjected)}%`;

    // Solar Card — use tier-scaled values
    const s = res.solar || {};
    setBadge('solarBadge', tierSolar.status || s.status);
    document.getElementById('solarAction').textContent = tierSolar.action || s.action || '-';
    document.getElementById('solarCurrent').textContent = fmt(s.current_kw);
    document.getElementById('solarRec').textContent = fmt(tierSolar.recommended_kw || s.recommended_kw_100);
    document.getElementById('solarGain').textContent = tierSolar.production_gain_kwh != null ? `+${fmt(tierSolar.production_gain_kwh)} kWh/day` : '-';

    // Battery Card — use tier-scaled values
    const b = res.battery || {};
    setBadge('batteryBadge', tierBatt.status || b.status);
    document.getElementById('batteryAction').textContent = tierBatt.action || b.action || '-';
    document.getElementById('batCurrent').textContent = fmt(b.current_kwh);
    document.getElementById('batRec').textContent = fmt(tierBatt.recommended_kwh || b.recommended_kwh_100);
    document.getElementById('batGain').textContent = tierBatt.backup_hours_gain != null ? `+${fmt(tierBatt.backup_hours_gain)} hrs` : '-';

    // Inverter Card — use tier-scaled values
    const inv = res.inverter || {};
    setBadge('inverterBadge', tierInv.status || inv.status);
    document.getElementById('inverterAction').textContent = tierInv.action || inv.action || '-';
    document.getElementById('invCurrent').textContent = fmt(inv.current_kw);
    document.getElementById('invRec').textContent = fmt(tierInv.recommended_kw || inv.recommended_kw_100);

    // Grid Impact — use tier-scaled values
    const gi = res.grid_impact || {};
    document.getElementById('impDailyBefore').textContent = `${fmt(gi.current_daily_import_kwh)} kWh`;
    document.getElementById('impDailyAfter').textContent = `${fmt(tierGrid.projected_daily_import_kwh != null ? tierGrid.projected_daily_import_kwh : gi.projected_daily_import_kwh)} kWh`;
    document.getElementById('impNightBefore').textContent = `${fmt(gi.current_night_import_kwh)} kWh`;
    document.getElementById('impNightAfter').textContent = `${fmt(tierGrid.projected_night_import_kwh != null ? tierGrid.projected_night_import_kwh : gi.projected_night_import_kwh)} kWh`;
    document.getElementById('impAnnual').textContent = tierGrid.annual_savings_kwh != null ? `${fmt(tierGrid.annual_savings_kwh)} kWh` : '-';

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

// ─── THEME SWITCHER ───
const themeToggle = document.getElementById('themeToggle');
const htmlElement = document.documentElement;

function setTheme(theme) {
    htmlElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
}

// Check for saved theme or system preference
const savedTheme = localStorage.getItem('theme');
const systemTheme = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
setTheme(savedTheme || systemTheme);

if (themeToggle) {
    themeToggle.addEventListener('click', () => {
        const currentTheme = htmlElement.getAttribute('data-theme');
        setTheme(currentTheme === 'dark' ? 'light' : 'dark');
    });
}

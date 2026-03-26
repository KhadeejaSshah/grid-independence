let currentSystemId = null;
let currentSystemName = '';
let targetIndependence = 100;
let lastRecommendation = null;
let lastSystemData = null;
let analysisHasRun = false;

// UI Elements
const searchInput = document.getElementById('systemSearch');
const searchResults = document.getElementById('searchResults');
const welcomeScreen = document.getElementById('welcomeScreen');
const systemOverview = document.getElementById('systemOverview');
const analysisSection = document.getElementById('analysisSection');
const loadingOverlay = document.getElementById('loadingOverlay');
const loadingText = document.getElementById('loadingText');

// ─── DIAL SYSTEM ───
const dialFill = document.getElementById('dialFill');
const dialKnob = document.getElementById('dialKnob');
const dialSvg = document.querySelector('.dial-svg');
const ARC_LENGTH = 251.3;
const TIER_DETENTS = [25, 50, 75, 100];

function setDialValue(pct) {
    const ratio = pct / 100;
    const offset = ARC_LENGTH * (1 - ratio);
    dialFill.style.strokeDashoffset = offset;

    const angle = Math.PI * (1 - ratio);
    const cx = 100 + 80 * Math.cos(angle);
    const cy = 100 - 80 * Math.sin(angle);
    dialKnob.setAttribute('cx', cx);
    dialKnob.setAttribute('cy', cy);

    document.getElementById('currentTarget').textContent = pct;
}

function snapToTier(pct) {
    // Snap to closest detent
    let closest = TIER_DETENTS[0];
    let minDist = Infinity;
    for (const d of TIER_DETENTS) {
        const dist = Math.abs(pct - d);
        if (dist < minDist) { minDist = dist; closest = d; }
    }
    return closest;
}

function selectTier(value) {
    targetIndependence = value;
    setDialValue(value);
    document.querySelectorAll('.dial-label').forEach(l => {
        l.classList.toggle('active', parseInt(l.dataset.value) === value);
    });
    if (analysisHasRun && lastRecommendation) {
        renderRecommendations(lastRecommendation);
    }
}

// Initialize
setDialValue(100);
updateDialLabelsState();

function updateDialLabelsState() {
    document.querySelectorAll('.dial-label').forEach(l => {
        l.classList.toggle('disabled', !analysisHasRun);
    });
}

// Label clicks — only work after analysis
document.querySelectorAll('.dial-label').forEach(label => {
    label.addEventListener('click', () => {
        if (!analysisHasRun) return;
        selectTier(parseInt(label.dataset.value));
    });
});

// ─── DRAGGABLE DIAL ───
(function initDraggableDial() {
    let isDragging = false;

    function getAngleFromEvent(e) {
        const rect = dialSvg.getBoundingClientRect();
        // Center of arc in screen coords (SVG viewBox 0 0 200 120, center at 100,100)
        const scaleX = rect.width / 200;
        const scaleY = rect.height / 120;
        const centerX = rect.left + 100 * scaleX;
        const centerY = rect.top + 100 * scaleY;

        const clientX = e.touches ? e.touches[0].clientX : e.clientX;
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        const dx = clientX - centerX;
        const dy = centerY - clientY; // inverted Y
        let angle = Math.atan2(dy, dx); // radians
        // Clamp to 0..π (the arc range)
        if (angle < 0) angle = 0;
        if (angle > Math.PI) angle = Math.PI;
        // Convert: angle π=0%, angle 0=100%
        return Math.round((1 - angle / Math.PI) * 100);
    }

    function onMove(e) {
        if (!isDragging) return;
        e.preventDefault();
        const raw = getAngleFromEvent(e);
        const snapped = snapToTier(raw);
        setDialValue(raw); // smooth follow
        // Update label highlight to nearest
        document.querySelectorAll('.dial-label').forEach(l => {
            l.classList.toggle('active', parseInt(l.dataset.value) === snapped);
        });
    }

    function onEnd(e) {
        if (!isDragging) return;
        isDragging = false;
        document.body.style.userSelect = '';
        const raw = getAngleFromEvent(e.changedTouches ? e.changedTouches[0] : e);
        const snapped = snapToTier(raw);
        selectTier(snapped);
    }

    dialSvg.addEventListener('mousedown', (e) => {
        isDragging = true;
        document.body.style.userSelect = 'none';
        onMove(e);
    });
    dialSvg.addEventListener('touchstart', (e) => {
        isDragging = true;
        onMove(e);
    }, { passive: false });

    document.addEventListener('mousemove', onMove);
    document.addEventListener('touchmove', onMove, { passive: false });
    document.addEventListener('mouseup', onEnd);
    document.addEventListener('touchend', onEnd);

    // Click on arc to jump
    dialSvg.style.cursor = 'pointer';
})();

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
        // Reset analysis state for new system
        analysisHasRun = false;
        lastRecommendation = null;
        updateDialLabelsState();
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
        analysisHasRun = true;
        updateDialLabelsState();
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
    animateCountUp('giCurrentVal', giCurrent, '%', 0);
    animateCountUp('giProjectedVal', giProjected, '%', 0);

    // Solar Card — use tier-scaled values
    const s = res.solar || {};
    setBadge('solarBadge', tierSolar.status || s.status);
    document.getElementById('solarAction').textContent = tierSolar.action || s.action || '-';
    document.getElementById('solarCurrent').textContent = fmt(s.current_kw);
    animateCountUp('solarRec', tierSolar.recommended_kw || s.recommended_kw_100, '', 2);
    document.getElementById('solarGain').textContent = tierSolar.production_gain_kwh != null ? `+${fmt(tierSolar.production_gain_kwh)} kWh/day` : '-';

    // Battery Card — use tier-scaled values
    const b = res.battery || {};
    setBadge('batteryBadge', tierBatt.status || b.status);
    document.getElementById('batteryAction').textContent = tierBatt.action || b.action || '-';
    document.getElementById('batCurrent').textContent = fmt(b.current_kwh);
    animateCountUp('batRec', tierBatt.recommended_kwh || b.recommended_kwh_100, '', 2);
    document.getElementById('batGain').textContent = tierBatt.backup_hours_gain != null ? `+${fmt(tierBatt.backup_hours_gain)} hrs` : '-';

    // Inverter Card — use tier-scaled values
    const inv = res.inverter || {};
    setBadge('inverterBadge', tierInv.status || inv.status);
    document.getElementById('inverterAction').textContent = tierInv.action || inv.action || '-';
    document.getElementById('invCurrent').textContent = fmt(inv.current_kw);
    animateCountUp('invRec', tierInv.recommended_kw || inv.recommended_kw_100, '', 2);

    // Grid Impact — use tier-scaled values
    const gi = res.grid_impact || {};
    document.getElementById('impDailyBefore').textContent = `${fmt(gi.current_daily_import_kwh)} kWh`;
    document.getElementById('impDailyAfter').textContent = `${fmt(tierGrid.projected_daily_import_kwh != null ? tierGrid.projected_daily_import_kwh : gi.projected_daily_import_kwh)} kWh`;
    document.getElementById('impNightBefore').textContent = `${fmt(gi.current_night_import_kwh)} kWh`;
    document.getElementById('impNightAfter').textContent = `${fmt(tierGrid.projected_night_import_kwh != null ? tierGrid.projected_night_import_kwh : gi.projected_night_import_kwh)} kWh`;
    animateCountUp('impAnnual', tierGrid.annual_savings_kwh, ' kWh', 0);
    animateCountUp('impMonthlyPKR', tierGrid.monthly_savings_pkr, ' PKR', 0, true);
    animateCountUp('impAnnualPKR', tierGrid.annual_savings_pkr, ' PKR', 0, true);

    // Summary
    document.getElementById('aiSummary').textContent = res.summary || "No summary provided.";

    // Stagger card entrance animations
    triggerCardAnimations(container);
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

// ─── COUNT-UP ANIMATION ───
function animateCountUp(elId, value, suffix = '', decimals = 0, useLocale = false) {
    const el = document.getElementById(elId);
    if (value == null || isNaN(Number(value))) { el.textContent = '-'; return; }
    const target = Number(value);
    const duration = 800;
    const startTime = performance.now();

    function tick(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        // Ease out cubic
        const eased = 1 - Math.pow(1 - progress, 3);
        const current = target * eased;
        if (useLocale) {
            el.textContent = Math.round(current).toLocaleString() + suffix;
        } else {
            el.textContent = (decimals > 0 ? current.toFixed(decimals) : Math.round(current).toString()) + suffix;
        }
        if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
}

// ─── STAGGERED CARD ANIMATIONS ───
function triggerCardAnimations(container) {
    const animatable = container.querySelectorAll('.rec-card, .gi-bar-section, .grid-impact-section, .summary-box, .btn-download');
    animatable.forEach((el, i) => {
        el.style.opacity = '0';
        el.style.transform = 'translateY(20px)';
        setTimeout(() => {
            el.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
            el.style.opacity = '1';
            el.style.transform = 'translateY(0)';
        }, i * 80);
    });
}

// ─── CURSOR GLOW FOLLOWER ───
(function initCursorGlow() {
    const glow = document.createElement('div');
    glow.className = 'cursor-glow';
    document.body.appendChild(glow);

    let mouseX = -200, mouseY = -200;
    let glowX = -200, glowY = -200;

    document.addEventListener('mousemove', (e) => {
        mouseX = e.clientX;
        mouseY = e.clientY;
    });

    function animate() {
        // Smooth lerp
        glowX += (mouseX - glowX) * 0.15;
        glowY += (mouseY - glowY) * 0.15;
        glow.style.left = glowX + 'px';
        glow.style.top = glowY + 'px';
        requestAnimationFrame(animate);
    }
    animate();
})();

// ─── FLOATING BACKGROUND PARTICLES ───
(function initParticles() {
    const canvas = document.createElement('canvas');
    canvas.className = 'bg-particles';
    document.body.prepend(canvas);
    const ctx = canvas.getContext('2d');

    let w, h;
    function resize() {
        w = canvas.width = window.innerWidth;
        h = canvas.height = window.innerHeight;
    }
    resize();
    window.addEventListener('resize', resize);

    const PARTICLE_COUNT = 35;
    const particles = [];
    for (let i = 0; i < PARTICLE_COUNT; i++) {
        particles.push({
            x: Math.random() * w,
            y: Math.random() * h,
            r: Math.random() * 2 + 0.5,
            vx: (Math.random() - 0.5) * 0.3,
            vy: (Math.random() - 0.5) * 0.3,
            alpha: Math.random() * 0.3 + 0.05
        });
    }

    function draw() {
        ctx.clearRect(0, 0, w, h);
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';

        for (const p of particles) {
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < 0) p.x = w;
            if (p.x > w) p.x = 0;
            if (p.y < 0) p.y = h;
            if (p.y > h) p.y = 0;

            ctx.beginPath();
            ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
            ctx.fillStyle = isDark
                ? `rgba(99, 102, 241, ${p.alpha})`
                : `rgba(99, 102, 241, ${p.alpha * 0.6})`;
            ctx.fill();
        }

        // Draw subtle connecting lines between nearby particles
        for (let i = 0; i < particles.length; i++) {
            for (let j = i + 1; j < particles.length; j++) {
                const dx = particles[i].x - particles[j].x;
                const dy = particles[i].y - particles[j].y;
                const dist = Math.sqrt(dx * dx + dy * dy);
                if (dist < 150) {
                    ctx.beginPath();
                    ctx.moveTo(particles[i].x, particles[i].y);
                    ctx.lineTo(particles[j].x, particles[j].y);
                    ctx.strokeStyle = isDark
                        ? `rgba(99, 102, 241, ${0.06 * (1 - dist / 150)})`
                        : `rgba(99, 102, 241, ${0.04 * (1 - dist / 150)})`;
                    ctx.lineWidth = 0.5;
                    ctx.stroke();
                }
            }
        }

        requestAnimationFrame(draw);
    }
    draw();
})();

// ─── THEME SWITCHER ───
const themeToggle = document.getElementById('themeToggle');
const htmlElement = document.documentElement;

function setTheme(theme) {
    htmlElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
}

const savedTheme = localStorage.getItem('theme');
const systemTheme = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
setTheme(savedTheme || systemTheme);

if (themeToggle) {
    themeToggle.addEventListener('click', () => {
        const currentTheme = htmlElement.getAttribute('data-theme');
        setTheme(currentTheme === 'dark' ? 'light' : 'dark');
    });
}

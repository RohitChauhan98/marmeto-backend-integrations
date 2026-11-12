/* Marmeto Shipment Gateway — operations dashboard (no build step). */

const $ = (sel) => document.querySelector(sel);

const state = { courierMode: '', seed: '', lastOrderId: null, lastWebhookId: null };

function fmtTime(ms) {
  if (!ms) return '—';
  return new Date(ms).toLocaleTimeString();
}

function badge(text, kind) {
  return `<span class="badge ${kind || text}">${text}</span>`;
}

async function api(path, options) {
  const res = await fetch(path, options);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `HTTP ${res.status}`);
  return json;
}

/* ---------- overview + health ---------- */

async function refreshOverview() {
  try {
    const data = await api('/admin/api/overview');
    $('#dot-server').classList.add('on');
    $('#dot-courier').classList.toggle('on', data.courier.reachable);
    $('#dot-courier').classList.toggle('off', !data.courier.reachable);

    const s = data.stats;
    $('#stat-orders').textContent = s.orders;
    $('#stat-deliveries').textContent = s.deliveries;
    $('#stat-confirmed').textContent = s.shipments.confirmed || 0;
    $('#stat-uncertain').textContent =
      (s.shipments.uncertain || 0) + (s.shipments.processing || 0);
    $('#stat-pending').textContent = s.jobs.pending || 0;
    $('#stat-retrying').textContent = s.jobs.retrying || 0;
    $('#stat-dead').textContent = s.jobs.dead_letter || 0;

    const modeLabel = $('#courier-mode');
    const current = state.courierMode;
    modeLabel.textContent = current || 'probabilistic';
    modeLabel.className = `badge ${current ? 'dead_letter' : 'confirmed'}`;
  } catch {
    $('#dot-server').classList.remove('on');
    $('#dot-courier').classList.remove('on');
  }
}

/* ---------- orders + deliveries ---------- */

const SHIPMENT_KIND = {
  confirmed: 'confirmed',
  pending: 'pending',
  processing: 'processing',
  uncertain: 'uncertain',
  lookup_empty: 'retrying',
  failed: 'failed',
};
const JOB_KIND = {
  completed: 'confirmed',
  pending: 'pending',
  retrying: 'retrying',
  processing: 'processing',
  dead_letter: 'dead_letter',
};

async function refreshOrders() {
  const data = await api('/admin/api/orders?limit=30');
  const tbody = $('#orders tbody');
  tbody.innerHTML = '';
  $('#orders-empty').hidden = data.orders.length > 0;
  for (const row of data.orders) {
    const next = row.next_attempt_at && row.job_state !== 'completed'
      ? fmtTime(row.next_attempt_at)
      : '—';
    tbody.insertAdjacentHTML(
      'beforeend',
      `<tr>
        <td class="mono">${row.id}</td>
        <td>${badge(row.shipment_status || '—', SHIPMENT_KIND[row.shipment_status])}</td>
        <td class="mono">${row.courier_shipment_id || '<span class="muted">—</span>'}</td>
        <td>${badge(row.job_state || '—', JOB_KIND[row.job_state])}</td>
        <td>${row.attempts ?? 0}/${row.max_attempts ?? '—'}</td>
        <td class="muted">${next}</td>
        <td class="muted truncate" title="${(row.last_error || '').replace(/"/g, '&quot;')}">${row.last_error || '—'}</td>
        <td></td>
      </tr>`
    );
  }

  const dtbody = $('#deliveries tbody');
  dtbody.innerHTML = '';
  for (const d of data.deliveries) {
    dtbody.insertAdjacentHTML(
      'beforeend',
      `<tr>
        <td class="mono">${d.webhook_id}</td>
        <td class="mono">${d.order_id}</td>
        <td>${badge(d.status, 'confirmed')}</td>
        <td class="muted">${fmtTime(d.received_at)}</td>
      </tr>`
    );
  }
}

/* ---------- dead letters ---------- */

async function refreshDeadLetters() {
  const data = await api('/admin/api/dead-letters');
  const tbody = $('#dead tbody');
  tbody.innerHTML = '';
  $('#dead-empty').hidden = data.jobs.length > 0;
  for (const job of data.jobs) {
    let shipment = '—';
    try {
      const orders = await api('/admin/api/orders?limit=200');
      const match = orders.orders.find((o) => o.id === job.order_id);
      shipment = match ? `${match.shipment_status}` : '—';
    } catch {
      /* leave dash */
    }
    tbody.insertAdjacentHTML(
      'beforeend',
      `<tr>
        <td class="mono truncate" title="${job.id}">${job.id.slice(0, 13)}…</td>
        <td class="mono">${job.order_id}</td>
        <td>${job.attempts}/${job.max_attempts}</td>
        <td class="muted truncate" title="${(job.last_error || '').replace(/"/g, '&quot;')}">${job.last_error || '—'}</td>
        <td>${badge(shipment, SHIPMENT_KIND[shipment])}</td>
        <td><button class="replay-btn" data-job="${job.id}">Replay</button></td>
      </tr>`
    );
  }
  for (const btn of tbody.querySelectorAll('.replay-btn')) {
    btn.addEventListener('click', () => replayDeadLetter(btn.dataset.job));
  }
}

async function replayDeadLetter(jobId) {
  try {
    await api(`/admin/api/jobs/${encodeURIComponent(jobId)}/replay`, { method: 'POST' });
    flash($('#courier-result'), `replayed job ${jobId.slice(0, 8)}…`, 'good');
  } catch (error) {
    flash($('#courier-result'), error.message, 'error');
  }
  await Promise.all([refreshDeadLetters(), refreshOrders(), refreshOverview()]);
}

/* ---------- actions ---------- */

function flash(el, text, kind) {
  el.textContent = text;
  el.className = `result ${kind || ''}`;
  clearTimeout(el.__timer);
  el.__timer = setTimeout(() => {
    el.textContent = '';
    el.className = 'result';
  }, 6000);
}

document.querySelectorAll('.btn[data-mode]').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const mode = btn.dataset.mode;
    state.seed = $('#seed').value.trim() || `${mode}-${Date.now()}`;
    const payload = { mode, seed: state.seed };
    if (state.lastOrderId && mode !== 'valid') payload.orderId = state.lastOrderId;
    if (state.lastWebhookId && mode === 'duplicate') payload.webhookId = state.lastWebhookId;
    try {
      const data = await api('/admin/api/send-webhook', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const lines = data.results.map((r) => `HTTP ${r.status}: ${JSON.stringify(r.body)}`);
      const bad = data.results.some((r) => r.status >= 400);
      flash(
        $('#send-result'),
        `${mode} → ${data.results.length} delivery(ies)\n${lines.join('\n')}`,
        bad ? 'error' : 'good'
      );
      if (data.results[0]?.body?.jobId) state.lastOrderId = null;
      if (mode !== 'invalid') state.lastWebhookId = null;
    } catch (error) {
      flash($('#send-result'), error.message, 'error');
    }
    await Promise.all([refreshOrders(), refreshOverview(), refreshDeadLetters()]);
  });
});

document.querySelectorAll('.btn.mode').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const mode = btn.dataset.cmode;
    try {
      const data = await api('/admin/api/courier/mode', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mode }),
      });
      state.courierMode = mode;
      document
        .querySelectorAll('.btn.mode')
        .forEach((b) => b.classList.toggle('active', b === btn));
      flash($('#courier-result'), `courier mode: ${data.mode}`, 'good');
    } catch (error) {
      flash($('#courier-result'), error.message, 'error');
    }
    await refreshOverview();
  });
});

$('#btn-reconcile').addEventListener('click', async () => {
  const box = $('#reconcile-result');
  box.hidden = false;
  box.textContent = 'running reconciliation…';
  try {
    const data = await api('/admin/api/reconcile', { method: 'POST' });
    box.textContent =
      `orders examined:     ${data.ordersExamined}\n` +
      `shipments recovered: ${data.shipmentsRecovered}\n` +
      `jobs requeued:       ${data.jobsRequeued}\n` +
      `errors encountered:  ${data.errorsEncountered}\n` +
      data.details.map((d) => `  - ${d}`).join('\n');
  } catch (error) {
    box.textContent = `error: ${error.message}`;
  }
  await Promise.all([refreshOrders(), refreshDeadLetters(), refreshOverview()]);
});

/* ---------- polling loop ---------- */

async function refreshAll() {
  await Promise.allSettled([refreshOverview(), refreshOrders(), refreshDeadLetters()]);
}
refreshAll();
setInterval(refreshAll, 2000);

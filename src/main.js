// recon_status checkpoints emitted to InboundInteractionLogger at each stage
const pipeline = [
  {
    phase: 'Webhook',
    nodes: [
      {
        fn: 'webhook_receive()',
        file: 'sendgrid_webhook/sendgrid_email_sync.py',
        desc: 'SendGrid HTTP webhook receives raw email. Stores to S3.',
        status: 'SCANNED',
        email_type: 'INBOUND',
      },
    ],
  },
  {
    phase: 'Lambda — Email Processor',
    nodes: [
      {
        fn: 'handler() → email_sync_consumer()',
        file: 'sendgrid_webhook/email_sync_consumer.py',
        desc: 'SQS triggers Lambda. Parses email, resolves building_id, inserts to all_mail.',
        status: 'QUEUED',
        email_type: 'INBOUND',
      },
      {
        fn: 'handle_classify()',
        file: 'eliseai_classify',
        desc: 'Extracts structured lead info — name, email, building, intent — as classifier_output.',
        status: 'CLASSIFIED',
        email_type: 'INBOUND',
        branch: { label: 'classify fails', text: 'FAILED_CLASSIFY → stop' },
      },
      {
        fn: 'StateMatcher.match()',
        file: 'event_api/components/state_matcher.py',
        desc: 'Looks up existing state via mc_id or state_id. Checks Redis cache first, then PostgreSQL.',
        tags: ['Redis', 'mc_id lookup', 'state_id lookup'],
        branch: { label: 'no user found', text: 'UNMATCHED → stop' },
      },
      {
        fn: 'create_event_api_action() → EventAPIClient.handle_action_async()',
        file: 'email_sync_consumer_operations.py:272 / event_api_client.py:143',
        desc: 'Builds InboundEmailAction and pushes to SQS FIFO queue, grouped by state_id.',
        status: 'SENT_TO_EVENTAPI',
        email_type: 'INBOUND',
        tags: ['prod-event-api-request.fifo'],
      },
    ],
  },
  {
    phase: null,
    async: true,
    label: 'SQS → Lambda',
  },
  {
    phase: 'Lambda — Event API Consumer',
    nodes: [
      {
        fn: 'RequestHandler.handle() → EventApi.handle_action()',
        file: 'event_api/request_handler.py:37 / event_api.py:371',
        desc: 'Deserializes action from SQS. Emits RECEIVED_BY_EVENTAPI tracking. Runs XL, staging, and no-op routing checks.',
        status: 'RECEIVED_BY_EVENTAPI',
        email_type: 'INBOUND',
        branch: { label: 'routed / no-op', text: 'EVENTAPI_FILTERED → stop' },
      },
      {
        fn: 'EventApi._handle_action() → DataRouter.process_input_events()',
        file: 'event_api/event_api.py:970 / data_router.py:555',
        desc: 'Strips invalid phone/name. Deduplicates. Acquires state lock. Fastens email event to state. Runs NLU + prediction pipeline.',
        tags: ['_strip_action', 'fasten_events', '_transform_state'],
      },
      {
        fn: 'PostProcessor → StateIO.upsert_state()',
        file: 'ConversationAI/post_processing.py:68 / StateIO.py:285',
        desc: 'Persists final state to PostgreSQL automation_db. Backs up to MongoDB. Updates ElasticSearch slim index.',
        tags: ['PostgreSQL', 'MongoDB', 'ElasticSearch'],
      },
    ],
    terminal_statuses: [
      { status: 'EVENTAPI_PROCESSED', color: 'green', desc: 'Successfully processed' },
      { status: 'EVENTAPI_DELAYED', color: 'yellow', desc: 'Response delayed' },
      { status: 'EVENTAPI_FILTERED', color: 'red', desc: 'Filtered / dropped' },
    ],
  },
]

const outputs = [
  {
    stream: 'snowflake_inbound_email',
    logger: 'InboundEventsLogger',
    table: 'INBOUND_EMAIL_EVENTS',
    desc: 'One row per email event — message_id, thread_id, addresses, state_id, inbox_type, campaign_id.',
    color: 'blue',
  },
  {
    stream: 'inbound_interaction_events',
    logger: 'InboundInteractionLogger',
    table: 'INBOUND_INTERACTION_EVENTS',
    desc: 'One row per lifecycle step. Stable interaction_id (UUID5 on provider+message_id) links SCANNED → QUEUED → CLASSIFIED → SENT_TO_EVENTAPI → EVENTAPI_PROCESSED.',
    color: 'green',
  },
]

function statusBadge(status, email_type) {
  if (!status) return ''
  const typeStr = email_type ? ` <span class="badge-type">${email_type}</span>` : ''
  return `<div class="status-row"><span class="status-badge">${status}</span>${typeStr}<span class="logger-name">→ InboundInteractionLogger</span></div>`
}

function renderNode(n) {
  return `
    <div class="node">
      <div class="node-fn">${n.fn}</div>
      <div class="node-file">${n.file}</div>
      <div class="node-desc">${n.desc}</div>
      ${n.tags ? `<div class="tags">${n.tags.map(t => `<span class="tag">${t}</span>`).join('')}</div>` : ''}
      ${statusBadge(n.status, n.email_type)}
      ${n.branch ? `<div class="branch-inline"><span class="branch-label">${n.branch.label}</span><span class="branch-text">${n.branch.text}</span></div>` : ''}
    </div>`
}

function renderPhase(p) {
  if (p.async) {
    return `<div class="async-boundary"><span>${p.label}</span></div>`
  }
  const terminalHtml = p.terminal_statuses ? `
    <div class="terminal-statuses">
      ${p.terminal_statuses.map(t => `
        <div class="terminal-status ${t.color}">
          <span class="ts-status">${t.status}</span>
          <span class="ts-desc">${t.desc}</span>
          <span class="logger-name">→ InboundInteractionLogger</span>
        </div>`).join('')}
    </div>` : ''
  return `
    <div class="phase">
      <div class="phase-label">${p.phase}</div>
      <div class="phase-nodes">
        ${p.nodes.map((n, i) => renderNode(n) + (i < p.nodes.length - 1 ? '<div class="arrow"></div>' : '')).join('')}
        ${terminalHtml}
      </div>
    </div>`
}

function renderOutput(o) {
  return `
    <div class="output-node ${o.color}">
      <div class="output-meta">
        <span class="output-logger">${o.logger}</span>
        <span class="output-stream">firehose: ${o.stream}</span>
      </div>
      <div class="output-table">${o.table}</div>
      <div class="node-desc">${o.desc}</div>
    </div>`
}

const pages = [
  { title: 'Pipeline',      href: 'index.html',                  desc: 'Full Event API call chain — webhook → SQS → EventApi → StateIO → Snowflake.' },
  { title: 'Email Flow',    href: 'email-flow.html',              desc: 'Original 5-step inbound flow from SES receipt to state resolution.' },
  { title: 'Action Models', href: 'event-api-actions.html',       desc: 'All action types that flow through RequestHandler — inbound, agent, resident, AI.' },
  { title: 'Null %',        href: 'null-timeseries.html',         desc: 'Daily null rate per column in INBOUND_INTERACTION_EVENTS, by building and org.' },
  { title: 'Email Nulls',   href: 'null-emails/index.html',       desc: 'Full null analysis from email_null_analysis.ipynb — funnel, audit, drop-off.' },
  { title: 'Notebook',      href: 'http://localhost:8888/lab/tree/snowflake/notebooks/email_null_analysis.ipynb', desc: 'email_null_analysis.ipynb — live Snowflake queries (local only).', external: true },
]

document.querySelector('#app').innerHTML = `
  <h1>Inbound Email → INBOUND_INTERACTION_EVENTS</h1>
  <p class="page-subtitle">EliseAI · Email tracking infrastructure &amp; data quality</p>

  <div class="index-grid">
    ${pages.map(p => `
      <a href="${p.href}" class="index-card${p.external ? ' external' : ''}" ${p.external ? 'target="_blank"' : ''}>
        <div class="index-card-title">${p.title}${p.external ? ' ↗' : ''}</div>
        <div class="index-card-desc">${p.desc}</div>
      </a>`).join('')}
  </div>

  <div class="section-title" style="margin-top:2rem">Pipeline Trace</div>

  <div class="flow">
    ${pipeline.map((p, i) => {
      const phaseHtml = renderPhase(p)
      const nextIsAsync = pipeline[i + 1]?.async
      const isAsync = p.async
      const isLast = i === pipeline.length - 1
      return phaseHtml + (!isLast && !isAsync && !nextIsAsync ? '<div class="arrow"></div>' : '')
    }).join('')}
  </div>

  <div class="kinesis-section">
    <div class="kinesis-label">Kinesis Firehose → Snowflake</div>
    <div class="outputs">
      ${outputs.map(renderOutput).join('')}
    </div>
  </div>

  <div class="pr-note">
    <span class="pr-tag">PR #92038</span>
    Normalizing <code>email_type</code>: <code>CRM_INBOUND</code> → <code>INBOUND</code> on all inbound paths &nbsp;·&nbsp; <code>OUTBOUND</code> added to all outbound paths in email_sender.py
  </div>
`

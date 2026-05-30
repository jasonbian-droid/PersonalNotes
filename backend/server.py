import sys, json, logging, warnings
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

warnings.filterwarnings('ignore')
logging.getLogger('snowflake.connector').setLevel(logging.ERROR)
sys.path.insert(0, '/Users/jason.bian/EliseAI')

from snowflake.sf_connect import connect

PORT = 8899
conn = connect(role='ALL_DEV')

RAW = 'ELISE.PUBLIC.INBOUND_INTERACTION_EVENTS'
STG = 'ELISE.DEV_JASONBIAN.STG_INBOUND_INTERACTION_EVENTS'


def _stg_available():
    try:
        c = conn.cursor()
        c.execute(f'select 1 from {STG} limit 1')
        c.fetchone()
        return True
    except Exception:
        return False


STG_OK = _stg_available()

SOURCE = STG if STG_OK else f"""(
  with n as (
    select id, interaction_id, channel, provider, message_id,
      nullif(sender,'') as sender, nullif(recipient,'') as recipient, nullif(subject,'') as subject,
      org_id, building_id, state_id, recon_status,
      case email_type when 'CRM_INBOUND' then 'INBOUND' when 'CRM_OUTBOUND' then 'OUTBOUND' else email_type end as email_type,
      gap_reason, try_to_timestamp(emitted_at) as emitted_at,
      row_number() over (partition by interaction_id, recon_status, coalesce(gap_reason,'') order by try_to_timestamp(emitted_at)) as _rk
    from {RAW}
  )
  select * from n where _rk = 1
)"""


def rows(sql, params):
    c = conn.cursor()
    c.execute(sql, params)
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in c.fetchall()]


def conv(o):
    import decimal, datetime
    if isinstance(o, dict):
        return {k: conv(v) for k, v in o.items()}
    if isinstance(o, list):
        return [conv(v) for v in o]
    if isinstance(o, decimal.Decimal):
        return float(o)
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    return o


def resolve_ids(term):
    found = rows(f"""
        select distinct interaction_id
        from {SOURCE}
        where interaction_id = %(t)s or id = %(t)s or state_id = %(t)s or message_id = %(t)s
        limit 25
    """, {'t': term})
    if found:
        return [r['INTERACTION_ID'] for r in found if r['INTERACTION_ID']]
    like = rows(f"""
        select distinct interaction_id
        from {SOURCE}
        where interaction_id ilike %(p)s or message_id ilike %(p)s or sender ilike %(p)s or recipient ilike %(p)s
        limit 25
    """, {'p': f'%{term}%'})
    return [r['INTERACTION_ID'] for r in like if r['INTERACTION_ID']]


def summaries(ids):
    if not ids:
        return []
    placeholders = ",".join(f"%(i{n})s" for n in range(len(ids)))
    params = {f'i{n}': v for n, v in enumerate(ids)}
    data = rows(f"""
        with s as (select * from {SOURCE} where interaction_id in ({placeholders}))
        select interaction_id,
               any_value(provider) as provider,
               max_by(recon_status, emitted_at) as terminal_recon_status,
               count(*) as lifecycle_steps,
               max(iff(state_id is not null,1,0)) as resident_resolved,
               datediff('second', min(emitted_at), max(emitted_at)) as duration_s
        from s group by 1
        order by lifecycle_steps desc
    """, params)
    return conv(data)


def trace(iid):
    steps = rows(f"""
        select interaction_id, recon_status, emitted_at, gap_reason, state_id,
               provider, channel, email_type, sender, recipient, subject, org_id, building_id, message_id
        from {SOURCE}
        where interaction_id = %(i)s
        order by emitted_at
    """, {'i': iid})
    if not steps:
        return None
    f = steps[0]
    dur = rows(f"""
        select datediff('second', min(emitted_at), max(emitted_at)) as d,
               max(iff(state_id is not null,1,0)) as rr,
               max_by(recon_status, emitted_at) as term
        from {SOURCE} where interaction_id = %(i)s
    """, {'i': iid})[0]
    return conv({
        'interaction_id': iid,
        'provider': f['PROVIDER'],
        'terminal_recon_status': dur['TERM'],
        'lifecycle_steps': len(steps),
        'resident_resolved': bool(dur['RR']),
        'duration_s': dur['D'],
        'subject': f['SUBJECT'], 'sender': f['SENDER'], 'recipient': f['RECIPIENT'],
        'email_type': f['EMAIL_TYPE'], 'channel': f['CHANNEL'], 'message_id': f['MESSAGE_ID'],
        'steps': [{
            'recon_status': s['RECON_STATUS'], 'emitted_at': s['EMITTED_AT'],
            'gap_reason': s['GAP_REASON'], 'state_id': s['STATE_ID'],
            'org_id': s['ORG_ID'], 'building_id': s['BUILDING_ID'],
        } for s in steps],
    })


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        try:
            if u.path == '/api/health':
                return self._send(200, {'ok': True, 'source': 'dbt:stg' if STG_OK else 'raw'})
            if u.path == '/api/search':
                term = (qs.get('q', [''])[0]).strip()
                if not term:
                    return self._send(400, {'error': 'missing q'})
                ids = resolve_ids(term)
                return self._send(200, {'results': summaries(ids)})
            if u.path == '/api/trace':
                iid = (qs.get('interaction_id', [''])[0]).strip()
                if not iid:
                    return self._send(400, {'error': 'missing interaction_id'})
                t = trace(iid)
                return self._send(200, {'trace': t}) if t else self._send(404, {'error': 'not found'})
            return self._send(404, {'error': 'unknown route'})
        except Exception as e:
            return self._send(500, {'error': str(e)})


if __name__ == '__main__':
    print(f'journey backend on http://localhost:{PORT}  source={"dbt:stg" if STG_OK else "raw"}')
    ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()

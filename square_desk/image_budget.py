"""Release only durable v27 reservations on terminal, never-submitted drafts."""
from .models import utc

def release_terminal(store):
    rows=store.list('draft',['expired','rejected'],since=utc()-3*86400,limit=1000)
    for row in rows:
        r=row['payload'].get('image_reservation')
        if not isinstance(r,dict) or r.get('released') or row['payload'].get('submitted_at'):continue
        with store.transaction():
            current=store.get(row['id'])
            if not current or current['status'] not in ('expired','rejected'):continue
            p=dict(current['payload']);r=dict(p.get('image_reservation') or {})
            if not r.get('day') or r.get('released') or p.get('submitted_at'):continue
            r['released']=True;p['image_reservation']=r
            store.conn.execute('UPDATE budgets SET used=CASE WHEN used>=1 THEN used-1 ELSE 0 END WHERE day=? AND category=?',
                               (r['day'],'images'))
            store.update(row['id'],payload=p)

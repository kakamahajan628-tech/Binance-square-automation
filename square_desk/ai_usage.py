"""Durable usage evidence. Unknown provider usage never becomes a free retry."""
import asyncio
import uuid
import httpx
from .db_actor import run_db
from .models import utc

async def tracked_send(store,send,request,article,day,reserved):
    from .content import AI_REQUEST
    ident=uuid.uuid4().hex
    payload={'day':day,'reserved_tokens':reserved,'at':utc(),'used_tokens':None}
    started=False
    try:
        await run_db(store,lambda:store.insert('ai_usage',payload,'pending',id=ident))
        started=True
        response=await send(request,article)
        try:
            body=response.json();usage=body.get('usage',{}) if isinstance(body,dict) else {}
            used=usage.get('total_tokens') if isinstance(usage,dict) else None
        except (ValueError,TypeError):used=None
        status='settled' if type(used) is int and 0<used<=reserved else 'unknown'
        if response.status_code!=200:status='rejected'
        payload['used_tokens']=used if status=='settled' else None
        await run_db(store,lambda:store.update(ident,status=status,payload=payload))
        return response
    except BaseException as error:
        if not isinstance(error,(httpx.HTTPError,asyncio.CancelledError)):raise
        marker=AI_REQUEST.get() or {}
        status='not_sent' if not marker.get('sent') or isinstance(error,(httpx.ConnectError,httpx.ConnectTimeout)) else 'unknown'
        # The initial insert may have completed while its await was cancelled.
        def finish():
            row=store.get(ident)
            if row:store.update(ident,status=status,payload=payload)
        await run_db(store,finish)
        raise

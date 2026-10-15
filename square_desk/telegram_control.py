"""One independent poller; mutations stay on the fenced operational loop."""
import asyncio
from concurrent.futures import Future
import json
import logging
from threading import Event,Thread
import time

import httpx


class TelegramControl:
    def __init__(self,desk,worker_loop,offset=0,*,client_factory=None):
        self.desk,self.worker_loop=desk,worker_loop
        self.client_factory=client_factory or (lambda:httpx.AsyncClient(timeout=4,follow_redirects=False,trust_env=False,
            limits=httpx.Limits(max_connections=2,max_keepalive_connections=2)))
        self.offset=int(offset)
        self.saved_offset=self.offset
        self.pending={}
        self.forward=None
        self.offset_future=None
        self.offset_retry=0
        self.rates={}
        self.retry_until=0
        self.next_webhook_reconcile=0
        self.api_status='starting'
        self.authority_status='not_checked'
        self.loop,self.task,self.thread=None,None,None
        self.stopped=Event()
        self.activated=False

    async def start(self):
        ready=Future()
        def run():
            asyncio.run(self._main(ready))
        self.thread=Thread(target=run,name='square-telegram-control',daemon=True)
        self.thread.start()
        await asyncio.wrap_future(ready)

    async def _main(self,ready):
        self.loop=asyncio.get_running_loop()
        self.task=asyncio.current_task()
        try:
            async with self.client_factory() as client:
                self.client=client
                ready.set_result(None)
                while not self.stopped.is_set():
                    self.desk.runtime_status.watchdog()
                    if self.desk.lease_owned:self.activated=True
                    if self.activated or getattr(self.desk.db.conn,'replicated',False):
                        try:await self.step()
                        except Exception as error:
                            self.api_status='control_error:'+type(error).__name__
                            logging.getLogger('square_desk').warning('Telegram control failure: %s',type(error).__name__)
                    await asyncio.sleep(2)
        except asyncio.CancelledError:pass
        finally:
            if not ready.done():ready.set_exception(RuntimeError('Telegram control startup failed'))

    async def api(self,method,body):
        if time.monotonic()<self.retry_until:return None
        try:
            response=await self.client.post('https://api.telegram.org/bot'+self.desk.s.telegram_token+'/'+method,json=body)
            if response.status_code==429:
                try:delay=float(response.json().get('parameters',{}).get('retry_after',60))
                except (ValueError,TypeError,AttributeError):delay=60
                self.retry_until=time.monotonic()+max(1,min(3600,delay))
                self.api_status='rate_limited'
                return None
            if response.status_code!=200:
                if response.status_code==409 and method=='getUpdates' and time.monotonic()>=self.next_webhook_reconcile:
                    journal=getattr(self.desk.db.conn,'journal',None)
                    owned=(journal is not None and journal.epoch is not None and time.monotonic()<journal.deadline) or (
                        journal is None and self.desk.lease_owned)
                    if owned and self.desk.s.telegram_polling:
                        self.next_webhook_reconcile=time.monotonic()+60
                        info=await self.api('getWebhookInfo',{})
                        if isinstance(info,dict) and info.get('url'):
                            deleted=await self.api('deleteWebhook',{'drop_pending_updates':False})
                            if deleted:
                                self.api_status='polling_webhook_conflict_resolved'
                                self.retry_until=time.monotonic()+1
                                return None
                        if self.api_status=='rate_limited':return None
                self.api_status='poller_conflict' if response.status_code==409 else 'http_error'
                self.retry_until=time.monotonic()+(10 if response.status_code==409 else 3)
                return None
            data=response.json()
            if not isinstance(data,dict) or not data.get('ok'):
                self.api_status='invalid_response';return None
            self.api_status='ok'
            return data.get('result')
        except (httpx.HTTPError,ValueError,TypeError):
            self.api_status='network_unavailable'
            self.retry_until=time.monotonic()+3
            return None

    async def send(self,chat,text):
        # Only fixed help and bounded diagnostic structures use this path.
        if len(text.encode('utf-16-le'))>6900:
            text='Runtime report exceeded Telegram size. Open the authenticated /api/runtime endpoint.'
        return await self.api('sendMessage',{'chat_id':chat,'text':text})

    async def execute(self,update):
        if self.stopped.is_set() or not self.desk.lease_owned:return False
        await self.desk.telegram.receive(update,flush=False,answer_callback=False)
        return True

    async def authority(self):
        journal=getattr(self.desk.db.conn,'journal',None)
        if journal is None:
            self.authority_status='single_backend'
            return True
        if journal.epoch is None:
            self.authority_status='standby'
            return False
        if time.monotonic()<journal.deadline:
            self.authority_status='writer_lease_valid'
            return True
        # Passive, primary-routed ownership observation. Never renews the
        # writer lease or changes its epoch/sequence. A retiring poller stops
        # when another instance owns authority, even if its worker is frozen.
        statement={'sql':'''UPDATE square_cluster SET id=id WHERE id=1 AND cluster=?
            AND owner=? AND epoch=? RETURNING epoch''','args':[
                {'type':'text','value':journal.cluster},{'type':'text','value':journal.owner},
                {'type':'integer','value':str(journal.epoch)}]}
        try:
            response=await self.client.post(journal.url,headers={'Authorization':'Bearer '+journal.token},
                json={'requests':[{'type':'execute','stmt':statement},{'type':'close'}]})
            data=response.json()
            if response.status_code!=200:raise ValueError
            result=data['results'][0]
            if result['type']!='ok' or result['response']['type']!='execute':raise ValueError
            rows=result['response']['result']['rows']
            if not rows:
                self.authority_status='standby';return False
            if len(rows)!=1 or int(rows[0][0]['value'])!=journal.epoch:raise ValueError
            self.authority_status='verified_owner'
            return True
        except (httpx.HTTPError,ValueError,TypeError,KeyError,IndexError):
            self.authority_status='unverified_readonly'
            return None

    async def save_offset(self,value):
        if self.stopped.is_set():return
        db=self.desk.db
        with db.transaction():
            previous=db.state('telegram_offset',0)
            if value>previous:db.set('telegram_offset',value)

    def allowed_rate(self,sender):
        minute=int(time.time()//60)
        old,count=self.rates.get(sender,(minute,0))
        count=count if old==minute else 0
        if count>=15:return False
        self.rates[sender]=(minute,count+1)
        return True

    async def step(self):
        authority=await self.authority()
        if authority is False:return
        if self.forward is not None and self.forward[1].done():
            ident,future=self.forward
            executed=True
            try:executed=future.result() is not False
            except Exception as error:
                logging.getLogger('square_desk').warning('Telegram command outcome unconfirmed: %s',type(error).__name__)
                await self.send(self.pending[ident]['message']['from']['id'],
                    'Command outcome unconfirmed because the worker/database failed. No automatic command replay. Check /status and /errors before issuing a new command.')
            if executed:
                callback=self.pending[ident]['update'].get('callback_query')
                if callback:await self.api('answerCallbackQuery',{'callback_query_id':callback['id']})
                self.pending[ident]['done']=True
            self.forward=None
        if self.offset_future is not None and self.offset_future[1].done():
            value,future=self.offset_future
            try:future.result();self.saved_offset=max(self.saved_offset,value)
            except Exception:self.offset_retry=time.monotonic()+30
            self.offset_future=None
        result=await self.api('getUpdates',{'offset':self.offset,'timeout':0,'limit':100,
                                          'allowed_updates':['message','callback_query']})
        if not isinstance(result,list):return
        for update in result[:100]:
            if not isinstance(update,dict) or type(update.get('update_id')) is not int:continue
            ident=update['update_id']
            if ident<self.offset or ident in self.pending:continue
            if len(self.pending)>=100:break
            callback=update.get('callback_query')
            message=update.get('message',{})
            if isinstance(callback,dict):
                message={**callback.get('message',{}),'from':callback.get('from',{}),'text':callback.get('data','')}
            authorized=self.desk.telegram.authorized(message)
            self.pending[ident]={'update':update,'message':message,'done':not authorized,'direct':False,'sent':False}
            if not authorized:continue
            text=str(message.get('text','')).strip()
            parts=text.split(maxsplit=1)
            name=parts[0].split('@')[0].lower() if parts else ''
            direct=(name in ('/runtime','/help','/start') or name in ('/status','/publish_status') and len(parts)==1)
            self.pending[ident]['direct']=direct
            if not self.allowed_rate(message['from']['id']):self.pending[ident]['done']=True
        # Read-only telemetry can pass a slow outstanding command without
        # acknowledging that command or running it a second time.
        for ident,item in sorted(self.pending.items()):
            if item['done']:continue
            if item['direct']:
                text=str(item['message'].get('text','')).strip().split(maxsplit=1)[0].split('@')[0].lower()
                if text in ('/help','/start'):
                    result_text='Use /status for nonblocking runtime observations, /status full for database details, /queue and /errors. Publishing and safety commands retain database checks.'
                else:result_text=json.dumps(self.desk.runtime_status.snapshot(),separators=(',',':'),ensure_ascii=False)
                sent=await self.send(item['message']['from']['id'],result_text)
                if sent:
                    item['done']=True
                    callback=item['update'].get('callback_query')
                    if callback:await self.api('answerCallbackQuery',{'callback_query_id':callback['id']})
            elif self.forward is None and authority is True and self.desk.lease_owned:
                self.forward=(ident,asyncio.run_coroutine_threadsafe(self.execute(item['update']),self.worker_loop))
        if authority is True:
            for ident in sorted(self.pending):
                if not self.pending[ident]['done']:break
                self.offset=max(self.offset,ident+1)
        self.pending={ident:item for ident,item in self.pending.items() if ident>=self.offset}
        if authority is True and self.offset>self.saved_offset and self.offset_future is None and time.monotonic()>=self.offset_retry:
            self.offset_future=(self.offset,asyncio.run_coroutine_threadsafe(self.save_offset(self.offset),self.worker_loop))

    async def close(self):
        self.stopped.set()
        if self.forward:self.forward[1].cancel()
        if self.offset_future:self.offset_future[1].cancel()
        if self.loop and self.task:self.loop.call_soon_threadsafe(self.task.cancel)
        if self.thread:await asyncio.to_thread(self.thread.join,5)

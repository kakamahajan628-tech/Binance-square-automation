import asyncio
import httpx
import pytest
import json
import time

from square_desk.service import Desk
from square_desk.models import utc
from test_square_media import live_settings, attached_chart
from test_square_desk import snapshot, event, draft_payload
from square_desk.chart_alignment import for_draft
from square_desk.content_mix import provenance, REVISION as CONTENT_REVISION
from square_desk.media import SquareMedia, MediaPending, MediaError


def test_processing_pending_keeps_approved_chart_and_resumes_same_ticket(tmp_path, monkeypatch):
    async def no_sleep(seconds): pass
    monkeypatch.setattr('square_desk.media.asyncio.sleep', no_sleep)
    async def run():
        calls=[]
        ready=[False]
        def respond(request):
            calls.append(request)
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{
                    'presignedUrl':'https://upload.example/image','fileTicket':'private-ticket'}})
            if request.method=='PUT': return httpx.Response(200)
            assert request.url.path.endswith('/image/imageStatus')
            return httpx.Response(200,json={'code':'000000','data':
                {'status':1,'imageUrl':'https://image.example/chart.png'} if ready[0] else {'status':0}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path),client=client)
            ident,_=attached_chart(desk)
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            row=desk.db.get(ident)
            assert row['status']=='approved'
            assert row['payload']['approved_at']
            assert row['payload']['image_pending']['file_ticket']=='private-ticket'
            row['payload']['image_pending']['next_check_at']=utc()-1
            desk.db.update(ident,payload=row['payload'])
            ready[0]=True
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            assert desk.db.get(ident)['payload']['image_upload']['url']=='https://image.example/chart.png'
            assert sum(r.method=='PUT' for r in calls)==1
            assert sum(r.url.path.endswith('/image/presignedUrl') for r in calls)==1
            await desk.close()
    asyncio.run(run())


def test_actual_render_receives_post_focus_instead_of_coin_rotation(tmp_path,monkeypatch):
    desk=Desk(live_settings(tmp_path))
    snap=snapshot()
    p={**draft_payload(event(snap)), 'title':'BTC RSI momentum in context',
       'content_style':'market_detective','body':'RSI shows momentum. Use the recorded support for context.'}
    contexts=[]
    def chart(*args,context=None):
        contexts.append(context)
        return 'chart.png'
    monkeypatch.setattr('square_desk.service.images.chart',chart)
    desk.render_image({'payload':p,'data':snap.dict()})
    assert contexts[0] and contexts[0]['chart_view']=='momentum'
    assert contexts[0]['show_macd'] is False
    asyncio.run(desk.close())


@pytest.mark.parametrize('title,body,view',[
    ('RSI momentum','Momentum beneath price','momentum'),
    ('EMA20 and EMA50 trend','Moving averages smooth prices','trend'),
    ('Volume expansion','Trading volume against baseline','value'),
    ('Volatility compression','Bollinger bands and ATR','volatility'),
    ('Support and resistance','Prior range and retest','structure')])
def test_topic_mapping(title,body,view):
    plan=for_draft({'title':title,'body':body})
    assert plan['chart_view']==view
    assert plan['show_macd'] is False
    assert plan['show_vwap'] is False


def test_peer_box_requires_the_posts_original_provenance():
    ev=event()
    related=[{'symbol':'ETH','source':ev['metrics']['source'],'quote':ev['metrics']['quote'],
              'as_of':ev['as_of'],'observations':{'hourly_direction':'up','volume':'above its baseline'}}]
    draft={**draft_payload(ev),'content_style':'multi_coin','content_mix_version':CONTENT_REVISION,
           'internal_evidence':provenance(ev,{'related':related})}
    assert for_draft(draft)['related'][0]['symbol']=='ETH'
    draft['internal_evidence']['related'][0]['symbol']='OTHER'
    assert for_draft(draft)['related']==[]
    assert for_draft({'title':'RSI greed','body':'MACD momentum','content_style':'psychology'})['chart_view']=='structure'


@pytest.mark.parametrize('where',['put','status','presign'])
def test_transient_network_failure_recovers_without_duplicate_upload_or_public_post(tmp_path,where):
    async def run():
        requests=[]
        failed=[False]
        def respond(request):
            requests.append(request)
            stage='put' if request.method=='PUT' else 'presign' if request.url.path.endswith('/image/presignedUrl') else 'status'
            assert not request.url.path.endswith('/content/add')
            if stage==where and not failed[0]:
                failed[0]=True
                raise httpx.ReadTimeout('private transport URL')
            if stage=='presign':return httpx.Response(200,json={'code':'000000','data':{
                'presignedUrl':'https://upload.example/object?private-signature','fileTicket':'private-ticket'}})
            if stage=='put':return httpx.Response(200)
            return httpx.Response(200,json={'code':'000000','data':{'status':1,'imageUrl':'https://image.example/ready.png'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path),client=client)
            ident,_=attached_chart(desk)
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            row=desk.db.get(ident)
            assert row['status']=='approved' and row['payload']['approved_at']
            deadline=row['payload']['image_pending']['deadline_at']
            row['payload']['image_pending']['next_check_at']=utc()-1
            desk.db.update(ident,status='uploading_image',payload=row['payload'])
            await desk.publisher.prepare_image_job(ident)
            assert desk.db.get(ident)['payload']['image_upload']['url'].endswith('ready.png')
            assert sum(r.method=='PUT' for r in requests)==1
            assert sum(r.url.path.endswith('/image/presignedUrl') for r in requests)==(2 if where=='presign' else 1)
            assert 'image_pending' not in desk.db.get(ident)['payload']
            assert not any(secret in json.dumps(desk.db.logs()) for secret in ('private-ticket','private-signature','private-square-key'))
            await desk.close()
    asyncio.run(run())


@pytest.mark.parametrize('file_lost,evidence_lost', [(False,False),(True,False),(True,True)])
def test_saved_ticket_restart_recovers_exact_png_or_reviews_and_never_repeats_put(tmp_path, file_lost, evidence_lost):
    async def run():
        ready=[False]
        requests=[]
        def respond(request):
            requests.append(request)
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{'presignedUrl':'https://upload.example/object','fileTicket':'ticket'}})
            if request.method=='PUT':return httpx.Response(200)
            if request.url.path.endswith('/image/imageStatus'):
                return httpx.Response(200,json={'code':'000000','data':{'status':1,'imageUrl':'https://image.example/chart.png'} if ready[0] else {'status':0}})
            assert request.url.path.endswith('/content/add')
            assert json.loads(request.content)['imageList']==['https://image.example/chart.png']
            return httpx.Response(200,json={'code':'000000','data':{'id':'confirmed-image-post'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            settings=live_settings(tmp_path)
            settings.database=str(tmp_path/'persistent.sqlite3')
            desk=Desk(settings,client=client)
            ident,filename=attached_chart(desk)
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            if evidence_lost:
                desk.db.conn.execute("DELETE FROM entities WHERE kind='snapshot'")
            await desk.close()
            if file_lost:
                (tmp_path/filename).unlink()
            before_restart_requests=len(requests)
            ready[0]=True
            desk=Desk(settings,client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
            row=desk.db.get(ident)
            row['payload']['image_pending']['next_check_at']=utc()-1
            desk.db.update(ident,status='uploading_image',payload=row['payload'])
            await desk.publisher.prepare_image_job(ident)
            desk.scheduler.allocate()
            await desk.publisher.publish(ident)
            await desk.publisher.publish(ident)
            assert desk.db.get(ident)['status']==('review' if evidence_lost else 'published')
            assert sum(r.method=='PUT' for r in requests)==1
            assert sum(r.url.path.endswith('/image/presignedUrl') for r in requests)==1
            assert sum(r.url.path.endswith('/content/add') for r in requests)==(0 if evidence_lost else 1)
            assert (tmp_path/filename).is_file() is not evidence_lost
            if evidence_lost:
                assert len(requests)==before_restart_requests
                assert desk.db.get(ident)['payload']['rejection_code']=='media_recovery_unavailable'
                assert not desk.db.state('publisher_blocked',False)
            await desk.close()
    asyncio.run(run())


def test_processing_recovery_is_bounded_and_never_posts_expired_checkpoint(tmp_path):
    async def run():
        calls=[]
        def respond(request):
            calls.append(request)
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{'presignedUrl':'https://upload.example/object','fileTicket':'ticket'}})
            if request.method=='PUT':return httpx.Response(200)
            return httpx.Response(200,json={'code':'000000','data':{'status':0}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path),client=client)
            ident,_=attached_chart(desk)
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            row=desk.db.get(ident)
            pending=row['payload']['image_pending']
            pending.update(started_at=utc()-181,next_check_at=utc()-1)
            pending['deadline_at']=pending['started_at']+180
            before=len(calls)
            desk.db.update(ident,status='uploading_image',payload=row['payload'])
            await desk.publisher.prepare_image_job(ident)
            after=desk.db.get(ident)
            assert len(calls)==before
            assert after['status']=='review' and after['payload']['approved_at'] is None
            assert after['payload']['rejection_code']=='media_processing_expired'
            assert not desk.db.state('publisher_blocked',False)
            assert not desk.db.list('draft',['uncertain'])
            await desk.close()
    asyncio.run(run())


def test_image_can_prepare_before_spacing_slot_but_content_cannot_publish_early(tmp_path):
    async def run():
        requests=[]
        def respond(request):
            requests.append(request)
            assert not request.url.path.endswith('/content/add')
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{'presignedUrl':'https://upload.example/object','fileTicket':'ticket'}})
            if request.method=='PUT':return httpx.Response(200)
            return httpx.Response(200,json={'code':'000000','data':{'status':1,'imageUrl':'https://image.example/chart.png'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path,gap_seconds=1200),client=client)
            last=utc()-1100
            desk.db.insert('draft',{'title':'Previous','body':'Earlier unrelated research note.',
                                  'category':'education','submitted_at':last},'published')
            ident,_=attached_chart(desk)
            desk.start_image_upload()
            assert desk.image_upload_task is not None
            await desk.image_upload_task
            assert desk.db.get(ident)['payload']['image_upload']
            desk.scheduler.allocate()
            assert not desk.scheduler.due()
            assert desk.db.get(ident)['due']>=last+1200
            assert len(requests)==3
            await desk.close()
    asyncio.run(run())


def test_slow_checkpoint_writes_do_not_spend_the_network_request_deadline(tmp_path):
    async def run():
        desk=Desk(live_settings(tmp_path))
        _,filename=attached_chart(desk)
        data=(tmp_path/filename).read_bytes()
        await desk.close()
        async def respond(request):
            await asyncio.sleep(.005)
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{'presignedUrl':'https://upload.example/object','fileTicket':'ticket'}})
            if request.method=='PUT':return httpx.Response(200)
            return httpx.Response(200,json={'code':'000000','data':{'status':1,'imageUrl':'https://image.example/chart.png'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            started=time.monotonic()
            result=await SquareMedia('test-key',client,timeout=.2).upload(filename,data,
                checkpoint=lambda state:time.sleep(.15))
            assert time.monotonic()-started>.2
            assert result=='https://image.example/chart.png'
    asyncio.run(run())


def test_presign_timeout_has_a_durable_attempt_bound_and_never_posts(tmp_path):
    async def run():
        calls=[]
        def respond(request):
            calls.append(request)
            assert request.url.path.endswith('/image/presignedUrl')
            raise httpx.ReadTimeout('private-key-in-url')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path),client=client)
            ident,_=attached_chart(desk)
            for attempt in range(1,4):
                row=desk.db.get(ident)
                if row['payload'].get('image_pending'):row['payload']['image_pending']['next_check_at']=utc()-1
                desk.db.update(ident,status='uploading_image',payload=row['payload'])
                await desk.publisher.prepare_image_job(ident)
                result=desk.db.get(ident)
                assert result['payload']['image_pending']['attempts']==attempt
                assert result['status']==('review' if attempt==3 else 'approved')
            assert len(calls)==3
            assert not desk.db.state('publisher_blocked',False)
            await desk.close()
    asyncio.run(run())


def test_ready_status_after_worker_lease_loss_cannot_attach_upload_or_post(tmp_path):
    async def run():
        desk=None
        def respond(request):
            assert not request.url.path.endswith('/content/add')
            if request.url.path.endswith('/image/presignedUrl'):
                return httpx.Response(200,json={'code':'000000','data':{'presignedUrl':'https://upload.example/object','fileTicket':'ticket'}})
            if request.method=='PUT':return httpx.Response(200)
            desk.db.set('worker_lease',{'owner':'successor','until':utc()+60})
            return httpx.Response(200,json={'code':'000000','data':{'status':1,'imageUrl':'https://image.example/chart.png'}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            desk=Desk(live_settings(tmp_path),client=client)
            ident,_=attached_chart(desk)
            desk.publisher.worker_owner='retiring'
            desk.db.set('worker_lease',{'owner':'retiring','until':utc()+60})
            desk.db.update(ident,status='uploading_image')
            await desk.publisher.prepare_image_job(ident)
            result=desk.db.get(ident)
            assert result['status']=='uploading_image' and 'image_upload' not in result['payload']
            assert result['payload']['approved_at']
            await desk.close()
    asyncio.run(run())

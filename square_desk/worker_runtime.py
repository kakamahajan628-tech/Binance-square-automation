"""One operational event loop, isolated from HTTP liveness processing."""
import asyncio
from concurrent.futures import Future
import inspect
from threading import Thread


class WorkerRuntime:
    def __init__(self,desk):
        self.desk,self.loop,self.thread=desk,None,None

    def _run(self,ready):
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop=loop
        # Publish readiness only after run_forever has entered its loop.
        loop.call_soon(ready.set_result,None)
        try:
            loop.run_forever()
        finally:
            pending=asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending,return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    async def start(self):
        ready=Future()
        self.thread=Thread(target=self._run,args=(ready,),name='square-worker-loop',daemon=True)
        self.thread.start()
        await asyncio.wrap_future(ready)
        try:
            await self.call(self.desk.start)
        except BaseException:
            await self.close()
            raise

    async def call(self,function,*args,**kwargs):
        async def invoke():
            value=function(*args,**kwargs)
            return await value if inspect.isawaitable(value) else value
        if self.loop is None or asyncio.get_running_loop() is self.loop:
            return await invoke()
        if not self.loop.is_running():
            raise RuntimeError('Operational worker unavailable')
        future=asyncio.run_coroutine_threadsafe(invoke(),self.loop)
        return await asyncio.wrap_future(future)

    async def close(self):
        if self.loop is None:
            return await self.desk.close()
        try:
            await self.call(self.desk.close)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            await asyncio.to_thread(self.thread.join,10)

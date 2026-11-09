"""Bounded DB execution; cancellation never abandons an in-flight transaction."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from threading import BoundedSemaphore, Lock


class DatabaseActor:
    def __init__(self):
        self.executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix='square-db-owner')
        self.slots=BoundedSemaphore(32)
        self.lock=Lock()
        self.pending=0
        self.completed=0
        self.closed=False

    async def run(self,function):
        while not self.slots.acquire(blocking=False):
            await asyncio.sleep(.01)
        try:
            with self.lock:
                if self.closed:raise RuntimeError('Database executor closed')
                self.pending+=1
            future=asyncio.wrap_future(self.executor.submit(copy_context().run,function))
            cancelled=False
            while not future.done():
                try:await asyncio.shield(future)
                except asyncio.CancelledError:cancelled=True
                except BaseException:break
            if cancelled:
                if not future.cancelled():future.exception()
                raise asyncio.CancelledError()
            return future.result()
        finally:
            with self.lock:
                self.pending=max(0,self.pending-1)
                self.completed+=1
            self.slots.release()

    def close(self):
        with self.lock:self.closed=True
        # May be called by Repository.close on this very executor thread.
        self.executor.shutdown(wait=False,cancel_futures=False)


async def run_db(store,function):
    # RAM SQLite has no remote/filesystem I/O and needs no thread handoff.
    if getattr(store,'in_memory',False):return function()
    if getattr(store,'closed',False):return function()
    actor=getattr(store,'_db_actor',None)
    if actor is None:
        actor=DatabaseActor()
        store._db_actor=actor
    return await actor.run(function)

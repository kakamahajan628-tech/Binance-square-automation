"""Judge chart deadlines on the render thread, independent of loop delivery."""
import asyncio
from threading import Lock
import time


class RenderWork:
    def __init__(self, render, plan, timeout):
        self.render, self.plan = render, plan
        self.timeout = timeout
        self.submitted = time.monotonic()
        self.deadline = self.submitted + timeout
        self.started = self.finished = self.delivered = None
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            self.started = time.monotonic()
        try:
            return self.render(self.plan)
        finally:
            with self.lock:
                self.finished = time.monotonic()

    def completed_on_time(self, future):
        with self.lock:
            finished = self.finished
        return (future.done() and not future.cancelled()
                and finished is not None and finished <= self.deadline)

    async def wait(self, future):
        try:
            try:
                value = await asyncio.wait_for(asyncio.wrap_future(future),
                    timeout=max(0, self.deadline - time.monotonic()))
            except asyncio.TimeoutError:
                # A synchronous DB call can delay both the completion callback
                # and the loop timer. Rescue only already-finished work whose
                # actual completion was inside the original, unchanged deadline.
                if not self.completed_on_time(future):
                    raise
                value = future.result()  # Already done: no blocking or retry.
            # Enforce the deadline even if an overdue callback delivers a late
            # result before asyncio gets around to cancelling its waiter.
            if not self.completed_on_time(future):
                raise asyncio.TimeoutError
            return value
        finally:
            with self.lock:
                self.delivered = time.monotonic()

    def timings(self):
        with self.lock:
            started, finished, delivered = self.started, self.finished, self.delivered
        def elapsed(a, b):
            return None if a is None or b is None else round(max(0, a-b), 3)
        return {'deadline_seconds': self.timeout,
            'queue_wait_seconds': elapsed(started, self.submitted),
            'worker_seconds': elapsed(finished, started),
            'result_observed_after_seconds': elapsed(delivered, self.submitted),
            'completion_delivery_delay_seconds': elapsed(delivered, finished)
                if delivered is not None and finished is not None and delivered >= finished else None,
            'completed_within_deadline': None if finished is None else finished <= self.deadline}

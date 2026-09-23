import asyncio
from contextlib import contextmanager

from inferscale.ledger import RequestLedger
from inferscale.models import NoSchedulableWorker, Outcome, RequestContext


@contextmanager
def request_scope(ledger: RequestLedger, context: RequestContext):
    """P1 local lease ownership. Network resource cleanup belongs to the P2 proxy."""
    lease = ledger.acquire(context)
    outcome = Outcome.COMPLETED
    try:
        yield lease
    except asyncio.CancelledError:
        outcome = Outcome.CANCELLED
        raise
    except TimeoutError:
        outcome = Outcome.TIMEOUT
        raise
    except NoSchedulableWorker:
        outcome = Outcome.REJECTED
        raise
    except BaseException:
        outcome = Outcome.FAILED
        raise
    finally:
        ledger.finish(lease, outcome)

"""Purser — a Redis-backed priority work queue.

The quartermaster of the Eyry recon suite: it takes hosts from a producer
(Foretop), holds them in hot / warm / cold priority lanes, hands them out to
workers (Vedette) with at-least-once delivery, retries what fails, and drops
what keeps failing into a dead-letter queue.
"""

from .job import Job
from .queue import EnqueueResult, Purser

__version__ = "0.1.0"
__all__ = ["Purser", "Job", "EnqueueResult", "__version__"]

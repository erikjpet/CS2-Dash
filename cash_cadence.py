"""Cash-only continuation of the latest saved explicit portfolio; no import I/O."""
import re
import threading
import time

import cash_prices

INTERVAL_SECONDS = 300
POLL_SECONDS = 30
INITIAL_DELAY_SECONDS = 60
SCOPE = 'latest_saved_cash_portfolio'
JOB_STATES = cash_prices.ACTIVE_STATES | {'completed', 'paused', 'interrupted', 'failed'}


def saved_scope(value):
    job = value.get('job') if isinstance(value, dict) else None
    if (not isinstance(value, dict) or value.get('provider') != cash_prices.PROVIDER
            or value.get('currency') != cash_prices.CURRENCY or type(value.get('nextAllowedAt')) is not int
            or value['nextAllowedAt'] < 0 or type(value.get('workerRunning')) is not bool
            or not isinstance(job, dict) or not re.fullmatch('[0-9a-f]{32}', str(job.get('id') or ''))
            or not isinstance(job.get('portfolioId'), str) or not 0 < len(job['portfolioId']) <= 128
            or not re.fullmatch('[0-9a-f]{64}', str(job.get('holdingsFingerprint') or ''))
            or job.get('status') not in JOB_STATES or type(job.get('startedAt')) is not int):
        return None
    return job


class CashCadence:
    def __init__(self, service_factory, clock=time.time, monotonic=time.monotonic,
                 thread_factory=threading.Thread, stop_event=None):
        self.service_factory, self.clock, self.monotonic = service_factory, clock, monotonic
        self.thread_factory = thread_factory
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self.lock = threading.RLock()
        self.step_lock = threading.Lock()
        self.thread = None
        self.initial_at = self.initial_monotonic = None
        self.next_attempt = 0
        self.state, self.error = 'not_started', None
        self.last_checked = self.last_attempt = self.portfolio_id = None

    def start(self):
        """Host calls only after constructing the HTTP listener, never from GET."""
        with self.lock:
            if self.thread is not None or self.stop_event.is_set():
                return False
            self.initial_at = int(self.clock()) + INITIAL_DELAY_SECONDS
            self.initial_monotonic = self.monotonic() + INITIAL_DELAY_SECONDS
            self.state = 'initial_delay'
            self.thread = self.thread_factory(target=self._loop, name='skinport-cash-cadence', daemon=True)
            try:
                self.thread.start()
            except BaseException:
                self.thread = None
                self.state = 'blocked'
                raise
            return True

    def _loop(self):
        while not self.stop_event.wait(POLL_SECONDS):
            self.tick()

    def status(self, saved=None):
        """Pure annotation of an already-read saved status; no service/DB action."""
        job = saved_scope(saved)
        with self.lock:
            enabled = bool(self.thread and self.thread.is_alive() and not self.stop_event.is_set())
            deadline = max(self.initial_at or 0, self.next_attempt,
                           saved['nextAllowedAt'] if job else 0,
                           job['startedAt'] + INTERVAL_SECONDS if job else 0)
            state = self.state
            if enabled and saved is not None and not job:
                state = 'no_saved_scope'
            elif enabled and job and self.portfolio_id not in (None, job['portfolioId']):
                state = 'scope_pending'
            return {'enabled': enabled, 'scope': SCOPE,
                    'portfolioId': job['portfolioId'] if job else self.portfolio_id if saved is None else None,
                    'intervalSeconds': INTERVAL_SECONDS, 'pollSeconds': POLL_SECONDS,
                    'initialNotBefore': self.initial_at, 'nextAllowedAt': deadline or None,
                    'state': state, 'lastError': self.error, 'lastCheckedAt': self.last_checked,
                    'lastAttemptAt': self.last_attempt,
                    'freshnessSemantics': 'capture_age_and_recorded_provider_time_unchanged'}

    def tick(self):
        if self.stop_event.is_set() or self.initial_monotonic is None or self.monotonic() < self.initial_monotonic:
            return
        if not self.step_lock.acquire(blocking=False):
            return
        try:
            if self.stop_event.is_set():
                return
            service = self.service_factory()
            saved = service.status()
            job = saved_scope(saved)
            now = int(self.clock())
            with self.lock:
                self.last_checked = now
                if self.portfolio_id != (job['portfolioId'] if job else None):
                    self.error = None
                self.portfolio_id = job['portfolioId'] if job else None
                if not job:
                    self.state = 'no_saved_scope'
                    return
                if saved['workerRunning'] or job['status'] in cash_prices.ACTIVE_STATES:
                    self.state = 'worker_active'
                    return
                due = max(saved['nextAllowedAt'], job['startedAt'] + INTERVAL_SECONDS,
                          self.initial_at, self.next_attempt)
                if due > now:
                    self.state = 'provider_or_cadence_wait'
                    return
                if self.stop_event.is_set():
                    return
                self.last_attempt = now
                self.next_attempt = now + INTERVAL_SECONDS
                self.state = 'requesting'
            # The existing action performs final persisted provider admission
            # and single-flight checks. The expected ID fences an intervening
            # explicit portfolio switch under its existing writer transaction.
            result = service.action({'provider': cash_prices.PROVIDER, 'portfolio_id': job['portfolioId'],
                                     'action': 'start'}, expected_saved_job_id=job['id'])
            with self.lock:
                self.state = 'requested' if result.get('accepted') else 'existing_job_reused'
                self.error = None
        except cash_prices.CashAdmissionBusy as error:
            # This subtype is emitted only before writer ownership/SQLite
            # open/reservation. Persisted provider and uncertainty gates
            # remain part of the next cycle's max(due) calculation.
            with self.lock:
                self.next_attempt = int(self.clock()) + POLL_SECONDS
                self.state = 'writer_admission_busy'
                self.error = (type(error).__name__ + ': ' + str(error))[:400]
        except Exception as error:
            with self.lock:
                self.state = 'blocked'
                self.error = (type(error).__name__ + ': ' + str(error))[:400]
        finally:
            self.step_lock.release()

    def stop(self):
        """No new cycles; an admitted cash worker retains its normal safe completion."""
        self.stop_event.set()
        thread = self.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2)
        closed = not thread or not thread.is_alive()
        with self.lock:
            self.state = 'stopped' if closed else 'stopping'
        return closed

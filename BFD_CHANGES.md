# Black Flag changes

Based on upstream v0.2.1 (154638445c3949c1958b01812f69d9a1e4d82684).
The `bfd` branch carries the production patch; releases use v0.2.1-bfd.N
and upstream Linux archive and checksum names.

- Wake autoscaling after committed scale-set updates, including job demand.
  Notifications coalesce, and the five-second ticker remains the backstop.
- Reconcile each instance immediately when its manager starts.
- Generate JIT configurations with at most eight concurrent requests per scale
  set. Database writes and runner bookkeeping stay serialized under the worker
  lock. Stream results so the first instance need not wait for the whole burst.
- Stop scheduling JIT requests on error, drain successes, and wait at least one
  minute before another scale-up attempt to avoid hammering GitHub on throttling.
- Check cached runner tools immediately, removing the additional one-second
  wait on every autoscale pass. Protect action-service request construction
  against concurrent token refresh.
- Record JIT request start/end, pending-create persistence, provider preparation
  and provider-call start/end at info level. Join by runner name with EC2 and
  boot evidence to locate startup delays. Durations use the monotonic clock;
  logs contain no JIT configuration, tokens, bootstrap arguments or raw errors.
  `call_succeeded` means the provider call returned without an error, not that
  the runner is ready or that latency acceptance passed.
- Preserve pending/installing runners during ordinary scale-down, even when
  their provider state is already Running. They become eligible once idle;
  existing bootstrap timeout cleanup remains in force. A canceled burst may
  therefore retain startup compute until bootstrap or its configured timeout.

Fleet sizing, warm counts, placement and provider behavior are unchanged.

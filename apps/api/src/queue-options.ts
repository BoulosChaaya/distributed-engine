/**
 * Default options for every job the API enqueues.
 *
 * PostgreSQL is the record of every task's outcome, so Redis only has to keep
 * finished jobs long enough to inspect them. Failed jobs kept forever
 * (removeOnFail: false) would fill it: each is about 2.6 KB (payload,
 * failedReason, stacktrace), production Redis runs with maxmemory 400mb and
 * noeviction, so roughly 150,000 permanent failures and from then on Redis
 * refuses every write and nothing is published at all.
 *
 * Failed jobs are kept for `failedJobRetentionSeconds` and at most
 * `failedJobMaxCount` of them, whichever is reached first. BullMQ trims on
 * each failure, so the footprint stays bounded without a cleanup job.
 * Removing them changes nothing for recovery: the reconciler treats a
 * missing job and a failed job the same (orphan), and an operator retry
 * removes a still-kept dead job before re-adding (removeDeadJob).
 */
export function buildDefaultJobOptions(queue: { failedJobRetentionSeconds: number; failedJobMaxCount: number }) {
  return {
    removeOnComplete: 100,
    removeOnFail: { age: queue.failedJobRetentionSeconds, count: queue.failedJobMaxCount },
  };
}

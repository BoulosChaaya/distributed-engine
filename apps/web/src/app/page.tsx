'use client';

import { engine, type MetricsView } from '@/lib/client';
import { usePoll } from '@/lib/use-poll';

const STATUSES = ['QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED', 'SCHEDULED'] as const;

export default function Overview() {
  const { data, error } = usePoll(() => engine<MetricsView>('metrics'), 5000);

  return (
    <>
      <h1>Overview</h1>
      {error && <p className="error" role="alert">{error}</p>}
      {!data && !error && <p className="muted">Loading…</p>}
      {data && (
        <>
          <section className="cards" aria-label="Tasks by status">
            <Stat label="Total tasks" value={data.tasks.total} />
            {STATUSES.map((s) => (
              <Stat
                key={s}
                label={s.charAt(0) + s.slice(1).toLowerCase()}
                value={data.tasks.byStatus[s] ?? 0}
                tone={(data.tasks.byStatus[s] ?? 0) > 0 ? s.toLowerCase() : undefined}
              />
            ))}
          </section>
          <h2>Pipeline</h2>
          <section className="cards" aria-label="Queue and outbox">
            <Stat label="Queue waiting" value={data.queue.waiting} />
            <Stat label="Queue active" value={data.queue.active} />
            <Stat label="Outbox pending" value={data.outbox.pending} />
            <Stat label="Outbox failed" value={data.outbox.failed} tone={data.outbox.failed > 0 ? 'failed' : undefined} />
            <Stat label="API uptime (seconds)" value={data.uptime} />
            <Stat label="API error rate" value={`${data.requests.errorRate}%`} />
          </section>
        </>
      )}
    </>
  );
}

function Stat({ label, value, tone }: { label: string; value: number | string; tone?: string }) {
  return (
    <div className={`card${tone ? ` tone-${tone}` : ''}`}>
      <div className="value">{value}</div>
      <div className="label">{label}</div>
    </div>
  );
}

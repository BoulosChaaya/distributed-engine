'use client';

import { engine, type WorkerView } from '@/lib/client';
import { usePoll } from '@/lib/use-poll';

export default function WorkersPage() {
  const { data, error } = usePoll(() => engine<{ total: number; workers: WorkerView[] }>('workers'), 5000);

  return (
    <>
      <h1>Workers</h1>
      <p className="muted">Live from worker heartbeats in Redis.</p>
      {error && <p className="error" role="alert">{error}</p>}
      {data && data.workers.length === 0 && <p className="muted">No workers connected.</p>}
      {data && data.workers.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Worker</th>
                <th>Status</th>
                <th>Uptime (s)</th>
                <th>Processed</th>
                <th>Completed</th>
                <th>Failed</th>
                <th>Last heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {data.workers.map((w) => (
                <tr key={w.id}>
                  <td className="mono">{w.id}</td>
                  <td><span className={`badge badge-${w.status.toLowerCase()}`}>{w.status}</span></td>
                  <td>{w.uptime}</td>
                  <td>{w.jobsProcessed}</td>
                  <td>{w.jobsCompleted}</td>
                  <td>{w.jobsFailed}</td>
                  <td>{new Date(w.lastHeartbeat).toLocaleTimeString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

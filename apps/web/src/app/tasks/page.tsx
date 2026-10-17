'use client';

import { useState, type FormEvent } from 'react';
import { engine, type TaskPage, type TaskPriority, type TaskView } from '@/lib/client';
import { usePoll } from '@/lib/use-poll';

const PAGE_SIZE = 20;

// Built-in task types and a starting payload for each. The name is the type:
// workers run the handler registered under it, and unknown types fail at once.
const TASK_TYPES: Record<string, string> = {
  echo: '{ "hello": "world" }',
  sleep: '{ "ms": 3000 }',
  flaky: '{ "failTimes": 1 }',
  http: '{ "url": "https://example.com/", "method": "GET" }',
};
const CANCELLABLE = new Set(['SCHEDULED', 'BLOCKED', 'QUEUED', 'PROCESSING']);

export default function TasksPage() {
  const [page, setPage] = useState(1);
  const { data, error, refresh } = usePoll(
    () => engine<TaskPage>(`tasks?page=${page}&pageSize=${PAGE_SIZE}`),
    3000,
  );
  const [actionError, setActionError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  async function cancel(task: TaskView) {
    setBusyId(task.id);
    setActionError(null);
    try {
      await engine(`tasks/${task.id}/cancel`, { method: 'PUT' });
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Cancel failed');
    } finally {
      setBusyId(null);
    }
  }

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;

  return (
    <>
      <h1>Tasks</h1>
      <SubmitTask onSubmitted={refresh} />

      {(error || actionError) && <p className="error" role="alert">{actionError ?? error}</p>}
      {!data && !error && <p className="muted">Loading…</p>}
      {data && data.items.length === 0 && <p className="muted">No tasks yet.</p>}
      {data && data.items.length > 0 && (
        <>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Retries</th>
                  <th>Updated</th>
                  <th><span className="sr-only">Actions</span></th>
                </tr>
              </thead>
              <tbody>
                {data.items.map((t) => (
                  <tr key={t.id}>
                    <td className="mono" title={t.id}>{t.id.slice(0, 8)}</td>
                    <td>
                      {t.name}
                      {t.stepKey && <div className="muted mono">step {t.stepKey}</div>}
                      {t.error && <div className="row-error" title={t.error}>{t.error}</div>}
                    </td>
                    <td><span className={`badge badge-${t.status.toLowerCase()}`}>{t.status}</span></td>
                    <td>{t.priority}</td>
                    <td>{t.retries}/{t.maxRetries}</td>
                    <td>{new Date(t.updatedAt).toLocaleString()}</td>
                    <td>
                      {CANCELLABLE.has(t.status) && (
                        <button className="secondary" disabled={busyId === t.id} onClick={() => cancel(t)}>
                          {busyId === t.id ? 'Cancelling…' : 'Cancel'}
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="pager">
            <button className="secondary" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>Previous</button>
            <span>Page {page} of {totalPages} · {data.total} tasks</span>
            <button className="secondary" disabled={!data.hasMore} onClick={() => setPage((p) => p + 1)}>Next</button>
          </div>
        </>
      )}
    </>
  );
}

function SubmitTask({ onSubmitted }: { onSubmitted: () => Promise<void> }) {
  const [name, setName] = useState('echo');
  const [payloadTouched, setPayloadTouched] = useState(false);
  const [priority, setPriority] = useState<TaskPriority>('NORMAL');
  const [maxRetries, setMaxRetries] = useState(3);
  const [payload, setPayload] = useState(TASK_TYPES.echo);

  function chooseType(value: string) {
    setName(value);
    if (!payloadTouched && TASK_TYPES[value]) setPayload(TASK_TYPES[value]);
  }
  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState<{ kind: 'ok' | 'error'; text: string } | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setMessage(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload || '{}');
    } catch {
      setMessage({ kind: 'error', text: 'Payload must be valid JSON.' });
      return;
    }
    setSubmitting(true);
    try {
      const task = await engine<TaskView>('tasks', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim(), priority, maxRetries, payload: parsed }),
      });
      setMessage({ kind: 'ok', text: `Submitted ${task.id.slice(0, 8)}.` });
      await onSubmitted();
    } catch (err) {
      setMessage({ kind: 'error', text: err instanceof Error ? err.message : 'Submit failed' });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="panel form" onSubmit={submit}>
      <h2>Submit a task</h2>
      <label>
        Task type
        <input required maxLength={255} list="task-types" value={name}
          onChange={(e) => chooseType(e.target.value)} disabled={submitting} />
        <datalist id="task-types">
          {Object.keys(TASK_TYPES).map((t) => <option key={t} value={t} />)}
        </datalist>
      </label>
      <div className="row">
        <label>
          Priority
          <select value={priority} onChange={(e) => setPriority(e.target.value as TaskPriority)} disabled={submitting}>
            <option>LOW</option>
            <option>NORMAL</option>
            <option>HIGH</option>
            <option>CRITICAL</option>
          </select>
        </label>
        <label>
          Max retries
          <input type="number" min={0} max={10} value={maxRetries}
            onChange={(e) => setMaxRetries(Number(e.target.value))} disabled={submitting} />
        </label>
      </div>
      <label>
        Payload (JSON)
        <textarea rows={3} value={payload} onChange={(e) => { setPayloadTouched(true); setPayload(e.target.value); }} disabled={submitting} spellCheck={false} />
      </label>
      <button type="submit" disabled={submitting || !name.trim()}>{submitting ? 'Submitting…' : 'Submit'}</button>
      {message && <p className={message.kind === 'ok' ? 'ok' : 'error'} role="status">{message.text}</p>}
    </form>
  );
}

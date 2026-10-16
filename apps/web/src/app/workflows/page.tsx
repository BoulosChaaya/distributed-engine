'use client';

import { useState, type FormEvent } from 'react';
import {
  engine,
  type WorkflowDetailView,
  type WorkflowStepView,
  type WorkflowSummaryView,
} from '@/lib/client';
import { usePoll } from '@/lib/use-poll';

// fetch -> (two shards, one flaky) -> merge -> report: fan-out, retry, fan-in.
const EXAMPLE = JSON.stringify(
  [
    { key: 'fetch', name: 'echo', payload: { source: 'orders' } },
    { key: 'shard-a', name: 'sleep', payload: { ms: 2000 }, dependsOn: ['fetch'] },
    { key: 'shard-b', name: 'flaky', payload: { failTimes: 1 }, dependsOn: ['fetch'] },
    { key: 'merge', name: 'echo', payload: { step: 'merge' }, dependsOn: ['shard-a', 'shard-b'] },
    { key: 'report', name: 'echo', payload: { step: 'report' }, dependsOn: ['merge'] },
  ],
  null,
  2,
);

const PAGE_SIZE = 20;

export default function WorkflowsPage() {
  const [selected, setSelected] = useState<string | null>(null);
  const list = usePoll(
    () => engine<{ items: WorkflowSummaryView[]; total: number }>(`workflows?page=1&pageSize=${PAGE_SIZE}`),
    3000,
  );

  return (
    <>
      <h1>Workflows</h1>
      <p className="muted">
        A workflow is a DAG of tasks. A step runs when every step it depends on has completed. If a step
        fails, everything downstream of it is cancelled, and independent branches keep going.
      </p>
      <SubmitWorkflow onCreated={async (id) => { setSelected(id); await list.refresh(); }} />

      {selected && <WorkflowDetail id={selected} onChanged={list.refresh} onClose={() => setSelected(null)} />}

      {list.error && <p className="error" role="alert">{list.error}</p>}
      {list.data && list.data.items.length === 0 && <p className="muted">No workflows yet.</p>}
      {list.data && list.data.items.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {list.data.items.map((w) => (
                <tr key={w.id} className={`clickable${selected === w.id ? ' selected' : ''}`} onClick={() => setSelected(w.id)}>
                  <td>
                    <button className="link" onClick={() => setSelected(w.id)}>{w.name}</button>
                    <div className="mono muted">{w.id.slice(0, 8)}</div>
                  </td>
                  <td><span className={`badge badge-${w.status.toLowerCase()}`}>{w.status}</span></td>
                  <td>
                    <Progress done={w.completedSteps} failed={w.failedSteps} total={w.totalSteps} />
                  </td>
                  <td>{new Date(w.createdAt).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function Progress({ done, failed, total }: { done: number; failed: number; total: number }) {
  const pct = total ? Math.round((done / total) * 100) : 0;
  return (
    <div className="progress" aria-label={`${done} of ${total} steps completed`}>
      <div className="bar"><span style={{ width: `${pct}%` }} /></div>
      <span className="muted">{done}/{total}{failed > 0 ? ` · ${failed} failed` : ''}</span>
    </div>
  );
}

function WorkflowDetail({ id, onChanged, onClose }: { id: string; onChanged: () => Promise<void>; onClose: () => void }) {
  const detail = usePoll(() => engine<WorkflowDetailView>(`workflows/${id}`), 1500);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function cancel() {
    if (!window.confirm('Cancel this workflow? Every step that has not finished will be cancelled.')) return;
    setBusy(true);
    setError(null);
    try {
      await engine(`workflows/${id}/cancel`, { method: 'PUT' });
      await detail.refresh();
      await onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Cancel failed');
    } finally {
      setBusy(false);
    }
  }

  const levels: WorkflowStepView[][] = [];
  for (const step of detail.data?.steps ?? []) (levels[step.level] ??= []).push(step);

  return (
    <section className="panel" aria-label="Workflow detail">
      <div className="detail-head">
        <div>
          <h2>{detail.data?.workflow.name ?? 'Workflow'}</h2>
          {detail.data && (
            <span className={`badge badge-${detail.data.workflow.status.toLowerCase()}`}>{detail.data.workflow.status}</span>
          )}
        </div>
        <div className="row-actions">
          {detail.data?.workflow.status === 'RUNNING' && (
            <button className="secondary" disabled={busy} onClick={cancel}>{busy ? 'Cancelling…' : 'Cancel workflow'}</button>
          )}
          <button className="secondary" onClick={onClose}>Close</button>
        </div>
      </div>
      {(error || detail.error) && <p className="error" role="alert">{error ?? detail.error}</p>}

      <div className="dag" role="list" aria-label="Steps by dependency level">
        {levels.map((steps, level) => (
          <div className="dag-level" key={level} role="listitem">
            <div className="muted level-label">{level === 0 ? 'start' : `level ${level}`}</div>
            {steps.map((s) => (
              <div className={`step step-${s.status.toLowerCase()}`} key={s.key}>
                <div className="step-head">
                  <strong>{s.key}</strong>
                  <span className={`badge badge-${s.status.toLowerCase()}`}>{s.status}</span>
                </div>
                <div className="muted mono">{s.name}{s.retries > 0 ? ` · retries ${s.retries}/${s.maxRetries}` : ''}</div>
                {s.dependsOn.length > 0 && <div className="muted">after {s.dependsOn.join(', ')}</div>}
                {s.error && s.status !== 'COMPLETED' && <div className="row-error" title={s.error}>{s.error}</div>}
              </div>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}

function SubmitWorkflow({ onCreated }: { onCreated: (id: string) => Promise<void> }) {
  const [name, setName] = useState('nightly-report');
  const [steps, setSteps] = useState(EXAMPLE);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(steps);
    } catch {
      setError('Steps must be valid JSON (an array of steps).');
      return;
    }
    setSubmitting(true);
    try {
      const created = await engine<WorkflowDetailView>('workflows', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim(), steps: parsed }),
      });
      await onCreated(created.workflow.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Create failed');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="panel form" onSubmit={submit}>
      <h2>Run a workflow</h2>
      <label>
        Name
        <input required maxLength={255} value={name} onChange={(e) => setName(e.target.value)} disabled={submitting} />
      </label>
      <label>
        Steps (JSON): key, name (task type), payload, dependsOn
        <textarea rows={10} value={steps} onChange={(e) => setSteps(e.target.value)} disabled={submitting} spellCheck={false} />
      </label>
      <button type="submit" disabled={submitting || !name.trim()}>{submitting ? 'Starting…' : 'Start workflow'}</button>
      {error && <p className="error" role="alert">{error}</p>}
    </form>
  );
}

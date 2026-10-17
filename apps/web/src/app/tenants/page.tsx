'use client';

import { useState, type FormEvent } from 'react';
import { engine, type PlanView, type TenantView } from '@/lib/client';
import { usePoll } from '@/lib/use-poll';

export default function TenantsPage() {
  const tenants = usePoll(() => engine<TenantView[]>('tenants'), 10000);
  const plans = usePoll(() => engine<PlanView[]>('plans'), 60000);
  const [issuedKey, setIssuedKey] = useState<{ tenant: string; key: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  async function act(id: string, fn: () => Promise<void>) {
    setBusy(id);
    setError(null);
    try {
      await fn();
      await tenants.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Request failed');
    } finally {
      setBusy(null);
    }
  }

  const setStatus = (t: TenantView, status: 'ACTIVE' | 'SUSPENDED') =>
    act(t.id, async () => {
      await engine(`tenants/${t.id}/status`, { method: 'PUT', body: JSON.stringify({ status }) });
    });

  const rotate = (t: TenantView) =>
    act(t.id, async () => {
      if (!window.confirm(`Rotate the API key for ${t.name}? The current key stops working immediately.`)) return;
      const r = await engine<{ apiKey: string }>(`tenants/${t.id}/api-key`, { method: 'POST' });
      setIssuedKey({ tenant: t.name, key: r.apiKey });
    });

  const planName = (id: string) => plans.data?.find((p) => p.id === id)?.name ?? id;

  return (
    <>
      <h1>Tenants</h1>
      <CreateTenant plans={plans.data ?? []} onCreated={async (name, key) => {
        setIssuedKey({ tenant: name, key });
        await tenants.refresh();
      }} />

      {issuedKey && (
        <div className="panel key-panel" role="status">
          <strong>API key for {issuedKey.tenant}</strong>
          <p className="muted">Shown once. Only its hash is stored, so copy it now.</p>
          <code className="key">{issuedKey.key}</code>
          <div className="row-actions">
            <button className="secondary" onClick={() => navigator.clipboard?.writeText(issuedKey.key)}>Copy</button>
            <button className="secondary" onClick={() => setIssuedKey(null)}>Done</button>
          </div>
        </div>
      )}

      {(error || tenants.error) && <p className="error" role="alert">{error ?? tenants.error}</p>}
      {tenants.data && tenants.data.length === 0 && <p className="muted">No tenants yet.</p>}
      {tenants.data && tenants.data.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Plan</th>
                <th>Status</th>
                <th>Created</th>
                <th><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {tenants.data.map((t) => (
                <tr key={t.id}>
                  <td>{t.name}<div className="mono muted">{t.id.slice(0, 8)}</div></td>
                  <td>{planName(t.planId)}</td>
                  <td><span className={`badge badge-${t.status === 'ACTIVE' ? 'active' : 'failed'}`}>{t.status}</span></td>
                  <td>{new Date(t.createdAt).toLocaleDateString()}</td>
                  <td>
                    <div className="row-actions">
                      {t.status === 'ACTIVE' ? (
                        <button className="secondary" disabled={busy === t.id} onClick={() => setStatus(t, 'SUSPENDED')}>Suspend</button>
                      ) : (
                        <button className="secondary" disabled={busy === t.id} onClick={() => setStatus(t, 'ACTIVE')}>Activate</button>
                      )}
                      <button className="secondary" disabled={busy === t.id} onClick={() => rotate(t)}>Rotate key</button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function CreateTenant({ plans, onCreated }: { plans: PlanView[]; onCreated: (name: string, key: string) => Promise<void> }) {
  const [name, setName] = useState('');
  const [planId, setPlanId] = useState('plan_starter');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const r = await engine<{ tenant: TenantView; apiKey: string }>('tenants', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim(), planId }),
      });
      setName('');
      await onCreated(r.tenant.name, r.apiKey);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Create failed');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="panel form" onSubmit={submit}>
      <h2>New tenant</h2>
      <div className="row">
        <label>
          Name
          <input required maxLength={255} value={name} onChange={(e) => setName(e.target.value)} disabled={submitting} />
        </label>
        <label>
          Plan
          <select value={planId} onChange={(e) => setPlanId(e.target.value)} disabled={submitting}>
            {plans.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name} · {p.maxConcurrentExecutions} concurrent
              </option>
            ))}
          </select>
        </label>
      </div>
      <button type="submit" disabled={submitting || !name.trim()}>{submitting ? 'Creating…' : 'Create tenant'}</button>
      {error && <p className="error">{error}</p>}
    </form>
  );
}

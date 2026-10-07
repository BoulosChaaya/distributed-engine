'use client';

import React, { useEffect, useState } from 'react';

interface DashboardStats {
  totalTasks: number;
  queuedTasks: number;
  processingTasks: number;
  completedTasks: number;
  failedTasks: number;
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStats>({
    totalTasks: 0,
    queuedTasks: 0,
    processingTasks: 0,
    completedTasks: 0,
    failedTasks: 0,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/metrics`
        );
        const data = await response.json();

        if (data.success) {
          const byStatus = data.data.tasks.byStatus || {};
          setStats({
            totalTasks: data.data.tasks.total || 0,
            queuedTasks: (byStatus.QUEUED || 0) + (byStatus.PENDING || 0),
            processingTasks: byStatus.PROCESSING || 0,
            completedTasks: byStatus.COMPLETED || 0,
            failedTasks: byStatus.FAILED || 0,
          });
          setError(null);
        }
      } catch (err) {
        setError('Unable to connect to API');
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
    const interval = setInterval(fetchStats, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      <h1>Dashboard</h1>
      <p>System Overview</p>

      {loading ? (
        <p>Loading...</p>
      ) : error ? (
        <p style={{ color: 'red' }}>{error}</p>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '16px' }}>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.totalTasks}</h3>
            <p>Total Tasks</p>
          </div>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.queuedTasks}</h3>
            <p>Queued</p>
          </div>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.processingTasks}</h3>
            <p>Processing</p>
          </div>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.completedTasks}</h3>
            <p>Completed</p>
          </div>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.failedTasks}</h3>
            <p>Failed</p>
          </div>
        </div>
      )}

      <div style={{ marginTop: '32px', background: 'white', padding: '16px', borderRadius: '4px' }}>
        <h2>Quick Actions</h2>
        <button onClick={() => (window.location.href = '/tasks')}>Manage Tasks</button>
        <button onClick={() => (window.location.href = '/workers')} style={{ marginLeft: '8px' }}>
          View Workers
        </button>
      </div>
    </div>
  );
}

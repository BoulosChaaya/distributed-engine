'use client';

import React, { useEffect, useState } from 'react';

interface DashboardStats {
  totalTasks: number;
  pendingTasks: number;
  processingTasks: number;
  completedTasks: number;
  failedTasks: number;
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStats>({
    totalTasks: 0,
    pendingTasks: 0,
    processingTasks: 0,
    completedTasks: 0,
    failedTasks: 0,
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/tasks`
        );
        const data = await response.json();

        if (data.success) {
          const tasks = data.data.items || [];
          setStats({
            totalTasks: data.data.total || 0,
            pendingTasks: tasks.filter((t: any) => t.status === 'PENDING').length,
            processingTasks: tasks.filter((t: any) => t.status === 'PROCESSING').length,
            completedTasks: tasks.filter((t: any) => t.status === 'COMPLETED').length,
            failedTasks: tasks.filter((t: any) => t.status === 'FAILED').length,
          });
        }
      } catch (error) {
        console.error('Failed to fetch stats:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
    const interval = setInterval(fetchStats, 5000); // Refresh every 5s
    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      <h1>Dashboard</h1>
      <p>System Overview</p>

      {loading ? (
        <p>Loading...</p>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '16px' }}>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.totalTasks}</h3>
            <p>Total Tasks</p>
          </div>
          <div style={{ background: 'white', padding: '16px', borderRadius: '4px' }}>
            <h3>{stats.pendingTasks}</h3>
            <p>Pending</p>
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

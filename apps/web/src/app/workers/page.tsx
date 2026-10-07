'use client';

import React, { useEffect, useState } from 'react';

interface WorkerInfo {
  id: string;
  status: string;
  uptime: number;
  jobsProcessed: number;
  jobsCompleted: number;
  jobsFailed: number;
  lastHeartbeat: string;
}

export default function WorkersPage() {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchWorkers = async () => {
      try {
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/workers`
        );
        const data = await response.json();

        if (data.success) {
          setWorkers(data.data.workers || []);
          setError(null);
        }
      } catch (err) {
        setError('Unable to connect to API');
      } finally {
        setLoading(false);
      }
    };

    fetchWorkers();
    const interval = setInterval(fetchWorkers, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      <h1>Workers</h1>
      <p>Connected worker instances (live from Redis heartbeats)</p>

      {loading ? (
        <p>Loading workers...</p>
      ) : error ? (
        <p style={{ color: 'red' }}>{error}</p>
      ) : (
        <div style={{ background: 'white', borderRadius: '4px', overflow: 'hidden' }}>
          {workers.length === 0 ? (
            <p style={{ padding: '16px' }}>No workers connected</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Worker ID</th>
                  <th>Status</th>
                  <th>Uptime (s)</th>
                  <th>Processed</th>
                  <th>Completed</th>
                  <th>Failed</th>
                  <th>Last Heartbeat</th>
                </tr>
              </thead>
              <tbody>
                {workers.map((worker) => (
                  <tr key={worker.id}>
                    <td>{worker.id}</td>
                    <td>
                      <span
                        className={`status-badge status-${worker.status.toLowerCase()}`}
                      >
                        {worker.status}
                      </span>
                    </td>
                    <td>{worker.uptime}</td>
                    <td>{worker.jobsProcessed}</td>
                    <td>{worker.jobsCompleted}</td>
                    <td>{worker.jobsFailed}</td>
                    <td>{new Date(worker.lastHeartbeat).toLocaleTimeString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}

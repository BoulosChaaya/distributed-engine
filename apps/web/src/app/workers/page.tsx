'use client';

import React, { useEffect, useState } from 'react';

interface Worker {
  id: string;
  status: string;
  capacity: number;
  activeJobs: number;
  processedJobs: number;
  failedJobs: number;
  lastHeartbeat: string;
}

export default function WorkersPage() {
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchWorkers = async () => {
      try {
        // Placeholder: will fetch from real endpoint in next phase
        setWorkers([
          {
            id: 'worker-1',
            status: 'ONLINE',
            capacity: 5,
            activeJobs: 2,
            processedJobs: 42,
            failedJobs: 1,
            lastHeartbeat: new Date().toISOString(),
          },
        ]);
      } catch (error) {
        console.error('Failed to fetch workers:', error);
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
      <p>Connected worker instances</p>

      {loading ? (
        <p>Loading workers...</p>
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
                  <th>Capacity</th>
                  <th>Active Jobs</th>
                  <th>Processed</th>
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
                    <td>{worker.capacity}</td>
                    <td>{worker.activeJobs}</td>
                    <td>{worker.processedJobs}</td>
                    <td>{worker.failedJobs}</td>
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

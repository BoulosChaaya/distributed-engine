'use client';

import React, { useEffect, useState } from 'react';
import { Task } from '@repo/shared';

export default function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [newTaskName, setNewTaskName] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const fetchTasks = async () => {
    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/tasks`
      );
      const data = await response.json();
      if (data.success) {
        setTasks(data.data.items || []);
      }
    } catch (error) {
      console.error('Failed to fetch tasks:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 3000); // Refresh every 3s
    return () => clearInterval(interval);
  }, []);

  const handleSubmitTask = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTaskName.trim()) return;

    setSubmitting(true);
    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/tasks`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: newTaskName,
            payload: { createdAt: new Date().toISOString() },
          }),
        }
      );

      if (response.ok) {
        setNewTaskName('');
        fetchTasks();
      }
    } catch (error) {
      console.error('Failed to submit task:', error);
    } finally {
      setSubmitting(false);
    }
  };

  const getStatusClass = (status: string) => {
    return `status-badge status-${status.toLowerCase()}`;
  };

  return (
    <div>
      <h1>Tasks</h1>

      <div style={{ background: 'white', padding: '16px', borderRadius: '4px', marginBottom: '24px' }}>
        <h2>Submit New Task</h2>
        <form onSubmit={handleSubmitTask}>
          <input
            type="text"
            placeholder="Task name"
            value={newTaskName}
            onChange={(e) => setNewTaskName(e.target.value)}
            disabled={submitting}
            style={{ padding: '8px', marginRight: '8px', width: '300px' }}
          />
          <button type="submit" disabled={submitting}>
            {submitting ? 'Submitting...' : 'Submit'}
          </button>
        </form>
      </div>

      {loading ? (
        <p>Loading tasks...</p>
      ) : (
        <div style={{ background: 'white', borderRadius: '4px', overflow: 'hidden' }}>
          {tasks.length === 0 ? (
            <p style={{ padding: '16px' }}>No tasks yet</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Created</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((task) => (
                  <tr key={task.id}>
                    <td style={{ fontSize: '12px', fontFamily: 'monospace' }}>{task.id.slice(0, 8)}</td>
                    <td>{task.name}</td>
                    <td>
                      <span className={getStatusClass(task.status)}>{task.status}</span>
                    </td>
                    <td>{task.priority}</td>
                    <td>{new Date(task.createdAt).toLocaleString()}</td>
                    <td>{new Date(task.updatedAt).toLocaleString()}</td>
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

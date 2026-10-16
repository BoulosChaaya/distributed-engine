'use client';

export interface ApiEnvelope<T> {
  success: boolean;
  data?: T;
  error?: string;
}

export class EngineError extends Error {
  constructor(message: string, readonly status: number) {
    super(message);
  }
}

/** Calls the engine through the dashboard's own server (/api/engine/*). */
export async function engine<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api/engine/${path}`, {
    ...init,
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
  });
  let body: ApiEnvelope<T> | null = null;
  try {
    body = (await res.json()) as ApiEnvelope<T>;
  } catch {
    // non-JSON error page
  }
  if (!res.ok || !body?.success) {
    throw new EngineError(body?.error ?? `Request failed (${res.status})`, res.status);
  }
  return body.data as T;
}

export type TaskStatus = 'SCHEDULED' | 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
export type TaskPriority = 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL';

export interface TaskView {
  id: string;
  name: string;
  status: TaskStatus;
  priority: TaskPriority;
  retries: number;
  maxRetries: number;
  error?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface TaskPage {
  items: TaskView[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export interface MetricsView {
  uptime: number;
  tasks: { total: number; byStatus: Partial<Record<TaskStatus, number>> };
  queue: { total: number; active: number; waiting: number };
  outbox: { pending: number; failed: number };
  requests: { total: number; errors: number; errorRate: string };
}

export interface WorkerView {
  id: string;
  status: string;
  uptime: number;
  jobsProcessed: number;
  jobsCompleted: number;
  jobsFailed: number;
  lastHeartbeat: string;
}

export interface PlanView {
  id: string;
  name: string;
  maxConcurrentExecutions: number;
  maxJobsPerPeriod: number;
  rateLimit: number;
  weight: number;
}

export interface TenantView {
  id: string;
  name: string;
  status: 'ACTIVE' | 'SUSPENDED';
  planId: string;
  createdAt: string;
}

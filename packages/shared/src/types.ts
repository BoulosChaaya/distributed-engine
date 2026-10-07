export type TaskStatus = 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
export type TaskPriority = 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL';

export interface Task {
  id: string;
  name: string;
  status: TaskStatus;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  retries: number;
  maxRetries: number;
  result?: Record<string, unknown>;
  error?: string;
  createdAt: Date;
  updatedAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  version: number;
  claimedBy?: string;
  claimToken?: string;
  claimExpiresAt?: Date;
}

export type WorkerStatus = 'ONLINE' | 'OFFLINE' | 'BUSY';

export interface Worker {
  id: string;
  status: WorkerStatus;
  capacity: number;
  activeJobs: number;
  processedJobs: number;
  failedJobs: number;
  lastHeartbeat: Date;
}

export interface QueueMessage {
  taskId: string;
  task: Task;
  timestamp: Date;
  attempt: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  timestamp: Date;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export interface OutboxEvent {
  id: string;
  taskId: string;
  eventType: 'TASK_CREATED';
  payload: Record<string, unknown>;
  status: 'PENDING' | 'DELIVERED' | 'FAILED';
  attempts: number;
  createdAt: Date;
  processedAt?: Date;
  claimedBy?: string;
  claimedAt?: Date;
}

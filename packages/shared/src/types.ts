// Task types
export type TaskStatus = 'PENDING' | 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
export type TaskPriority = 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL';

export interface Task {
  id: string;
  name: string;
  status: TaskStatus;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  retries: number;
  maxRetries: number;
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  error?: string;
}

// Worker types
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

// Queue message types
export interface QueueMessage {
  taskId: string;
  task: Task;
  timestamp: Date;
  attempt: number;
}

// API Response types
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
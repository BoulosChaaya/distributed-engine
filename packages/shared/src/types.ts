export type TaskStatus = 'SCHEDULED' | 'QUEUED' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
export type TaskPriority = 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL';

export interface Task {
  id: string;
  tenantId: string;
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
  scheduledFor?: Date;
  scheduleId?: string;
  version: number;
  claimedBy?: string;
  claimToken?: string;
  claimExpiresAt?: Date;
  idempotencyKey?: string;
  idempotencyHash?: string;
}

export type ScheduleStatus = 'ACTIVE' | 'PAUSED' | 'DISABLED';
export type MisfirePolicy = 'CATCH_UP_ALL' | 'SKIP_MISSED' | 'RUN_ONCE';
export type OverlapPolicy = 'ALLOW_OVERLAP' | 'FORBID_OVERLAP';

export interface RecurringSchedule {
  id: string;
  tenantId: string;
  name: string;
  taskName: string;
  taskPriority: TaskPriority;
  taskPayload: Record<string, unknown>;
  taskMaxRetries: number;
  cronExpression: string;
  timezone: string;
  nextRunAt: Date;
  status: ScheduleStatus;
  misfirePolicy: MisfirePolicy;
  overlapPolicy: OverlapPolicy;
  executionLeaseToken?: string;
  executionLeaseExpiresAt?: Date;
  version: number;
  createdAt: Date;
  updatedAt: Date;
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

export type TenantStatus = 'ACTIVE' | 'SUSPENDED';

export interface Plan {
  id: string;
  name: string;
  rateLimit: number;
  maxConcurrentExecutions: number;
  maxJobsPerPeriod: number;
  billingPeriodDays: number;
  maxComputeUnitsPerPeriod: number;
  maxStorageMb: number;
  weight: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface Tenant {
  id: string;
  name: string;
  status: TenantStatus;
  planId: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface TenantOverride {
  id: string;
  tenantId: string;
  rateLimit?: number;
  maxConcurrentExecutions?: number;
  maxJobsPerPeriod?: number;
  billingPeriodDays?: number;
  maxComputeUnitsPerPeriod?: number;
  maxStorageMb?: number;
  weight?: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface TenantUsage {
  id: string;
  tenantId: string;
  billingPeriodStart: Date;
  acceptedJobs: number;
  computeUnits: number;
  storageMb: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface TenantConcurrencyLease {
  id: string;
  tenantId: string;
  taskId: string;
  workerId: string;
  leaseToken: string;
  expiresAt: Date;
  createdAt: Date;
}

export interface EffectiveLimits {
  rateLimit: number;
  maxConcurrentExecutions: number;
  maxJobsPerPeriod: number;
  billingPeriodDays: number;
  maxComputeUnitsPerPeriod: number;
  maxStorageMb: number;
  weight: number;
}

export type OutboxEventType = 'TASK_CREATED' | 'SCHEDULED_TASK_RELEASED';

export interface OutboxEvent {
  id: string;
  taskId: string;
  eventType: OutboxEventType;
  payload: Record<string, unknown>;
  status: 'PENDING' | 'DELIVERED' | 'FAILED';
  attempts: number;
  createdAt: Date;
  processedAt?: Date;
  claimedBy?: string;
  claimedAt?: Date;
  traceContext?: Record<string, string>;
}

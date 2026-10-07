import { TaskStatus } from './types';

const VALID_TRANSITIONS: Record<TaskStatus, TaskStatus[]> = {
  QUEUED: ['PROCESSING', 'CANCELLED'],
  PROCESSING: ['COMPLETED', 'FAILED', 'QUEUED', 'CANCELLED'],
  COMPLETED: [],
  FAILED: ['QUEUED'],
  CANCELLED: [],
};

export function isValidTransition(from: TaskStatus, to: TaskStatus): boolean {
  return VALID_TRANSITIONS[from]?.includes(to) ?? false;
}

export function assertValidTransition(from: TaskStatus, to: TaskStatus): void {
  if (!isValidTransition(from, to)) {
    throw new InvalidTransitionError(from, to);
  }
}

export class InvalidTransitionError extends Error {
  constructor(
    public readonly from: TaskStatus,
    public readonly to: TaskStatus,
  ) {
    super(`Invalid state transition: ${from} -> ${to}`);
    this.name = 'InvalidTransitionError';
  }
}

export { VALID_TRANSITIONS };

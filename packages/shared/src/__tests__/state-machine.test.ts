import { describe, it, expect } from '@jest/globals';
import { isValidTransition, assertValidTransition, InvalidTransitionError, VALID_TRANSITIONS } from '../state-machine';
import { TaskStatus } from '../types';

describe('Task State Machine', () => {
  describe('isValidTransition', () => {
    it('should allow QUEUED -> PROCESSING', () => {
      expect(isValidTransition('QUEUED', 'PROCESSING')).toBe(true);
    });

    it('should allow QUEUED -> CANCELLED', () => {
      expect(isValidTransition('QUEUED', 'CANCELLED')).toBe(true);
    });

    it('should allow PROCESSING -> COMPLETED', () => {
      expect(isValidTransition('PROCESSING', 'COMPLETED')).toBe(true);
    });

    it('should allow PROCESSING -> FAILED', () => {
      expect(isValidTransition('PROCESSING', 'FAILED')).toBe(true);
    });

    it('should allow PROCESSING -> QUEUED (BullMQ retry)', () => {
      expect(isValidTransition('PROCESSING', 'QUEUED')).toBe(true);
    });

    it('should allow PROCESSING -> CANCELLED', () => {
      expect(isValidTransition('PROCESSING', 'CANCELLED')).toBe(true);
    });

    it('should allow FAILED -> QUEUED (retry)', () => {
      expect(isValidTransition('FAILED', 'QUEUED')).toBe(true);
    });

    it('should reject COMPLETED -> any', () => {
      const statuses: TaskStatus[] = ['QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'];
      for (const status of statuses) {
        expect(isValidTransition('COMPLETED', status)).toBe(false);
      }
    });

    it('should reject CANCELLED -> any', () => {
      const statuses: TaskStatus[] = ['QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'];
      for (const status of statuses) {
        expect(isValidTransition('CANCELLED', status)).toBe(false);
      }
    });

    it('should reject QUEUED -> COMPLETED (skipping PROCESSING)', () => {
      expect(isValidTransition('QUEUED', 'COMPLETED')).toBe(false);
    });
  });

  describe('assertValidTransition', () => {
    it('should not throw for valid transitions', () => {
      expect(() => assertValidTransition('QUEUED', 'PROCESSING')).not.toThrow();
      expect(() => assertValidTransition('PROCESSING', 'COMPLETED')).not.toThrow();
    });

    it('should throw InvalidTransitionError for invalid transitions', () => {
      expect(() => assertValidTransition('COMPLETED', 'PROCESSING')).toThrow(InvalidTransitionError);
    });

    it('should include from/to in error', () => {
      try {
        assertValidTransition('COMPLETED', 'PROCESSING');
      } catch (e) {
        const err = e as InvalidTransitionError;
        expect(err.from).toBe('COMPLETED');
        expect(err.to).toBe('PROCESSING');
        expect(err.message).toContain('COMPLETED');
        expect(err.message).toContain('PROCESSING');
      }
    });
  });

  describe('VALID_TRANSITIONS exhaustiveness', () => {
    it('should define transitions for all statuses', () => {
      const allStatuses: TaskStatus[] = ['QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'];
      for (const status of allStatuses) {
        expect(VALID_TRANSITIONS).toHaveProperty(status);
        expect(Array.isArray(VALID_TRANSITIONS[status])).toBe(true);
      }
    });

    it('should not have self-transitions', () => {
      for (const [from, targets] of Object.entries(VALID_TRANSITIONS)) {
        expect(targets).not.toContain(from);
      }
    });
  });
});

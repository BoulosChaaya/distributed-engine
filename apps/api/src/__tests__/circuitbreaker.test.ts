import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { CircuitBreaker, CircuitState } from '../circuitbreaker';

describe('CircuitBreaker', () => {
  let breaker: CircuitBreaker;

  beforeEach(() => {
    // Create breaker with low thresholds for testing
    breaker = new CircuitBreaker(
      3,      // failureThreshold
      2,      // successThreshold
      100     // resetTimeout (100ms for fast tests)
    );
  });

  describe('Initial State', () => {
    it('should start in CLOSED state', () => {
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('CLOSED → OPEN Transition', () => {
    it('should allow requests in CLOSED state', async () => {
      const fn = jest.fn().mockResolvedValue('success');
      const result = await breaker.execute(fn);
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalled();
    });

    it('should open after failure threshold', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('fail'));

      // Fail 3 times
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fn);
        } catch (e) {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN);
    });

    it('should reject requests immediately when OPEN', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('fail'));

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fn);
        } catch (e) {
          // Expected
        }
      }

      // Try to execute when open
      const fnNotCalled = jest.fn();
      try {
        await breaker.execute(fnNotCalled);
      } catch (e) {
        expect((e as Error).message).toContain('Circuit breaker is OPEN');
      }

      // Function should never be called (instant rejection)
      expect(fnNotCalled).not.toHaveBeenCalled();
    });
  });

  describe('OPEN → HALF_OPEN Transition', () => {
    it('should transition to HALF_OPEN after reset timeout', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('fail'));

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fn);
        } catch (e) {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN);

      // Wait for reset timeout
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Should transition to HALF_OPEN on next check
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);
    });
  });

  describe('HALF_OPEN → CLOSED Transition', () => {
    it('should close after success threshold in HALF_OPEN', async () => {
      const fnFail = jest.fn().mockRejectedValue(new Error('fail'));
      const fnSucceed = jest.fn().mockResolvedValue('success');

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fnFail);
        } catch (e) {
          // Expected
        }
      }

      // Wait for HALF_OPEN
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      // Succeed twice to close
      await breaker.execute(fnSucceed);
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      await breaker.execute(fnSucceed);
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('HALF_OPEN → OPEN Transition', () => {
    it('should reopen if failure occurs in HALF_OPEN', async () => {
      const fnFail = jest.fn().mockRejectedValue(new Error('fail'));

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fnFail);
        } catch (e) {
          // Expected
        }
      }

      // Wait for HALF_OPEN
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      // Fail again
      try {
        await breaker.execute(fnFail);
      } catch (e) {
        // Expected
      }

      // Should be back to OPEN
      expect(breaker.getState()).toBe(CircuitState.OPEN);
    });
  });

  describe('Manual Reset', () => {
    it('should reset circuit to CLOSED', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('fail'));

      // Open the circuit
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(fn);
        } catch (e) {
          // Expected
        }
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN);

      // Manual reset
      breaker.reset();
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });
});

import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { CircuitBreaker, CircuitState } from '../circuitbreaker';

describe('CircuitBreaker', () => {
  let breaker: CircuitBreaker;

  beforeEach(() => {
    breaker = new CircuitBreaker(3, 2, 100);
  });

  describe('Initial State', () => {
    it('should start in CLOSED state', () => {
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('CLOSED -> OPEN Transition', () => {
    it('should allow requests in CLOSED state', async () => {
      const fn = jest.fn<() => Promise<string>>().mockResolvedValue('success');
      const result = await breaker.execute(fn);
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalled();
    });

    it('should open after failure threshold', async () => {
      const fn = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fn); } catch {}
      }

      expect(breaker.getState()).toBe(CircuitState.OPEN);
    });

    it('should reject requests immediately when OPEN', async () => {
      const fn = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fn); } catch {}
      }

      const fnNotCalled = jest.fn<() => Promise<string>>();
      await expect(breaker.execute(fnNotCalled)).rejects.toThrow('Circuit breaker is OPEN');
      expect(fnNotCalled).not.toHaveBeenCalled();
    });

    it('should reset failure count on success', async () => {
      const fnFail = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));
      const fnSucceed = jest.fn<() => Promise<string>>().mockResolvedValue('ok');

      try { await breaker.execute(fnFail); } catch {}
      try { await breaker.execute(fnFail); } catch {}
      await breaker.execute(fnSucceed);
      try { await breaker.execute(fnFail); } catch {}

      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('OPEN -> HALF_OPEN Transition', () => {
    it('should transition to HALF_OPEN after reset timeout', async () => {
      const fn = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fn); } catch {}
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN);

      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);
    });
  });

  describe('HALF_OPEN -> CLOSED Transition', () => {
    it('should close after success threshold in HALF_OPEN', async () => {
      const fnFail = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));
      const fnSucceed = jest.fn<() => Promise<string>>().mockResolvedValue('success');

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fnFail); } catch {}
      }

      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      await breaker.execute(fnSucceed);
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      await breaker.execute(fnSucceed);
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('HALF_OPEN -> OPEN Transition', () => {
    it('should reopen if failure occurs in HALF_OPEN', async () => {
      const fnFail = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fnFail); } catch {}
      }

      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      try { await breaker.execute(fnFail); } catch {}
      expect(breaker.getState()).toBe(CircuitState.OPEN);
    });
  });

  describe('Manual Reset', () => {
    it('should reset circuit to CLOSED', async () => {
      const fn = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fn); } catch {}
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN);

      breaker.reset();
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });
  });

  describe('Full cycle', () => {
    it('should go through CLOSED -> OPEN -> HALF_OPEN -> CLOSED', async () => {
      const fnFail = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));
      const fnSucceed = jest.fn<() => Promise<string>>().mockResolvedValue('ok');

      expect(breaker.getState()).toBe(CircuitState.CLOSED);

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fnFail); } catch {}
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN);

      await new Promise(r => setTimeout(r, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      await breaker.execute(fnSucceed);
      await breaker.execute(fnSucceed);
      expect(breaker.getState()).toBe(CircuitState.CLOSED);
    });

    it('should go through CLOSED -> OPEN -> HALF_OPEN -> OPEN on failure', async () => {
      const fnFail = jest.fn<() => Promise<string>>().mockRejectedValue(new Error('fail'));

      for (let i = 0; i < 3; i++) {
        try { await breaker.execute(fnFail); } catch {}
      }
      expect(breaker.getState()).toBe(CircuitState.OPEN);

      await new Promise(r => setTimeout(r, 150));
      expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

      try { await breaker.execute(fnFail); } catch {}
      expect(breaker.getState()).toBe(CircuitState.OPEN);
    });
  });
});

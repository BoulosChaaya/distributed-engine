import { describe, it, expect } from '@jest/globals';
import { isValidTransition, assertValidTransition, InvalidTransitionError, VALID_TRANSITIONS } from '../state-machine';

describe('State machine: SCHEDULED state', () => {
  it('should allow SCHEDULED -> QUEUED transition', () => {
    expect(isValidTransition('SCHEDULED', 'QUEUED')).toBe(true);
  });

  it('should allow SCHEDULED -> CANCELLED transition', () => {
    expect(isValidTransition('SCHEDULED', 'CANCELLED')).toBe(true);
  });

  it('should forbid SCHEDULED -> PROCESSING', () => {
    expect(isValidTransition('SCHEDULED', 'PROCESSING')).toBe(false);
  });

  it('should forbid SCHEDULED -> COMPLETED', () => {
    expect(isValidTransition('SCHEDULED', 'COMPLETED')).toBe(false);
  });

  it('should forbid SCHEDULED -> FAILED', () => {
    expect(isValidTransition('SCHEDULED', 'FAILED')).toBe(false);
  });

  it('should forbid transitions into SCHEDULED from any state', () => {
    const allStates = Object.keys(VALID_TRANSITIONS) as Array<keyof typeof VALID_TRANSITIONS>;
    for (const state of allStates) {
      expect(isValidTransition(state, 'SCHEDULED')).toBe(false);
    }
  });

  it('should throw InvalidTransitionError for forbidden SCHEDULED transitions', () => {
    expect(() => assertValidTransition('SCHEDULED', 'PROCESSING')).toThrow(InvalidTransitionError);
    expect(() => assertValidTransition('SCHEDULED', 'COMPLETED')).toThrow(InvalidTransitionError);
    expect(() => assertValidTransition('SCHEDULED', 'FAILED')).toThrow(InvalidTransitionError);
  });

  it('should not throw for valid SCHEDULED transitions', () => {
    expect(() => assertValidTransition('SCHEDULED', 'QUEUED')).not.toThrow();
    expect(() => assertValidTransition('SCHEDULED', 'CANCELLED')).not.toThrow();
  });

  it('should preserve all existing state transitions', () => {
    // QUEUED transitions
    expect(isValidTransition('QUEUED', 'PROCESSING')).toBe(true);
    expect(isValidTransition('QUEUED', 'CANCELLED')).toBe(true);

    // PROCESSING transitions
    expect(isValidTransition('PROCESSING', 'COMPLETED')).toBe(true);
    expect(isValidTransition('PROCESSING', 'FAILED')).toBe(true);
    expect(isValidTransition('PROCESSING', 'QUEUED')).toBe(true);
    expect(isValidTransition('PROCESSING', 'CANCELLED')).toBe(true);

    // Terminal states
    expect(isValidTransition('COMPLETED', 'QUEUED')).toBe(false);
    expect(isValidTransition('CANCELLED', 'QUEUED')).toBe(false);

    // FAILED -> retry
    expect(isValidTransition('FAILED', 'QUEUED')).toBe(true);
  });
});

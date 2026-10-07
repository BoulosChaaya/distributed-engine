import { describe, it, expect } from '@jest/globals';
import { SubmitTaskSchema, PaginationSchema, ValidationError } from '../validation';

describe('API Validation', () => {
  describe('SubmitTaskSchema', () => {
    it('should accept valid task submission', () => {
      const result = SubmitTaskSchema.parse({
        name: 'test-task',
        payload: { key: 'value' },
        priority: 'HIGH',
        maxRetries: 3,
      });

      expect(result.name).toBe('test-task');
      expect(result.priority).toBe('HIGH');
      expect(result.maxRetries).toBe(3);
    });

    it('should apply defaults for optional fields', () => {
      const result = SubmitTaskSchema.parse({ name: 'minimal-task' });
      expect(result.priority).toBe('NORMAL');
      expect(result.maxRetries).toBe(3);
      expect(result.payload).toEqual({});
    });

    it('should reject empty name', () => {
      expect(() => SubmitTaskSchema.parse({ name: '' })).toThrow();
    });

    it('should reject invalid priority', () => {
      expect(() => SubmitTaskSchema.parse({ name: 'test', priority: 'INVALID' })).toThrow();
    });

    it('should reject negative maxRetries', () => {
      expect(() => SubmitTaskSchema.parse({ name: 'test', maxRetries: -1 })).toThrow();
    });

    it('should reject maxRetries > 10', () => {
      expect(() => SubmitTaskSchema.parse({ name: 'test', maxRetries: 11 })).toThrow();
    });

    it('should accept all valid priorities', () => {
      for (const priority of ['LOW', 'NORMAL', 'HIGH', 'CRITICAL']) {
        const result = SubmitTaskSchema.parse({ name: 'test', priority });
        expect(result.priority).toBe(priority);
      }
    });
  });

  describe('PaginationSchema', () => {
    it('should parse valid pagination', () => {
      const result = PaginationSchema.parse({ page: '2', pageSize: '20' });
      expect(result.page).toBe(2);
      expect(result.pageSize).toBe(20);
    });

    it('should apply defaults', () => {
      const result = PaginationSchema.parse({});
      expect(result.page).toBe(1);
      expect(result.pageSize).toBe(10);
    });

    it('should reject page < 1', () => {
      expect(() => PaginationSchema.parse({ page: '0' })).toThrow();
    });

    it('should reject pageSize > 100', () => {
      expect(() => PaginationSchema.parse({ pageSize: '101' })).toThrow();
    });
  });

  describe('ValidationError', () => {
    it('should format errors as JSON', () => {
      try {
        SubmitTaskSchema.parse({ name: '' });
      } catch (e: any) {
        const validationError = new ValidationError(e);
        const json = validationError.toJSON();
        expect(json.errors).toBeDefined();
        expect(json.errors.length).toBeGreaterThan(0);
        expect(json.errors[0].field).toBeDefined();
        expect(json.errors[0].message).toBeDefined();
      }
    });
  });
});

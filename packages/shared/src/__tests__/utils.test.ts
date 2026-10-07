import { describe, it, expect } from '@jest/globals';
import { generateId, retryWithBackoff, AppError } from '../utils';

describe('Shared Utilities', () => {
  describe('generateId', () => {
    it('should generate a unique ID each time', () => {
      const id1 = generateId();
      const id2 = generateId();
      expect(id1).not.toBe(id2);
    });

    it('should generate IDs with correct format', () => {
      const id = generateId();
      expect(typeof id).toBe('string');
      expect(id.length).toBe(32);
    });
  });

  describe('retryWithBackoff', () => {
    it('should succeed on first attempt', async () => {
      const fn = jest.fn().mockResolvedValue('success');
      const result = await retryWithBackoff(fn);
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('should retry on failure and eventually succeed', async () => {
      const fn = jest
        .fn()
        .mockRejectedValueOnce(new Error('fail 1'))
        .mockRejectedValueOnce(new Error('fail 2'))
        .mockResolvedValueOnce('success');

      const result = await retryWithBackoff(fn, 3, 10);
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('should fail after max retries exceeded', async () => {
      const fn = jest.fn().mockRejectedValue(new Error('always fails'));

      await expect(retryWithBackoff(fn, 2, 10)).rejects.toThrow('always fails');
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('should use exponential backoff delays', async () => {
      const fn = jest
        .fn()
        .mockRejectedValueOnce(new Error('fail'))
        .mockResolvedValue('ok');

      const start = Date.now();
      await retryWithBackoff(fn, 3, 50);
      const elapsed = Date.now() - start;
      expect(elapsed).toBeGreaterThanOrEqual(40);
    });
  });

  describe('AppError', () => {
    it('should create error with status code', () => {
      const error = new AppError(404, 'Not found');
      expect(error.statusCode).toBe(404);
      expect(error.message).toBe('Not found');
    });

    it('should be an instance of Error', () => {
      const error = new AppError(500, 'Server error');
      expect(error).toBeInstanceOf(Error);
    });

    it('should have name set to AppError', () => {
      const error = new AppError(400, 'Bad request');
      expect(error.name).toBe('AppError');
    });
  });
});

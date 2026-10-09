import { describe, it, expect } from '@jest/globals';
import { getNextOccurrence, getNextOccurrences, getMissedOccurrences } from '../scheduler/cron-utils';

describe('cron-utils', () => {
  describe('getNextOccurrence', () => {
    it('should return the next occurrence after a given date', () => {
      const after = new Date('2025-01-01T00:00:00Z');
      const next = getNextOccurrence('0 * * * *', 'UTC', after);
      expect(next.toISOString()).toBe('2025-01-01T01:00:00.000Z');
    });

    it('should respect timezone for daily cron', () => {
      const after = new Date('2025-01-01T00:00:00Z');
      const nextUTC = getNextOccurrence('0 9 * * *', 'UTC', after);
      expect(nextUTC.toISOString()).toBe('2025-01-01T09:00:00.000Z');

      const nextNY = getNextOccurrence('0 9 * * *', 'America/New_York', after);
      expect(nextNY.toISOString()).toBe('2025-01-01T14:00:00.000Z');
    });

    it('should handle every-5-minutes cron', () => {
      const after = new Date('2025-01-01T00:03:00Z');
      const next = getNextOccurrence('*/5 * * * *', 'UTC', after);
      expect(next.toISOString()).toBe('2025-01-01T00:05:00.000Z');
    });

    it('should handle day-of-week cron (weekdays only)', () => {
      // 2025-01-04 is Saturday
      const saturday = new Date('2025-01-04T00:00:00Z');
      const next = getNextOccurrence('0 9 * * 1-5', 'UTC', saturday);
      // Next weekday is Monday Jan 6
      expect(next.toISOString()).toBe('2025-01-06T09:00:00.000Z');
    });

    it('should handle DST spring-forward (America/New_York)', () => {
      // 2025-03-09 DST spring forward in New York (2:00 AM -> 3:00 AM)
      const beforeDST = new Date('2025-03-09T06:00:00Z'); // 1:00 AM EST
      const next = getNextOccurrence('30 2 * * *', 'America/New_York', beforeDST);
      // 2:30 AM doesn't exist on spring-forward day; cron-parser skips to next valid
      expect(next.getTime()).toBeGreaterThan(beforeDST.getTime());
    });

    it('should handle DST fall-back (America/New_York)', () => {
      // 2025-11-02 DST fall back in New York (2:00 AM -> 1:00 AM)
      const beforeFallback = new Date('2025-11-02T04:00:00Z'); // midnight EDT
      const next = getNextOccurrence('0 9 * * *', 'America/New_York', beforeFallback);
      // 9:00 AM EST = 14:00 UTC (EST is UTC-5 after fall back)
      expect(next.toISOString()).toBe('2025-11-02T14:00:00.000Z');
    });
  });

  describe('getNextOccurrences', () => {
    it('should return the requested number of occurrences', () => {
      const after = new Date('2025-01-01T00:00:00Z');
      const results = getNextOccurrences('0 * * * *', 'UTC', after, 5);
      expect(results.length).toBe(5);
      expect(results[0].toISOString()).toBe('2025-01-01T01:00:00.000Z');
      expect(results[4].toISOString()).toBe('2025-01-01T05:00:00.000Z');
    });

    it('should return occurrences in ascending order', () => {
      const after = new Date('2025-01-01T00:00:00Z');
      const results = getNextOccurrences('*/15 * * * *', 'UTC', after, 4);
      for (let i = 1; i < results.length; i++) {
        expect(results[i].getTime()).toBeGreaterThan(results[i - 1].getTime());
      }
    });
  });

  describe('getMissedOccurrences', () => {
    it('should return all missed occurrences within range', () => {
      const from = new Date('2025-01-01T00:00:00Z');
      const until = new Date('2025-01-01T05:00:00Z');
      const missed = getMissedOccurrences('0 * * * *', 'UTC', from, until, 100);
      expect(missed.length).toBe(5);
      expect(missed[0].toISOString()).toBe('2025-01-01T01:00:00.000Z');
      expect(missed[4].toISOString()).toBe('2025-01-01T05:00:00.000Z');
    });

    it('should respect the limit (bounded batch)', () => {
      const from = new Date('2025-01-01T00:00:00Z');
      const until = new Date('2025-01-01T23:59:00Z');
      const missed = getMissedOccurrences('0 * * * *', 'UTC', from, until, 3);
      expect(missed.length).toBe(3);
      expect(missed[0].toISOString()).toBe('2025-01-01T01:00:00.000Z');
      expect(missed[2].toISOString()).toBe('2025-01-01T03:00:00.000Z');
    });

    it('should return empty array when no occurrences in range', () => {
      const from = new Date('2025-01-01T00:00:00Z');
      const until = new Date('2025-01-01T00:30:00Z');
      const missed = getMissedOccurrences('0 * * * *', 'UTC', from, until, 100);
      expect(missed.length).toBe(0);
    });

    it('should not include occurrences past until boundary', () => {
      const from = new Date('2025-01-01T00:00:00Z');
      const until = new Date('2025-01-01T02:30:00Z');
      const missed = getMissedOccurrences('0 * * * *', 'UTC', from, until, 100);
      expect(missed.length).toBe(2);
      expect(missed[1].toISOString()).toBe('2025-01-01T02:00:00.000Z');
    });

    it('should handle timezone-aware missed occurrences', () => {
      // After midnight UTC → before 9 PM UTC; 9 AM Tokyo = 0:00 UTC
      const from = new Date('2025-01-01T00:01:00Z');
      const until = new Date('2025-01-01T20:00:00Z');
      const missed = getMissedOccurrences('0 9 * * *', 'Asia/Tokyo', from, until, 10);
      // 9 AM JST on Jan 2 = 2025-01-02T00:00:00Z which is past until, so 0 occurrences
      // However 9 AM JST on Jan 1 = 2025-01-01T00:00:00Z which is before from (0:01Z)
      expect(missed.length).toBe(0);
    });

    it('should handle DST transitions across missed window', () => {
      // Window spanning DST spring-forward in America/New_York (March 9, 2025)
      const from = new Date('2025-03-09T00:00:00Z');
      const until = new Date('2025-03-10T00:00:00Z');
      const missed = getMissedOccurrences('0 * * * *', 'America/New_York', from, until, 100);
      // 24 hours of UTC window — cron fires every hour by local clock
      // Spring-forward skips 2 AM local, but the UTC endpoints are fixed at 24 hours apart
      expect(missed.length).toBe(24);
    });

    it('should support bounded catch-up across multiple passes', () => {
      const from = new Date('2025-01-01T00:00:00Z');
      const until = new Date('2025-01-01T10:00:00Z');

      // First batch of 3
      const batch1 = getMissedOccurrences('0 * * * *', 'UTC', from, until, 3);
      expect(batch1.length).toBe(3);
      expect(batch1[2].toISOString()).toBe('2025-01-01T03:00:00.000Z');

      // Second batch of 3 starting from the last of batch1
      const batch2 = getMissedOccurrences('0 * * * *', 'UTC', batch1[2], until, 3);
      expect(batch2.length).toBe(3);
      expect(batch2[0].toISOString()).toBe('2025-01-01T04:00:00.000Z');

      // All occurrences eventually covered
      const batch3 = getMissedOccurrences('0 * * * *', 'UTC', batch2[2], until, 10);
      expect(batch3.length).toBe(4);
      expect(batch3[3].toISOString()).toBe('2025-01-01T10:00:00.000Z');
    });
  });
});

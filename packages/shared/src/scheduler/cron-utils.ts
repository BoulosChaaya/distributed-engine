import { parseExpression } from 'cron-parser';

export function getNextOccurrence(
  cronExpression: string,
  timezone: string,
  after: Date,
): Date {
  const interval = parseExpression(cronExpression, {
    currentDate: after,
    tz: timezone,
  });
  return interval.next().toDate();
}

export function getNextOccurrences(
  cronExpression: string,
  timezone: string,
  after: Date,
  count: number,
): Date[] {
  const interval = parseExpression(cronExpression, {
    currentDate: after,
    tz: timezone,
  });
  const results: Date[] = [];
  for (let i = 0; i < count; i++) {
    results.push(interval.next().toDate());
  }
  return results;
}

export function getMissedOccurrences(
  cronExpression: string,
  timezone: string,
  from: Date,
  until: Date,
  limit: number,
): Date[] {
  const interval = parseExpression(cronExpression, {
    currentDate: from,
    endDate: until,
    tz: timezone,
  });

  const results: Date[] = [];
  try {
    while (results.length < limit) {
      const next = interval.next().toDate();
      if (next > until) break;
      results.push(next);
    }
  } catch {
    // Iterator exhausted
  }
  return results;
}

import { CronExpressionParser } from 'cron-parser';

// cron-parser v5: CronExpressionParser.parse replaces v4's parseExpression;
// the options used here (currentDate, endDate, tz) keep their meaning.
const parseExpression = (expression: string, options: { currentDate: Date; endDate?: Date; tz: string }) =>
  CronExpressionParser.parse(expression, options);

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

/**
 * The most recent occurrence at or before `atOrBefore`. Used to tell "due now"
 * from "missed": the gap between it and now is how late the schedule is.
 */
export function getLatestOccurrence(
  cronExpression: string,
  timezone: string,
  atOrBefore: Date,
): Date {
  const interval = parseExpression(cronExpression, {
    currentDate: new Date(atOrBefore.getTime() + 1),
    tz: timezone,
  });
  return interval.prev().toDate();
}

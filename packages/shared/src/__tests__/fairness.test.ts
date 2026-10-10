import { describe, it, expect } from '@jest/globals';
import { WeightedFairScheduler, TenantWeightEntry } from '../fairness';

describe('WeightedFairScheduler', () => {
  it('should return null for empty tenant list', () => {
    const scheduler = new WeightedFairScheduler();
    expect(scheduler.selectNext([])).toBeNull();
  });

  it('should return the only tenant for singleton list', () => {
    const scheduler = new WeightedFairScheduler();
    expect(scheduler.selectNext([{ tenantId: 'a', weight: 1 }])).toBe('a');
  });

  it('should distribute proportionally to weight over many iterations', () => {
    const scheduler = new WeightedFairScheduler();
    const tenants: TenantWeightEntry[] = [
      { tenantId: 'heavy', weight: 3 },
      { tenantId: 'light', weight: 1 },
    ];

    const counts: Record<string, number> = { heavy: 0, light: 0 };
    const totalRounds = 400;

    for (let i = 0; i < totalRounds; i++) {
      const selected = scheduler.selectNext(tenants)!;
      counts[selected]++;
    }

    const heavyRatio = counts.heavy / totalRounds;
    const lightRatio = counts.light / totalRounds;
    expect(heavyRatio).toBeGreaterThan(0.6);
    expect(heavyRatio).toBeLessThan(0.85);
    expect(lightRatio).toBeGreaterThan(0.15);
    expect(lightRatio).toBeLessThan(0.4);
  });

  it('STARVATION PROOF: continuously backlogged high-weight tenant cannot permanently starve lower-weight tenant', () => {
    const scheduler = new WeightedFairScheduler();
    const tenants: TenantWeightEntry[] = [
      { tenantId: 'enterprise', weight: 10 },
      { tenantId: 'starter', weight: 1 },
    ];

    const windowSize = 50;
    let starterSeen = false;
    let maxGapWithoutStarter = 0;
    let currentGap = 0;

    for (let i = 0; i < 1000; i++) {
      const selected = scheduler.selectNext(tenants)!;

      if (selected === 'starter') {
        starterSeen = true;
        if (currentGap > maxGapWithoutStarter) {
          maxGapWithoutStarter = currentGap;
        }
        currentGap = 0;
      } else {
        currentGap++;
      }
    }

    expect(starterSeen).toBe(true);

    // The lighter tenant should appear regularly — bounded gap
    // With weights 10:1, the starter gets ~1/11 of selections
    // So the gap between consecutive starter selections should be bounded
    expect(maxGapWithoutStarter).toBeLessThan(15);

    // Over any window of 50 rounds, the lighter tenant must appear at least once
    const selections: string[] = [];
    scheduler.reset();
    for (let i = 0; i < 200; i++) {
      selections.push(scheduler.selectNext(tenants)!);
    }

    for (let start = 0; start <= selections.length - windowSize; start++) {
      const window = selections.slice(start, start + windowSize);
      const starterInWindow = window.filter(s => s === 'starter').length;
      expect(starterInWindow).toBeGreaterThan(0);
    }
  });

  it('should handle equal weights fairly', () => {
    const scheduler = new WeightedFairScheduler();
    const tenants: TenantWeightEntry[] = [
      { tenantId: 'a', weight: 1 },
      { tenantId: 'b', weight: 1 },
      { tenantId: 'c', weight: 1 },
    ];

    const counts: Record<string, number> = { a: 0, b: 0, c: 0 };
    for (let i = 0; i < 300; i++) {
      counts[scheduler.selectNext(tenants)!]++;
    }

    expect(counts.a).toBe(100);
    expect(counts.b).toBe(100);
    expect(counts.c).toBe(100);
  });

  it('should clean up departed tenants', () => {
    const scheduler = new WeightedFairScheduler();

    scheduler.selectNext([
      { tenantId: 'a', weight: 1 },
      { tenantId: 'b', weight: 1 },
    ]);

    const selected = scheduler.selectNext([
      { tenantId: 'b', weight: 1 },
      { tenantId: 'c', weight: 1 },
    ]);

    expect(selected).toBeDefined();
  });

  it('should reset state', () => {
    const scheduler = new WeightedFairScheduler();
    const tenants: TenantWeightEntry[] = [
      { tenantId: 'a', weight: 1 },
      { tenantId: 'b', weight: 1 },
    ];

    for (let i = 0; i < 100; i++) {
      scheduler.selectNext(tenants);
    }

    scheduler.reset();
    const first = scheduler.selectNext(tenants);
    expect(first).toBe('a');
  });
});

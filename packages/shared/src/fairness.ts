export interface TenantWeightEntry {
  tenantId: string;
  weight: number;
}

export class WeightedFairScheduler {
  private deficit: Map<string, number> = new Map();

  selectNext(tenants: TenantWeightEntry[]): string | null {
    if (tenants.length === 0) return null;
    if (tenants.length === 1) return tenants[0].tenantId;

    for (const t of tenants) {
      if (!this.deficit.has(t.tenantId)) {
        this.deficit.set(t.tenantId, 0);
      }
    }

    const eligible = new Set(tenants.map((t) => t.tenantId));
    for (const key of this.deficit.keys()) {
      if (!eligible.has(key)) this.deficit.delete(key);
    }

    let minDeficit = Infinity;
    let selected: string | null = null;

    for (const t of tenants) {
      const d = this.deficit.get(t.tenantId)!;
      if (d < minDeficit) {
        minDeficit = d;
        selected = t.tenantId;
      }
    }

    if (selected) {
      const entry = tenants.find((t) => t.tenantId === selected)!;
      const quantum = 1 / entry.weight;
      this.deficit.set(selected, (this.deficit.get(selected) ?? 0) + quantum);
    }

    return selected;
  }

  reset(): void {
    this.deficit.clear();
  }
}

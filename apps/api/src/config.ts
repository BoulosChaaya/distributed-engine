export interface AppConfig {
  port: number;
  nodeEnv: 'development' | 'staging' | 'production';

  redis: {
    host: string;
    port: number;
    password?: string;
  };

  postgres: {
    host: string;
    port: number;
    database: string;
    user: string;
    password?: string;
    maxConnections: number;
  };

  gracefulShutdown: {
    timeoutMs: number;
  };

  outbox: {
    pollIntervalMs: number;
    batchSize: number;
    maxAttempts: number;
    publishTimeoutMs: number;
    deliveredRetentionHours: number;
  };

  operator: {
    apiKey?: string;
  };

  health: {
    dependencyTimeoutMs: number;
  };

  reconciler: {
    intervalMs: number;
    queuedGraceMs: number;
    batchSize: number;
  };

  scheduler?: {
    pollIntervalMs: number;
    batchSize: number;
    catchUpBatchSize: number;
    executionLeaseDurationMs: number;
    misfireThresholdMs: number;
  };

  logging: {
    level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
  };
}

function parseNumber(value: string | undefined, defaultValue: number): number {
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

export function loadConfig(): AppConfig {
  const nodeEnv = (process.env.NODE_ENV as AppConfig['nodeEnv']) || 'development';

  return {
    port: parseNumber(process.env.PORT, 3000),
    nodeEnv,

    redis: {
      host: process.env.REDIS_HOST || 'localhost',
      port: parseNumber(process.env.REDIS_PORT, 6379),
      password: process.env.REDIS_PASSWORD,
    },

    postgres: {
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseNumber(process.env.POSTGRES_PORT, 5432),
      database: process.env.POSTGRES_DB || 'distributed_engine',
      user: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASSWORD,
      maxConnections: parseNumber(process.env.POSTGRES_MAX_CONNECTIONS, 20),
    },

    gracefulShutdown: {
      timeoutMs: parseNumber(process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS, 30000),
    },

    outbox: {
      pollIntervalMs: parseNumber(process.env.OUTBOX_POLL_INTERVAL_MS, 1000),
      batchSize: parseNumber(process.env.OUTBOX_BATCH_SIZE, 10),
      maxAttempts: parseNumber(process.env.OUTBOX_MAX_ATTEMPTS, 5),
      publishTimeoutMs: parseNumber(process.env.OUTBOX_PUBLISH_TIMEOUT_MS, 5000),
      deliveredRetentionHours: parseNumber(process.env.OUTBOX_DELIVERED_RETENTION_HOURS, 168),
    },

    operator: {
      apiKey: process.env.OPERATOR_API_KEY || undefined,
    },

    health: {
      dependencyTimeoutMs: parseNumber(process.env.HEALTH_DEPENDENCY_TIMEOUT_MS, 1000),
    },

    reconciler: {
      intervalMs: parseNumber(process.env.RECONCILER_INTERVAL_MS, 30000),
      queuedGraceMs: parseNumber(process.env.RECONCILER_QUEUED_GRACE_MS, 60000),
      batchSize: parseNumber(process.env.RECONCILER_BATCH_SIZE, 100),
    },

    scheduler: {
      pollIntervalMs: parseNumber(process.env.SCHEDULER_POLL_INTERVAL_MS, 5000),
      batchSize: parseNumber(process.env.SCHEDULER_BATCH_SIZE, 50),
      catchUpBatchSize: parseNumber(process.env.SCHEDULER_CATCH_UP_BATCH_SIZE, 10),
      executionLeaseDurationMs: parseNumber(process.env.SCHEDULER_EXECUTION_LEASE_DURATION_MS, 300000),
      misfireThresholdMs: parseNumber(process.env.SCHEDULER_MISFIRE_THRESHOLD_MS, 60000),
    },

    logging: {
      level: (process.env.LOG_LEVEL as AppConfig['logging']['level']) || 'INFO',
    },
  };
}

export function validateConfig(config: AppConfig): void {
  const errors: string[] = [];

  if (config.port < 1 || config.port > 65535) {
    errors.push('PORT must be between 1 and 65535');
  }
  if (config.redis.port < 1 || config.redis.port > 65535) {
    errors.push('REDIS_PORT must be between 1 and 65535');
  }
  if (config.postgres.port < 1 || config.postgres.port > 65535) {
    errors.push('POSTGRES_PORT must be between 1 and 65535');
  }
  if (config.postgres.maxConnections < 1) {
    errors.push('POSTGRES_MAX_CONNECTIONS must be at least 1');
  }
  if (config.outbox.pollIntervalMs < 100) {
    errors.push('OUTBOX_POLL_INTERVAL_MS must be at least 100');
  }
  if (config.outbox.batchSize < 1) {
    errors.push('OUTBOX_BATCH_SIZE must be at least 1');
  }
  if (config.outbox.publishTimeoutMs < 100) {
    errors.push('OUTBOX_PUBLISH_TIMEOUT_MS must be at least 100');
  }
  if (config.outbox.deliveredRetentionHours < 0) {
    errors.push('OUTBOX_DELIVERED_RETENTION_HOURS must be 0 (keep forever) or more');
  }
  if (config.outbox.maxAttempts < 1) {
    errors.push('OUTBOX_MAX_ATTEMPTS must be at least 1');
  }
  if (config.operator.apiKey !== undefined && config.operator.apiKey.length < 32) {
    errors.push('OPERATOR_API_KEY must be at least 32 characters (generate one with: openssl rand -hex 32)');
  }
  if (config.reconciler.intervalMs < 1000) {
    errors.push('RECONCILER_INTERVAL_MS must be at least 1000');
  }
  if (config.reconciler.queuedGraceMs < 5000) {
    errors.push('RECONCILER_QUEUED_GRACE_MS must be at least 5000');
  }
  if (config.reconciler.batchSize < 1) {
    errors.push('RECONCILER_BATCH_SIZE must be at least 1');
  }
  if (config.health.dependencyTimeoutMs < 50) {
    errors.push('HEALTH_DEPENDENCY_TIMEOUT_MS must be at least 50');
  }
  if (config.gracefulShutdown.timeoutMs < 1000) {
    errors.push('GRACEFUL_SHUTDOWN_TIMEOUT_MS must be at least 1000');
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }
}

export const config = loadConfig();
validateConfig(config);

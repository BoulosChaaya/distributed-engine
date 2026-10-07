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

  queue: {
    concurrency: number;
    maxAttempts: number;
    backoffDelayMs: number;
  };

  gracefulShutdown: {
    timeoutMs: number;
  };

  outbox: {
    pollIntervalMs: number;
    batchSize: number;
    maxAttempts: number;
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

    queue: {
      concurrency: parseNumber(process.env.QUEUE_CONCURRENCY, 5),
      maxAttempts: parseNumber(process.env.QUEUE_MAX_ATTEMPTS, 3),
      backoffDelayMs: parseNumber(process.env.QUEUE_BACKOFF_DELAY_MS, 2000),
    },

    gracefulShutdown: {
      timeoutMs: parseNumber(process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS, 30000),
    },

    outbox: {
      pollIntervalMs: parseNumber(process.env.OUTBOX_POLL_INTERVAL_MS, 1000),
      batchSize: parseNumber(process.env.OUTBOX_BATCH_SIZE, 10),
      maxAttempts: parseNumber(process.env.OUTBOX_MAX_ATTEMPTS, 5),
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
  if (config.queue.concurrency < 1) {
    errors.push('QUEUE_CONCURRENCY must be at least 1');
  }
  if (config.queue.maxAttempts < 1) {
    errors.push('QUEUE_MAX_ATTEMPTS must be at least 1');
  }
  if (config.queue.backoffDelayMs < 0) {
    errors.push('QUEUE_BACKOFF_DELAY_MS must be non-negative');
  }
  if (config.outbox.pollIntervalMs < 100) {
    errors.push('OUTBOX_POLL_INTERVAL_MS must be at least 100');
  }
  if (config.outbox.batchSize < 1) {
    errors.push('OUTBOX_BATCH_SIZE must be at least 1');
  }
  if (config.outbox.maxAttempts < 1) {
    errors.push('OUTBOX_MAX_ATTEMPTS must be at least 1');
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

// Centralized configuration management
// All environment variables and configuration in one place for production

export interface AppConfig {
  // Server
  port: number;
  nodeEnv: 'development' | 'staging' | 'production';

  // Redis
  redis: {
    host: string;
    port: number;
    password?: string;
    db?: number;
    maxRetries: number;
    retryDelayMs: number;
    connectTimeoutMs: number;
    keepAlive: boolean;
  };

  // Queue
  queue: {
    concurrency: number;
    maxAttempts: number;
    backoffDelayMs: number;
  };

  // Circuit Breaker
  circuitBreaker: {
    failureThreshold: number;
    successThreshold: number;
    resetTimeoutMs: number;
  };

  // Graceful Shutdown
  gracefulShutdown: {
    timeoutMs: number;
  };

  // Logging
  logging: {
    level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
  };
}

function parseNumber(value: string | undefined, defaultValue: number): number {
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

function parseBoolean(value: string | undefined, defaultValue: boolean): boolean {
  if (!value) return defaultValue;
  return value.toLowerCase() === 'true' || value === '1';
}

// Load and validate configuration from environment
export function loadConfig(): AppConfig {
  const nodeEnv = (process.env.NODE_ENV as any) || 'development';

  return {
    port: parseNumber(process.env.PORT, 3000),
    nodeEnv,

    redis: {
      host: process.env.REDIS_HOST || 'localhost',
      port: parseNumber(process.env.REDIS_PORT, 6379),
      password: process.env.REDIS_PASSWORD,
      db: parseNumber(process.env.REDIS_DB, 0),
      maxRetries: parseNumber(process.env.REDIS_MAX_RETRIES, 10),
      retryDelayMs: parseNumber(process.env.REDIS_RETRY_DELAY_MS, 50),
      connectTimeoutMs: parseNumber(process.env.REDIS_CONNECT_TIMEOUT_MS, 10000),
      keepAlive: parseBoolean(process.env.REDIS_KEEP_ALIVE, true),
    },

    queue: {
      concurrency: parseNumber(process.env.QUEUE_CONCURRENCY, 5),
      maxAttempts: parseNumber(process.env.QUEUE_MAX_ATTEMPTS, 3),
      backoffDelayMs: parseNumber(process.env.QUEUE_BACKOFF_DELAY_MS, 2000),
    },

    circuitBreaker: {
      failureThreshold: parseNumber(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD, 5),
      successThreshold: parseNumber(process.env.CIRCUIT_BREAKER_SUCCESS_THRESHOLD, 2),
      resetTimeoutMs: parseNumber(process.env.CIRCUIT_BREAKER_RESET_TIMEOUT_MS, 30000),
    },

    gracefulShutdown: {
      timeoutMs: parseNumber(process.env.GRACEFUL_SHUTDOWN_TIMEOUT_MS, 30000),
    },

    logging: {
      level: (process.env.LOG_LEVEL as any) || 'INFO',
    },
  };
}

// Validate configuration
export function validateConfig(config: AppConfig): void {
  const errors: string[] = [];

  if (config.port < 1 || config.port > 65535) {
    errors.push('PORT must be between 1 and 65535');
  }

  if (config.redis.port < 1 || config.redis.port > 65535) {
    errors.push('REDIS_PORT must be between 1 and 65535');
  }

  if (config.queue.concurrency < 1) {
    errors.push('QUEUE_CONCURRENCY must be at least 1');
  }

  if (config.circuitBreaker.failureThreshold < 1) {
    errors.push('CIRCUIT_BREAKER_FAILURE_THRESHOLD must be at least 1');
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
  }
}

// Export singleton config instance
export const config = loadConfig();
validateConfig(config);

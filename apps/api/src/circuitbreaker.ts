// Circuit Breaker pattern for production resilience
// Prevents cascading failures when Redis is temporarily unavailable

export enum CircuitState {
  CLOSED = 'CLOSED',      // Normal operation, requests pass through
  OPEN = 'OPEN',          // Failing, reject requests immediately
  HALF_OPEN = 'HALF_OPEN', // Testing recovery, allow limited requests
}

export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private successCount: number = 0;
  private lastFailureTime: number = 0;

  constructor(
    private failureThreshold: number = 5,      // Open after 5 failures
    private successThreshold: number = 2,      // Close after 2 successes in HALF_OPEN
    private resetTimeout: number = 30000       // Try recovery after 30s
  ) {}

  getState(): CircuitState {
    // Auto-transition from OPEN to HALF_OPEN after timeout
    if (
      this.state === CircuitState.OPEN &&
      Date.now() - this.lastFailureTime > this.resetTimeout
    ) {
      this.state = CircuitState.HALF_OPEN;
      this.successCount = 0;
      return CircuitState.HALF_OPEN;
    }
    return this.state;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    const currentState = this.getState();

    // Reject requests when circuit is OPEN
    if (currentState === CircuitState.OPEN) {
      throw new Error(
        `Circuit breaker is OPEN. Service temporarily unavailable. Retry after ${this.resetTimeout}ms`
      );
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess() {
    this.failureCount = 0;

    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= this.successThreshold) {
        // Fully recovered, close circuit
        this.state = CircuitState.CLOSED;
        this.successCount = 0;
      }
    }
  }

  private onFailure() {
    this.lastFailureTime = Date.now();

    if (this.state === CircuitState.HALF_OPEN) {
      this.state = CircuitState.OPEN;
      this.failureCount = 0;
      this.successCount = 0;
      return;
    }

    this.failureCount++;
    if (this.failureCount >= this.failureThreshold) {
      this.state = CircuitState.OPEN;
    }
  }

  // Manual reset for testing/operations
  reset() {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = 0;
  }
}

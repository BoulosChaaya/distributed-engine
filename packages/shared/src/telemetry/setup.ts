import {
  NodeTracerProvider,
  BatchSpanProcessor,
  SimpleSpanProcessor,
  TraceIdRatioBasedSampler,
  AlwaysOnSampler,
} from '@opentelemetry/sdk-trace-node';
import type { SpanProcessor } from '@opentelemetry/sdk-trace-base';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks';
import { context, trace, Tracer, diag, DiagLogLevel } from '@opentelemetry/api';

export interface TelemetryConfig {
  serviceName: string;
  serviceVersion?: string;
  enabled?: boolean;
  otlpEndpoint?: string;
  sampleRate?: number;
  useBatchProcessor?: boolean;
  spanProcessor?: SpanProcessor;
}

let provider: NodeTracerProvider | null = null;

function parseSampleRate(): number | undefined {
  const envVal = process.env.OTEL_TRACES_SAMPLER_ARG;
  if (!envVal) return undefined;
  const parsed = parseFloat(envVal);
  if (isNaN(parsed) || parsed < 0 || parsed > 1) return undefined;
  return parsed;
}

export function initTelemetry(config: TelemetryConfig): void {
  const enabled = config.enabled ?? (process.env.OTEL_SDK_DISABLED !== 'true');
  if (!enabled) return;

  if (provider) return;

  if (process.env.OTEL_LOG_LEVEL === 'debug') {
    diag.setLogger(
      { error: console.error, warn: console.warn, info: console.info, debug: console.debug, verbose: console.debug },
      DiagLogLevel.DEBUG,
    );
  }

  const contextManager = new AsyncHooksContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  const sampleRate = config.sampleRate ?? parseSampleRate() ?? 1.0;
  const sampler = sampleRate >= 1.0
    ? new AlwaysOnSampler()
    : new TraceIdRatioBasedSampler(sampleRate);

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: config.serviceName,
    [ATTR_SERVICE_VERSION]: config.serviceVersion ?? '0.1.0',
    'deployment.environment': process.env.NODE_ENV ?? 'development',
  });

  const spanProcessors: SpanProcessor[] = [];

  if (config.spanProcessor) {
    spanProcessors.push(config.spanProcessor);
  } else {
    const otlpEndpoint = config.otlpEndpoint ?? process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    if (otlpEndpoint) {
      const exporter = new OTLPTraceExporter({ url: `${otlpEndpoint}/v1/traces` });
      const useBatch = config.useBatchProcessor ?? true;
      spanProcessors.push(useBatch
        ? new BatchSpanProcessor(exporter)
        : new SimpleSpanProcessor(exporter));
    }
  }

  provider = new NodeTracerProvider({
    resource,
    sampler,
    spanProcessors,
  });

  provider.register();
}

export async function shutdownTelemetry(timeoutMs: number = 5000): Promise<void> {
  if (!provider) return;

  try {
    await Promise.race([
      provider.shutdown(),
      new Promise<void>((_, reject) =>
        setTimeout(() => reject(new Error('Telemetry shutdown timeout')), timeoutMs),
      ),
    ]);
  } catch {
    // Telemetry shutdown failure must not block process exit
  }

  provider = null;
}

export function getTracer(name?: string): Tracer {
  return trace.getTracer(name ?? 'distributed-engine');
}

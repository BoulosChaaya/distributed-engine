import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';

registerInstrumentations({
  instrumentations: [
    new HttpInstrumentation({
      ignoreIncomingRequestHook: (req) => {
        const url = req.url ?? '';
        return url === '/health' || url === '/ready' || url === '/live';
      },
    }),
    new PgInstrumentation({
      enhancedDatabaseReporting: false,
    }),
  ],
});

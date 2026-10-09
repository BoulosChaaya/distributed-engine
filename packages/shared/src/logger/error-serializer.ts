export interface SafeError {
  name: string;
  message: string;
  stack?: string;
}

const MAX_MESSAGE_LENGTH = 1024;
const MAX_STACK_LINES = 20;

const SENSITIVE_PATTERNS = [
  /password/i,
  /secret/i,
  /token/i,
  /authorization/i,
  /api[_-]?key/i,
  /credential/i,
  /connection[_-]?string/i,
];

function isSensitiveKey(key: string): boolean {
  return SENSITIVE_PATTERNS.some((p) => p.test(key));
}

function scrubCredentials(text: string): string {
  let result = text;
  result = result.replace(/:\/\/([^:@\s]*:)[^@\s]+@/gi, '://$1[REDACTED]@');
  result = result.replace(/(Bearer|Basic)\s+\S+/gi, '$1 [REDACTED]');
  result = result.replace(
    /(password|secret|token|api[_-]?key|credential|authorization|cookie)\s*[=:]\s*\S+/gi,
    '$1=[REDACTED]',
  );
  return result;
}

function sanitizeMessage(message: string): string {
  let result = scrubCredentials(message);
  if (result.length > MAX_MESSAGE_LENGTH) {
    return result.substring(0, MAX_MESSAGE_LENGTH) + '…[truncated]';
  }
  return result;
}

function sanitizeStack(stack: string | undefined, includeStack: boolean): string | undefined {
  if (!includeStack || !stack) return undefined;
  const lines = stack.split('\n');
  const truncated = lines.slice(0, MAX_STACK_LINES);
  let result = truncated.join('\n');
  result = scrubCredentials(result);
  return result;
}

export function serializeError(error: unknown, includeStack = true): SafeError {
  if (error instanceof Error) {
    const safe: SafeError = {
      name: error.name,
      message: sanitizeMessage(error.message),
    };
    const stack = sanitizeStack(error.stack, includeStack);
    if (stack) safe.stack = stack;

    if ('cause' in error && error.cause instanceof Error) {
      safe.message += ` [caused by: ${error.cause.name}: ${sanitizeMessage(error.cause.message)}]`;
    }

    return safe;
  }

  if (error === null || error === undefined) {
    return { name: 'UnknownError', message: 'null or undefined error' };
  }

  if (typeof error === 'string') {
    return { name: 'StringError', message: sanitizeMessage(error) };
  }

  if (typeof error === 'object') {
    const obj = error as Record<string, unknown>;
    const entries = Object.entries(obj);
    const safeEntries: string[] = [];
    for (const [key, value] of entries.slice(0, 10)) {
      if (isSensitiveKey(key)) {
        safeEntries.push(`${key}: [REDACTED]`);
      } else if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        safeEntries.push(`${key}: ${String(value).substring(0, 200)}`);
      } else {
        safeEntries.push(`${key}: [${typeof value}]`);
      }
    }
    return { name: 'ObjectError', message: `{${safeEntries.join(', ')}}` };
  }

  return { name: 'UnknownError', message: String(error).substring(0, MAX_MESSAGE_LENGTH) };
}

export function parseStrictInt(value: string | undefined, defaultValue: number, name: string): number {
  if (value === undefined || value === '') {
    return defaultValue;
  }
  if (!/^-?\d+$/.test(value)) {
    throw new Error(`Invalid integer for ${name}: "${value}"`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Value out of safe integer range for ${name}: "${value}"`);
  }
  return parsed;
}

export function parseStrictIntInRange(
  value: string | undefined,
  defaultValue: number,
  name: string,
  min: number,
  max: number,
): number {
  const parsed = parseStrictInt(value, defaultValue, name);
  if (parsed < min || parsed > max) {
    throw new Error(`${name} must be between ${min} and ${max}, got ${parsed}`);
  }
  return parsed;
}

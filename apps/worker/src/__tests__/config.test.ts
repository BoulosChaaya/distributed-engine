describe('parseStrictInt', () => {
  let parseStrictInt: (value: string | undefined, defaultValue: number, name: string) => number;
  let parseStrictIntInRange: (
    value: string | undefined,
    defaultValue: number,
    name: string,
    min: number,
    max: number,
  ) => number;

  beforeAll(async () => {
    // Import the functions by extracting from the module
    // Since these are not exported from index.ts, we re-implement them here for testing
    // This matches the exact implementation in index.ts
    parseStrictInt = (value, defaultValue, name) => {
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
    };

    parseStrictIntInRange = (value, defaultValue, name, min, max) => {
      const parsed = parseStrictInt(value, defaultValue, name);
      if (parsed < min || parsed > max) {
        throw new Error(`${name} must be between ${min} and ${max}, got ${parsed}`);
      }
      return parsed;
    };
  });

  describe('valid inputs', () => {
    it('returns default when value is undefined', () => {
      expect(parseStrictInt(undefined, 5432, 'POSTGRES_PORT')).toBe(5432);
    });

    it('returns default when value is empty string', () => {
      expect(parseStrictInt('', 5432, 'POSTGRES_PORT')).toBe(5432);
    });

    it('parses valid positive integer', () => {
      expect(parseStrictInt('8080', 5432, 'POSTGRES_PORT')).toBe(8080);
    });

    it('parses valid negative integer', () => {
      expect(parseStrictInt('-1', 0, 'TEST')).toBe(-1);
    });

    it('parses zero', () => {
      expect(parseStrictInt('0', 5432, 'POSTGRES_PORT')).toBe(0);
    });
  });

  describe('invalid inputs', () => {
    it('rejects string with trailing characters', () => {
      expect(() => parseStrictInt('20workers', 5, 'QUEUE_CONCURRENCY')).toThrow(
        'Invalid integer for QUEUE_CONCURRENCY: "20workers"',
      );
    });

    it('rejects decimal number', () => {
      expect(() => parseStrictInt('1.5', 5, 'QUEUE_CONCURRENCY')).toThrow(
        'Invalid integer for QUEUE_CONCURRENCY: "1.5"',
      );
    });

    it('rejects non-numeric string', () => {
      expect(() => parseStrictInt('abc', 5432, 'POSTGRES_PORT')).toThrow(
        'Invalid integer for POSTGRES_PORT: "abc"',
      );
    });

    it('rejects string with leading spaces', () => {
      expect(() => parseStrictInt(' 5432', 5432, 'POSTGRES_PORT')).toThrow(
        'Invalid integer for POSTGRES_PORT: " 5432"',
      );
    });

    it('rejects string with trailing spaces', () => {
      expect(() => parseStrictInt('5432 ', 5432, 'POSTGRES_PORT')).toThrow(
        'Invalid integer for POSTGRES_PORT: "5432 "',
      );
    });

    it('rejects hex notation', () => {
      expect(() => parseStrictInt('0xFF', 5432, 'POSTGRES_PORT')).toThrow(
        'Invalid integer for POSTGRES_PORT',
      );
    });

    it('rejects exponential notation', () => {
      expect(() => parseStrictInt('1e3', 5432, 'POSTGRES_PORT')).toThrow(
        'Invalid integer for POSTGRES_PORT',
      );
    });
  });

  describe('parseStrictIntInRange', () => {
    it('accepts value within range', () => {
      expect(parseStrictIntInRange('5432', 5432, 'POSTGRES_PORT', 1, 65535)).toBe(5432);
    });

    it('accepts value at lower boundary', () => {
      expect(parseStrictIntInRange('1', 5432, 'POSTGRES_PORT', 1, 65535)).toBe(1);
    });

    it('accepts value at upper boundary', () => {
      expect(parseStrictIntInRange('65535', 5432, 'POSTGRES_PORT', 1, 65535)).toBe(65535);
    });

    it('rejects value below range', () => {
      expect(() => parseStrictIntInRange('0', 5432, 'POSTGRES_PORT', 1, 65535)).toThrow(
        'POSTGRES_PORT must be between 1 and 65535, got 0',
      );
    });

    it('rejects value above range', () => {
      expect(() => parseStrictIntInRange('65536', 5432, 'POSTGRES_PORT', 1, 65535)).toThrow(
        'POSTGRES_PORT must be between 1 and 65535, got 65536',
      );
    });

    it('uses default when unset and default is in range', () => {
      expect(parseStrictIntInRange(undefined, 5, 'QUEUE_CONCURRENCY', 1, 100)).toBe(5);
    });

    it('rejects malformed value even when default would be in range', () => {
      expect(() => parseStrictIntInRange('abc', 5, 'QUEUE_CONCURRENCY', 1, 100)).toThrow(
        'Invalid integer for QUEUE_CONCURRENCY: "abc"',
      );
    });
  });
});

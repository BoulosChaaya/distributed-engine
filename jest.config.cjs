/** @type {import('jest').Config} */
const config = {
  projects: [
    '<rootDir>/packages/*/jest.config.cjs',
    '<rootDir>/apps/*/jest.config.cjs',
  ],
  maxWorkers: 1,
  testEnvironment: 'node',
  collectCoverageFrom: [
    '**/*.ts',
    '!**/*.d.ts',
    '!**/dist/**',
    '!**/node_modules/**',
  ],
};

module.exports = config;

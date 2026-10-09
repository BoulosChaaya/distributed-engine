/** @type {import('jest').Config} */
const config = {
  displayName: 'worker',
  testEnvironment: 'node',
  roots: ['<rootDir>'],
  testMatch: ['**/__tests__/**/*.test.ts'],
  preset: 'ts-jest',
  moduleFileExtensions: ['ts', 'js', 'json'],
  moduleNameMapper: {
    '^@repo/shared$': '<rootDir>/../../packages/shared/src',
  },
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.test.ts',
  ],
};

module.exports = config;

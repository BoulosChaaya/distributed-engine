/** @type {import('jest').Config} */
const config = {
  displayName: '@repo/shared',
  testEnvironment: 'node',
  roots: ['<rootDir>'],
  testMatch: ['**/__tests__/**/*.test.ts'],
  preset: 'ts-jest',
  moduleFileExtensions: ['ts', 'js', 'json'],
  maxWorkers: 1,
};

module.exports = config;

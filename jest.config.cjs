/** @type {import('jest').Config} */
const config = {
  projects: [
    '<rootDir>/packages/*/jest.config.cjs',
    '<rootDir>/apps/*/jest.config.cjs',
  ],
  maxWorkers: 1,
  // 'github-actions' turns each failing test into an annotation on the
  // check run (only when GITHUB_ACTIONS is set), so a CI failure names the
  // test in the PR and through the checks API, without downloading logs.
  reporters: ['default', 'github-actions'],
  testEnvironment: 'node',
  collectCoverageFrom: [
    '**/*.ts',
    '!**/*.d.ts',
    '!**/dist/**',
    '!**/node_modules/**',
  ],
};

module.exports = config;

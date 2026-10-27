/** @type {import('jest').Config} */
module.exports = {
  displayName: 'web',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/__tests__/**/*.test.ts'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', { tsconfig: { module: 'CommonJS', moduleResolution: 'Node', ignoreDeprecations: '6.0', jsx: 'react-jsx', esModuleInterop: true } }],
  },
  moduleNameMapper: { '^@/(.*)$': '<rootDir>/src/$1' },
};

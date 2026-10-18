// One lint configuration for the whole monorepo.
//
// The rules that matter most here are the type-aware promise rules: in a
// system built on queues, leases and timers, a promise nobody awaits is a
// failure nobody sees (an unhandled rejection, a lease that is never
// released, a job acknowledged before its write commits). Those are errors.
// Style is left to the compiler settings (strict, noUnused*) and is not
// enforced here.
import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import reactHooks from 'eslint-plugin-react-hooks';
import globals from 'globals';

export default tseslint.config(
  {
    ignores: [
      '**/node_modules/**',
      '**/dist/**',
      '**/.next/**',
      '**/coverage/**',
      'apps/web/next-env.d.ts',
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      // Tests are excluded from the build tsconfigs, so each package has a
      // tsconfig.eslint.json that adds them; tests get the promise rules too.
      parserOptions: {
        project: [
          './apps/api/tsconfig.eslint.json',
          './apps/worker/tsconfig.eslint.json',
          './packages/shared/tsconfig.eslint.json',
          './apps/web/tsconfig.json',
        ],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': ['error', { checksVoidReturn: { arguments: false, attributes: false } }],
      '@typescript-eslint/await-thenable': 'error',
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrors: 'none' }],
      // `catch {}` is how best-effort cleanup says "ignore"; other empty blocks are mistakes.
      'no-empty': ['error', { allowEmptyCatch: true }],
    },
  },
  {
    // Next.js loads its config outside any tsconfig project.
    files: ['apps/web/next.config.ts'],
    ...tseslint.configs.disableTypeChecked,
  },
  {
    // Tests build fakes and reach into internals; `any` and loose function
    // types there are deliberate. The promise rules still apply.
    files: ['**/__tests__/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unsafe-function-type': 'off',
      '@typescript-eslint/no-require-imports': 'off',
      'preserve-caught-error': 'off',
    },
  },
  {
    files: ['apps/web/**/*.tsx'],
    plugins: { 'react-hooks': reactHooks },
    rules: reactHooks.configs.recommended.rules,
  },
  {
    files: ['**/*.mjs', '**/*.js', '**/*.cjs'],
    languageOptions: { globals: { ...globals.node } },
  },
  {
    // k6 scripts run in k6's JavaScript runtime, not Node.
    files: ['load/k6/**/*.js'],
    languageOptions: { globals: { __ENV: 'readonly', __VU: 'readonly', __ITER: 'readonly' } },
  },
);

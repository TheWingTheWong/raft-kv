import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The chaos suite runs thousands of simulated clusters; give it room on slow CI runners.
    testTimeout: 120_000,
    hookTimeout: 30_000,
  },
});

import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import pluginVue from 'eslint-plugin-vue';

export default tseslint.config(
  {
    ignores: ['dist', 'node_modules', '.quasar', 'src-capacitor', 'src-cordova', '*.temporary.*']
  },

  js.configs.recommended,
  ...tseslint.configs.recommended,
  ...pluginVue.configs['flat/recommended'],

  {
    languageOptions: {
      globals: {
        console: 'readonly',
        window: 'readonly',
        document: 'readonly',
        process: 'readonly',
        MouseEvent: 'readonly',
      },
      parserOptions: {
        parser: tseslint.parser,
        project: './tsconfig.vue-tsc.json',
        tsconfigRootDir: import.meta.dirname,
        extraFileExtensions: ['.vue'],
      },
    },
    rules: {
      // Relaxed Vue rules for Quasar, which has its own conventions that may not align with strict Vue style rules.
      // These can be tightened up in the future if desired, but for now we want to allow some flexibility in how components are structured and named.
      'vue/multi-word-component-names': 'off',
      'vue/max-attributes-per-line': 'off',
      'vue/html-indent': 'off',
      'vue/attributes-order': 'off',
      'vue/singleline-html-element-content-newline': 'off',
      'vue/html-self-closing': 'off',
      'vue/v-slot-style': 'off',

      // TypeScript - lenient during build, but can be tightened up in the future.
      // These are set to 'warn' instead of 'error' to avoid build failures, but they will still show up in the console and can be addressed over time.
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_'
      }],

      // General, allowing console and other globals for now, but can be tightened up in the future as well.
      'no-console': 'off',
      'no-undef': 'off',
      'no-redeclare': 'off',
    },
  }
);

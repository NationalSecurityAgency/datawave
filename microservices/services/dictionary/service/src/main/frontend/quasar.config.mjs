/*
 * This file runs in a Node context. This is the default configuration for a Quasar app created with the CLI with some modifications.
 */

// Configuration for Data Dictionary frontend application built with Quasar Framework and Vite.
// https://v2.quasar.dev/quasar-cli-vite/quasar-config-js

import { configure } from 'quasar/wrappers';

export default configure(function (ctx) {
  return {
    // https://v2.quasar.dev/quasar-cli-vite/prefetch-feature
    // preFetch: true,

    // app boot file (/src/boot) - This is for our Axios configuration and API client setup.
    boot: ['axios'],

    // https://v2.quasar.dev/quasar-cli-vite/quasar-config-js#css - Access our global css file.
    css: ['app.css'],

    // https://github.com/quasarframework/quasar/tree/dev/extras - These are needed for some of the table icons in Data Dictionary.
    extras: [
      'ionicons-v4',
      'roboto-font',
      'material-icons',
      'bootstrap-icons'
    ],

    // Full list of options: https://v2.quasar.dev/quasar-cli-vite/quasar-config-js#build
    build: {
      target: {
        browser: ['es2019', 'edge88', 'firefox78', 'chrome87', 'safari13.1'],
        node: 'node20'
      },

      vueRouterMode: 'hash',

      extendViteConf(viteConf) {
        viteConf.base = '';
      },

      env: {
        BACKEND_BASE_PATH: ctx.dev ? 'https://localhost:8643/dictionary/' : undefined,
      },

      vitePlugins: [
        ['vite-plugin-checker', {
          vueTsc: {
            tsconfigPath: 'tsconfig.vue-tsc.json'
          },
          eslint: {
            lintCommand: 'eslint "./**/*.{js,ts,mjs,cjs,vue}"'
          }
        }, { server: false }]
      ]
    },

    // Full list of options: https://v2.quasar.dev/quasar-cli-vite/quasar-config-js#devServer
    devServer: {
      https: true,
      open: true
    },

    // https://v2.quasar.dev/quasar-cli-vite/quasar-config-js#framework
    framework: {
      config: {},
      plugins: ['Notify']
    },

    animations: [],

    // SSR - Used default settings - https://quasar.dev/quasar-cli-vite/developing-ssr/configuring-ssr
    ssr: {
      pwa: false,
      prodPort: 3000,
      middlewares: ['render']
    },

    // PWA - Used default settings - https://quasar.dev/quasar-cli-vite/developing-pwa/configuring-pwa
    pwa: {
      workboxMode: 'generateSW',
      injectPwaMetaTags: true,
      swFilename: 'sw.js',
      manifestFilename: 'manifest.json',
      useCredentialsForManifestTag: false,
    },

    cordova: {},

    capacitor: {
      hideSplashscreen: true
    },

    electron: {
      inspectPort: 5858,
      bundler: 'packager',
      packager: {},
      builder: {
        appId: 'frontend'
      }
    },

    bex: {
      contentScripts: ['my-content-script']
    }
  }
});

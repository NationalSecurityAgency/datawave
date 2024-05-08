# Vue 3 + TypeScript + Vite

This template should help get you started developing with Vue 3 and TypeScript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

## Recommended IDE Setup

- [VS Code](https://code.visualstudio.com/) + [Volar](https://marketplace.visualstudio.com/items?itemName=Vue.volar) (and disable Vetur) + [TypeScript Vue Plugin (Volar)](https://marketplace.visualstudio.com/items?itemName=Vue.vscode-typescript-vue-plugin).

## Type Support For `.vue` Imports in TS

TypeScript cannot handle type information for `.vue` imports by default, so we replace the `tsc` CLI with `vue-tsc` for type checking. In editors, we need [TypeScript Vue Plugin (Volar)](https://marketplace.visualstudio.com/items?itemName=Vue.vscode-typescript-vue-plugin) to make the TypeScript language service aware of `.vue` types.

If the standalone TypeScript plugin doesn't feel fast enough to you, Volar has also implemented a [Take Over Mode](https://github.com/johnsoncodehk/volar/discussions/471#discussioncomment-1361669) that is more performant. You can enable it by the following steps:

1. Disable the built-in TypeScript Extension
   1. Run `Extensions: Show Built-in Extensions` from VSCode's command palette
   2. Find `TypeScript and JavaScript Language Features`, right click and select `Disable (Workspace)`
2. Reload the VSCode window by running `Developer: Reload Window` from the command palette.

# Noob Notes

# NVM & NPM Setup

NVM (Node Version Manager) is a utility that can be used to manage node.js versions on your machine.

To install, run the following command:
`curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash`

After installing nvm, open a new terminal, or source bash so you can run nvm commands.

To install node (and NPM) run the following command:
`nvm install v16.20.2`

After that has finished, run the following command `npm version`, and verify that you get the following output

```json
{
  "npm": "8.19.4",
  "node": "16.20.2",
  "v8": "9.4.146.26-node.26",
  "uv": "1.43.0",
  "zlib": "1.2.11",
  "brotli": "1.0.9",
  "ares": "1.19.1",
  "modules": "93",
  "nghttp2": "1.47.0",
  "napi": "8",
  "llhttp": "6.0.11",
  "openssl": "1.1.1v+quic",
  "cldr": "41.0",
  "icu": "71.1",
  "tz": "2022f",
  "unicode": "14.0",
  "ngtcp2": "0.8.1",
  "nghttp3": "0.7.0"
}
```

# NPM

## Essential commands

`npm init` initializes a new project. You will be prompted for things like name, version, description, etc.

`npm install <module>` is used to install node modules into your local `node_modules` folder. Running this command without a module argument, within a project (containing a package.json) will cause all of the `dependencies` and `devDependencies` to be installed. Adding a `--save` or `--save-dev` flag will add the installed module to the list of `dependencies` or `devDependencies` in `package.json`.

`npm run <script>` is used to run either a built-in or user-defined script defined in package.json. This is commonly used to create a `build`, or `dev` script for building/running your project.

## package.json

package.json is analogous to pom.xml in maven. The project `name`, `version`, `type`, `dependencies`, `devDependencies`, and `scripts` are all enumerated here.

# Vite

Vite is a tool which is used to both build, and host your project during development utilizing Hot Module Replacement (HMR) to immediately reflect source code changes on the rendered site. Vite can be used with a variety of javascript frameworks (e.g. react, vue, vanilla javascript) and suppports javascript, typescript, and JSX.

Rollup.js is used by Vite to bundle and build your project for production.

vite.config.js is a TypeScript file that contains your Vite configuration.

# TypeScript

TypeScript is a strongly typed programming language that builds on JavaScript. Ultimately TypeScript is `transpiled` into plain JavaScript for use in a browser. TypeScript itself cannot be run natively in a browser.

tsconfig.json is used to specify the root files and compiler options required to compile the project.

tsconfig.node.json is used to specify the root files and compiler options required to compile vite.config.ts

# VSCode Setup

To facilitate rapid development you will want to add the following extensions to VSCode.

- Vite
- Vue Language Features (Volar)
- Browse Lite

These plugins should enbable syntax highlighting when loading _.ts and _.vue files in the editor.

You may run into issues with unexpected syntax errors when first loading the project. To make sure the syntax errors are valid, make sure that you have selected the appropriate typescript version (contained in node_modules) instead of the VSCode default. You can do this by pressing `Ctrl + Shift + P` and searching for `TypeScript: Select TypeScript Version...` (in the case of \*.ts files) or `Volar: Select Typescript Version...` and selecting `Use Workspace Version`. Assuming there are no actual syntax errors with your project, this should clear up the initial set of errors. Actually, it probably makes sense just to disable the built in TypeScript & Javascript extension for this workspace as it will conflict with Volar.

For the Vite extension, you will want to go into the settings and set `Vite: Dev Command` to `npm run dev --` to ensure that your dev server can be started correctly.

For the Browse Lite extension, you will need to set `Browse-lite: Chrome Executable` to the full path to your `google-chrome` executable.

# Browser setup

To simplify debugging, install the Vue.js devtools extension in chrome. This will allow you to inspect the vue components, and more importantly inspect the state stored in Pinia.

# Functionality

- Plot the geometries from a query on the map
  - Group query geometries by field
  - Expand geometry collections into individual geometries
- Plot geo ranges from a query on the map
  - Group geo ranges by field, and tier
- Plot any given WKT, GeoJSON on the map
- Export any geometry to WKT, or GeoJSON
- Load a query given a query ID
- List queries for the current user?
- Use a marker for zoomed-out, hard to see geometries
  - Add ability to turn markers on or off
- Change default polygon colors depending on the basemap
- Utilities:
  - Convert (single? multiple?) WKT to GeoWave Index
  - Convert (single? multiple?) WKT to Geo Index
  - Find/highlight range/term, and geo function matching geometry
  - Split a query geometry naturally (in case of geometry collection) or forcefully
  - Set portions of a query as evaluation only with the click of a mouse (query editor?)
  - Submit query, and expand into index terms/ranges for analysis. with and without optimization. add ability to turn knobs to see the effect
  - Add ability to draw custom shapes on map.
  - Use ANDI bookmarklet to gauge accessibility
  - See arcgis.com for example app layout
  - Pull geowave/geo fields from 1) config, 2) dictionary, 3) user override
  -

# Questions

- Single query per page, or multiple?
- Endpoint that returns all computer information for a query and it's geometries/ranges?
- Cache individual user's data so that it persists between sessions?
- Add ability to save session to file?
- Create a layout like ArcGIS?

# Technologies to check out

- Formkit
- PrimeVue
- Pinia

# Example Query Data

{
queryId: 'abc-123',
functions: [
{
function: '#INTERSECTS((FIELD1 || FIELD2), ...)',
fields: ['FIELD1', 'FIELD2'],
wkt: '...',
geojson: '...'
},
],
fields: {
'FIELD1': {
type: 'GeoWave',
tiers: [
{
tier: '0',
wkt: '...'
geojson: '...'
},
]
},
'FIELD2': {
type: 'Geo',
wkt: '...',
geojson: '...'
},
}
}

# Quasar

## Install the dependencies

```bash
npm install
```

### Start the app in development mode (hot-code reloading, error reporting, etc.)

```bash
quasar dev
```

### Lint the files

```bash
npm run lint
```

### Format the files

```bash
npm run format
```

### Build the app for production

```bash
quasar build
```

### Customize the configuration

See [Configuring quasar.config.js](https://v2.quasar.dev/quasar-cli-vite/quasar-config-js).

// Filename: main.js

// Require.js allows us to configure shortcut alias
// Their usage will become more apparent further long in the tutorial
require.config({
  paths: {
    jquery: 'libs/jquery/jquery',
    underscore: 'libs/underscore/underscore-min',
    backbone: 'libs/backbone/backbone-min-format',
    jqueryui: 'libs/jquery-ui/jquery-ui',
    // d3: 'libs/d3/d3-v2',
    // cubism: 'libs/d3/cubism/cubism',
    // nvd3: 'libs/d3/nvd3/nv.d3',
    // highcharts: 'libs/highcharts/highcharts.src',
    highstocks: 'libs/highstocks/highstock.src',
    // highchartsmore: 'libs/highcharts/highcharts-more',
    highchartstheme: 'libs/highcharts/themes/gray',
    templates: '../templates',
    datatables: 'libs/datatables/datatables',
    bootstrap: 'libs/bootstrap/bootstrap',
  },
  shim: {
    underscore: {
      exports: '_'
    },
    jquery: {
      exports: '$'
    },
    backbone: {
      deps: ['underscore', 'jquery'],
      exports: 'Backbone'
    },
    jqueryui: {
      deps: ['jquery'],
      exports: 'jQueryUI'
    },
    /***
    d3: {
      exports: 'd3'
    },
    nvd3: {
      deps: ['d3'],
      exports: 'nv'
    },
    cubism: {
      deps: ['d3'],
      exports: 'cubism'
    },
    highcharts: {
      exports: 'Highcharts',
      deps: ['jquery']
    },
    highchartsmore: {
      // exports: 'Highcharts',
      deps: ['jquery','highstocks']
    },
    ***/
    datatables: {
      exports: 'DataTables',
      deps: ['jquery']
    },
    bootstrap: {
      exports: 'Bootstrap',
      deps: ['jquery']      
    },
    highstocks: {
      exports: 'Highstocks',
      deps: ['jquery']
    },
    highchartstheme: {
      deps: ['highstocks']
    },
  }
});

require([
  // Load our app module and pass it to our definition function
  'app',
  'highstocks',
  'highchartstheme'
], function (App) {
  // The "app" dependency is passed in as "App"
  // Again, the other dependencies passed in are not "AND" therefore don't pass a parameter to this function
  
  App.initialize();
});  
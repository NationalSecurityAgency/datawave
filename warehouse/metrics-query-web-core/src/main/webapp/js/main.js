// Filename: main.js

// Require.js allows us to configure shortcut alias
// Their usage will become more apparent further long in the tutorial
require.config({
  paths: {
    jquery: 'libs/jquery/jquery',
    underscore: 'libs/underscore/underscore-min',
    backbone: 'libs/backbone/backbone-min-format',
    jqueryui: 'libs/jquery-ui/jquery-ui',
    d3: 'libs/d3/d3-v2',
    cubism: 'libs/d3/cubism/cubism',
    nvd3: 'libs/d3/nvd3/nv.d3',
    highcharts: 'libs/highcharts/highcharts.src',
    highchartsmore: 'libs/highcharts/highcharts-more',
    templates: '../templates',
    datatables: 'libs/datatables/datatables',
    bootstrap: 'libs/bootstrap/bootstrap'
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
      deps: ['jquery','highcharts']
    },
    datatables: {
      exports: 'DataTables',
      deps: ['jquery']
    },
    bootstrap: {
      exports: 'Bootstrap',
      deps: ['jquery']      
    }
  }
});

require([
  // Load our app module and pass it to our definition function
  'app',
  'highcharts',
  'highchartsmore'
], function (App) {
  // The "app" dependency is passed in as "App"
  // Again, the other dependencies passed in are not "AND" therefore don't pass a parameter to this function
  
  /**
   * Grid theme for Highcharts JS
   * @author Torstein Hønsi
   */
  
  Highcharts.theme = {
      colors: ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],
      chart: {
          backgroundColor: {
              linearGradient: { x1: 0, y1: 0, x2: 1, y2: 1 },
              stops: [
                  [0, 'rgb(255, 255, 255)'],
                  [1, 'rgb(240, 240, 255)']
              ]
          },
          borderWidth: 2,
          plotBackgroundColor: 'rgba(255, 255, 255, .9)',
          plotShadow: true,
          plotBorderWidth: 1
      },
      title: {
          style: {
              color: '#000',
              font: 'bold 16px "Trebuchet MS", Verdana, sans-serif'
          }
      },
      subtitle: {
          style: {
              color: '#666666',
              font: 'bold 12px "Trebuchet MS", Verdana, sans-serif'
          }
      },
      xAxis: {
          gridLineWidth: 1,
          lineColor: '#000',
          tickColor: '#000',
          labels: {
              style: {
                  color: '#000',
                  font: '11px Trebuchet MS, Verdana, sans-serif'
              }
          },
          title: {
              style: {
                  color: '#333',
                  fontWeight: 'bold',
                  fontSize: '12px',
                  fontFamily: 'Trebuchet MS, Verdana, sans-serif'  
              }
          }
      },
      yAxis: {
          minorTickInterval: 'auto',
          lineColor: '#000',
          lineWidth: 1,
          tickWidth: 1,
          tickColor: '#000',
          labels: {
              style: {
                  color: '#000',
                  font: '11px Trebuchet MS, Verdana, sans-serif'
              }
          },
          title: {
              style: {
                  color: '#333',
                  fontWeight: 'bold',
                  fontSize: '12px',
                  fontFamily: 'Trebuchet MS, Verdana, sans-serif'
              }
          }
      },
      legend: {
          itemStyle: {
              font: '9pt Trebuchet MS, Verdana, sans-serif',
              color: 'black'    
          },
          itemHoverStyle: {
              color: '#039'
          },
          itemHiddenStyle: {
              color: 'gray'
          }
      },
      labels: {
          style: {
              color: '#99b'
          }
      },  
      navigation: {
          buttonOptions: {
              theme: {
                  stroke: '#CCCCCC'
              }
          }
      }
  };
  
  // Apply the theme
  var highchartsOptions = Highcharts.setOptions(Highcharts.theme);
  
  App.initialize();
});  
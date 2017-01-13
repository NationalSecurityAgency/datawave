define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'models/metrics/QueryRateModel',
  'highchartsmore'
], function($, _, Backbone, D3, QueryRateModel) {

  var QueryRateView = Backbone.View.extend({
  
    className: "queryRatePlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'gauge'
      },
      title: { 
        text: 'Query Rate'
      },
      pane: {
        startAngle: -150,
        endAngle: 150,
        background: [{
          backgroundColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [
              [0, '#FFF'],
              [1, '#333']
            ]
          },
          borderWidth: 0,
          outerRadius: '109%'
        }, {
          backGroundColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [
              [0, '#333'],
              [1, '#FFF']
            ]
          },
          borderWidth: 1,
          outerRadius: '107%'
        }, {
          // default background
        }, {
          backgroundColor: '#DDD',
          borderWidth: 0,
          outerRadius: '105%',
          innerRadius: '103%'
        }],
      },
      yAxis: {
        min: 0,
        max: 25000,
        
        minorTickInterval: 'auto',
        minorTickWidth: 1,
        minorTickPosition: 'inside',
        mintorTickColor: '#666',
        
        tickPixelInterval: 30,
        tickWidth: 2,
        tickPosition: 'inside',
        tickLength: 10,
        tickColor: '#666',
        labels: {
          step: 2,
          rotation: 'auto'
        },
        plotBands: [{
          from: 0,
          to: 12000,
          color: '#55BF3B' // green
        }, {
          from: 12000,
          to: 160000,
          color: '#DDDF0D' // yellow
        }, {
          from: 16000,
          to: 25000,
          color: '#DF5353' // red
        }]
      },
      series: [{
        name: 'Query Rate',
        data: [0]
      }],
      credits: {
        enabled: false
      }
    },
          
    onDataHandler: function(model, response, options) {
      this._updateChartData(model, options);
    },
    
    initialize: function() {
      
      _.bindAll(this);
      
      this.model = new QueryRateModel();
      this.model.on('change:0', this._updateChartData);
      // this.model.fetch({ success: this.onDataHandler, dataType: 'json'});
      this.model.startLongPolling(5000);
      this.model.executeLongPolling();
      this.id = "queryRateView-" + (Math.floor(Math.random() * 900000) + 100000)
      this.chartOptions = $.extend(true, {}, this.chartOptions) // deep clone
    },
    
    render: function() {
    
      if (this.chart)
        this.chart.destroy();
        
      var thatElement = this.$el;
      var options     = this.chartOptions; 
      
      options.chart.renderTo = thatElement[0];
      options.chart.width    = thatElement.parent().width();
      options.chart.height   = thatElement.parent().height();
      
      this.chart = new Highcharts.Chart(options);
      
      return this;
    },
    
    _updateChartData: function(model, options) {
      var data     = model.toJSON();
      var coptions = this.chartOptions;
      
      if (this.chart) {
        var point = this.chart.series[0].points[0];
      
        if(typeof data[0] !== 'undefined') point.update(data[0]);
      }
      else {
        if (data[0]) coptions.series[0].data[0] = data[0];
        this.render();
      }
    }
  });
  
  return QueryRateView;
});
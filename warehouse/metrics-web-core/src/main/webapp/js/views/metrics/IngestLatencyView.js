define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'models/metrics/IngestLatencyModel',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, IngestLatencyModel, metricsTemplate) {

  var IngestLatencyView   = BaseMetricView.extend({
    
    tagName: 'div',
    
    className: 'span4',

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.model = new IngestLatencyModel();
      this.model.onSuccess(this._update);
      this.model.startLongPolling();
      this._init = false;
    },
    
    _update: function(model, response, options) {
      
      var date = (new Date()).getTime();
      var series = model.get('series');
      var types = model.get('types');
      
      _.each(types, function(type, idx) {
        var nseries = _.filter(this.chart.series, function(val) {
                        return val.name == type;
                      }, this);
        
        if(nseries.length > 0) {
          nseries = nseries[0];
        } else {
          nseries = this.chart.addSeries({
                      turboThreshold: 0,
                        dataGrouping: {
                        enabled: false
                      },
                      name: type,
                      data: []
                    }, false);
        }
        
        var size  = nseries.data.length;
        var shift = size >= 17280;
        
        var total = _.reduce(series, function(total, aseries) {
          return aseries[idx] + total;
        }, 0, this);
          
        nseries.addPoint({ x: date, y: total}, false, shift);
        
        if(true) {
          var max = new Date().getTime();
          var min = max - (60000 * 5);
      
          this.chart.xAxis[0].setExtremes(min, max);
          this._init = true;
        }
      }, this);
      
      this.chart.redraw();
      
      return this;
    },
    
    _updateLabelBox: function() {
      // Do nothing;
    },
    
    chartOptions: {
      plotOptions: {
        line: {
          connectNulls: true
        }
      },
      title: { 
        text: 'Live Ingest Latency'
      },
      yAxis: {
        title: {
          text: 'Latency (ms)'
        }
       },
      rangeSelector: {
        selected: 1
      },
      series: [],
      legend: {
        enabled: true
      },
      tooltip: {
        valueDecimals: 2
      }
    }
  });
  
  return IngestLatencyView ;
});
define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'collections/metrics/IngestRateCollection',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, IngestRateCollection, metricsTemplate) {

  var IngestEventRateView = BaseMetricView.extend({
    
    tagName: 'div',
    
    className: 'span4',
    
    _interval: 6e4,
    
    _ticks: 1440,

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.liveCollection = new IngestRateCollection(null, {ingestType: 'live', latency: 6e5});
      this.liveCollection.interval = this._interval;
      this.liveCollection.onSuccess(this._updateIngest);
      this.liveCollection.startLongPolling();
    },
    
    _formatLabel: function(value) {
      if (_.isNumber(value)) {
        return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      } else {
        return value.toString();
      }
    },
    
    _updateIngest: function(collection, response, options) {
      
      var total = 0;
      collection.each(function(d,i) {
        total += d.get('1');
      }, this);
      
      if (this.chart && _.isNumber(total)) {
        var size  = this.chart.series[0].data.length;
        var shift = size >= this.ticks;
        
        this.chart.series[0].addPoint({ x: (new Date()).getTime(), y: total}, true, shift);        
      }
      
      return this;
    },
    
    chartOptions: {
      title: { 
        text: 'Live Event Ingest Rate'
      },
      yAxis: {
        title: {
          text: 'Events Per Interval'
        }
      },
      rangeSelector: {
        selected: 2
      },
      series: [
        {
          dataGrouping: {
            enabled: false
          },
          turboThreshold: 0,
          name: 'Live',
          data: (function(){
            var data = [], time = (new Date()).getTime(), i;
            
            for( i = -1440; i < 0 ;i++) {
              data.push([time + i * 6e4, 0]);
            }
            
            return data;
          })()
        }
      ]
    }
  });
  
  return IngestEventRateView;
});
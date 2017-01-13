define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'collections/metrics/IngestRateCollection',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, IngestRateCollection, metricsTemplate) {

  var IngestEventRateView = BaseMetricView.extend({

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.bulkCollection = new IngestRateCollection(null, {ingestType: 'bulk', latency: 6e5});
      this.bulkCollection.onSuccess(this._updateIngest);
      this.bulkCollection.startLongPolling();
      this.lastNonZero = true;
    },
    
    render: function() {
      
      this.$el.empty();
      
      var template = _.template(metricsTemplate, {loadingMsg: "Retrieving user metrics..."});
      
      this.$el.append(template);
      this._show();      
      this._renderChart();
     
      return this;
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
        var shift = size >= 17280;
        
        this.chart.series[0].addPoint({ x: (new Date()).getTime(), y: total }, true, shift);
      }
      
      return this;
    },
    
    chartOptions: {
      title: { 
        text: 'Bulk Event Ingest Rate'
      },
      yAxis: {
        title: {
          text: 'Events Per Interval'
        }
      },
      series: [
        {
          dataGrouping: {
            enabled: false
          },
          turboThreshold: 0,
          name: 'Bulk',
          data: (function(){
            var data = [], time = (new Date()).getTime(), i;
            
            for( i = -17280; i < 0 ;i++) {
              data.push([time + i * 5000, 0]);
            }
            
            return data;
          })()
        }
      ]
    }
  });
  
  return IngestEventRateView;
});
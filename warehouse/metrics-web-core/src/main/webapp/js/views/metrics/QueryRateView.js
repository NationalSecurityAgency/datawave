define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'models/metrics/QueryMetricSummaryModel',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, QueryMetricSummaryModel, metricsTemplate) {

  var QueryRateView = BaseMetricView.extend({
    
    tagName: 'div',
    
    className: 'span4',

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.model = new QueryMetricSummaryModel();
      this.model.onSuccess(this._update);
      this.model.startLongPolling();
    },
    
    _update: function(model, response, options) {
      if (this.chart && _.isNumber(model.get('All').QueryCount)) {
        var data = model.get('All').QueryCount;
        var shift = this.chart.series[0].data.length >= 17280; 
        this.chart.series[0].addPoint([(new Date()).getTime(), data], true, shift);
        // this._updateLabelBox(this._formatLabel);
      }
      
      return this;
    },
    
    _formatLabel: function(value) {
      if (_.isNumber(value)) {
        return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      } else {
        return value.toString();
      }
    },
    
    chartOptions: {
      title: { 
        text: 'Query Rate'
      },
      yAxis: {
        title: {
          text: 'Queries Per Interval'
        }
      },
      series: [
        {
          turboThreshold: 0,
          dataGrouping: {
            enabled: false
          },
          name: 'Query Count',
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
  
  return QueryRateView;
});

define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'models/metrics/QueryByteRateModel',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, QueryByteRateModel, metricsTemplate) {

  var QueryByteRateView = BaseMetricView.extend({
    
    tagName: 'div',
    
    className: 'span4',

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.queryModel = new QueryByteRateModel();
      this.queryModel.onSuccess(this._updateQuery);
      this.queryModel.startLongPolling();
    },
    
    _updateQuery: function(model, response, options) {
      var data     = model.toJSON();
      
      if (this.chart && _.isNumber(data[0])) {
        var size  = this.chart.series[0].data.length;
        var shift = size >= 17280;
        
        this.chart.series[0].addPoint({ x: (new Date()).getTime(), y: data[0]}, true, shift);
        // this._updateLabelBox(this._formatLabel);
      }
      
      return this;
    },
    
    chartOptions: {
      title: { 
        text: 'Query Byte Rate'
      },
      yAxis: {
        title: {
          text: 'Rate (bytes/sec)'
        },
        min: 0
      },
      series: [
        {
          turboThreshold: 0,
          name: 'Query',
          data: (function(){
            var data = [], time = (new Date()).getTime(), i;
            
            for( i = -17280; i < 0 ;i++) {
              data.push([(time + i * 5000), 0]);
            }
            return data;
          })(),
          dataGrouping: {
            enabled: false
          }
        }
      ]
    }
  });
  
  return QueryByteRateView;
});
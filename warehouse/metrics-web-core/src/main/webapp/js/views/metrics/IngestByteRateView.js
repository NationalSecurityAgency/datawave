define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/base/BaseMetricView',
  'models/metrics/IngestByteRateModel',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, BaseMetricView, IngestByteRateModel, metricsTemplate) {

  var IngestByteRateView = BaseMetricView.extend({
    
    tagName: 'div',
    
    className: 'span4',

    initialize: function(options) {
      this.constructor.__super__.initialize.apply(this, [options]);
      _.bindAll(this);
      var that = this;
      
      this.ingestModel = new IngestByteRateModel();
      this.ingestModel.onSuccess(this._updateIngest);
      this.ingestModel.startLongPolling();
    },
    
    _updateIngest: function(model, response, options) {
      var data     = model.toJSON();
      
      if (this.chart && _.isNumber(data[0])) {
        var size  = this.chart.series[0].data.length;
        var shift = size >= 17280;
        
        this.chart.series[0].addPoint({ x: (new Date()).getTime(), y: data[0] }, true, shift);
      }
      
      return this;
    },
    
    chartOptions: {
      title: { 
        text: 'Ingest Byte Rate'
      },
      yAxis: {
        title: {
          text: 'Rate (bytes/sec)'
        }
      },
      series: [
        {
          dataGrouping: {
            enabled: false
          },
          turboThreshold: 0,
          name: 'Ingest',
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
  
  return IngestByteRateView;
});
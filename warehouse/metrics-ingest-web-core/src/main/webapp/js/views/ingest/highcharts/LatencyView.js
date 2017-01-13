define([
  'jquery',
  'underscore',
  'backbone',
  'highcharts',
  'models/metrics/ingest/LatencyModel'
], function($, _, Backbone, HC, LatencyModel) {

  var LatencyView = Backbone.View.extend({
  
    className: "ingestLatencyPlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'column'
      },
      title: { 
        text: 'Ingest Latency'
      },
      xAxis: {
        categories: [],
        title: {
          text: 'Data Type'
        }
      },
      yAxis: {
        min: 0,
        title: {
          text: 'Latency (ms)'
        }
      },
      series: [],
      credits: {
        enabled: false
      },
      legend: {
        align: 'right',
        layout: 'vertical',
        verticalAlign: 'middle'
      }
    },
    
    onDataHandler: function(model, response, options) {
      this._updateChartData(model, options);
    },
    
    initialize: function(options) {
      
      _.bindAll(this);
      var that = this;
      
      this.model = new LatencyModel();
      this.on('change', this._updateChartData);
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.model.fetch({ success: this.onDataHandler, dataType: 'json'});
      this.id = "latencyView-" + (Math.floor(Math.random() * 900000) + 100000)
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
    
    update: function(model) {
      this.model.start(      model.start.getTime());
      this.model.end(        model.end.getTime());
      this.model.ingestType( model.ingestType);
      
      this.model.fetch({ success: this.onDataHandler, dataType: 'json'});
    },
    
    _updateChartData: function(model, options) {
      var data     = model.toJSON();
      var coptions = this.chartOptions;
       
      coptions.yAxis.plotLines = [];
      coptions.xAxis.categories = [];
      coptions.series = [];
      
      if (data.threshold) {
        coptions.yAxis.plotLines.push({zIndex: 5, color: '#f00', width: 1, value: data.threshold, label: {text: 'Threshold (' + data.threshold + 'ms)'}});
      }
      
      _.each(data.types, function(d, i) {
        coptions.xAxis.categories.push(d);
      }, this);
      
      _.each(data.labels, function(d,i) {
        coptions.series.push({name: d.label,data: []});
        _.each(data.series[i], function(d2,j) {
          coptions.series[i].data.push(d2);
        }, this);
      }, this);
      
      this.render();
    }
  });
  
  return LatencyView;
});
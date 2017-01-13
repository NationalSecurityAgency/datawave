define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'collections/metrics/ingest/DataFlowCollection',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, NVD3, DataFlowCollection, svgTemplate) {

  var DataFlowView = Backbone.View.extend({
  
    className: "dataFlowPlot metric-chart",
    
    chartOptions: {
      chart: {
        type: 'area'
      },        
      title: { 
        text: 'Events by Data Type'
      },
      xAxis: {
        labels: {
          formatter: function() {
            var d = new Date(this.value);
            var s = d.toUTCString();
            var a = s.split(' ');
            
            return a[2].toUpperCase() + ' ' +
                   a[1]  
          }
        },
        type: 'datetime',
        startOnTick: true,
        endOnTick: true,
        minTickInterval: 864e5   
      },
      yAxis: {
        min: 0,
        title: {
          text: '# of Events'
        }
      },
      plotOptions: {
        column: {
          pointRange: (24 * 3600 * 1000)
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
    
    onDataHandler: function(collection, response, options) {
      this._updateChartData(collection, options);
    },
    
    initialize: function(options) {
      
      _.bindAll(this);
      
      this.collection = new DataFlowCollection();
      this.collection.on('reset', this._updateChartData);
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
      this.id = "dataFlowView-" + (Math.floor(Math.random() * 900000) + 100000)
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
      this.collection.start(      model.start.getTime());
      this.collection.end(        model.end.getTime());
      this.collection.ingestType( model.ingestType);
      
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
    },
    
    _updateChartData: function(collection, options) {
      var data     = collection.toJSON();
      var coptions = this.chartOptions;
       
      coptions.series = [];
      
      _.each(data, function(d, i) {
        coptions.series.push({name: d.key, data: []});
        _.each(d.values, function(d2, j) {
          coptions.series[coptions.series.length-1].data.push(d2);
        }, this);
      }, this);
      
      this.render();
    }
  });
  
  return DataFlowView;
});
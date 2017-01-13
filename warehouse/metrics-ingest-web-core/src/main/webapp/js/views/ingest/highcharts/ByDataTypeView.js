define([
  'jquery',
  'underscore',
  'backbone',
  'collections/metrics/ingest/ByDataTypeCollection',
  'highcharts'
], function($, _, Backbone, ByDataTypeCollection, svgTemplate) {

  var ByDataTypeView = Backbone.View.extend({
    
    className: "ingestDataTypePlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false
      },        
      title: { 
        text: 'Events by Data Type'
      },
      plotOptions: {
        pie: {
          allowPointSelect: true,
          cursor: 'pointer',
          dataLabels: {
            enabled: false,
          },
          showInLegend: true,
        }
      },
      series: [{
        type: 'pie',
        name: 'Events Ingested',
        data: []
      }],
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
      

      
      this.collection = new ByDataTypeCollection();
      this.collection.on('reset', this._updateChartData);
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
      this.id = "byDataTypeView-" + (Math.floor(Math.random() * 900000) + 100000)
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
    
    update: function(model) {
      this.collection.start(      model.start.getTime());
      this.collection.end(        model.end.getTime());
      this.collection.ingestType( model.ingestType);
      
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
    },
    
    _updateChartData: function(collection, options) {
      var data     = collection.toJSON();
      var coptions = this.chartOptions;
      
      coptions.series[0].data = [];
      
      _.each(data, function(model, i) {
        coptions.series[0].data.push([model[0], model[1]]);
      }, this);
      
      this.render();
    }
  });
  
  return ByDataTypeView;
});
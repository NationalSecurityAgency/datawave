define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'collections/metrics/TableSizeCollection',
  'highcharts',
], function($, _, Backbone, D3, TableSizeCollection) {

  var TableSizeView = Backbone.View.extend({
  
    className: "tableSizePlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false
      },
      title: { 
        text: 'Table Sizes'
      },
      plotOptions: {
        pie: {
          allowPointSelect: true,
          cursor: 'pointer',
          dataLabels: {
            enabled: false,
          },
          showInLegend: true,
          innerSize: '30%'
        }
      },
      series: [{
        type: 'pie',
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
    
    initialize: function() {
      
      _.bindAll(this);
      
      this.collection = new TableSizeCollection();
      this.collection.on('reset', this._updateChartData);
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
      this.id = "tableSizeView-" + (Math.floor(Math.random() * 900000) + 100000)
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
  
  return TableSizeView;
});
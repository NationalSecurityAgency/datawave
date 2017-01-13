define([
  'jquery',
  'underscore',
  'backbone',
  'models/metrics/TabletSizeModel',
  'highcharts'
], function($, _, Backbone, TabletSizeModel) {

  var TableSizeView = Backbone.View.extend({
  
    className: "tabletSizePlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'column'
      },
      title: { 
        text: 'Tablet Sizes'
      },
      xAxis: {
        categories: []
      },
      yAxis: {
        min: 0,
        title: {
          text: 'Size (bytes)'
        }
      },
      plotOptions: {
      },
      series: [
        {
          name: "Total RFile Size",
          data: []
        },
        {
          name: "Total Key Values",
          data: []
        }
      ],
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
    
    initialize: function() {
      
      _.bindAll(this);
      var that = this;
      
      this.model = new TabletSizeModel();
      this.model.on('change', this._updateChartData);
      this.model.fetch({ success: this.onDataHandler, dataType: 'json'});
      this.id = "tabletSizeView-" + (Math.floor(Math.random() * 900000) + 100000);
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
      this.model.fetch({ success: this.onDataHandler, dataType: 'json'});
    },
    
    _updateChartData: function(model, options) {
      var data     = model.toJSON();
      var coptions = this.chartOptions;
      
      // Reset models
      coptions.xAxis.categories = [];
      
      _.each(coptions.series, function(d,i) {
        d.data = [];
      }, this);
      
      if(data.ticks) {
        _.each(data.ticks, function(d, i) {
          coptions.xAxis.categories.push(d);
        }, this);
      }
      
      _.each(data.values, function(d, i) {
        _.each(d,function(d2,j) {
          coptions.series[i].data.push(d2);
        }, this);
      }, this);
      
      this.render();
    } 
  });
  
  return TableSizeView;
});
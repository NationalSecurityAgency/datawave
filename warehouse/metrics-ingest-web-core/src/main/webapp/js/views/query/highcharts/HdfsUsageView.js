define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'models/metrics/HDFSUsageModel',
  'models/metrics/HDFSMaxModel',
  'highchartsmore'
], function($, _, Backbone, D3, HdfsUsageModel, HdfsMaxModel) {

  var HdfsUsageView = Backbone.View.extend({
  
    className: "hdfsUsagePlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false
      },
      title: { 
        text: 'HDFS Usage'
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
          
    onDataHandler: function(model, response, options) {
      this._updateChartData(model, options);
    },
    
    initialize: function() {
      
      _.bindAll(this);
      
      this.hdfsMax = new HdfsMaxModel();
      this.hdfsMax.fetch({ success: this._success, update: false, dataType: 'json'});
      
      this.id = "hdfsUsageView-" + (Math.floor(Math.random() * 900000) + 100000)
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
    
    _updateChartData: function(model, response, options) {
      var data     = model.toJSON();
      var coptions = this.chartOptions;
      
      coptions.series[0].data = [];
      
      if(this.hdfsMax) {
         var hdfsMax = this.hdfsMax.toJSON();
         
         if(_.isNumber(hdfsMax[0])) {
           var free = hdfsMax[0] - data[0];
           var seriesData = new Array();
           
           seriesData.push(["Free", free]);
           seriesData.push(["Used", data[0]]);
           
           if (this.chart) {
             this.chart.series[0].setData(seriesData, true);
             return this;
           } else {
             coptions.series[0].data = seriesData;
             return this.render();
           }
         }
      }
    },
    
    _success: function(model, response, options) {
          
      this.model = new HdfsUsageModel();
      this.model.on('change:0', this._updateChartData);
      this.model.startLongPolling(5000);
      this.model.executeLongPolling();
    }
  });
  
  return HdfsUsageView;
});
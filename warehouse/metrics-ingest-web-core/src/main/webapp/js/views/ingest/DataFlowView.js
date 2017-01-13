define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'models/metrics/ingest/DataFlowModel',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, NVD3, DataFlowModel, svgTemplate) {

  var DataFlowView = Backbone.View.extend({
    className: "dataFlowPlot",
    
    initialize: function(options) {
      
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.model = new DataFlowModel();
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.model.fetch({ success: onDataHandler, dataType: 'json'});
      this.id = "DataFlowView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      var thatElement = this.$el;
      var compiledTemplate = _.template(svgTemplate);
    
      this.$el.append(compiledTemplate);
      
      var data = this.model.toJSON();
      var newData = new Array();
      
      for(var d in data) {
        if (data[d].key && data[d].values) {
          newData.push({key: data[d].key,values: data[d].values[0]});
        }
      }
      
      NVD3.addGraph(function() {
      
        var chart = NVD3.models.stackedAreaChart();
        
        chart = chart
                  .x(function(d) { return d[0] })
                  .y(function(d) { return d[1] })
                  .margin({top: 30, right: 45, bottom: 50, left: 130});

        chart.xAxis.tickFormat(function(d) { return d3.time.format('%m/%d/%Y')(new Date(d)) });
          
        D3.selectAll(thatElement.children('svg'))
            .datum(newData)
            .transition().duration(500)
            .call(chart);
        
        return chart;
      });
    }    
  });
  
  return DataFlowView;
});
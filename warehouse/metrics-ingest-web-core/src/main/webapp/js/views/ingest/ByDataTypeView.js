define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'models/metrics/ingest/ByDataTypeModel',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, NVD3, ByDataTypeModel, svgTemplate) {

  var ByDataTypeView = Backbone.View.extend({
    className: "ingestDataTypePlot",
    
    initialize: function(options) {
      
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.model = new ByDataTypeModel();
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.model.fetch({ success: onDataHandler, dataType: 'json'});
      this.id = "ByDataTypeView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      var thatElement = this.$el;
      var compiledTemplate = _.template(svgTemplate);
    
      this.$el.append(compiledTemplate);
      
      var data = this.model.toJSON();
      console.log(data);
      var newData = new Array();
      
      for(var d in data) {
        if (Object.prototype.toString.call(data[d]) === '[object Array]' &&
            data[d].length > 1) {
          newData.push({label: data[d][0],value: data[d][1]});
        }
      }
      
      var pieData = new Array();
      pieData.push({key: "Events by Data Type", values: newData});
      
      NVD3.addGraph(function() {
      
        var chart = NVD3.models.pieChart();
        
        chart.x(function(d) { return d.label })
             .y(function(d) { return d.value })
             .values(function(d) { return d[0].values })
             .showLabels(false)
             .donut(true);
          
        D3.selectAll(thatElement.children('svg'))
            .datum([pieData])
            .transition().duration(500)
            .call(chart);
        
        return chart;
      });
    }    
  });
  
  return ByDataTypeView;
});
define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'models/metrics/TabletSizeModel',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, NVD3, TabletSizeModel, svgTemplate) {

  var TableSizeView = Backbone.View.extend({
    className: "tabletSizePlot",
    
    initialize: function() {
      
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.model = new TabletSizeModel();
      this.model.fetch({ success: onDataHandler, dataType: 'json'});
      this.id = "tabletSizeView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      var thatElement = this.$el;
      var compiledTemplate = _.template(svgTemplate);
    
      this.$el.append(compiledTemplate);
      
      var data = this.model.toJSON();
      console.log(data);
      
      var pieData = new Array();
      pieData.push({key: 'Total RFile Size', values: [{x: 'default_tablet', y: 46357200}]});
      pieData.push({key: 'Total Key Value Pairs', values: [{x: 'default_tablet', y: 3453495}]});
      
      NVD3.addGraph(function() {
      
        var chart = NVD3.models.multiBarChart();
        chart = chart.margin({left: 200}).stacked(true);
          
        D3.selectAll(thatElement.children('svg'))
            .datum(pieData)
            .transition().duration(500)
            .call(chart);
        
        return chart;
      });
    }    
  });
  
  return TableSizeView;
});
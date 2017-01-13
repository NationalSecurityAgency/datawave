define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'collections/metrics/TableSizeCollection',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, NVD3, TableSizeCollection, svgTemplate) {

  var TableSizeView = Backbone.View.extend({
    className: "tableSizePlot",
    
    /***
    nameSpace: "http://www.w3.org/2000/svg",
 
    // Namespace support 
    _ensureElement: function() {
       
       if(!this.el) {
         var attrs = _.extend({}, _.result(this, 'attributes'));
         
         if (this.id) attrs.id = _.result(this, 'id');
         if (this.className) attrs['class'] = _.result(this, 'className');
         var $el = $(window.document.createElementNS(_.result(this, 'nameSpace'), _.result(this, 'tagName'))).attr(attrs);
         this.setElement($el, false);
       }
    },
    ***/
    
    initialize: function() {
      
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.collection = new TableSizeCollection();
      this.collection.fetch({ success: onDataHandler, dataType: 'json'});
      this.id = "tableSizeView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      var thatElement = this.$el;
      var compiledTemplate = _.template(svgTemplate);
    
      this.$el.append(compiledTemplate);
      
      var data = this.collection.toJSON();
      var newData = new Array();
      
      for(var d in data) {
          newData.push({label: data[d][0],value: data[d][1]});
      }
      
      var pieData = new Array();
      pieData.push({key: "Table Sizes", values: newData});
      
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
  
  return TableSizeView;
});

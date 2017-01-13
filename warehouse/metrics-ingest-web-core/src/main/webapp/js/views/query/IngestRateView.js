define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'cubism',
  'models/metrics/IngestRateModel',
  'text!templates/svg/svgTemplate.html'
], function($, _, Backbone, D3, Cubism, IngestRateModel, svgTemplate) {

  var IngestRateView = Backbone.View.extend({
    className: "ingestRatePlot",
    
    initialize: function() {
    
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.model = new IngestRateModel();
      this.model.fetch({ success: onDataHandler, dataType: 'json'});
    },
    
    render: function() {
      var thatElement = this.el;
      // var compiledTemplate = _.template(svgTemplate);
      
      function random(name) {
        var value = 0,
            values = [],
            i = 0,
            last;
         
        return context.metric(function(start, stop, step, callback) {
          start = +start, stop = +stop;
          if (isNaN(last)) last = start;
          while (last < stop) {
            last += step;
            value = Math.max(-10, Math.min(10, value + .8 * Math.random() - .4 + .2 * Math.cos(i += .2)));
            values.push(value);
          }
          callback(null, values = values.slice((start-stop) / step));          
        }, name);
      }
      
      var context = Cubism.context()
                    .serverDelay(0)
                    .clientDelay(0)
                    .step(1e3);
                    
      var foo = random("foo");
      
      D3.select(thatElement).append("div").call(function(div) {
        div.append("div")
          .attr("class", "axis")
          .call(context.axis().orient("top"));
          
        div.selectAll(".horizon")
            .data([foo])
          .enter().append("div")
            .attr("class", "horizon")
            .call(context.horizon().extent([-20, 20]));
            
        div.append("div")
            .attr("class", "rule")
            .call(context.rule());
      });
    }
  });
  
  return IngestRateView;
});
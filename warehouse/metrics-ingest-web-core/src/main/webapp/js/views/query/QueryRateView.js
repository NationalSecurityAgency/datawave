define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'cubism',
  'models/metrics/QueryRateModel',
], function($, _, Backbone, D3, Cubism, QueryRateModel) {

  var QueryRateView = Backbone.View.extend({
    className: "queryRatePlot",
    
    initialize: function() {
    
      var that = this;
      
      this.model = new QueryRateModel();
      this.model.startLongPolling();
      this.model.fetch();
      this.render();
    },
    
    render: function() {
      var thatElement = this.el;
      
      function update(model) {
        var values = [];
         
        return context.metric(function(start, stop, step, callback) {
          model.fetch();
          var data = model.toJSON();
          if (data[0]) values.push(parseInt(data[0]));
          callback(null, values = values.slice(-context.size()));          
        }, "Query Rate (MB/s)");
      }
      
      var context = Cubism.context()
                    .serverDelay(30 * 1000)
                    .clientDelay(0)
                    .step(5000);
                    
      var foo = update(this.model);
      
      D3.select(thatElement).append("div").call(function(div) {
        div.append("div")
          .attr("class", "axis")
          .call(context.axis().orient("top"));
          
        div.selectAll(".horizon")
            .data([foo])
          .enter().append("div")
            .attr("class", "horizon")
            .call(context.horizon([0,100]));
            
        div.append("div")
            .attr("class", "rule")
            .call(context.rule());
      });
    }
  });
  
  return QueryRateView;
});
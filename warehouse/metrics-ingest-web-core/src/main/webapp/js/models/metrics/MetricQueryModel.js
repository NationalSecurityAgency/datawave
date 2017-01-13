define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var MetricQueryModel = Backbone.Model.extend({
  
    start: (new Date()).setHours(0,0,0,0) - 12096e5,
      
    end: (new Date()).setHours(23,59,59,999) + 1,
    
    ingestType: 'bulk'
  });
  
  return MetricQueryModel;
});
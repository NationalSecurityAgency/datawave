define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var TableSizeModel = Backbone.Model.extend({
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=tablesize"
      }
  });
  
  return TableSizeModel;
});
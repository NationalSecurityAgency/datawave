define([
  'underscore',
  'backbone',
  'models/metrics/TableSizeModel'
], function(_, Backbone, TableSizeModel) {
  
  var TableSizeCollection = Backbone.Collection.extend({
  
      model: TableSizeModel,
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=tablesize"
      }
  });
  
  return TableSizeCollection;
});
define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var HDFSMaxModel = Backbone.Model.extend({
      
      initialize: function() {
        _.bindAll(this);
      },
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=hdfsMax"
      }
  });
  
  return HDFSMaxModel;
});
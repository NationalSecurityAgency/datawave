define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var TabletSizeModel = Backbone.Model.extend({
  
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=rfilesize&tableName=shard&startNumber=0&amount=10"
      }
  });
  
  return TabletSizeModel;
});
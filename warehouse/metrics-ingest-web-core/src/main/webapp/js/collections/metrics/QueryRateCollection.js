define([
  'underscore',
  'backbone',
  'models/metrics/QueryRateModel'
], function(_, Backbone, QueryRateModel) {
  
  var QueryRateCollection = Backbone.Collection.extend({
  
      model: QueryRateModel,
      
      longPolling : false,
      
      initialize: function() {
        _.bindAll(this);
      },
      
      startLongPolling: function(interval) {
        this.longPolling = true;
        if (interval) {
          this.interval = interval;
        }
        this.executeLongPolling();
      },
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=QueryRate"
      },
      
      stopLongPolling: function() {
        this.longPolling = false;
      },
      
      executeLongPolling: function() {      
        this.fetch({success: this.onFetch});
      },
      
      onFetch: function() {
        if (this.longPolling ) {
          setTimeout(this.executeLongPolling, this.interval);
        }
      }
  });
  
  return QueryRateCollection;
});
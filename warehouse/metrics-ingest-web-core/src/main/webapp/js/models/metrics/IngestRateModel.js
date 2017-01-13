define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var IngestRateModel = Backbone.Model.extend({
      
      longPolling: false,
      interval: 5000, // in milliseconds
      
      initialize: function() {
        _.bindAll(this);
      },
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=ingestRate"
      },
      
      startLongPolling: function(interval) {
        this.longPolling = true;
        if(interval) {
          this.interval = interval;
        }
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
  
  return IngestRateModel;
});
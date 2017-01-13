define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var QueryRateModel = Backbone.Model.extend({
      
      longPolling: false,
      interval: 5000, // in milliseconds
      
      initialize: function() {
        _.bindAll(this);
      },
      
      url: function() {
          return "/DataWave/Ingest/Metrics/services/statistics?type=queryRate"
      },
      
      startLongPolling: function(interval) {
        this.longPolling = true;
        if(interval) {
          this.interval = interval;
        }
        this.executeLongPolling();
      },
      
      stopLongPolling: function() {
        this.longPolling = false;
      },
      
      executeLongPolling: function() {
        this.fetch({success: this.onFetch, error: this.onFail});
      },
      
      onFetch: function(model, response, options) {
        if (this.longPolling ) {
          setTimeout(this.executeLongPolling, this.interval);
        }
        
        if (this._successCallback) {
          this._successCallback(model, response, options);
        }
      },
      
      onFail: function(model, response, options) {
        if (this.longPolling ) {
          setTimeout(this.executeLongPolling, this.interval);
        }
        
        if (this._errorCallback) {
          this._errorCallback(model, response, options);
        }
      },
      
      onSuccess: function(_) {        
        if(!arguments.length) return this._successCallback;
        this._successCallback = _;
        return this._successCallback;
      },
      
      onError: function(_) {
        if(!arguments.length) return this._errorCallback;
        this._errorCallback = _;
        return this._errorCallback;
      }
  });
  
  return QueryRateModel;
});
define([
  'underscore',
  'backbone',
  'models/metrics/IngestRateModel'
], function(_, Backbone, IngestRateModel) {
  
  var IngestRateCollection = Backbone.Collection.extend({
    
    longPolling: false,
    interval:    5000, // in milliseconds
    latency:     5000,
    ingestType:  'bulk',
  
    model: IngestRateModel,
      
    initialize: function(models, options) {
      _.bindAll(this);
      
      if(options.latency)
        this.latency = options.latency;
      
      if(options.ingestType)
        this.ingestType = options.ingestType;
      
      if(options.interval)
        this.interval = options.interval;
    },
    
    url: function() {
      return "/DataWave/Ingest/Metrics/services/ecs";
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
      this._start = null;
      this._end   = null;
    },
    
    executeLongPolling: function() {
      
      var date = new Date();
      
      if(this._end) {
        this._start = this._end;
        this._end   = date.getTime() - this.latency;
      }
      else {
        this._start = date.getTime() - this.latency - this.interval;
        this._end   = date.getTime() - this.latency;
      } 
      
      this.fetch({success: this.onFetch, error: this.onFail, data: {ingestType: this.ingestType, start: this._start, end: this._end}});
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
  
  return IngestRateCollection;
});
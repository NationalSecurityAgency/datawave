define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {

  var QueryMetricSummaryModel = Backbone.Model.extend({
    
    longPolling: false,
    interval:    5000, // in milliseconds
    latency:     300000,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    /***
    parse: function(response, options) {
      return response.Summary;
    },
    ***/
    
    url: function() {
      return '/DataWave/Query/Metrics/summary.json';
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
      
      var start = new Date();
      var end   = new Date();
      
      start.setTime(this._start);
      end.setTime(this._end);

      this.fetch({success: this.onFetch, error: this.onFail, data: {ingestType: this.ingestType, begin: this._formatDate(this._start), end: this._formatDate(this._end)}});
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
    },
    
    _formatDate: function(_date) {
      
      var date = _date;
      
      if(_.isNumber(_date)) {
        date = new Date()
        date.setTime(_date);
      }
        
      
      if(_.isDate(date)) {
        dateString = date.getUTCFullYear() +
                       ('0' + (date  .getUTCMonth() + 1)).slice(-2) + 
                       ('0' + (date  .getUTCDate())).slice(-2) + ' ' +
                       ('0' + (date  .getUTCHours())).slice(-2) +
                       ('0' + (date  .getUTCMinutes())).slice(-2) +
                       ('0' + (date  .getUTCSeconds())).slice(-2);
      } else {
        dateString = date.toString();
      }
      
      return dateString;
    }
  });
  
  return QueryMetricSummaryModel;
});

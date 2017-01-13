define([
  'underscore',
  'backbone',
  'models/metrics/error/ErrorCountModel'
], function(_, Backbone, ErrorCountModel) {
  
  var ErrorCountCollection = Backbone.Collection.extend({
  
      model: ErrorCountModel,
      
      _start: (new Date()).setHours(0,0,0,0) - 12096e5,
      
      _end: (new Date()).setHours(23,59,59,999) + 1,
      
      initialize: function() {
        _.bindAll(this);
      },

      url: function() {
          return "/DataWave/Ingest/Metrics/services/errorMetrics?start=" + this._start + "&end=" + this._end + "&type=cnt" ;
      },
      
      start: function(_) {
        if(!arguments.length) return this._start;
        this._start = _;
        return this._start;
      },
      
      end: function(_) {
        if(!arguments.length) return this._end;
        this._end = _;
        return this._end;
      }
  });
  
  return ErrorCountCollection;
});
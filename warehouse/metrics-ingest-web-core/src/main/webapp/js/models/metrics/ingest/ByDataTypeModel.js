define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var ByDataTypeModel = Backbone.Model.extend({
      
      _start: (new Date()).setHours(0,0,0,0) - 12096e5,
      
      _end: (new Date()).setHours(23,59,59,999) + 1,
      
      _ingestType: "bulk",
 
      url: function() { 
          return "/DataWave/Ingest/Metrics/services/jobSummary?start=" + this._start + "&end=" + this._end + "&ingestType=" + this._ingestType;
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
      },
      
      ingestType: function(_) {
        if(!arguments.length) return this._ingestType;
        this._ingestType = _;
        return this._ingestType;
      }
  });
  
  return ByDataTypeModel;
});

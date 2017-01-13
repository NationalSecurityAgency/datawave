define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var HistoryModel = Backbone.Model.extend({
      
      _start: (new Date()).setHours(0,0,0,0) - 12096e5,
      
      _end: (new Date()).setHours(23,59,59,999) + 1,
      
      _ingestType: "bulk",
      
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
        return _ingestType;
      }
  });
  
  return HistoryModel;
});
define([
  'underscore',
  'backbone',
  'models/metrics/ingest/ByDataTypeModel'
], function(_, Backbone, ByDataTypeModel) {
  
  var ByDataTypeCollection = Backbone.Collection.extend({
  
      model: ByDataTypeModel,
      
      _start: (new Date()).setHours(0,0,0,0) - 12096e5,
      
      _end: (new Date()).setHours(23,59,59,999) + 1,
      
      _ingestType: "bulk",
      
      initialize: function() {
        _.bindAll(this);
      },

      url: function() {
          return "/DataWave/Ingest/Metrics/services/typeDistribution?start=" + this._start + "&end=" + this._end + "&ingestType=" + this._ingestType;
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
  
  return ByDataTypeCollection;
});
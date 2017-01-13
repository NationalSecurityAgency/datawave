define([
  'jquery',
  'underscore',
  'backbone',
  'models/metrics/QueryMetricModel'
], function($, _, Backbone, QueryMetricModel) {

  var QueryMetricCollection = Backbone.Collection.extend({
    
    model: QueryMetricModel,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    parse: function(response, options) {
      return response.result;
    },
    
    url: function() {
      return '/DataWave/Query/Metrics/list.json';
    }
  });
  
  return QueryMetricCollection;
});
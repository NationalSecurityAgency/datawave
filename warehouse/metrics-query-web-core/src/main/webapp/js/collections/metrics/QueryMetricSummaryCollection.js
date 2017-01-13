define([
  'jquery',
  'underscore',
  'backbone',
  'models/metrics/QueryMetricModel'
], function($, _, Backbone, QueryMetricModel) {

  var QueryMetricSummaryCollection = Backbone.Collection.extend({
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    parse: function(response, options) {
      return response.Summary;
    },
    
    url: function() {
      return '/DataWave/Query/Metrics/summary/time.json';
    }
  });
  
  return QueryMetricSummaryCollection;
});

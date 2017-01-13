define([
  'jquery',
  'underscore',
  'backbone',
  'models/metrics/user/UserQueryMetricSummaryModel'
], function($, _, Backbone, UseryQueryMetricSummaryModel) {

  var UserQueryMetricSummaryCollection = Backbone.Collection.extend({
    
    model: UseryQueryMetricSummaryModel,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    parse: function(response, options) {
      return response.Summary;
    },
    
    url: function() {
      return '/DataWave/Query/Metrics/summary/user.json';
    }
  });
  
  return UserQueryMetricSummaryCollection;
});
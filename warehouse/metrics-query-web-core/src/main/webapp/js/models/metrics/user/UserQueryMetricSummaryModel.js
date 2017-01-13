define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {

  var UserQueryMetricSummaryModel = Backbone.Model.extend({
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    }

  });
  
  return UserQueryMetricSummaryModel;
});

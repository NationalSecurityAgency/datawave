define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {

  var SummaryModel = Backbone.Model.extend({
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    url: function() {
      return "/DataWave/Query/Metrics/summaryAsHtml"
    }
  });
  
  return SummaryModel;
});

define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {

  var QueryMetricModel = Backbone.Model.extend({
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    }    
  });
  
  return QueryMetricModel;
});

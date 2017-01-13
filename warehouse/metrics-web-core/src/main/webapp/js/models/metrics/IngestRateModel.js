define([
  'underscore',
  'backbone'
], function(_, Backbone) {
  
  var IngestRateModel = Backbone.Model.extend({

    initialize: function() {
      _.bindAll(this);
    }
  });
  
  return IngestRateModel;
});
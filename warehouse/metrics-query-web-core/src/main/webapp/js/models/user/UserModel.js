define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {

  var UserModel = Backbone.Model.extend({
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    }
  });
  
  return UserModel;
});

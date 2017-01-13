define([
  'jquery',
  'underscore',
  'backbone',
  'models/user/UserModel'
], function($, _, Backbone, UserModel) {

  var UserCollection = Backbone.Collection.extend({
    
    model: UserModel,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    }
  });
  
  return UserCollection;
});
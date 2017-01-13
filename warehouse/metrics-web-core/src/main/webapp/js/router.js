// Filename: router.js
define([
  'jquery',
  'underscore',
  'backbone',
  'views/MainView',
], function($, _, Backbone, MainView) {

  var AppRouter = Backbone.Router.extend({
    routes: {
      // Define some URL routes
      
      // Default
      '*actions': 'defaultAction'
    }
  });
  
  var initialize = function () {
  
    var app_router = new AppRouter;
    
    var mainView = new MainView();
    
    mainView.render();
    
    Backbone.history.start();
  };
  
  return {
    initialize: initialize
  };
});

define([
  'jquery',
  'underscore',
  'backbone',
  'models/metrics/QueryMetricModel'
], function($, _, Backbone, QueryMetricModel) {

  var UserQueryMetricCollection = Backbone.Collection.extend({
    
    model: QueryMetricModel,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      // if(options && options.user) this._user = options.user;
    },
    
    parse: function(response, options) {
      return response.result;
    },
    
    url: function() {
      return './DataWave/Query/Metrics/user/' + this._user + '.json';
    },
    
    user: function(_) {
      if(!arguments.length) return this._user;
      this._user = _;
      return this._user;
    }
  });
  
  return UserQueryMetricCollection;
});
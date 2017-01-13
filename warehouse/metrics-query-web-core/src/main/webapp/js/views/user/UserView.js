define([
  'jquery',
  'underscore',
  'backbone',
  'collections/user/UserCollection',
  'collections/metrics/user/UserQueryMetricSummaryCollection',
  'views/user/UserListView',
  'views/user/UserDetailView',
  'text!templates/user/userViewTemplate.html'
], function($, _, Backbone, UserCollection, UserQueryMetricSummaryCollection,  UserListView, UserDetailView, userViewTemplate) {

  var UserView = Backbone.View.extend({
    
    className: 'user-view-container',
    
    events: {
      'selectUser' : '_selectUser'
    },
    
    spanInDays: 30,
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
    },
    
    render: function() {
      
      var el = this.$el;
      
      var compiledTemplate = _.template(userViewTemplate);
      
      el.append(compiledTemplate);
      
      if(!this.userListView) {
        this.userListView = new UserListView();
        el.find('.user-view-list-container').append(this.userListView.render().$el);
        // this.listenTo(this.userListView,'selectUser', this._selectUser);
      }
            
      if(! this.userDetailView) {
        this.userDetailView = new UserDetailView();
        el.find('.user-view-detail-container').append(this.userDetailView.render().$el);
      }
      
      return this;
    },
    
    setRange: function(range,interval) {

      if(this.userDetailView)
        this.userDetailView.setRange(range,interval);
      
      if(this.userListView)
        this.userListView.setRange(range);  

      return this;
    },
    
    _show: function() {
      if (this.userDetailView)
        this.userDetailView._renderTabs();
    },
    
    _selectUser: function(event) {
      if(this.userDetailView)
        this.userDetailView.user(this.userListView.user());
    }
  });
  
  return UserView;
});

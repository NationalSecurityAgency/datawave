define([
  'jquery',
  'underscore',
  'backbone',
  'collections/metrics/user/UserQueryMetricCollection',
  'views/metrics/user/UserQueryCountView',
  'views/metrics/user/UserQueryTimeView',
  'views/metrics/user/UserQueryDetailView',
  'text!templates/user/userDetailTemplate.html'
], function($, _, Backbone, UserQueryMetricCollection, UserQueryCountView, UserQueryTimeView, UserQueryDetailView, userDetailTemplate) {

  var UserDetailView = Backbone.View.extend({
    
    events: {
      "click .nav li a" : "selectTab",
      "shown .nav li a" : "shown"
    },
    
    className: 'user-detail-container well margin-bottom-0',
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      this._id = (Math.floor(Math.random() * 900000) + 100000);
    },
    
    render: function() {
      
     this.$el.empty();
    
      var compiledTemplate = _.template(userDetailTemplate, {id: this._id});
      this.$el.append(compiledTemplate);
      
      this._renderTabs();
      
      this.$el.find('.dropdown-toggle').dropdown();
      
      return this;
    },
    
    _renderTabs: function(collection) {
      
      var el = this.$el;
      
      var target = el.find('.user-detail-tabs');
      
      target = el.find('.qpd-view-container');
            
      if(!this.queryCountView) {
        this.queryCountView = new UserQueryCountView();
        target.append(this.queryCountView.render().$el);
      }
      
      target = el.find('.uqt-view-container');
            
      if(!this.queryTimeView) {
        this.queryTimeView = new UserQueryTimeView();
        target.append(this.queryTimeView.render().$el);
      }
      
      target = el.find('.query-detail-container');
      
      if(!this.queryDetailView) {
        this.queryDetailView = new UserQueryDetailView();
        target.append(this.queryDetailView.render().$el);
      }
      
      if (collection) {      
        this.queryCountView.interval(this._interval, false);
        this.queryCountView.metrics(collection);
        this.queryTimeView.interval(this._interval, false);
        this.queryTimeView.metrics(collection);
        this.queryDetailView.metrics(collection);
      }
      
      return this;
    },
    
    _success: function(collection, response, options) {
      console.log(collection);
      this._renderTabs(collection);
      return this;
    },
    
    user: function(_) {
      if(!arguments.length) return this._user;
      this._user = _;
      this._update();
      return this._user;
    },
  
    _update: function() {
      
      var beginString, endString;
      
      var begin = this._dateRange[0];
      var end   = this._dateRange[1];
      
      if(_.isDate(begin)) {
        beginString = begin.getUTCFullYear() +
                        ('0' + (begin.getUTCMonth() + 1)).slice(-2) + 
                        ('0' + (begin.getUTCDate())).slice(-2);
      }
                        
      if(_.isDate(end)) {       
        endString = end.getUTCFullYear() +
                      ('0' + (end.getUTCMonth() + 1)).slice(-2) + 
                      ('0' + (end.getUTCDate())).slice(-2);
      }
  
      if(this.jqXHR)
        this.jqXHR.abort();
      
      if(this._user) {
        this._loading();
        this.collection = new UserQueryMetricCollection();
        this.collection.user(this._user);
        this.jqXHR = this.collection.fetch({data: {begin: beginString, end: endString}, success: this._success});
      }
      
      return this;
    },
    
    selectTab: function(event) {
      event.preventDefault();
      $(event.currentTarget).tab('show');
    },
    
    shown: function(event) {
      event.preventDefault();
      event.stopPropagation();
      
      console.log(event.currentTarget);
      
      if($(event.currentTarget).attr('data-target').indexOf("#tab-qpd-") == 0) {
        if(this.queryCountView) this.queryCountView._renderChart();
      } else if($(event.currentTarget).attr('data-target').indexOf("#tab-uqt-") == 0) {
        if(this.queryTimeView) this.queryTimeView._renderChart();
      }
    },
    
    setRange: function(range,interval) {
      this._dateRange = range;
      this._interval = interval;
      this._update();
      return this;
    },
    
    _loading: function() {
      if(this.queryCountView) {
        this.queryCountView._loading();
      }
      
      if(this.queryTimeView) {
        this.queryTimeView._loading();
      }
      
      if(this.queryDetailView) {
        this.queryDetailView._loading();
      }
    }
  });
  
  return UserDetailView;
});

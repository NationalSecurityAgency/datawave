define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'views/user/UserView',
  'views/metrics/summary/QueryMetricsSummaryView',
  'text!templates/home/dashboardTemplate.html'
], function($, jQueryUI, _, Backbone, UserView, QueryMetricsSummaryView, dashboardTemplate) {

  var DashboardView = Backbone.View.extend({
    
    className: 'span12',
    
    events: {
      "click .nav-tabs a": "selectTab",
      'shown .nav-tabs a': "shown"
    },
    
    initialize: function() {
      _.bindAll(this);
      this._id = (Math.floor(Math.random() * 900000) + 100000);
    },
    
    render: function() {
    
      var compiledTemplate = _.template(dashboardTemplate, { id: this._id });
            
      this.$el.append(compiledTemplate);
      
      var summaryTab = this.$el.find('#query-metrics-summary-tab-' + this._id);
      var userTab  = this.$el.find('#user-query-metrics-tab-' + this._id);
      
      this.summaryView = new QueryMetricsSummaryView();
      this.userView = new UserView();
      
      summaryTab.append(this.summaryView.render().$el);
      userTab.append(this.userView.render().$el);
      
      return this;
    },
    
    selectTab: function(event) {
      console.log(event.currentTarget)
      event.preventDefault();
      $(event.currentTarget).tab('show');
    },
    
    shown: function(event) {
      console.log("shown");
      console.log(event);
      
      event.preventDefault();
      
      switch ($(event.currentTarget).attr('data-target')) {
        case ('#query-metrics-summary-tab-' + this._id):
          this.summaryView._show();  
          break;
        case ('#user-query-metrics-tab-' + this._id):
          this.userView._show();  
          break;
      }
    },
    
    update: function(begin,end,interval) {
      console.log(begin);
      console.log(end);
      console.log(interval);
      
      if(this.summaryView) this.summaryView.setRange([begin,end],interval);
      if(this.userView) {
        this.userView.setRange([begin,end], interval);
      }
    }
  });
  
  return DashboardView;
});
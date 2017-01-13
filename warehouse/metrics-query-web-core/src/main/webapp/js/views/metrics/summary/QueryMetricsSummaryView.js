define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'bootstrap',
  'collections/metrics/QueryMetricSummaryCollection',
  'views/metrics/summary/QueryResultsSummaryView',
  'views/metrics/summary/PageResultsSummaryView',
  'views/metrics/summary/TimeResultsSummaryView',
  'text!templates/metrics/summary/querySummaryViewTemplate.html'
], function($, jQueryUI, _, Backbone, Bootstrap, QueryMetricSummaryCollection, QueryResultsSummaryView, PageResultsSummaryView, TimeResultsSummaryView, mainTemplate) {

  var QueryMetricsSummaryView = Backbone.View.extend({
    
    events: {
      "click .nav-tabs a": "selectTab",
      "shown .nav-tabs a": "shown"
    },
    
    className: 'query-metrics-summary-container row-fluid',

    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      this._id = (Math.floor(Math.random() * 900000) + 100000);
    },
    
    render: function() {
      
      var compiledTemplate = _.template(mainTemplate, {id: this._id});
      
      this.$el.append(compiledTemplate);
      
      if(!this._clearfix) {
        this._clearfix = $(document.createElement('div'));
        this._clearfix.addClass('clearfix');
        this.$el.append(this._clearfix);
      }
      
      if(this._queryResultsSummaryView) {
        this._queryResultsSummaryView.remove();
        this._queryResultsSummaryView = null;
      }
        
      if(this._pageResultsSummaryView) {
        this._pageResultsSummaryView.remove();
        this._pageResultsSummaryView = null;
      }
      
      if(this._timeResultsSummaryView) {
        this._timeResultsSummaryView.remove();
        this._timeResultsSummaryView = null;
      }
      
      this._queryResultsSummaryView = new QueryResultsSummaryView();
      this._pageResultsSummaryView  = new PageResultsSummaryView();
      this._timeResultsSummaryView  = new TimeResultsSummaryView();
      
      this.$el.find('#page-summary-tab-' + this._id).append(this._pageResultsSummaryView.render().$el);
      this.$el.find('#query-summary-tab-' + this._id).append(this._queryResultsSummaryView.render().$el);
      this.$el.find('#time-summary-tab-' + this._id).append(this._timeResultsSummaryView.render().$el);
     
      return this;
    },
    
    _success: function(collection,response,options) {
      
      this._update(collection, options.data.interval);
      
      return this;
    },
    
    _update: function(_) {
      
      var collection = this.collection;
      
      if(arguments.length > 0)
        collection = _;
        
      if(arguments.length > 1)
        interval = arguments[1]
      else
        interval = 864e5;
      
      if(this._queryResultsSummaryView)
        this._queryResultsSummaryView._update(collection, interval);
        
      if(this._pageResultsSummaryView)
        this._pageResultsSummaryView._update(collection, interval);
        
      if(this._timeResultsSummaryView)
        this._timeResultsSummaryView._update(collection, interval);
        
      return this;
    },
    
    _error: function(collection,response,options) {
      
      if(this._queryResultsSummaryView)
        this._queryResultsSummaryView._error();
        
      if(this._pageResultsSummaryView)
        this._pageResultsSummaryView._error();
        
      if(this._timeResultsSummaryView)
        this._timeResultsSummaryView._error();
      
      return this;
    },
    
    selectTab: function(event) {
      event.preventDefault();
      event.stopPropagation();
      $(event.currentTarget).tab('show');
      
      this._redrawTab($(event.currentTarget).attr('data-target'));
    },
    
    shown: function(event) {
      console.log(event);
      event.stopPropagation();
    },
    
    _show: function() {
      this._redrawTab(this.$el.find('.tab-pane.active').attr('id'));
    },
    
    _redrawTab: function(id) {
      
      var d = id.indexOf('#') >= 0 ? id : '#' + id;
      
      switch (d) {
        case ('#page-summary-tab-' + this._id):
          this._pageResultsSummaryView._renderChart()  
          break;
        case ('#query-summary-tab-' + this._id):
          this._queryResultsSummaryView._renderChart();
          break;
        case ('#time-summary-tab-' + this._id):
          this._timeResultsSummaryView._renderChart();
          break;
      }
    },
    
    setRange: function(range, interval) {
            
      var beginDate, endDate, beginString, endString;
      
      var begin = range[0];
      var end = range[1];
      
      if(_.isDate(begin)) {
        beginDate = begin;
        beginString = begin.getUTCFullYear() +
                        ('0' + (beginDate.getUTCMonth() + 1)).slice(-2) + 
                        ('0' + (beginDate.getUTCDate())).slice(-2);
      } else {
        beginString = begin.toString();
        beginDate = new Date();
        beginDate.setUTCFullYear(begin.substring(0,4));
        beginDate.setUTCMonth(begin.substring(4,6) + 1);
        beginDate.setUTCDate(begin.substring(6));
        beginDate.setUTCHours(0);
        beginDate.setUTCMinutes(0);
        beginDate.setUTCSeconds(0);
        beginDate.setUTCMilliseconds(0);
      }
                        
      if(_.isDate(end)) {
        endDate = end;                  
        endString = endDate.getUTCFullYear() +
                      ('0' + (endDate.getUTCMonth() + 1)).slice(-2) + 
                      ('0' + (endDate.getUTCDate())).slice(-2);
      } else {
        endString = end.toString();
        endDate = new Date();
        endDate.setUTCFullYear(end.substring(0,4));
        endDate.setUTCMonth(end.substring(4,6) + 1);
        endDate.setUTCDate(end.substring(6));
        endDate.setUTCHours(0);
        endDate.setUTCMinutes(0);
        endDate.setUTCSeconds(0);
        endDate.setUTCMilliseconds(0);
      }
      
      this._begin = beginString;
      this._end   = endString;
      this._interval = interval;
        
      return this.fetchMetrics();
    },
    
    interval: function(_) {
      if(!arguments.length) return this._interval;
      this._interval = _;
      this.fetchMetrics();
      return this._interval;
    },
    
    fetchMetrics: function() {
      
      if(this._queryResultsSummaryView)
        this._queryResultsSummaryView._loading();
        
      if(this._pageResultsSummaryView)
        this._pageResultsSummaryView._loading();
        
      if(this._timeResultsSummaryView)
        this._timeResultsSummaryView._loading();
        
      if (this.jqXHR)
        this.jqXHR.abort();
        
      this.collection  = new QueryMetricSummaryCollection();
      this.jqXHR = this.collection.fetch({data: {begin: this._begin, endString: this._end, interval: this._interval}, success: this._success, error: this._error});
      
      return this;
    }
  });
  
  return QueryMetricsSummaryView;
});
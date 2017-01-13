define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'views/query/highcharts/QueryRateView',
  'views/query/highcharts/TableSizeView',
  'views/query/IngestRateView',
  'views/query/highcharts/TabletSizeView',
  'views/ingest/highcharts/ByDataTypeView',
  'views/ingest/highcharts/DataFlowView',
  'views/ingest/highcharts/LatencyView',
  'views/ingest/highcharts/HistoryView',
  'views/query/highcharts/IngestRateView',
  'views/query/highcharts/HdfsUsageView',
  'text!templates/dashboard/dashboardTemplate.html'
], function($, jQueryUI, _, Backbone, QueryRateView, TableSizeView, IngestRateView, TabletSizeView,
            ByDataTypeView, DataFlowView, LatencyView, HistoryView, 
            IngestRateView, HdfsUsageView,
            dashboardTemplate) {

  var DashboardView = Backbone.View.extend({
  
    _tabs: {},
    
    _childViews: [],
    
    initialize: function() {
      _.bindAll(this);
      this._id = (Math.floor(Math.random() * 900000) + 100000);
    },
    
    initializeChildViews: function() {
    
      this._childViews.push(this._tableSizeView      = new TableSizeView());
      this._childViews.push(this._tabletSizeView     = new TabletSizeView());
      this._childViews.push(this._byDataTypeView     = new ByDataTypeView());
      this._childViews.push(this._dataFlowView       = new DataFlowView());
      this._childViews.push(this._ingestLatencyView  = new LatencyView());
      this._childViews.push(this._ingestHistoryView  = new HistoryView());
      this._childViews.push(this._queryRateView      = new QueryRateView());
      this._childViews.push(this._ingestRateView     = new IngestRateView());
      this._childViews.push(this._hdfsUsageView      = new HdfsUsageView());
    },
    
    clearChildViews: function() {
      _.each(this._childViews, function(view, i) {
        this.$el.remove('#' + view.id);
      }, this);
    },
    
    render: function() {
    
      var compiledTemplate = _.template(dashboardTemplate, { id: this._id });
      
      var dBoard = $(compiledTemplate);
      
      if(dBoard.length > 0) {
        
        this.initializeChildViews();
        
        var statsTab = dBoard.find('#stats-tab-' + this._id);
        var ingestTab = dBoard.find('#ingest-tab-' + this._id);
        
        if (statsTab.length > 0 )
          this.addMetricViews(statsTab, [this._tableSizeView,
                                         this._queryRateView,
                                         this._tabletSizeView,
                                         this._hdfsUsageView,
                                         this._ingestRateView,]);

        if (ingestTab.length > 0 )
          this.addMetricViews(ingestTab, [this._ingestLatencyView,
                                          this._byDataTypeView,
                                          this._dataFlowView,
                                          this._ingestHistoryView]);
      }
            
      this.$el.append(dBoard);
    },
    
    addMetricViews: function(container, views) {                                        
      _.each(views, function(view, i) {
        container.append($('<div></div>').addClass('metric-container')
                                         .addClass('pull-left')
                                         .append(view.$el));
                                         
        if (! this._tabs[container[0].id]) this._tabs[container[0].id] = {activated: false, children: []};
        
        this._tabs[container[0].id].children.push(view);
                                                 
        if ((i % 2) == 1) container.append($('<div></div>').addClass('clearfix'));
      }, this);
      
      if ( _.size(views) % 2 == 1  ) container.append($('<div></div>').addClass('clearfix'));
    },
    
    onShow: function() {
      var that = this;      
      var b = this.$el.find('.metrics-tabs-container');
      

      b.tabs({activate: function(event, ui) {
          /***
          _.each(that._tabs[ui.newPanel[0].id].children, function(view,i) {
            view.render();
          }, this);
          ***/
        }
      });
    },
    
    update: function(model) {
      _.each(this._childViews, function(view,i) {
        if (view.update) view.update(model);
        console.log(view);
      }, this);
    }
  });
  
  return DashboardView;
});

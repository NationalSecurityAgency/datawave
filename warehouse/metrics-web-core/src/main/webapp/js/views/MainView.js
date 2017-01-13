define([
  'jquery',
  'underscore',
  'backbone',
  'views/metrics/IngestByteRateView',
  'views/metrics/QueryByteRateView',
  'views/metrics/LiveIngestEventRateView',
  'views/metrics/BulkIngestEventRateView',
  'views/metrics/QueryRateView',
  'views/metrics/IngestLatencyView',
], function($, _, Backbone, IngestByteRateView, QueryByteRateView, LiveIngestRateView, BulkIngestRateView, QueryRateView, IngestLatencyView) {

  var MainView = Backbone.View.extend({
    
    spanInDays: 30,
  
    el: $('#content-container'),
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      // Initializing views
      this.ingestByteRateView = new IngestByteRateView();
      this.queryByteRateView  = new QueryByteRateView();
      this.liveIngestRateView = new LiveIngestRateView();
      this.bulkIngestRateView = new BulkIngestRateView();
      this.queryRateView      = new QueryRateView();
      this.ingestLatencyView  = new IngestLatencyView();
    },
    
    render: function() {
      this.$el.find('#ingest-container').append(this.liveIngestRateView.render().$el);
      this.$el.find('#ingest-container').append(this.bulkIngestRateView.render().$el);
      this.$el.find('#ingest-container').append(this.ingestByteRateView.render().$el);
      this.$el.find('#query-container').append(this.queryByteRateView.render().$el);
      this.$el.find('#query-container').append(this.queryRateView.render().$el);
      this.$el.find('#query-container').append(this.ingestLatencyView.render().$el);
      
      return this;
    }
  });
  
  return MainView;
});

define([
  'jquery',
  'underscore',
  'backbone',
  'd3',
  'nvd3',
  'models/metrics/ingest/IngestTypeTableModel',
], function($, _, Backbone, D3, NVD3, IngestTypeTableModel) {

  var IngestTypeTableView = Backbone.View.extend({
    className: "ingestDataTypePlot",
    
    initialize: function(options) {
      
      var that = this;
      
      var onDataHandler = function(collection) {
        that.render();
      }
      
      this.model = new IngestTypeTableModel();
      
      // Initialize the model manually
      if (options && options.start) this.model.start(options.start);
      if (options && options.end) this.model.start(options.end);
      if (options && options.ingestType) this.model.start(options.ingestType);
      
      this.model.fetch({ success: onDataHandler, dataType: 'json'});
      this.id = "IngestTypeTableView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      var thatElement = this.$el;
    
      this.$el.append(compiledTemplate);
      
      var data = this.model.toJSON();      
     
    }    
  });
  
  return IngestTypeTableView;
});
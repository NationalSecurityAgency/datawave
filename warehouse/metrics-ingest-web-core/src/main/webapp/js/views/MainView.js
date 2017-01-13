define([
  'jquery',
  'underscore',
  'backbone',
  'views/dashboard/DashboardView',
  'views/dashboard/DatePickerView',
  'models/metrics/MetricQueryModel',
  'text!templates/main.html'
], function($, _, Backbone, DashboardView, DatePickerView, MetricQueryModel, mainTemplate) {

  var MainView = Backbone.View.extend({
  
    el: $('#main'),
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      this.model = new MetricQueryModel();
      this.datePickerView = new DatePickerView();
      this.dashboardView = new DashboardView();
      this.datePickerView.on('scan', this._scan);
    },
    
    render: function() {
    
      var compiledTemplate = _.template(mainTemplate);

      this.$el.append(compiledTemplate);
      this.datePickerView.setElement('#datePicker').render();
      this.dashboardView.setElement('#dashboard').render();
    
      // Because tabs can't run till rendered.
      this.dashboardView.onShow();
    },
    
    _scan: function(event) {
      this.dashboardView.update(event);
    }
  });
  
  return MainView;
});
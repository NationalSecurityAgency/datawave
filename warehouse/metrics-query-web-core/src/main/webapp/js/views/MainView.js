define([
  'jquery',
  'underscore',
  'backbone',
  'views/home/DatePickerView',
  'views/home/DashboardView',
  'text!templates/mainTemplate.html'
], function($, _, Backbone, DatePickerView, DashboardView, mainTemplate) {

  var MainView = Backbone.View.extend({
    
    spanInDays: 30,
  
    el: $('#content-container'),
    
    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      // Initializing views
      this.datePickerView = new DatePickerView();      
      this.dashboardView = new DashboardView();
      this.dashboardView.listenTo(this.datePickerView, 'date-picker.scan', this.dashboardView.update);
      this.dashboardView.listenTo(this.datePickerView, 'date-picker.interval.change', this.dashboardView.update);
      
      var beginDate = new Date();
          beginDate = new Date(beginDate.getTime() - (3600000 * 24 * this.spanInDays));
      
      beginDate = this._beginDate ? this._beginDate : beginDate;
      
      var endDate = new Date();
            
      endDate = this._endDate ? this._endDate : endDate;
      
      var beginString = beginDate.getUTCFullYear() +
                        ('0' + (beginDate.getUTCMonth() + 1)).slice(-2) + 
                        ('0' + (beginDate.getUTCDate())).slice(-2);
                        
      var endString = endDate.getUTCFullYear() +
                      ('0' + (endDate.getUTCMonth() + 1)).slice(-2) + 
                      ('0' + (endDate.getUTCDate())).slice(-2);
    },
    
    render: function() {
    
      var compiledTemplate = _.template(mainTemplate);
      
      this.$el.append(compiledTemplate);
      this.$el.find('#datePicker').append(this.datePickerView.render().$el);
      this.$el.find('#dashboard').append(this.dashboardView.render().$el);
      
      this.datePickerView.btnClick();
      
      return this;
    }
  });
  
  return MainView;
});

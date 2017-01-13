define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'text!templates/home/datePickerTemplate.html'
], function($, jQueryUI, _, Backbone, datePickerTemplate) {

  var DatePickerView = Backbone.View.extend({
    
    className: 'span12',
    
    events: {
      "click button.datepicker-scan-btn": "btnClick",
      "change input.datepicker-interval": "btnClick"
    },
    
    initialize: function(options) {
      _.bindAll(this);
      var that = this;
      
      this.id = "datePicker-" + (Math.floor(Math.random() * 900000) + 100000);
    },
    
    render: function() {
    
      if(this.startDatePicker) this.startDatePicker.destroy();
      if(this.endDatePicker) this.endDatePicker.destroy();
    
      var compiledTemplate = _.template(datePickerTemplate, {id: this.id.split('-')[1]});
      
      var a = this._view = $(compiledTemplate);
      
      var b = a.find('#beginDate-' + this.id.split('-')[1]);
      var c = a.find('#endDate-' + this.id.split('-')[1]);
      
      this.startDatePicker = b.datepicker();
      this.endDatePicker = c.datepicker();
      var defaultStart = new Date((new Date()).setHours(0,0,0,0) - 12096e5);
      var defaultEnd   = new Date((new Date()).setHours(23,59,59,999) + 1);
      this.startDatePicker.datepicker('setDate', defaultStart);
      this.endDatePicker.datepicker('setDate', defaultEnd);
      
      this.$el.append(a);
      
      return this;
    },
    
    startDate: function(_) {
        if(!arguments.length) return this.startDatePicker.getDate();
        this._startDatePicker.setDate(_);
        return this.startDatePicker;
    },
    
    endDate: function(_) {
        if(!arguments.length) return this.endDatePicker.getDate();
        this._endDatePicker.setDate(_);
        return this.endDatePicker;
    },
    
    btnClick: function(event) {
      
      var a = this.$el;
      var b = a.find('.begin-datepicker').datepicker('getDate');
      var c = a.find('.end-datepicker').datepicker('getDate');
      var d = a.find('.datepicker-interval').is(":checked") ? 36e5 : 864e5;
      
      this.trigger('date-picker.scan', b, c, d );
    }
  });
  
  return DatePickerView;
});
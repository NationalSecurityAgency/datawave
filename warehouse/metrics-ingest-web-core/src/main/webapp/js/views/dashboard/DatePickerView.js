define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'models/metrics/MetricQueryModel',
  'text!templates/dashboard/datePickerTemplate.html'
], function($, jQueryUI, _, Backbone, MetricQueryModel, datePickerTemplate) {

  var DatePickerView = Backbone.View.extend({
    
    className: 'datePicker',
    
    initialize: function(options) {
      this.id = "datePicker-" + (Math.floor(Math.random() * 900000) + 100000);
    },
    
    events: {
      'click .datepicker-scan-btn': 'scan'
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
      
      // this.ingestRadios = a.find('input:radio[name=ingestType]');
      
      this.$el.append(a);
      
      // this.$el.find()
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
    
    ingestType: function(_) {
        if(!arguments.length) return this.ingestRadios.filter(':checked').val();
        this.ingestRadios.filter('[value=' + _ + ']').prop('checked', true);
        return this.ingestRadios;
    },
    
    _selectStartDate: function(dateText, obj) {
      this.model.startDate = obj.getDate();
    },
    
    _selectEndDate: function(dateText, obj) {
      this.model.endDate = obj.getDate();
    },
    
    scan: function(event) {
      var model = new MetricQueryModel();
      
      var a = this.$el;
      var b = a.find('.begin-datepicker');
      var c = a.find('.end-datepicker');
      var d = a.find('input:radio[name=ingestType]:checked');
      
      model.start = b.datepicker('getDate');
      model.end = c.datepicker('getDate');
      model.ingestType = d.val() || 'bulk';
      
      console.log(event);
      console.log(model); 
      
      this.trigger('scan', model);
    }
  });
  
  return DatePickerView;
});
define([
  'jquery',
  'jqueryui',
  'underscore',
  'backbone',
  'datatables',
  'collections/metrics/user/UserQueryMetricSummaryCollection',
  'text!templates/metrics/summary/queryUserSummaryViewTemplate.html'
], function($, jQueryUI, _, Backbone, Datatables, UserQueryMetricSummaryCollection, queryUserSummaryTemplate) {

  var QueryUserSummary = Backbone.View.extend({
    
    className: 'metrics-container',
    
    spanInDays: 30,

    initialize: function() {
      _.bindAll(this);
      var that = this;
      
      // Initializing views
      
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

      this.collection2 = new UserQueryMetricSummaryCollection();
      this.collection2.fetch({data: {begin: beginString, end: endString}, success: this._success});
      
      this._id = (Math.floor(Math.random() * 900000) + 100000);
    },
    
    render: function() {
      
      var el = this.$el;
    
      var compiledTemplate = _.template(queryUserSummaryTemplate);
      el.append(compiledTemplate);
      
      this._renderTable();
      
      return this;
    },
    
    _renderTable: function() {
      
      var thatElement = this.$el;
      
      if (this._table) {
        this._table.fnClearTable();
        this._update();
      } else {
        var table = thatElement.find('.query-user-summary-table');
        
        if(table.length > 0) {
          this._table = table.dataTable();
          this._update();
        }
      }
      
      return this;
    },
    
    metrics: function(_) {
      if(!arguments.length) return this._metrics;
      this._metrics = _;
      this._renderTable();
      return this._metrics;
    },
    
    _success: function(collection,response,options) {
      console.log(collection);
      this.metrics(collection);
    },
    
    _update: function() {
      
      if (this._metrics && this._table) {
        this._metrics.each(function(metric) {
          
          var rowData = [];
          
          rowData.push(metric.get('User'));
          rowData.push(metric.get('All').MaximumPageResultSize);
          rowData.push(Math.floor(metric.get('All').TotalPageResultSize / metric.get('All').TotalPages));
          
          if(_.isNaN(rowData[2])) {
            console.log(metric);
          }
          
          this._table.fnAddData(rowData, false);
        }, this);
        
        this._table.fnDraw();
      }
            
      return this;
    },
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'column'
      },
      title: { 
        text: 'Query Count'
      },
      plotOptions: {
      },
      xAxis: {
        categories: []
      },
      yAxis: {
      },
      series: [
        {
          name: 'Maxi Page Size',
          data: []
        },
        {
          name: 'Min Page Size',
          data: []
        },
        {
          name: 'Avg Page Size',
          data: []
        }
      ],
      credits: {
        enabled: false
      },
      legend: {
        align: 'right',
        layout: 'vertical',
        verticalAlign: 'middle',
        enabled: false
      }
    }   
  });
  
  return QueryUserSummary;
});
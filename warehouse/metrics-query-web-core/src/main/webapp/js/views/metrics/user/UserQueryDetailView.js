define([
  'jquery',
  'underscore',
  'backbone',
  'datatables',
  'text!templates/metrics/metricsContainerTemplate.html',
  'text!templates/user/userQueryDetailTemplate.html',
  'text!templates/user/queryDetailModalTemplate.html',
], function($, _, Backbone, Datatables, metricsTemplate, userQueryDetailTemplate, modalTemplate) {

  var UserQueryCountView = Backbone.View.extend({
    
    tagName: 'div',
    
    className: 'metrics-container',
    
    events: {
      "click .query-detail-modal-btn": "detailBtnClick"
    },

    initialize: function(options) {
      _.bindAll(this);
      var that = this;
    },
    
    render: function() {
      
      var el = this.$el;
    
      var compiledTemplate = _.template(metricsTemplate, {loadingMsg: 'Retrieving metrics data...'});;
      var tableTemplate = _.template(userQueryDetailTemplate);
      
      el.append(compiledTemplate);
      el.find('.metrics-view-container').append(tableTemplate)
      
      this._renderTable();
     
      return this;
    },
    
    _renderTable: function() {
      
      var thatElement = this.$el;
      
      if(this._table) {
        this._table.fnClearTable();
      }
      else {
        var table = thatElement.find('.user-query-detail-table');
        this._table = table.dataTable({"sScrollX": "100%",
                                       "bAutoWidth": false,
                                       "aoColumnDefs":[{
                                         "aTargets": [0],
                                         "bSortable": false
                                       }, {
                                         "bVisible": false,
                                         "bSearchable": false,
                                         "aTargets": [4,5,6,7,8]
                                       }] });
      }
      
      var tableData = [];
      
      if (this._metrics) {
        this._metrics.each(function(metric) {
          
          var rowData = [];
          rowData.push('<img class="query-detail-modal-btn" src="imgs/add.png"/>');
          _.each(['createDate', 'queryId', 'justification',
                  'queryName', 'queryAuthorizations',
                  'queryLogic', 'query'], function(attr) {
            
            var val = '';
            
            if(attr === 'createDate') {
              val = new Date(parseInt(metric.get(attr)));
              val = + val.getFullYear() + '-' + (val.getMonth()+1) + '-' + val.getDate() + ' ' +
                    val.getHours() + ':' + val.getMinutes() + ':' + val.getSeconds();
            } else {
              val = metric.get(attr);
            }
            
            val ? rowData.push(val) : rowData.push("");
          }, this);
          
          tableData.push(rowData);
        }, this);
        this._table.fnAddData(tableData);
      }
      
      this._show();
      
      return this;
    },
    
    metrics: function(_) {
      if(!arguments.length) return this._metrics;
      this._metrics = _;
      this._update(this._metrics);
      return this._metrics;
    },
    
    _update: function(collection) {
      // console.log(collection);
      
      return this._renderTable();
    },
    
    detailBtnClick: function(event) {
      console.log(event);
      console.log($(event.currentTarget).closest('tr'));
      console.log({query: this._table.fnGetData($(event.currentTarget).closest('tr')[0])});
      
      var el = this.$el.find('.user-query-detail-modal');
    
      var compiledTemplate = _.template(modalTemplate, {query: this._table.fnGetData($(event.currentTarget).closest('tr')[0])});
      el.html(compiledTemplate);
      
      $(el[0]).modal();
    },
    
    _loading: function() {
      this.$el.find('.metrics-view-container').addClass('hide');
      this.$el.find('.metrics-container-error').addClass('hide');
      this.$el.find('.metrics-container-loading').removeClass('hide');
      this.$el.find('.metrics-empty-container').addClass('hide');
    },
    
    _error: function() {
      this.$el.find('.metrics-view-container').addClass('hide');
      this.$el.find('.metrics-container-error').removeClass('hide');
      this.$el.find('.metrics-container-loading').addClass('hide');
      this.$el.find('.metrics-empty-container').addClass('hide');
    },
    
    _show: function() {
      this.$el.find('.metrics-view-container').removeClass('hide');
      this.$el.find('.metrics-container-error').addClass('hide');
      this.$el.find('.metrics-container-loading').addClass('hide');
      this.$el.find('.metrics-empty-container').addClass('hide');
    },
    
    _hide: function() {
      this.$el.find('.metrics-view-container').addClass('hide');
      this.$el.find('.metrics-container-error').addClass('hide');
      this.$el.find('.metrics-container-loading').addClass('hide');
      this.$el.find('.metrics-empty-container').removeClass('hide');
    },
  });
  
  return UserQueryCountView;
});

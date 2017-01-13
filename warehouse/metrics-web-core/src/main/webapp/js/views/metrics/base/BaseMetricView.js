define([
  'jquery',
  'underscore',
  'backbone',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, metricsTemplate) {

  var BaseMetricView = Backbone.View.extend({
    
    tagName: 'div',
    
    className: 'span4',
    
    initialize: function(options) {
      _.bindAll(this);
      this.label = {};
      this.chart = null;
    },
    
    render: function() {
      
      this.$el.empty();
      
      var template = _.template(metricsTemplate, {loadingMsg: "Retrieving user metrics..."});
      
      this.$el.append(template);
      this._show();      
      this._renderChart();
     
      return this;
    },
    
    _renderChart: function() {
      
      var that = this;
      var thatElement = this.$el.find('.metrics-view-container');
      
      if ($(thatElement[0]).hasClass('hide'))
        return this;
                  
      if (this.chart) {
        this.chart.destroy();
        this.chart = null;
      }
      
      var options = {};
      
      options = $.extend(true, options, this.defaultChartOptions, this.chartOptions,{ chart: {
                                                                                        events: {
                                                                                          "redraw": function() {
                                                                                            that._updateLabelBox(this._formatLabel);
                                                                                          }
                                                                                        }
                                                                                      }
                                                                                    });
      $(thatElement[0]).highcharts('StockChart', options);
      this.chart = $(thatElement[0]).highcharts();
      
      return this;
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
    
    _updateLabelBox: function() {
      
      var formatLabelFn = arguments.length > 0 ? arguments[0] : null;
      var lastNonZero = arguments.length > 1 ? arguments[1] : this.lastNonZero;
      
      if(this.chart) {
        var point = null;
        
        var series = this.chart.series[0];
        
        if(_.isObject(series)) {
          if(lastNonZero) {
            var nonZeros = _.filter(series.data, function(val) {
                           return val.y > 0;
                         }, this);
            point = _.last(nonZeros); 
          } else {
            point = _.last(series.data);
          }
        }
                                   
        if (this.label.path)   { this.label.path.destroy();   this.label.path   = null ; }
        if (this.label.marker) { this.label.marker.destroy(); this.label.marker = null ; }
        if (this.label.text)   { this.label.text.destroy();   this.label.text   = null ; }
        if (this.label.rect)   { this.label.rect.destroy();   this.label.rect   = null ; }
        
        if (point) {                                           
          var xPixel = this.chart.xAxis[0].toPixels(point.x);
          var yPixel = this.chart.yAxis[0].toPixels(point.y)
          
          if(yPixel > 0) {
            var text = _.isFunction(this._formatLabel) ? this._formatLabel(point.y) : point.y ;
            
            this.label.text = this.chart.renderer.text(text.toString(), this.chart.plotLeft + 50, this.chart.plotTop + 50).attr({zIndex: 9}).css({fontSize: '20px'}).add();
            var box = this.label.text.getBBox();
            this.label.rect = this.chart.renderer.rect(box.x - 5, box.y - 5, box.width + 10, box.height + 10, 5);
            this.label.rect = this.label.rect.attr({ fill: '#FCFFC5',
                                                     stroke: '#55BF3B',
                                                     'stroke-width': 2,
                                                     zIndex: 8
                                                   }).add();
        
            if(this.label.text && this.label.rect) {
              var size  = this.chart.series[0].data.length;
              var point = _.last(this.chart.series[0].data);
              box = this.label.rect.getBBox();
              this.label.path = this.chart.renderer.path(['M', box.x + box.width - 0, box.y + (box.height/2), 'L',  xPixel , yPixel]).attr({ stroke: '#55BF3B',
                                                              'stroke-width': 2,
                                                              zIndex: 7
                                                            }).add();
         
              this.label.marker = this.chart.renderer.circle(xPixel,
                                                             yPixel,
                                                             5 ).attr({
                                                               fill: '#55BF3B',
                                                               stroke: '#55BF3B',
                                                               'stroke-width': 0
                                                             }).add();                                          
            } 
          }
        }
      }
    },
    
    _formatLabel: function(value) {
      
      var arr = ['B', 'KiB', 'MiB', 'GiB'];
      
      var val = value;
      
      for(var i = 0 ; val > 1000 && arr.length > i ; i++ ) {
        val = val / 1000;
      }
      
      return (val != 0 ? (Math.round(val * 100)/100) : 0).toString() + " " + arr[Math.min(i,arr.length-1)] + "/s";
    },
    
    defaultChartOptions: {
      navigator: {
        adaptToUpdateData: false
      },
      xAxis: {
        type: 'datetime',
        labels: {
          enabled: false
        },
        gridLineWidth: 0,
        minRange: 1000 * 60
      },
      yAxis: {
        min: 0,
        minorTickInterval: null,
        minRange: 1
      },
      rangeSelector: {
        buttons: [{
          type: 'minute',
          count: 1,
          text: '1m'
        },
        {
          type: 'minute',
          count: 5,
          text: '5m'
        }
        ,
        {
          type: 'minute',
          count: 30,
          text: '30m'
        },
        {
          type: 'hour',
          count: 1,
          text: '1h'
        },
        {
          type: 'hour',
          count: 6,
          text: '6h'
        },
        {
          type: 'hour',
          count: 12,
          text: '12h'
        },
        {
          type: 'all',
          text: 'All'
        }],
        selected: 1
      },
      tooltip: {
        xDateFormat: '%Y-%m-%d %H:%M:%S'
      },
      credits: {
        enabled: false
      },
      legend: {
        enabled: false,
      }
    }
  });
  
  return BaseMetricView;
});

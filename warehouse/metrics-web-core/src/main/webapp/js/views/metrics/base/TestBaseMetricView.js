define([
  'jquery',
  'underscore',
  'backbone',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, metricsTemplate) {

  var TestBaseMetricView = Backbone.View.extend({
    
    tagName: 'div',
    
    className: 'span4',
    
    label: {},
    
    render: function() {
      
      this.$el.empty();
      
      var template = _.template(metricsTemplate, {loadingMsg: "Retrieving user metrics..."});
      
      this.$el.append(template);
      this._show();      
      this._renderChart();
      this.$el.find('.add-point-btn.button').click(this._addPoint);
     
      return this;
    },
    
    _addPoint: function() {
      var seen = [];
      JSON.stringify(this.chart, function(key, val) {
          if (typeof val == "object") {
            if (seen.indexOf(val) >= 0)
              return "";
              
            if(_.isArray(val) && key == "data")
              console.log("data: " + val.length);
              
            if(key == 'series'  && _.isArray(val) && _.has(val[0], 'data')) {
              _.each(val, function(series) {
                console.log("series.data: " + series.data.length);
              }, this);
            }
              
            seen.push(val);
          }
          return val;
      });
      this.queryModel.fetch({success: this.queryModel.onFetch, error: this.queryModel.onFail});
    },
    
    _renderChart: function() {
      
      var that = this;
      var thatElement = this.$el.find('.metrics-view-container').find('div');
      
      if ($(thatElement[0]).hasClass('hide'))
        return this;
                  
      if (this.chart)
        this.chart.destroy();
      
      var options = this.chartOptions;
      
      $.extend(options, { chart: {
                            events: {
                              "redraw": function() {
                                that._updateLabelBox();
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
      
      var text = arguments.length > 0 ? arguments[0] : null;
      
      if(this.chart) {
        if (this.label.path) { this.label.path.destroy(); this.label.path = null }
        if (this.label.marker) {this.label.marker.destroy(); this.label.marker = null };
        
        if(text) {
          if (this.label.text) { this.label.text.destroy(); this.label.text = null ; }
          if (this.label.rect) { this.label.rect.destroy(); this.label.rect = null ; }
          
          this.label.text = this.chart.renderer.text(text.toString().concat(" B/s"), this.chart.plotLeft + 50, this.chart.plotTop + 50).attr({zIndex: 5}).css({fontSize: '20px'}).add();
          var box = this.label.text.getBBox();
          this.label.rect = this.chart.renderer.rect(box.x - 5, box.y - 5, box.width + 10, box.height + 10, 5);
          this.label.rect = this.label.rect.attr({ fill: '#FCFFC5',
                                                   stroke: '#628E01',
                                                   'stroke-width': 2,
                                                   zIndex: 4
                                                 }).add();
        }
        
        if(this.label.text && this.label.rect) {
          var size  = this.chart.series[0].data.length;
          // var point = this.chart.series[0].data[size-1];
          var point = _.last(this.chart.series[0].data);
          box = this.label.rect.getBBox();
          // point.update({marker: {enabled: true, fillColor: '#628E01'}}, false);
          this.label.path = this.chart.renderer.path(['M', box.x + box.width - 0, box.y + (box.height/2), 'L',  this.chart.xAxis[0].toPixels(point.x) , this.chart.yAxis[0].toPixels(point.y)]).attr({ stroke: '#628E01',
                                                          'stroke-width': 2,
                                                          zIndex: 7
                                                        }).add();
         
          this.label.marker = this.chart.renderer.circle(this.chart.xAxis[0].toPixels(point.x),
                                                         this.chart.yAxis[0].toPixels(point.y),
                                                         5 ).attr({
                                                           fill: '#628E01',
                                                           stroke: '#628E01',
                                                           'stroke-width': 0
                                                         }).add();                                          
        }
      }
    },
    
    defaultChartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'line'
      },
      xAxis: {
        type: 'datetime',
        labels: {
          enabled: false
        },
        gridLineWidth: 0
      },
      yAxis: {
        min: 0,
        minorTickInterval: null
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
  
  return TestBaseMetricView;
});
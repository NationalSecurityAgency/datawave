define([
  'jquery',
  'underscore',
  'backbone',
  'collections/metrics/user/UserQueryMetricCollection',
  'text!templates/metrics/metricsContainerTemplate.html'
], function($, _, Backbone, UserQueryMetricCollection, metricsTemplate) {

  var UserQueryTimeView = Backbone.View.extend({
    
    tagName: 'div',
    
    className: '',
    
    _interval: 864e5,

    initialize: function(options) {
      _.bindAll(this);
      var that = this;
    },
    
    render: function() {
      
      this.$el.empty();
      
      var template = _.template(metricsTemplate, {loadingMsg: "Retrieving user metrics..."});
      
      this.$el.append(template);
      this.$el.find('.metrics-container-loading').addClass('hide');
      this.$el.find('.metrics-empty-container').removeClass('hide');
      
      if (this._metrics)
        this._renderChart();
     
      return this;
    },
    
    _renderChart: function() {
      
       var thatElement = this.$el.find('.metrics-view-container');
      
      if ($(thatElement[0]).hasClass('hide'))
        return this;
                  
      if (this.chart)
        this.chart.destroy();
      
      var options     = this.chartOptions;
      
      console.log(options); 
      
      options.chart.renderTo = thatElement[0];
      options.chart.width    = thatElement.parent().width();
      options.chart.height   = thatElement.parent().height();
      
      this.chart = new Highcharts.Chart(options);
      
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

    metrics: function(_) {
      if(!arguments.length) return this._metrics;
      this._metrics = _;
      this._update(this._metrics, this._interval);
      return this._metrics;
    },
    
    interval: function(_) {
      if(!arguments.length) return this._interval;
      this._interval = _;
      if (arguments.length > 2 && arguments[1]) this._update(this._metrics, this._interval);
      return this._interval;
    },
    
    _update: function(collection, interval) {
      
      var dInterval = arguments.length > 1 ? interval : 864e5; 
      
      var coptions = this.chartOptions;
      
      // Reset chart
      
      _.each(coptions.series, function(d,i) {
        d.data.length = 0;
      }, this);
      // end reset
      
      var lseries = [{},{},{},{}];
      
      if (collection) {
        collection.each(function(d, i) {
          var date = d.get('createDate') - ( d.get('createDate') % dInterval );        

          _.each(lseries, function(hash, x) {
            if(!hash.hasOwnProperty(date)) {
              hash[date] = 0;
            }
          }, this);
          
          _.each(d.get('pageTimes'), function(p) {
            lseries[0][date] = Math.max(lseries[0][date],p.returnTime);  // Maximum Page Response Time
            lseries[1][date] = Math.min(lseries[1][date],p.returnTime);  // Minimum Page Response Time
            lseries[2][date] += p.returnTime;                            // Total Page Response Time
            lseries[3][date]++                                           // Total Pages
          }, this);
          
        });
        
        console.log(lseries);
      
        for(var key in lseries[0]) {
          if(lseries[0].hasOwnProperty(key)) {
            coptions.series[0].data.push([parseInt(key), lseries[0][key]]);
            coptions.series[1].data.push([parseInt(key), lseries[1][key]]);
            if(lseries[3][key] > 0 ) coptions.series[2].data.push([parseInt(key), Math.floor( lseries[2][key] / lseries[3][key])]);
          }
        }
        
        coptions.tooltip.xDateFormat = (dInterval % 864e5) == 0 ? '%Y-%m-%d' : '%Y-%m-%d %H:%M';
        coptions.title.text = 'Performance for User: ' + collection._user;
        
        _.each(coptions.series, function(series) {
          series.data = _.sortBy(series.data, function(d) {
            return d[0];
          }, this) ;
        }, this);
        
        this._show();
      } else {
        this._hide();
      }

      this.$el.find('.metrics-view-container').removeClass('hide');
      this.$el.find('.metrics-container-error').addClass('hide');
      this.$el.find('.metrics-container-loading').addClass('hide');
      this.$el.find('.metrics-empty-container').addClass('hide');
      
      return this._renderChart();
    },
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'areaspline'
      },
      title: { 
        text: 'Performance'
      },
      xAxis: {
        // maxPadding: 0.25,
        // minRange: 7 * 24 * 3600000,
        type: 'datetime',
        dateTimeLabelFormats: {
          month: '%e. %b',
          year: '%b'
        }
      },
      yAxis: {
        min: 0,
        minorTickInterval: null,
        title: {
          text: 'Page Response Time (ms)'
        }
      },
      series: [
        {
          pointInterval: this._interval,
          name: 'Max Page Response',
          data: []
        },
        {
          pointInterval: this._interval,
          name: 'Min Page Response',
          data: []
        },
        {
          pointInterval: this._interval,
          name: 'Avg Page Response',
          data: []
        }
      ],
      tooltip: {
        xDateFormat: '%Y-%m-%d %H:%M'
      },
      credits: {
        enabled: false
      },
      legend: {
        align: 'left',
        layout: 'vertical',
        verticalAlign: 'top',
        enabled: true,
        floating: true,
        x: 90,
        y: 45,
        backgroundColor: '#fff'
      }
    }
    
    /***
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'line'
      },
      title: { 
        text: 'Query Count'
      },
      plotOptions: {
        line: {
          
        }
      },
      xAxis: {
        // maxPadding: 0.25,
        type: 'datetime',
        dateTimeLabelFormats: {
          month: '%e. %b',
          year: '%b'
        }
      },
      yAxis: {
        min: 0,
        title: {
          text: '# of Queries'
        }
      },
      series: [{
        // pointInterval: 24 * 3600000,
        name: 'Queries',
        data: []
      }],
      tooltip: {
        xDateFormat: '%Y-%m-%d'
      },
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
    ***/
  });
  
  return UserQueryTimeView;
});
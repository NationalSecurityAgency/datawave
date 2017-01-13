define([
  'jquery',
  'underscore',
  'backbone',
  'highcharts',
  'collections/metrics/ingest/HistoryCollection'
], function($, _, Backbone, Highcharts, HistoryCollection) {

  var HistoryView = Backbone.View.extend({
  
    className: "ingestHistoryPlot metric-chart",
    
    chartOptions: {
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'column'
      },
      title: { 
        text: 'Ingest History'
      },
      xAxis: {
        // categories: [],
        type: 'datetime',
        dateTimeLabelFormats: {
          month: '%e. %b',
          year: '%b'
        }
      },
      yAxis: {
        min: 0,
        title: {
          text: '# of Events'
        }
      },
      plotOptions: {
      },
      series: [
        {
          name: "Events Ingested",
          data: []
        }
      ],
      credits: {
        enabled: false
      },
      tooltip: {
        xDateFormat: '%Y-%m-%d %H:%M'
      },
      legend: {
        enabled: false,
        align: 'right',
        layout: 'vertical',
        verticalAlign: 'middle'
      }
    },
    
    onDataHandler: function(collection, response, options) {
      this._updateChartData(collection, options);
    },
    
    initialize: function(options) {
      
      _.bindAll(this);
      var that = this;
      
      this.collection = new HistoryCollection();
      this.collection.on('reset', that._updateChartData);
      
      // Initialize the collection manually
      if (options && options.start) this.collection.start(options.start);
      if (options && options.end) this.collection.start(options.end);
      if (options && options.ingestType) this.collection.start(options.ingestType);
      
      //this.collection.bind('reset', this._debug);
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
      this.id = "historyView-" + (Math.floor(Math.random() * 900000) + 100000)
    },
    
    render: function() {
    
      if (this.chart)
        this.chart.destroy();
        
      var thatElement = this.$el;
      var options     = this.chartOptions; 
      
      options.chart.renderTo = thatElement[0];
      options.chart.width    = thatElement.parent().width();
      options.chart.height   = thatElement.parent().height();
      
      this.chart = new Highcharts.Chart(options);
      
      return this;
    },
    
    update: function(model) {
      this.collection.start(      model.start.getTime());
      this.collection.end(        model.end.getTime());
      this.collection.ingestType( model.ingestType);
      
      this.collection.fetch({ update: false, success: this.onDataHandler, dataType: 'json'});
    },
    
    _updateChartData: function(collection, options) {
      var data     = collection.toJSON();
      var coptions = this.chartOptions;
       
      coptions.series[0].data = [];
      
      if (collection) {
        var start = collection.start();
        var end   = collection.end();
      
        var mod = (end - start) > 864e5 ? 864e5 : 36e5;
        
        var hash = {} 
        
        collection.each(function(d, i) {
          var date = d.get('0') - ( d.get('0') % mod );
        
          if (!hash[date]) 
            hash[date] =  d.get('1');
          else
            hash[date] += d.get('1');
          
        });
      
        for(var key in hash) {
          if(hash.hasOwnProperty(key)) {
            coptions.series[0].data.push([parseInt(key), hash[key]]);
          }
        }
      }
      
      coptions.series[0].data = _.sortBy(coptions.series[0].data, function(d) {
        return d[0];
      }, this);
      
      coptions.tooltip.xDateFormat = (mod % 864e5) == 0 ? '%Y-%m-%d' : '%Y-%m-%d %H:%M';
      
      this.render();
    }
  });
  
  return HistoryView;
});
Ext.define('HUD.controller.QuerySummaryMetrics', {
	extend: 'Ext.app.Controller',
	
	stores: ['QuerySummaryMetrics'],
	models: ['QuerySummaryMetrics'],
	
	views: [
	        'querysummary.List'
	        ],
	
	init: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the QuerySummaryMetrics controller.  This happens before the Application launch function is called');
		}
		this.control({
			'viewport > panel': {
				render: this.onPanelRendered
			},			
		    'metricsummarylist': {
			    itemclick: this.selectSummary
		    },

		});
	},
	
	onPanelRendered: function() {
		if (window.console && window.console.log) {	
			console.log('The panel was rendered');
		}
	},
	
	selectSummary: function(grid, record) {
		if (window.console && window.console.log) {	
			console.log('querySummaryMetrics :' + record.get('hours'));
		}
	},
	
		
});
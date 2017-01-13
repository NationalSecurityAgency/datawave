Ext.define('HUD.controller.Queries', {
	extend: 'Ext.app.Controller',
	
	stores: ['Queries'],
	models: ['Query'],
	
	views: [
	        'query.List'
	        ],
	
	init: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the Queries controller.  This happens before the Application launch function is called');
		}
		this.control({
			'viewport > panel': {
				render: this.onPanelRendered
			},
		    'querylist': {
			    itemclick: this.selectQuery
		    },

		});
	},
	
	onPanelRendered: function() {
		if (window.console && window.console.log) {	
			console.log('The panel was rendered');
		}
	},
	
	selectQuery: function(grid, record) {
		var pageMetrics = record.pageMetrics();
		this.application.querySelected(record);
	},
		
});
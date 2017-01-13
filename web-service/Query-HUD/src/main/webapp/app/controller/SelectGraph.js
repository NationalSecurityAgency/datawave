Ext.define('HUD.controller.SelectGraph', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.PageMetric',
	
	stores: ['PageMetrics'],
	models: ['PageMetric'],
	
	views: [
	        'querydetail.SelectGraph'
	        ]
});
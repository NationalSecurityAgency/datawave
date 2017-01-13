Ext.define('HUD.controller.PageMetrics', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.PageMetric',
	
	stores: ['PageMetrics'],
	models: ['PageMetric'],
	
	views: [
	        'querydetail.PageMetricsList'
	        ]			
});
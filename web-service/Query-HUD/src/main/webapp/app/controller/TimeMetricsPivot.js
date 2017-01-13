Ext.define('HUD.controller.TimeMetricsPivot', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.TimeMetricPivot',
	
	stores: ['TimeMetricsPivot'],
	models: ['TimeMetricPivot'],
	
	views: [
	        'querydetail.TimeMetricsPivot'
	        ],
});
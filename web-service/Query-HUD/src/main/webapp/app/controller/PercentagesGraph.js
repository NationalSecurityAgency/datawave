Ext.define('HUD.controller.PercentagesGraph', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.Percentages',
	
	stores: ['Percentages'],
	models: ['Percentages'],
	
	views: [
	        'querydetail.PercentagesGraph'
	        ]
});
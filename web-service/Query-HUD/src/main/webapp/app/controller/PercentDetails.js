Ext.define('HUD.controller.PercentDetails', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.PercentDetails',
	
	stores: ['PercentDetail'],
	models: ['PercentDetails'],
	
	views: [
	        'querydetail.PercentDetailsList'
	        ]
});
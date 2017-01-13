Ext.define('HUD.controller.Users', {
	extend: 'Ext.app.Controller',
	require: 'HUD.model.User',
	id: 'Users',
	
	stores: ['Users'],
	models: ['User'],
	
	views: [
	        'user.Select'
	        ]

});
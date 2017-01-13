Ext.define('HUD.store.Users', {
	extend: 'Ext.data.Store',
	model: 'HUD.model.User',
	storeId: 'Users',
	autoLoad: true,
	
	proxy: {
		type: 'ajax',
		url: '/DataWave/Query/queryhud/activeusers',
		reader: {
			type: 'json',
			successProperty: 'success'
		}
	}

});
Ext.define('HUD.store.Queries', {
	extend: 'Ext.data.Store',
	model: 'HUD.model.Query',
	proxy: {
		type: 'rest',
		url: '/DataWave/Query/queryhud/runningqueries',
		reader: {
			type: 'Queries',
			record: 'query'
		}
	}
});

Ext.define('HUD.reader.Queries', {
	extend: 'Ext.data.reader.Json',
	alias: 'reader.Queries',
	read: function(response) {
		var responseText = response.responseText;
		var queryArray = JSON.parse(responseText);
				
		for (var i = 0; i < queryArray.length; i++) {
			var beginDateRaw = queryArray[i]['beginDate'];
			var beginDateObj = new Date(parseInt(beginDateRaw));
			queryArray[i]['beginDate']=beginDateObj;
			queryArray[i]['beginDateRaw']=parseInt(beginDateRaw);
			
			var endDateRaw = queryArray[i]['endDate'];
			var endDateObj = new Date(parseInt(endDateRaw));
			queryArray[i]['endDate']=endDateObj;			
			queryArray[i]['endDateRaw']=parseInt(endDateRaw);			

			var expirationDateRaw = queryArray[i]['expirationDate'];
			var expirationDateObj = new Date(parseInt(expirationDateRaw));
			queryArray[i]['expirationDate']=expirationDateObj;	
			queryArray[i]['expirationDateRaw']=parseInt(expirationDateRaw);	
			
			var createDateRaw = queryArray[i]['createDate'];
			var createDateObj = new Date(parseInt(createDateRaw));
			queryArray[i]['createDate']=createDateObj;	
			queryArray[i]['createDateRaw']=parseInt(createDateRaw);	
			
			var lastUpdatedRaw = queryArray[i]['lastUpdated'];
			var lastUpdatedObj = new Date(parseInt(lastUpdatedRaw));
			queryArray[i]['lastUpdated']=lastUpdatedObj;			
			queryArray[i]['lastUpdatedRaw']=parseInt(lastUpdatedRaw);			
		}
		
		var convertedRecords = JSON.stringify(queryArray.map(function(el) {
			return { query: el };
		}));
		response.responseText = convertedRecords;
		return this.callParent([response]);
	}
});
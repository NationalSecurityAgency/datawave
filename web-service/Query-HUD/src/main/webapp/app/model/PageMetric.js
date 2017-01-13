Ext.define('HUD.model.PageMetric', {
	extend: 'Ext.data.Model',
	belongsTo: [{
		name: 'pageMetrics',
		model: 'HUD.model.Query',
		associationKey: 'pageMetrics'
	}],
	fields: [
	         'pagesize',
	         'returnTime',
	         'callTime',
	         'serializationTime',
	         'bytesWritten',
	         'pageRequested',
	         'pageReturned',
	         'pageNumber',
	         'workingTime',
	         'waitingTime'
	         ]	         
});
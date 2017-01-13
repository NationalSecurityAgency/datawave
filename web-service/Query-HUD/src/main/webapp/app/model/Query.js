Ext.define('HUD.model.Query', {
	extend: 'Ext.data.Model',
	id: 'QueryModel',
	hasMany: [
	          { 
	        	  name: "pageMetrics", 
	        	  model: "HUD.model.PageMetric",
	        	  associationKey: "pageMetrics"
	           }
	          ],
	fields: [
	         'queryName',
	         'id',
	         'userDN',
	         'beginDate',
	         'beginDateRaw',
	         'endDate',
	         'endDateRaw',
	         'expirationDate',
	         'expirationDateRaw',
	         'queryLogicName',
	         'userid',
	         'systemFrom',
	         'createDate',
	         'createDateRaw',
	         'lastUpdated',
	         'lastUpdatedRaw',
	         'numPages',
	         'numResults',
	         'lifeCycle',
	         ]
});


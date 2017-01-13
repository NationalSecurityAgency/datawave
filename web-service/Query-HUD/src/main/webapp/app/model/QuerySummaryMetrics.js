Ext.define('HUD.model.QuerySummaryMetrics', {
	extend: 'Ext.data.Model',
	idProperty: 'hours',
	fields: [
	         'hours',
	         'queryCount',
	         'minPageResponseTime',
	         'maxPageResponseTime',
	         'totalPageResponseTime',
	         'totalPages',
	         'minPageResultSize',
	         'maxPageResultSize',
	         'totalPageResultSize',
	         ],
 	proxy: {
		type: 'rest',
		format: 'json',
		url: '/DataWave/Query/queryhud/summaryall',
	}
});

/**
 * Constants and utility methods for testing
 */
 
USER_ID = "user00";
USER_DN  = "cn\u003dzero user zero user00, ou\u003d0000, ou\u003dabc, ou\u003ddef, o\u003dghijk, c\u003dyz";

SMALL_QUERY_ID   = "13383f57-45dc-4709-934a-363117e7c473";
SMALL_QUERY_NAME = "test small query";

SINGLE_USER_DATA = '[{"userid": "' + USER_ID + '"}]';
ADMIN_USER_DATA  = '[{"userid": "' + USER_ID + '"}, {"userid": "user01"}, {"userid": "user02"}]';

/** Need to make at least query be within the current hour for some tests to work */
var start = new Date().getTime();
var numPages = 10;
var pageStart = [];
var pageEnd   = [];

for (var i=0; i < numPages; i++) {
	if (i == 0) {
		pageStart[i] = start;
		pageEnd[i] = pageStart[i] + 395;	// time working for first page is relatively large	
	} else {
		pageStart[i] = pageEnd[i-1] + getRandomInt(500, 25000);	// time waiting for rest of pages is larger than time working	
		pageEnd[i] = pageStart[i] + 5 + i;		// time working for rest of pages is small
	}
}

SMALL_QUERY_DATA = "[{\"queryLogicName\":\"EventQuery\",\"id\":\"" + SMALL_QUERY_ID + "\",\"queryName\":\"" + SMALL_QUERY_NAME + "\",\"userDN\":\"" + USER_DN + "\",\"query\":\"FIELD1:SOMEVALUE and FIELD2:SOMEOTHERVALUE\",\"columnVisibility\":\"FOO\",\"beginDate\":" + pageStart[0] + ",\"endDate\":" + pageEnd[numPages-1] + ",\"queryAuthorizations\":\"A,B,C,D,E,F,G\",\"expirationDate\":1420070399999,\"userid\":\"" + USER_ID + "\",\"numPages\":10,\"results\":1000,\"lifeCycle\":\"RESULTS\",\"pageMetrics\":" +
                   "[{\"pagesize\":10,\"returnTime\":395,\"callTime\":587,\"serializationTime\":3,\"bytesWritten\":82735,\"pageRequested\":" + pageStart[0] +",\"pageReturned\":" + pageEnd[0] + ",\"pageNumber\":1}," +
                   " {\"pagesize\":10,\"returnTime\":6,\"callTime\":18,\"serializationTime\":3,\"bytesWritten\":82718,\"pageRequested\":pageStart[1],\"pageReturned\":pageEnd[1],\"pageNumber\":2}," +
                   " {\"pagesize\":10,\"returnTime\":6,\"callTime\":15,\"serializationTime\":3,\"bytesWritten\":80752,\"pageRequested\":pageStart[2],\"pageReturned\":pageEnd[2],\"pageNumber\":3}," +
                   " {\"pagesize\":10,\"returnTime\":4,\"callTime\":14,\"serializationTime\":2,\"bytesWritten\":62654,\"pageRequested\":pageStart[3],\"pageReturned\":pageEnd[3],\"pageNumber\":4}," +
                   " {\"pagesize\":10,\"returnTime\":5,\"callTime\":13,\"serializationTime\":2,\"bytesWritten\":62651,\"pageRequested\":pageStart[4],\"pageReturned\":pageEnd[4],\"pageNumber\":5}," +
                   " {\"pagesize\":10,\"returnTime\":4,\"callTime\":13,\"serializationTime\":3,\"bytesWritten\":62641,\"pageRequested\":pageStart[5],\"pageReturned\":pageEnd[5],\"pageNumber\":6}," +
                   " {\"pagesize\":10,\"returnTime\":4,\"callTime\":13,\"serializationTime\":3,\"bytesWritten\":62651,\"pageRequested\":pageStart[6],\"pageReturned\":pageEnd[6],\"pageNumber\":7}," +
                   " {\"pagesize\":10,\"returnTime\":5,\"callTime\":13,\"serializationTime\":2,\"bytesWritten\":62651,\"pageRequested\":pageStart[7],\"pageReturned\":pageEnd[7],\"pageNumber\":8}," +
                   " {\"pagesize\":10,\"returnTime\":5,\"callTime\":14,\"serializationTime\":3,\"bytesWritten\":62647,\"pageRequested\":pageStart[8],\"pageReturned\":pageEnd[8],\"pageNumber\":9}," +
                   " {\"pagesize\":10,\"returnTime\":5,\"callTime\":19,\"serializationTime\":2,\"bytesWritten\":62649,\"pageRequested\":pageStart[9],\"pageReturned\":pageEnd[9],\"pageNumber\":10}]}]";
SMALL_QUERY_RESPONSE = "{\"responseText\": " + SMALL_QUERY_DATA + "}";

/** test data in same format as DataWave/Query/queryhud/summaryall */
SUMMARY_ALL_DATA = "[{\"hours\":1, \"queryCount\":1,  \"minPageResponseTime\":0,\"totalPageResponseTime\":0,\"totalPages\":10,  \"maxPageResponseTime\":9, \"minPageResultSize\":10,\"totalPageResultSize\":100,  \"maxPageResultSize\":100}, " +
					"{\"hours\":6, \"queryCount\":4,  \"minPageResponseTime\":0,\"totalPageResponseTime\":0,\"totalPages\":40,  \"maxPageResponseTime\":13,\"minPageResultSize\":10,\"totalPageResultSize\":400,  \"maxPageResultSize\":100}, " +
					"{\"hours\":12,\"queryCount\":20, \"minPageResponseTime\":0,\"totalPageResponseTime\":0,\"totalPages\":200, \"maxPageResponseTime\":18,\"minPageResultSize\":10,\"totalPageResultSize\":2000, \"maxPageResultSize\":100}, " +
					"{\"hours\":24,\"queryCount\":100,\"minPageResponseTime\":0,\"totalPageResponseTime\":0,\"totalPages\":1000,\"maxPageResponseTime\":18,\"minPageResultSize\":10,\"totalPageResultSize\":10000,\"maxPageResultSize\":100}]";

function createComponentTestArea () {
	if (Ext.get("componentTestArea")) {
		Ext.removeNode(Ext.get("componentTestArea").dom);
	}
	Ext.DomHelper.append(Ext.getBody(), "<div id=\"componentTestArea\"></div>");
}


/** 
 * Loads sample data into users store and return userid if only 1 user 
 *
 * params:
 * usersStore - HUD.store.Users object
 * userData   - string with user json data, such as SINGLE_USER_DATA
 *
 * return:
 * userid - userid if one user in userData, null if multiple users in userData 
 */
function loadTestUserData (usersStore, userData) {
	var jsonData = Ext.decode(userData);
	usersStore.loadRawData(jsonData);	
				
	if (usersStore.getCount() == 1) {
   		var record = usersStore.data.items[0];
   		return record.get("userid");
   	} else {
   		return null;
   	}
}

/** 
 * Loads sample data into queries store.
 * NOTE: because queriesStore has a custom json reader AND the model has a sub-object, 
 *       data needs to be loaded using a reader
 *
 * params:
 * queriesStore - HUD.store.Queries object
 * queriesData  - string with queries json data, such as SMALL_QUERY_DATA
 */
function loadTestQueriesData (queriesStore, queriesData) {
	var jsonData = Ext.decode(queriesData);
	var queriesRecords = convertJsonToQuery(jsonData);

	for (var i = 0; i < queriesRecords.count; i++) {
		// simulate calculations done in queries store reader
		var beginDateRaw = queriesRecords.records[i].get("beginDate");
		var beginDateObj = new Date(parseInt(beginDateRaw));
		queriesRecords.records[i].set('beginDate', beginDateObj);
		queriesRecords.records[i].set('beginDateRaw', parseInt(beginDateRaw));

		var endDateRaw = queriesRecords.records[i].get("endDate");
		var endDateObj = new Date(parseInt(endDateRaw));
		queriesRecords.records[i].set('endDate', endDateObj);
		queriesRecords.records[i].set('endDateRaw', parseInt(endDateRaw));
			
		var expirationDateRaw = queriesRecords.records[i].get("expirationDateRaw");
		var expirationDateObj = new Date(parseInt(expirationDateRaw));
		queriesRecords.records[i].set('expirationDate', expirationDateObj);
		queriesRecords.records[i].set('expirationDateRaw', parseInt(expirationDateRaw));
			
		var createDateRaw = queriesRecords.records[i].get("endDate");
		var createDateObj = new Date(parseInt(createDateRaw));
		queriesRecords.records[i].set('createDate', createDateObj);
		queriesRecords.records[i].set('createDateRaw', parseInt(createDateRaw));
			
		var lastUpdatedRaw = queriesRecords.records[i].get("lastUpdated");
		var lastUpdatedObj = new Date(parseInt(lastUpdatedRaw));
		queriesRecords.records[i].set('lastUpdated', lastUpdatedObj);
		queriesRecords.records[i].set('lastUpdatedObj', parseInt(lastUpdatedRaw));
	
		queriesStore.add(queriesRecords.records[i]);
	}
}

/** 
 * Loads sample data into query summary metrics store  
 *
 * params:
 * querySummaryMetricsStore - HUD.store.QuerySummaryMetrics object
 * summaryMetricsData       - string with query summary metrics json data, such as SUMMARY_ALL_DATA
 */
function loadTestQuerySummaryMetricsData (querySummaryMetricsStore, summaryMetricsData) {
	var jsonData = Ext.decode(summaryMetricsData);
	querySummaryMetricsStore.loadRawData(jsonData);	
}

/**
 * Creates a reader to read and convert specified JSON data into HUD.model.Query object 
 *
 */
function convertJsonToQuery(queriesJson) {
	var reader = Ext.create('Ext.data.reader.Json', {
		model: 'HUD.model.Query'
	});
	
	var resultSet = reader.read(queriesJson);
	
	return resultSet;
}

/** remove all rows from specified store */
function clearData(store) {
	store.removeAll(true);
}

/** generates a random integer between min and max numbers specified */
function getRandomInt(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

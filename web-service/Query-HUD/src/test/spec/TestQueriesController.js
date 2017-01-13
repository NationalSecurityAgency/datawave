describe("ExtJS HUD Application Queries Test Suite", function() {
//	var console       = console || {log: function() {}};

	var queriesView       = null;
	var queriesModel      = null;
	var queriesStore      = null;
	var queriesController = null;
		
	/** setup method to be called before each test case */
	beforeEach (function() {
		// this assumes controller has already been created 
		queriesController = window.TestApplication.getController('Queries');
		queriesStore      = queriesController.getStore('Queries');
		queriesModel      = queriesController.getModel('Query');
		queriesView       = queriesController.getView('query.List');
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		queriesView       = null;
		queriesModel      = null;
		queriesStore      = null;
		queriesController = null;
	});

	/** Test Controller */
	it ('Queries controller should be loaded', function() {	
		expect(queriesController != null).toBeTruthy();
		expect(queriesController).toBeDefined();
	});		

	/** Test Store */
	it ('Queries store should be loaded', function() {
		expect(queriesStore != null).toBeTruthy();
		expect(queriesStore).toBeDefined();
		expect(queriesStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('Queries model should be loaded', function() {
		expect(queriesModel != null).toBeTruthy();
		expect(queriesModel).toBeDefined();
	});

	/** Test View */
	it ('Queries view should be loaded', function() {
		expect(queriesView != null).toBeTruthy();
		expect(queriesView).toBeDefined();
	});
		
	/** Test loading sample data */
	it ('Loaded test Queries data', function() {
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);
		
		expect(queriesStore.getCount()).toBe(1);
		selectedUser = queriesStore.data.items[0].get("userid");
		expect(selectedUser).toBe(USER_ID);
		var beginDate = queriesStore.data.items[0].get("beginDate");
		expect(beginDate != null).toBeTruthy();
		var beginDateRaw = queriesStore.data.items[0].get("beginDateRaw");
		expect(beginDateRaw != null).toBeTruthy();
		expect(queriesStore.data.items[0].get("queryName")).toBe(SMALL_QUERY_NAME);
		expect(queriesStore.data.items[0].get("numPages")).toBe(10);
		expect(queriesStore.data.items[0].pageMetrics().getCount()).toBe(10);
	});
		
	/** Test computing work and wait times */
	it ('Test updateQueryPageTimes', function() {
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);
		
		var selectedQuery = queriesStore.data.items[0];
		expect(selectedQuery.pageMetrics().getAt(0).get("workingTime")).toBe("");
		expect(selectedQuery.pageMetrics().getAt(0).get("waitingTime")).toBe("");

		var updatedQuery = updateQueryPageTimes(selectedQuery);
		
		var page1work = (selectedQuery.pageMetrics().getAt(0).get("pageReturned") - selectedQuery.pageMetrics().getAt(0).get("pageRequested"))/1000;
		expect(updatedQuery.pageMetrics().getAt(0).get("workingTime")).toBe(page1work);
		expect(updatedQuery.pageMetrics().getAt(0).get("waitingTime")).toBe(0);
		
		var page2work = (selectedQuery.pageMetrics().getAt(1).get("pageReturned") - selectedQuery.pageMetrics().getAt(1).get("pageRequested"))/1000;
		var page2wait = (selectedQuery.pageMetrics().getAt(1).get("pageRequested") - selectedQuery.pageMetrics().getAt(0).get("pageReturned"))/1000;
		expect(updatedQuery.pageMetrics().getAt(1).get("workingTime")).toBe(page2work);
		expect(updatedQuery.pageMetrics().getAt(1).get("waitingTime")).toBe(page2wait);
		
	});
});

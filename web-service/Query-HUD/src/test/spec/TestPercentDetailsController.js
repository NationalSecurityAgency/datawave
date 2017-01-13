describe("ExtJS HUD Application PercentDetails Test Suite", function() {
	var percentDetailsView       = null;
	var percentDetailsModel      = null;
	var percentDetailsStore      = null;
	var percentDetailsController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();

		// this assumes controller has already been created 
		percentDetailsController = window.TestApplication.getController('PercentDetails');
		percentDetailsStore      = percentDetailsController.getStore('PercentDetail');
		percentDetailsModel      = percentDetailsController.getModel('PercentDetails');
		percentDetailsView       = Ext.create("HUD.view.querydetail.PercentDetailsList", {renderTo: "componentTestArea"});
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		percentDetailsView       = null;
		percentDetailsModel      = null;
		percentDetailsStore      = null;
		percentDetailsController = null;
	});

	/** Test Controller */
	it ('PercentDetails controller should be loaded', function() {	
		expect(percentDetailsController != null).toBeTruthy();
		expect(percentDetailsController).toBeDefined();
	});		

	/** Test Store */
	it ('PercentDetails store should be loaded', function() {
		expect(percentDetailsStore != null).toBeTruthy();
		expect(percentDetailsStore).toBeDefined();
		expect(percentDetailsStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('PercentDetails model should be loaded', function() {
		expect(percentDetailsModel != null).toBeTruthy();
		expect(percentDetailsModel).toBeDefined();
	});

	/** Test View */
	it ('PercentDetails view should be loaded', function() {
		expect(percentDetailsView != null).toBeTruthy();		
		expect(percentDetailsView).toBeDefined();
	});
		
	/** Test loading data into percent details */
	it ('Test showQueryPercentages', function() {
		// load test queries data
		var querySummaryMetricsController = window.TestApplication.getController('QuerySummaryMetrics');
		var querySummaryMetricsStore      = querySummaryMetricsController.getStore('QuerySummaryMetrics');		
		loadTestQuerySummaryMetricsData(querySummaryMetricsStore, SUMMARY_ALL_DATA);
		
		var queriesController = window.TestApplication.getController('Queries');
		var queriesStore      = queriesController.getStore('Queries');		
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);

		var query = queriesStore.data.items[0];
		
		var computedPercentagesData = computeQueryMetricsPercentages(query.pageMetrics(), querySummaryMetricsStore);
	
		percentDetailsStore.loadRecords(computedPercentagesData);	
		
		expect(percentDetailsStore.getCount()).toBe(3);
		expect(percentDetailsStore.getAt(0).get("metric_type")).toBe("Pages:");
		expect(percentDetailsStore.getAt(0).get("last_hour_total")).toBe(10);
		
		// cleanup
		clearData(querySummaryMetricsStore);
		clearData(queriesStore);
	});
});
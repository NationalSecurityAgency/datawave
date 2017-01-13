describe("ExtJS HUD Application SelectGraph Test Suite", function() {
	var selectGraphView       = null;
	var selectGraphModel      = null;
	var selectGraphStore      = null;
	var selectGraphController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();

		// this assumes controller has already been created 
		selectGraphController = window.TestApplication.getController('SelectGraph');
		selectGraphStore      = selectGraphController.getStore('PageMetrics');
		selectGraphModel      = selectGraphController.getModel('PageMetric');
		selectGraphView       = Ext.create("HUD.view.querydetail.SelectGraph", {renderTo: "componentTestArea"});
//		selectGraphView       = selectGraphController.getView('SelectGraph');
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		selectGraphView       = null;
		selectGraphModel      = null;
		selectGraphStore      = null;
		selectGraphController = null;
	});

	/** Test Controller */
	it ('SelectGraph controller should be loaded', function() {	
		expect(selectGraphController != null).toBeTruthy();
		expect(selectGraphController).toBeDefined();
	});		

	/** Test Store */
	it ('SelectGraph store should be loaded', function() {
		expect(selectGraphStore != null).toBeTruthy();
		expect(selectGraphStore).toBeDefined();
	});

	/** Test Model */
	it ('SelectGraph model should be loaded', function() {
		expect(selectGraphModel != null).toBeTruthy();
		expect(selectGraphModel).toBeDefined();
	});

	/** Test View */
	it ('SelectGraph view should be loaded', function() {
		expect(selectGraphView != null).toBeTruthy();		
		expect(selectGraphView).toBeDefined();
	});
		
	/** Test loading data into query page metrics */
	it ('Test showQueryPageMetrics', function() {
		// load test queries data
		var queriesController = window.TestApplication.getController('Queries');
		var queriesStore      = queriesController.getStore('Queries');		
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);
		
		var query = queriesStore.data.items[0];
		var updatedQuery = updateQueryPageTimes(query);
		
		selectGraphStore.loadRecords(updatedQuery.pageMetrics().data.items);	
		
		expect(selectGraphStore.getCount()).toBe(updatedQuery.pageMetrics().getCount());
		
		// cleanup
		clearData(queriesStore);
	});
});
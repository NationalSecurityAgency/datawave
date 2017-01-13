describe("ExtJS HUD Application General Configuration Test Suite", function() {
	/** Verify Ext is defined with correct version */
	it ('Has loaded ExtJS 4', function() {
		expect(Ext).toBeDefined();
		expect(Ext.getVersion()).toBeTruthy();
		expect(Ext.getVersion().getMajor()).toEqual(4);
	});
	
	/** Verify test application is defined */
	it ('Has loaded test application', function() {
		expect(window.TestApplication).toBeDefined();
		expect(window.TestApplication.name).toEqual("HUD");
		
	});
	
});
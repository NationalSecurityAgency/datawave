/* used to create a query geometry map using leaflet */
/* dependent on leaflet, jquery, and possibly esri-leaflet */
var defaultStyle = {
    color: "#ff00ff",
    fillOpacity: 0.4,
    weight: 2};

var highlightStyle = {
    weight: 5,
    color: "#00ffff"};

var selected;
var layerClicked = false;

$(document).ready(function() {

    var map = L.map('map', {crs: L.CRS.EPSG4326});

    // the first basemap in the list is the default
    Object.values(basemaps)[0].addTo(map);

    // add the basemap selector panel
    L.control.layers(basemaps, null, {position: 'bottomright'}).addTo(map);

    // general map click handler
    map.on('click', function() {

        // do nothing if layer clicked
        if (!layerClicked) {
            // deselect feature and reset style on all features
            selected = undefined;
            geoJson.eachLayer(function(l){geoJson.resetStyle(l);});
            info.update();
        }
        layerClicked = false;
    })

    // setup an info panel to display the feature info
    var info = L.control();
    info.onAdd = function (map) {
        this._div = L.DomUtil.create('div', 'info');
        L.DomEvent.disableClickPropagation(this._div);
        this.update();
        return this._div;
    };

    // used to update the contents of the info panel
    info.update = function (props) {
        this._div.innerHTML = '<h4>Geo Function</h4>' +  (props ? props.function : 'Select a geometry');
    };

    info.addTo(map);

    // add our features to the map
    if (features) {

        // add the geojson features
        var geoJson = L.geoJSON(features, {
        onEachFeature: function(feature, layer) {

            // highlight the feature on mouseover
            layer.on('mouseover', function(e) {

                // display the query function for this feature
                info.update(layer.feature.properties);

                // highlight the feature
                this.setStyle(highlightStyle);
            });

            // remove highlight on mouse out
            layer.on('mouseout', function(e) {

                // either clear the info panel, or set it back to the selected feature
                if (selected) {
                    info.update(selected.properties);
                } else {
                    info.update();
                }

                // clear the highlight unless this is the selected feature
                if (selected !== e.target.feature) {
                    geoJson.resetStyle(this);
                };
            });

            // toggle highlight on selected feature
            layer.on('click', function(e) {

                // used to determine whether click os on/off layer
                layerClicked = true;

                // reset style on all features
                geoJson.eachLayer(function(l){geoJson.resetStyle(l);});

                // add highlight to new selected feature, or unset
                // selected feature if current feature was clicked
                if (selected !== e.target.feature) {
                    selected = e.target.feature;

                    // highlight the selected feature
                    e.target.setStyle(highlightStyle);

                    // display the selected feature's info
                    info.update(selected.properties);

                    // zoom to the feature
                    map.fitBounds(e.target.getBounds());
                } else {
                    selected = undefined;
                }
            });
        },
        // determines how points should be displayed
        pointToLayer: function(feature, latlon) {
            return L.circleMarker(latlon, {
                radius: 1,
                fillOpacity: 1});
        },
        // default style for all features
        style: function(feature) {
            return defaultStyle;
        }}).addTo(map);

        // set initial view to contain all features
        map.fitBounds(geoJson.getBounds());
    }
});
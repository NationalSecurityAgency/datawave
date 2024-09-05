import { GeoJsonProperties, Feature } from 'geojson';
import L, { Layer, Control } from 'leaflet';
import { SimpleMapStore } from './simple-map-store';

let leafletMap: SimpleMapStore;

let selected: Feature | undefined = undefined;
let layerClicked = false;
const info = new Control() as Control & InfoPanel;

interface InfoPanel {
    _div: HTMLElementTagNameMap['div'];
    update(props?: GeoJsonProperties): void;
}

export function addFeatureInfoPanel(leafletMapArg: SimpleMapStore) {
    leafletMap = leafletMapArg;

    // setup an info panel to display the feature info
    info.onAdd = function () {
        this._div = L.DomUtil.create('div', 'info');
        L.DomEvent.disableClickPropagation(this._div);
        this.update();
        return this._div;
    };

    // used to update the contents of the info panel
    info.update = function (props: GeoJsonProperties) {
        this._div.innerHTML = '<h4>Geo Feature</h4>' +  (props ? props.label : 'Select a feature');
    };

    // general map click handler
    (leafletMap.map as L.Map).on('click', function() {

        // do nothing if layer clicked
        if (!layerClicked) {
            // deselect feature and reset style on all features
            if (selected != undefined){
                resetStyle(selected, selected?.properties?.layer);
            }
            selected = undefined;
            info.update();
        }
        layerClicked = false;
    })

    leafletMap.addControl(info);
}

export function onEachFeature(feature: Feature, layer: Layer) {

    const geoJsonLayer = layer as L.GeoJSON;

    // highlight the feature on mouseover
    layer.on('mouseover', function() {

        // display the query function for this feature
        info.update(feature.properties);

        // highlight the feature
        highlightStyle(feature, geoJsonLayer);
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
            resetStyle(feature, geoJsonLayer);
        };
    });

    // toggle highlight on selected feature
    layer.on('click', function(e) {

        // used to determine whether click is on/off layer
        layerClicked = true;

        // reset style on selected feature
        if (selected != undefined && selected.properties != undefined) {
            resetStyle(selected, selected?.properties.layer);
        }

        // add highlight to new selected feature, or unset
        // selected feature if current feature was clicked
        if (selected !== e.target.feature) {
            selected = e.target.feature;

            // highlight the selected feature
            highlightStyle(feature, geoJsonLayer);

            // display the selected feature's info
            if (selected != undefined) {
                info.update(selected.properties);
            }
        } else {
            selected = undefined;
        }
    });
}

function resetStyle(feature: Feature, layer: L.GeoJSON) {
    if (feature.properties != undefined) {
        layer.setStyle({
            color: feature.properties.color,
            fillOpacity: 0.4,
            weight: 2
            });
    }
}

function highlightStyle(feature: Feature, layer: L.GeoJSON) {
    if (feature.properties != undefined) {
        layer.setStyle({
            color: feature.properties.color,
            fillOpacity: 0.4,
            weight: 5
            });
    }
}
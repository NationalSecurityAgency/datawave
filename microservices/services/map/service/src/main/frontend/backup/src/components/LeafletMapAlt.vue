<template>
    <div :id="mapId"></div>
</template>
  
<script lang="js">
import 'leaflet/dist/leaflet.css'
import L from 'leaflet';

export default {
    name: 'LeafletMap',
    data() {
        return {
            mapId: 'leaflet-map',
            mapOptions: {
                center: L.latLng(37.0902, -95.7129),
                zoom: 4,
                zoomControl: true,
                zoomAnimation: false,
                maxBounds: L.latLngBounds(
                    L.latLng(18.91619, -171.791110603),
                    L.latLng(71.3577635769, -66.96466)
                ),
                layers: [],
            },
            geojsonData: null,
            mapInstance: null,
            layerControlInstance: null,
        };
    },
    mounted() {
        console.log("MOUNTED");
        this.initMap();
        // this.fetchData();
    },
    destroyed() {
        console.log("DESTROYED");
        if (this.mapInstance) {
            console.log("REMOVED");
            this.mapInstance.remove();
        }
    },
    methods: {
        initMap() {
            console.log("INIT MAP");

            const leafletMap = L.map(this.mapId, this.mapOptions);

            const tile = L.tileLayer(
                `https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png`,
                {
                    attribution:
                        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                }
            ).addTo(leafletMap);

            this.layerControlInstance = L.control
                .layers({
                    OpenStreetMap: tile,
                })
                .addTo(leafletMap);

            leafletMap.on('zoomstart', () => {
                console.log('ZOOM STARTED');
            });

            this.mapInstance = leafletMap;
        },
        async fetchData() {
            const url = 'https://api.npoint.io/fdbc5b08a7e7eccb6052';

            try {
                const response = await fetch(url);
                const data = await response.json();
                this.geojsonData = data;
            } catch (err) {
                console.log('err', err);
            }
        },
        onEachFeature(feature, layer) {
            if (layer) {
                if (feature.properties && feature.properties.name) {
                    layer.bindPopup(feature.properties.name);
                    layer.on('mouseover', () => {
                        layer.openPopup();
                    });
                    layer.on('mouseout', () => {
                        layer.closePopup();
                    });
                }
            } else {
                console.log('Invalid layer:', feature);
            }
        },
    },
    watch: {
        geojsonData() {
            if (this.geojsonData) {
                try {
                    const geojsonLayer = L.geoJSON(this.geojsonData, {
                        onEachFeature: this.onEachFeature,
                    }).addTo(this.mapInstance);
                    this.layerControlInstance.addOverlay(
                        geojsonLayer,
                        'Maryland geoJSON Layer'
                    );
                    this.mapInstance.fitBounds(geojsonLayer.getBounds());
                } catch (err) {
                    console.log(err, err.message);
                }
            }
        },
    },
};
</script>
  
<style scoped>
/* @import 'leaflet/dist/leaflet.css'; */

#leaflet-map {
    height: 100vh;
    width: 100%;
}
</style>
  
<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
                       xsi:schemaLocation="http://www.opengis.net/sld
                       http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd"
                       xmlns="http://www.opengis.net/sld"
                       xmlns:ogc="http://www.opengis.net/ogc"
                       xmlns:xlink="http://www.w3.org/1999/xlink"
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <UserLayer>
        <Name>KDE Color Map</Name>
        <UserStyle>
            <Name>raster</Name>
            <FeatureTypeStyle>
                <FeatureTypeName>Feature</FeatureTypeName>
                <Rule>
                    <RasterSymbolizer>
                        <Opacity>1</Opacity>
                        <ChannelSelection>
                            <GrayChannel>
                                <SourceChannelName>3</SourceChannelName>
                            </GrayChannel>
                        </ChannelSelection>
                        <ColorMap type="ramp">
                            <ColorMapEntry color="#000000"  quantity="0"        opacity="0.6" />
                            <ColorMapEntry color="#000052"  quantity="0.1"      opacity="0.75" />
                            <ColorMapEntry color="#000075"  quantity="0.3"      opacity="0.8" />
                            <ColorMapEntry color="#380099"  quantity="0.5"      opacity="0.9" />
                            <ColorMapEntry color="#5700AD"  quantity="0.6"      opacity="0.95" />
                            <ColorMapEntry color="#7500BD"  quantity="0.7"      opacity="1" />
                            <ColorMapEntry color="#9A00BD"  quantity="0.8"      opacity="1" />
                            <ColorMapEntry color="#BD00BA"  quantity="0.85"     opacity="1" />
                            <ColorMapEntry color="#C20085"  quantity="0.90"     opacity="1" />
                            <ColorMapEntry color="#C40062"  quantity="0.92"     opacity="1" />
                            <ColorMapEntry color="#D1004D"  quantity="0.93"     opacity="1" />
                            <ColorMapEntry color="#D10031"  quantity="0.94"     opacity="1" />
                            <ColorMapEntry color="#D10000"  quantity="0.95"     opacity="1" />
                            <ColorMapEntry color="#E60F00"  quantity="0.955"    opacity="1" />
                            <ColorMapEntry color="#FF4400"  quantity="0.96"     opacity="1" />
                            <ColorMapEntry color="#FF1B1B"  quantity="0.965"    opacity="1" />
                            <ColorMapEntry color="#F75220"  quantity="0.97"     opacity="1" />
                            <ColorMapEntry color="#FF8112"  quantity="0.975"    opacity="1" />
                            <ColorMapEntry color="#FF9A2D"  quantity="0.98"     opacity="1" />
                            <ColorMapEntry color="#FFD54A"  quantity="0.985"    opacity="1" />
                            <ColorMapEntry color="#FFFF68"  quantity="0.99"     opacity="1" />
                            <ColorMapEntry color="#F7FC94"  quantity="0.995"    opacity="1" />
                            <ColorMapEntry color="#FFFFC9"  quantity="0.9995"   opacity="1" />
                            <ColorMapEntry color="#FFFFFF"  quantity="0.999999" opacity="1" />
                            <ColorMapEntry color="#FFFFFF"  quantity="1.0"      opacity="0" />
                        </ColorMap>
                    </RasterSymbolizer>
                </Rule>
            </FeatureTypeStyle>
        </UserStyle>
    </UserLayer>
</StyledLayerDescriptor>
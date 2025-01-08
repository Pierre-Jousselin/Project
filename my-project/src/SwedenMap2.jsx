import React, { memo } from "react";
import { ComposableMap, Geographies, Geography } from "react-simple-maps";
import { mapdata } from "./assets/mapdata";
import { Marker } from "react-simple-maps";
import { sweden } from "./sweden";
import { geoCentroid } from "d3-geo";
import { getColor } from "./getColor";

const SwedenMap2 = ({
  width,
  height,
  setRegion,
  selected_region,
  tooltip_data,
  setTooltipContent,
}) => {
  return (
    <ComposableMap
      width={width}
      height={height}
      projection="geoMercator"
      style={{ backgroundColor: "white" }}
      projectionConfig={{
        scale: width * 2.5,
        center: [16, 63],
      }}
    >
      <Geographies geography={mapdata}>
        {({ geographies }) => {
          return (
            <>
              {geographies.map((geo) => {
                if (tooltip_data == null) {
                  tooltip_data = {
                    SE_1: {
                      date_actual: 0,
                      actual: 0,
                      next_date: 0,
                      next_prediction: 0,
                    },
                    SE_2: {
                      date_actual: 0,
                      actual: 0,
                      next_date: 0,
                      next_prediction: 0,
                    },
                    SE_3: {
                      date_actual: 0,
                      actual: 0,
                      next_date: 0,
                      next_prediction: 0,
                    },
                    SE_4: {
                      date_actual: 0,
                      actual: 0,
                      next_date: 0,
                      next_prediction: 0,
                    },
                  };
                }
                const region_name = geo.properties.code;
                const color = getColor(tooltip_data[region_name].actual, 15000);
                return (
                  <Geography
                    onClick={() => setRegion(region_name)}
                    onMouseEnter={() => {
                      setTooltipContent(`${geo.properties.name}`);
                    }}
                    onMouseLeave={() => {}}
                    key={geo.rsmKey}
                    geography={geo}
                    style={
                      selected_region == region_name
                        ? {
                            default: {
                              fill: color,
                              stroke: "#0b8be6",
                              strokeWidth: 3,
                              outline: "none",
                            },
                            hover: {
                              fill: color,
                              stroke: "#0b8be6",
                              strokeWidth: 3,
                              outline: "none",
                            },
                            pressed: {
                              fill: color,
                              stroke: "#0b8be6",
                              strokeWidth: 3,
                              outline: "none",
                            },
                          }
                        : {
                            default: {
                              fill: color,
                              stroke: "#0b8be6",
                              strokeWidth: 1,
                              outline: "none",
                            },
                            hover: {
                              fill: color,
                              stroke: "#0b8be6",
                              strokeWidth: 3,
                              outline: "none",
                            },
                            pressed: { fill: color, outline: "none" },
                          }
                    }
                  />
                );
              })}
              {geographies.map((geo) => {
                const provinceCenter = geoCentroid(geo);
                return (
                  <Marker key={geo.rsmKey} coordinates={provinceCenter}>
                    <text
                      style={{
                        fill: "black",
                        strokeWidth: 0,
                      }}
                      textAnchor="middle"
                    >
                      {geo.properties.name}
                    </text>
                  </Marker>
                );
              })}
            </>
          );
        }}
      </Geographies>
    </ComposableMap>
  );
};

export default memo(SwedenMap2);

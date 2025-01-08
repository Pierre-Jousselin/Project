import * as d3 from "d3";
import { sweden } from "./sweden";
import { mapdata } from "./assets/mapdata";

const SwedenMap = ({ width, height }) => {
  const data = mapdata;
  const projection = d3
    .geoMercator()
    .scale(width * 2.5)
    .center([16, 63]);

  const geoPathGenerator = d3.geoPath().projection(projection);

  const allSvgPaths = data.features
    .filter((shape) => shape.id !== "ATA")
    .map((shape) => {
      return (
        <path
          key={shape.id}
          d={geoPathGenerator(shape)}
          stroke="lightGrey"
          strokeWidth={0.5}
          fill="grey"
          fillOpacity={0.7}
        />
      );
    });

  return (
    <div>
      <svg width={width} height={height}>
        {allSvgPaths}
      </svg>
    </div>
  );
};

export default SwedenMap;

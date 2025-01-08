import { useState, useEffect, useRef, useLayoutEffect } from "react";
import "./App.css";
import ChartPrediction from "./ChartPrediction";
import ChartHind from "./ChartHind";
import axios from "axios";
import { Rings } from "react-loader-spinner";
// import SwedenMap from "./SwedenMap";
import SwedenMap2 from "./SwedenMap2";

function App() {
  const divRef = useRef(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

  const [selected_region, setRegion] = useState("SE_1");
  const [prediction_data, setPredictionData] = useState(null);
  const [hindchart_data, setHindchartData] = useState(null);
  const [first_loading, setLoadingFirst] = useState(true);
  const [data_loading, setDataLoading] = useState(true);
  const [date_hindchart, setDateHindchart] = useState({});
  const [tooltip_data, setTooltipData] = useState(null);
  const [tooltip_data2, setTooltipData2] = useState(null);
  const [info, setInfo] = useState(null);

  useLayoutEffect(() => {
    // Function to update the dimensions
    const updateDimensions = () => {
      if (divRef.current) {
        setDimensions({
          width: divRef.current.offsetWidth,
          height: divRef.current.offsetHeight,
        });
      }
    };

    // Update the dimensions when the component mounts
    updateDimensions();

    // Optional: You can also update on window resize to track changes
    window.addEventListener("resize", updateDimensions);

    // Cleanup event listener on unmount
    return () => {
      window.removeEventListener("resize", updateDimensions);
    };
  }, [first_loading]);

  useEffect(() => {
    setLoadingFirst(true);

    const params = {
      country_code: "SE_1",
    };

    axios
      .get(`${import.meta.env.VITE_BACKEND_URL}/inference/region`, { params })
      .then((response) => {
        setPredictionData(response.data.data[0].entries);
        const dates = response.data.data[0].entries.map(
          (item) => new Date(item.date)
        );
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // console.log("Min Date:", minDate);
        // console.log("Max Date:", maxDate);
        // console.log(minDate.toISOString());

        setDateHindchart([minDate.toISOString(), maxDate.toISOString()]);

        setLoadingFirst(false);
        setDataLoading(false);
      })
      .catch((error) => console.error("Error fetching data:", error));

    axios
      .get(`${import.meta.env.VITE_BACKEND_URL}/energy/latest_all`, { params })
      .then((response) => {
        console.log(response);
        const date_target = new Date(response.data.data[0].latestEntry.date);
        const actual = response.data.data;
        const nextDate = new Date(date_target.getTime() + 24 * 60 * 60 * 1000);
        const isoDate = nextDate.toISOString();
        const target = { date: isoDate };

        axios
          .get(`${import.meta.env.VITE_BACKEND_URL}/inference/get_all`, {
            params: target,
          })
          .then((response) => {
            console.log(response);
            const result = {
              SE_1: {
                date_actual: date_target.toISOString(),
                actual: actual[0].latestEntry.data,
                next_date: isoDate,
                next_prediction: response.data.data[0].latestEntry.data,
              },
              SE_2: {
                date_actual: date_target.toISOString(),
                actual: actual[1].latestEntry.data,
                next_date: isoDate,
                next_prediction: response.data.data[1].latestEntry.data,
              },
              SE_3: {
                date_actual: date_target.toISOString(),
                actual: actual[2].latestEntry.data,
                next_date: isoDate,
                next_prediction: response.data.data[2].latestEntry.data,
              },
              SE_4: {
                date_actual: date_target.toISOString(),
                actual: actual[3].latestEntry.data,
                next_date: isoDate,
                next_prediction: response.data.data[3].latestEntry.data,
              },
            };
            setTooltipData(result);
          })
          .catch((error) => console.error("Error fetching data:", error));
      })
      .catch((error) => console.error("Error fetching data:", error));
  }, []);

  useEffect(() => {
    setDataLoading(true);

    const params = {
      country_code: selected_region,
    };

    axios
      .get(`${import.meta.env.VITE_BACKEND_URL}/inference/region`, { params })
      .then((response) => {
        setPredictionData(response.data.data[0].entries);
        const dates = response.data.data[0].entries.map(
          (item) => new Date(item.date)
        );
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // console.log("Min Date:", minDate);
        // console.log("Max Date:", maxDate);
        // console.log(minDate.toISOString());

        setDateHindchart({
          min_date: minDate.toISOString(),
          max_date: maxDate.toISOString(),
        });

        setLoadingFirst(false);
        setDataLoading(false);
      })
      .catch((error) => console.error("Error fetching data:", error));
  }, [selected_region]);

  useEffect(() => {
    if (tooltip_data != null && info != null) {
      setTooltipData2(tooltip_data[info.replace(/(\d)$/, "_$1")]);
    }
  }, [info]);

  useEffect(() => {
    setDataLoading(true);

    const params = {
      country_code: selected_region,
      start_date: date_hindchart.min_date,
      end_date: date_hindchart.max_date,
    };

    axios
      .get(`${import.meta.env.VITE_BACKEND_URL}/energy/region`, { params })
      .then((response) => {
        const list2Map = new Map(
          prediction_data.map((item) => [item.date, item.data])
        );

        const joinedList = response.data.data[0].entries.map((item1) => {
          const prediction = list2Map.get(item1.date) || null; // If no match, set to null
          return { ...item1, prediction };
        });

        setHindchartData(joinedList);

        setLoadingFirst(false);
        setDataLoading(false);
      })
      .catch((error) => console.error("Error fetching data:", error));
  }, [date_hindchart, prediction_data, selected_region, first_loading]);

  if (first_loading) {
    return (
      <div className="flex flex-col justify-center items-center h-screen w-screen">
        <Rings color="blue" />
        <p className="text-3xl">Loading...</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen w-screen bg-gray-100">
      <div className=" flow-root">
        <div className="flex justify-center items-center h-24 bg-gray-100 float-left">
          <div className="text-3xl font-semibold pl-8">
            Energy Load Prediction for Sweden All Region
          </div>
        </div>
        <div className="flex flex-col text-right h-24 bg-gray-100 float-right text-sm pr-8 pt-4">
          <p>Created as Project for ID2223 Autumn 2024 by</p>
          <p>Eron Ariodito Hermanto (ariodito@kth.se)</p>
          <p>Pierre Grégoire Malo Jousselin (pgmjo@kth.se)</p>
        </div>
      </div>
      <div className="w-screen h-[calc(100%-6rem)]">
        <div className="h-full grid grid-cols-3 w-full pb-4">
          <div className="h-full">
            <div className="col-span-1 rounded-lg flex align-middle  bg-white shadow-lg  justify-center items-center h-full mx-2">
              {/* <div>
              <button onClick={() => setRegion("SE_1")}>SE_1</button>
              <button onClick={() => setRegion("SE_2")}>SE_2</button>
              <button onClick={() => setRegion("SE_3")}>SE_3</button>
              <button onClick={() => setRegion("SE_4")}>SE_4</button>
            </div> */}
              {/* <SwedenMap width={dimensions.width} height={dimensions.height} /> */}
              {tooltip_data2 && (
                <div className="fixed top-32 left-6 text-md p-4 rounded-md shadow-lg transition-all duration-300 ease-in-out transform hover:scale-110 active:scale-95">
                  <p className=" text-center pb-4">Region: {info}</p>
                  <p>Latest Data</p>
                  <p>{tooltip_data2.date_actual}</p>
                  <p>{tooltip_data2.actual} MW</p>
                  <p>Prediction</p>
                  <p>{tooltip_data2.next_date}</p>
                  <p>{tooltip_data2.next_prediction} MW</p>
                </div>
              )}
              <div ref={divRef} className="h-[95%] w-[95%]">
                <SwedenMap2
                  width={dimensions.width}
                  height={dimensions.height}
                  setRegion={setRegion}
                  selected_region={selected_region}
                  tooltip_data={tooltip_data}
                  setTooltipContent={setInfo}
                />
              </div>
            </div>
          </div>
          <div className="col-span-2 flex flex-col mr-4">
            <div className="py-4 text-xl pl-8 bg-white rounded-lg shadow-lg">
              Data for Region: {selected_region.replace("_", " ")}
            </div>
            <div className=" grid grid-rows-2 gap-2 h-full pt-2">
              <div className="border flex flex-col items-center justify-center h-full bg-white rounded-lg shadow-lg">
                <p className="text-xl">Prediction Chart</p>
                <div className="h-[90%] w-[90%]">
                  {data_loading ? (
                    <div className="flex flex-col justify-center items-center h-full ">
                      <Rings color="blue" />
                      <p className="text-xl">Loading...</p>
                    </div>
                  ) : (
                    <ChartPrediction inputData={prediction_data} />
                  )}
                </div>
              </div>
              <div className="border flex flex-col items-center justify-center h-full bg-white rounded-lg shadow-lg">
                <p className="text-xl">Hindchart</p>
                <div className="h-[90%] w-[90%]">
                  {hindchart_data ? (
                    <ChartHind inputData={hindchart_data} />
                  ) : (
                    <div className="flex flex-col justify-center items-center h-full">
                      <Rings color="blue" />
                      <p className="text-xl">Loading...</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
export default App;

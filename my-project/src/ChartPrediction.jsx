import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  LogarithmicScale,
} from "chart.js";
import "chartjs-adapter-date-fns"; // Required for datetime support in Chart.js
import { Line } from "react-chartjs-2";
import { addDays, subDays } from "date-fns";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  LogarithmicScale
);

const ChartPrediction = ({ inputData }) => {
  // Example inputData: [{ date: "2024-11-19T00:00:00Z", value: 10 }, { date: "2024-11-20T00:00:00Z", value: 20 }]
  const labels = inputData.map((item) => new Date(item.date)); // Extract datetime labels
  const values = inputData.map((item) => Math.max(item.data, 0.0001));

  const firstDate = new Date(Math.min(...labels));
  const lastDate = new Date(Math.max(...labels));

  // Extend the range by one day before and after
  const minDate = subDays(firstDate, 1); // One day before the first date
  const maxDate = addDays(lastDate, 1); // One day after the last date

  //console.log(labels);
  //console.log(values);
  // Chart data
  const data = {
    labels, // Datetime labels
    datasets: [
      {
        label: "Predicted Energy Load",
        data: values, // Data values
        borderColor: "blue",
        borderWidth: 2,
        fill: false,
      },
    ],
  };

  // Chart options
  const options = {
    responsive: true,
    aspectRatio: 3,
    plugins: {
      legend: { display: true },
      tooltip: { enabled: true },
    },
    scales: {
      x: {
        type: "time", // Use time scale for datetime support
        time: {
          unit: "day", // Adjust unit to "day", "hour", etc. based on granularity
        },
        min: minDate, // Set the minimum date
        max: maxDate, // Set the maximum date
        title: {
          display: true,
          text: "Date",
        },
      },
      y: {
        title: {
          display: true,
          text: "Energy Load (MW)",
        },
      },
    },
  };

  const verticalBackgroundPlugin = {
    id: "vertical-background",
    beforeDraw: (chart) => {
      const ctx = chart.ctx;
      const chartArea = chart.chartArea;
      const yAxis = chart.scales.y;

      // Define colors and ranges
      const colors = [
        "rgba(144, 238, 144, 0.3)",
        "rgba(255, 255, 224, 0.3)",
        "rgba(255, 165, 0, 0.3)",
        "rgba(255, 99, 71, 0.3)",
        "rgba(138, 43, 226, 0.3)",
        "rgba(139, 69, 19, 0.3)",
      ];
      const ranges = [0, 49, 99, 149, 199, 299, 500]; // Specify Y-axis values for transitions

      // Draw vertical background sections
      ctx.save();
      for (let i = 0; i < ranges.length - 1; i++) {
        const yStart = yAxis.getPixelForValue(ranges[i + 1]); // Top of the range
        const yEnd = yAxis.getPixelForValue(ranges[i]); // Bottom of the range

        ctx.fillStyle = colors[i];
        ctx.fillRect(
          chartArea.left,
          yStart,
          chartArea.right - chartArea.left,
          yEnd - yStart
        );
      }
      ctx.restore();
    },
  };

  return <Line data={data} options={options} />;
};

export default ChartPrediction;

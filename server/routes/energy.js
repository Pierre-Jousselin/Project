const mongoose = require("mongoose");

const express = require("express");

module.exports = (connection) => {
  const router = express.Router();

  // Define the Schema
  const energySchema = new mongoose.Schema(
    {
      id: String,
      country_code: String,
      date: Date,
      data: mongoose.Schema.Types.Mixed,
    },
    { strict: false }
  ); // strict: false allows for flexible debugging

  // Create the model
  const Energy = connection.model("Energy", energySchema, "daily_energy_load");

  // Getting All
  router.get("/", (req, res) => {
    res.send("Hello World");
  });

  router.get("/region", async (req, res) => {
    try {
      const { country_code, start_date, end_date } = req.query;

      // Convert start_date and end_date to Date objects (if provided)
      const startDate = start_date ? new Date(start_date) : null;
      const endDate = end_date ? new Date(end_date) : null;

      // Build the aggregation pipeline
      const pipeline = [];

      // Match stage - only include if country_code is provided
      const matchStage = {};

      // Include country_code filter if provided
      if (country_code) {
        matchStage.country_code = country_code.toUpperCase();
      }

      // Include date range filter if provided
      if (startDate && endDate) {
        matchStage.date = { $gte: startDate, $lte: endDate }; // $gte: greater than or equal, $lte: less than or equal
      } else if (startDate) {
        matchStage.date = { $gte: startDate };
      } else if (endDate) {
        matchStage.date = { $lte: endDate };
      }

      // Only add the $match stage if filters are applied
      if (Object.keys(matchStage).length > 0) {
        pipeline.push({ $match: matchStage });
      }

      // Group stage
      pipeline.push({
        $group: {
          _id: "$country_code",
          entries: {
            $push: {
              date: "$date",
              data: "$load", // Ensure you're referencing the correct field for 'data'
            },
          },
          count: { $sum: 1 },
        },
      });

      pipeline.push({
        $project: {
          _id: 1,
          entries: {
            $sortArray: {
              input: "$entries",
              sortBy: { date: 1 }, // 1 for ascending order (date)
            },
          },
          count: 1,
        },
      });

      // Sort by country_code
      pipeline.push({
        $sort: { _id: 1 },
      });

      // Execute the aggregation query
      const groupedData = await Energy.aggregate(pipeline);

      res.json({
        success: true,
        data: groupedData,
        query: { country_code, start_date, end_date },
      });
    } catch (error) {
      console.error("Error fetching grouped data:", error);
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  });

  router.get("/latest_all", async (req, res) => {
    try {
      // Build the aggregation pipeline
      const pipeline = [];

      // Sort stage - ensure data is sorted by date in descending order
      pipeline.push({
        $sort: { date: -1 }, // -1 for descending order (latest date first)
      });

      // Group stage - get the latest entry for each country_code
      pipeline.push({
        $group: {
          _id: "$country_code",
          latestEntry: { $first: "$$ROOT" }, // $$ROOT references the whole document
        },
      });

      // Sort by country_code (optional)
      pipeline.push({
        $sort: { _id: 1 },
      });

      // Project the desired fields
      pipeline.push({
        $project: {
          _id: 1,
          latestEntry: {
            date: "$latestEntry.date",
            data: "$latestEntry.load", // Replace "load" with the correct field
          },
        },
      });

      // Execute the aggregation query
      const groupedData = await Energy.aggregate(pipeline);

      res.json({
        success: true,
        data: groupedData,
      });
    } catch (error) {
      console.error("Error fetching latest data:", error);
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  });

  return router;
};

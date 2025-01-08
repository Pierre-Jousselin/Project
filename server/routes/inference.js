require("dotenv").config();

const mongoose = require("mongoose");

const express = require("express");

module.exports = (connection) => {
  const router = express.Router();

  // Define the Schema
  const inferenceSchema = new mongoose.Schema(
    {
      id: String,
      country_code: String,
      date: Date,
      data: mongoose.Schema.Types.Mixed,
    },
    { strict: false }
  ); // strict: false allows for flexible debugging

  // Create the model
  const Inference = connection.model(
    "Inference",
    inferenceSchema,
    "inferences_data"
  );

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

      // Build the aggregation pipeline based on whether countrycode is provided
      const pipeline = [];
      // Match stage - only include if country_code is provided
      const matchStage = {};

      // Match stage - only include if countrycode is provided
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
              data: "$prediction",
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

      // Sort by countrycode
      pipeline.push({
        $sort: { _id: 1 },
      });

      const groupedData = await Inference.aggregate(pipeline);

      res.json({
        success: true,
        data: groupedData,
        query: { country_code },
      });
    } catch (error) {
      console.error("Error fetching grouped data:", error);
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  });

  router.get("/get_all", async (req, res) => {
    try {
      const { date } = req.query; // Retrieve the 'date' from the query parameters

      // Build the aggregation pipeline
      const pipeline = [];

      // Match stage for a specific date (if provided)
      if (date) {
        const targetDate = new Date(date);

        // Match documents with the given date
        pipeline.push({
          $match: {
            date: {
              $eq: targetDate, // Match the exact date
            },
          },
        });
      }

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
            data: "$latestEntry.prediction", // Replace "load" with the correct field
          },
        },
      });

      // Execute the aggregation query
      const groupedData = await Inference.aggregate(pipeline);

      res.json({
        success: true,
        data: groupedData,
        query: { date },
      });
    } catch (error) {
      console.error("Error fetching data:", error);
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  });

  return router;
};

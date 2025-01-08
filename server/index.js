require("dotenv").config();

const express = require("express");
const app = express();
const mongoose = require("mongoose");

const inferenceRouter = require("./routes/inference");
const energyRouter = require("./routes/energy");

const uri = process.env.DATABASE_URI;

const connectionA = mongoose.createConnection(uri, {
  useNewUrlParser: true,
  dbName: "inferences",
});

connectionA.on("error", (error) => console.error(error));
connectionA.once("open", () => {
  console.log("Connected to mongo server, inference database.");
});

const connectionB = mongoose.createConnection(uri, {
  useNewUrlParser: true,
  dbName: "Energy",
});

connectionB.on("error", (error) => console.error(error));
connectionB.once("open", () => {
  console.log("Connected to mongo server, Energy database.");
});

app.use(express.json());

app.use("/inference", inferenceRouter(connectionA));
app.use("/energy", energyRouter(connectionB));

app.listen(3000, () => {
  console.log("Server Started");
});

// var MongoClient = require("mongodb").MongoClient;
// var url = "mongodb://localhost:27017/";

// MongoClient.connect(url, function (err, db) {
//   if (err) throw err;
//   var dbo = db.db("iotDb");
//   dbo.collection("iotCollection").findOne({}, function (err, result) {
//     if (err) throw err;
//     console.log(result.Current);
//     db.close();
//   });
// });

const dboperations = require("./dboperations");

var express = require("express");
var bodyParser = require("body-parser");
var cors = require("cors");
const { response, request } = require("express");
var app = express();
var router = express.Router();

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(cors());

app.use("/api", router);

router.use((request, res, next) => {
  console.log("middleware");
  next();
});

router.route("/roomtemp/:deviceId").get((request, response) => {
  deviceId = request.params.deviceId;
  dboperations.getIotDataRoomTemp(deviceId).then((result) => {
    // console.log("Result:- ", result);
    response.json(result);
  });
});

router.route("/humidity/:deviceId").get((request, response) => {
  deviceId = request.params.deviceId;
  dboperations.getIotDataHumidity(deviceId).then((result) => {
    // console.log("Result:- ", result);
    response.json(result);
  });
});

router.route("/deviceIds").get((request, response) => {
  dboperations.getDeviceIds().then((result) => {
    // console.log("Result:- ", result);
    response.json(result);
  });
});

// Room Temp., Humidity, Unit Consumption of selected day
router.route("/:temp/:deviceId/:dt").get((request, response) => {
  dboperations
    .getIotDataRoomTempSelectedDay(
      request.params.temp,
      request.params.deviceId,
      request.params.dt
    )
    .then((result) => {
      response.json(result);
    });
});

var port = process.env.PORT || 3000;
app.listen(port);
console.log("node js is running at port :- ", port);

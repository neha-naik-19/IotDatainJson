var MongoClient = require("mongodb").MongoClient;
var conn = require("./dbconfig");

async function getDeviceIds() {
  return MongoClient.connect(conn)
    .then(function (db) {
      var dbo = db.db("iotDb");
      var collection = dbo.collection("iotCollection");

      // return collection.distinct("Device_ID");

      return collection.distinct("Device_ID", {
        $nor: [
          {
            Device_ID: /^A.*/,
          },
          {
            Device_ID: /^0.*/,
          },
        ],
      });
    })
    .then(function (items) {
      return items.sort();
    });
}

async function iotInputData(deviceId) {
  return MongoClient.connect(conn)
    .then(function (db) {
      var dbo = db.db("iotDb");
      var collection = dbo.collection("iotCollection");

      return collection
        .find({ Device_ID: deviceId })
        .sort({ Time_Stamp: -1 })
        .limit(1)
        .toArray();
    })
    .then(function (items) {
      var dt = new Date(items[0].Time_Stamp).toLocaleString("en-US", {
        timeZone: "Asia/Kolkata",
      });

      let dtwotime = `${new Date(dt).getFullYear()}-${(
        "0" +
        (new Date(dt).getMonth() + 1)
      ).slice(-2)}-${new Date(dt).getDate()}`;

      let dtMonth = new Date(dt).getMonth() + 1;

      let dtYear = new Date(dt).getFullYear();

      // console.log("Test :- ", dtwotime);
      // return items[0].Time_Stamp;

      return {
        deviceId: deviceId,
        dtwotime: dtwotime,
        dtMonth: dtMonth,
        dtYear: dtYear,
      };
    });
}

async function getIotDataRoomTemp(deviceId) {
  let input = await iotInputData(deviceId);

  const pipeline = [
    {
      $addFields: {
        changedt: {
          $dateFromString: {
            dateString: "$Time_Stamp",
          },
        },
      },
    },
    {
      $addFields: {
        addhours: {
          $dateAdd: {
            startDate: "$changedt",
            unit: "hour",
            amount: 5,
          },
        },
      },
    },
    {
      $addFields: {
        newTime_Stamp: {
          $dateAdd: {
            startDate: "$addhours",
            unit: "minute",
            amount: 30,
          },
        },
      },
    },
    {
      $addFields: {
        creationDate: {
          $dateToString: { format: "%Y-%m-%d", date: "$newTime_Stamp" },
        },
      },
    },
    {
      $addFields: {
        maxDate: {
          $max: "$creationDate",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        yearDate: {
          $year: "$newTime_Stamp",
        },
      },
    },
    {
      $match: {
        Device_ID: input.deviceId,
        // maxDate: input.dtwotime,
        monthDate: input.dtMonth,
        yearDate: input.dtYear,
      },
    },
    // {
    //   $project: {
    //     _id: 0,
    //     Device_ID: 1,
    //     changedt: 1,
    //     newTime_Stamp: 1,
    //     room_temp: 1,
    //     Humidity: 1,
    //     External_temp: 1,
    //     unit_consumption: 1,
    //     // addhours: 1,
    //     // maxDate: 1,
    //   },
    // },
    {
      $group: {
        _id: "$maxDate",
        avgroomtemp: { $avg: "$room_temp" },
        // avghumidity: { $avg: "$Humidity" },
        // avgunitconsumption: { $avg: "$unit_consumption" },
      },
    },
    {
      $project: {
        roomTemp: { $round: ["$avgroomtemp", 2] },
        // humidity: { $round: ["$avghumidity", 2] },
        // unitconsumption: { $round: ["$avgunitconsumption", 2] },
      },
    },
    { $sort: { _id: 1 } },
  ];

  return MongoClient.connect(conn)
    .then(function (db) {
      var dbo = db.db("iotDb");
      var collection = dbo.collection("iotCollection");
      return collection.aggregate(pipeline, { allowDiskUse: true }).toArray();
    })
    .then(function (items) {
      // console.log(items);
      return items;
    });

  // return MongoClient.connect(conn)
  //   .then(function (db) {
  //     var dbo = db.db("iotDb");
  //     var collection = dbo.collection("iotCollection");
  //     return collection
  //       .find({ Device_ID: deviceId })
  //       .sort({ Time_Stamp: -1 })
  //       .limit(1)
  //       .toArray();
  //   })
  //   .then(function (items) {
  //     console.log(items);
  //     // return items;
  //   });
}

async function getIotDataHumidity(deviceId) {
  let input = await iotInputData(deviceId);

  const pipeline = [
    {
      $addFields: {
        changedt: {
          $dateFromString: {
            dateString: "$Time_Stamp",
          },
        },
      },
    },
    {
      $addFields: {
        addhours: {
          $dateAdd: {
            startDate: "$changedt",
            unit: "hour",
            amount: 5,
          },
        },
      },
    },
    {
      $addFields: {
        newTime_Stamp: {
          $dateAdd: {
            startDate: "$addhours",
            unit: "minute",
            amount: 30,
          },
        },
      },
    },
    {
      $addFields: {
        creationDate: {
          $dateToString: { format: "%Y-%m-%d", date: "$newTime_Stamp" },
        },
      },
    },
    {
      $addFields: {
        maxDate: {
          $max: "$creationDate",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        yearDate: {
          $year: "$newTime_Stamp",
        },
      },
    },
    {
      $match: {
        Device_ID: input.deviceId,
        // maxDate: input.dtwotime,
        monthDate: input.dtMonth,
        yearDate: input.dtYear,
      },
    },
    {
      $group: {
        _id: "$maxDate",

        avghumidity: { $avg: "$Humidity" },
      },
    },
    {
      $project: {
        humidity: { $round: ["$avghumidity", 2] },
      },
    },
    { $sort: { _id: 1 } },
  ];

  return MongoClient.connect(conn)
    .then(function (db) {
      var dbo = db.db("iotDb");
      var collection = dbo.collection("iotCollection");
      return collection.aggregate(pipeline, { allowDiskUse: true }).toArray();
    })
    .then(function (items) {
      // console.log(items);
      return items;
    });

  // return MongoClient.connect(conn)
  //   .then(function (db) {
  //     var dbo = db.db("iotDb");
  //     var collection = dbo.collection("iotCollection");
  //     return collection
  //       .find({ Device_ID: deviceId })
  //       .sort({ Time_Stamp: -1 })
  //       .limit(1)
  //       .toArray();
  //   })
  //   .then(function (items) {
  //     console.log(items);
  //     // return items;
  //   });
}

async function getIotDataRoomTempSelectedDay(tempType, deviceId, dt) {
  if (tempType === "roomtemp") {
  }

  const pipeline = [
    {
      $addFields: {
        changedt: {
          $dateFromString: {
            dateString: "$Time_Stamp",
          },
        },
      },
    },
    {
      $addFields: {
        addhours: {
          $dateAdd: {
            startDate: "$changedt",
            unit: "hour",
            amount: 5,
          },
        },
      },
    },
    {
      $addFields: {
        newTime_Stamp: {
          $dateAdd: {
            startDate: "$addhours",
            unit: "minute",
            amount: 30,
          },
        },
      },
    },
    {
      $addFields: {
        creationDate: {
          $dateToString: { format: "%Y-%m-%d", date: "$newTime_Stamp" },
        },
      },
    },
    {
      $addFields: {
        maxDate: {
          $max: "$creationDate",
        },
      },
    },
    {
      $addFields: {
        dtDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        monthDate: {
          $month: "$newTime_Stamp",
        },
      },
    },
    {
      $addFields: {
        yearDate: {
          $year: "$newTime_Stamp",
        },
      },
    },
    {
      $match: {
        Device_ID: deviceId,
        dtDate: new Date(dt).getDate(),
        monthDate: new Date(dt).getMonth() + 1,
        yearDate: new Date(dt).getFullYear(),
      },
    },
    {
      $group: {
        _id: "$maxDate",
        avgroomtemp: { $avg: "$room_temp" },
        avghumidity: { $avg: "$Humidity" },
        avgunitconsumption: { $avg: "$unit_consumption" },
      },
    },
    {
      $project: {
        temp:
          tempType === "roomtemp"
            ? { $round: ["$avgroomtemp", 2] }
            : tempType === "humidity"
            ? { $round: ["$avghumidity", 2] }
            : { $round: ["$avgunitconsumption", 2] },
      },
    },
    { $sort: { _id: 1 } },
  ];

  return MongoClient.connect(conn)
    .then(function (db) {
      var dbo = db.db("iotDb");
      var collection = dbo.collection("iotCollection");
      return collection.aggregate(pipeline, { allowDiskUse: true }).toArray();
    })
    .then(function (items) {
      return items;
    });
}

module.exports = {
  getIotDataRoomTemp: getIotDataRoomTemp,
  getIotDataHumidity: getIotDataHumidity,
  getDeviceIds: getDeviceIds,
  getIotDataRoomTempSelectedDay: getIotDataRoomTempSelectedDay,
};

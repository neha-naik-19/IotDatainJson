// MUST BE AT TOP
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const fs = require("node:fs/promises");
const fse = require("fs");
const { MongoClient } = require("mongodb");
const jsonDiff = require("json-diff");
const uri = require("./dbconfig");

//=============================== Mongo Connection ===============================//
const conn = new MongoClient(uri.connString);
const database = conn.db("energygrid2");
const collection = database.collection("energygrid2");

//getDeviceIds from database and save in json file
async function getDeviceIds() {
  try {
    let delFile = false;

    let deviceIds = await collection.distinct("Device_ID", {
      $nor: [
        // {
        //   Device_ID: /^A.*/,
        // },
        {
          Device_ID: /^0.*/,
        },
      ],
    });

    deviceIds = deviceIds.sort();

    deviceIds = JSON.stringify(deviceIds);

    if (fse.existsSync(uri.filePath + "/deviceIds.json")) {
      // fileData = await fs.readFile(deviceIdsFile, { encoding: 'utf8' });
      jsonDiff.diffString(
        await fs.readFile(uri.filePath + "/deviceIds.json", {
          encoding: "utf8",
        }),
        deviceIds
      ).length > 0
        ? (delFile = false)
        : (delFile = true);
      if (!delFile) await fs.unlink(uri.filePath + "/deviceIds.json");
    } else {
      delFile = false;
    }

    if (!delFile)
      await fs.writeFile(uri.filePath + "/deviceIds.json", deviceIds);
  } finally {
    // Ensures that the client will close when you finish/error
    // await conn.close();
  }
}

//get last date for each device
async function iotInputData(deviceId) {
  let deviceDetails = await collection
    .find({ Device_ID: deviceId })
    .sort({ Time_Stamp: -1 })
    .limit(1)
    .toArray();

  if (
    !deviceDetails ||
    deviceDetails.length === 0 ||
    !deviceDetails[0].Time_Stamp
  ) {
    console.log(`No data found for device ${deviceId}, skipping...`);
    return null; // return null or undefined to indicate no data
  }

  // Convert to Date (assumes Time_Stamp is ISO or valid date string)
  const utcDate = new Date(deviceDetails[0].Time_Stamp);

  // Convert UTC -> IST (+5.30)
  const istDate = new Date(utcDate.getTime() + 330 * 60 * 1000);

  const year = istDate.getFullYear();
  const month = istDate.getMonth() + 1;
  const day = String(istDate.getDate()).padStart(2, "0");

  return {
    deviceId,
    dtwotime: `${year}-${String(month).padStart(2, "0")}-${day}`,
    dtMonth: month,
    dtYear: year,
  };
}

async function basePipeLine(contents, type) {
  const groupStage = {
    _id: "$creationDate",
  };

  if (type === "roomtemp") {
    groupStage.avgTemperature = { $avg: "$Temperature" };
  }

  if (type === "humidity") {
    groupStage.avgHumidity = { $avg: "$Humidity" };
  }

  if (type === "unitConsumption") {
    groupStage.avgUnitConsumption = { $avg: "$unit_consumption" };
  }

  const projectStage = {
    _id: 1,
  };

  if (type === "roomtemp") {
    projectStage.temperature = { $round: ["$avgTemperature", 2] };
  }

  if (type === "humidity") {
    projectStage.humidity = { $round: ["$avgHumidity", 2] };
  }

  if (type === "unitConsumption") {
    projectStage.unitConsumption = { $round: ["$avgUnitConsumption", 3] };
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
        Device_ID: contents.deviceId,
        monthDate: contents.dtMonth,
        yearDate: contents.dtYear,
      },
    },
    { $group: groupStage },
    { $project: projectStage },
    { $sort: { _id: 1 } },
  ];

  return JSON.stringify(
    await collection.aggregate(pipeline, { allowDiskUse: true }).toArray()
  );
}

//getIotDataRoomTemp from database and save in json file (to display current month RoomTemp data)
//===============================================================================================//
async function getIotDataRoomTemp() {
  const devices = JSON.parse(
    await fs.readFile(`${uri.filePath}/deviceIds.json`, "utf8")
  );

  await Promise.all(
    devices.map(async (deviceId) => {
      try {
        const contents = await iotInputData(deviceId);
        const result = await basePipeLine(contents, "roomtemp");

        const dataFile = `${uri.filePath}/roomtemp_${deviceId}.json`;

        let shouldWrite = true;

        if (fse.existsSync(dataFile)) {
          const existingData = await fs.readFile(dataFile, "utf8");

          // compare content
          if (existingData === result) {
            shouldWrite = false;
          }
        }

        if (shouldWrite) {
          await fs.writeFile(dataFile, result);
          console.log("Room Temp updated:", `roomtemp_${deviceId}.json`);
        } else {
          console.log("Room Temp unchanged:", `roomtemp_${deviceId}.json`);
        }
      } catch (err) {
        console.error(`Room Temp error for ${deviceId}:`, err.message);
      }
    })
  );
}

//===============================================================================================//
//getIotDataHumidity from database and save in json file (to display current month humidity data)
async function getIotDataHumidity() {
  const devices = JSON.parse(
    await fs.readFile(`${uri.filePath}/deviceIds.json`, "utf8")
  );

  await Promise.all(
    devices.map(async (deviceId) => {
      try {
        const contents = await iotInputData(deviceId);
        const result = await basePipeLine(contents, "humidity");

        const dataFile = `${uri.filePath}/humidity_${deviceId}.json`;

        let shouldWrite = true;

        if (fse.existsSync(dataFile)) {
          const existingData = await fs.readFile(dataFile, "utf8");

          // compare content
          if (existingData === result) {
            shouldWrite = false;
          }
        }

        if (shouldWrite) {
          await fs.writeFile(dataFile, result);
          console.log("Humidity updated:", `humidity_${deviceId}.json`);
        } else {
          console.log("Humidity unchanged:", `humidity_${deviceId}.json`);
        }
      } catch (err) {
        console.error(`Humidity error for ${deviceId}:`, err.message);
      }
    })
  );
}

//====================================================================================================//
//getIotDataUnitConsumption from database and save in json file (to display current month humidity data)
async function getIotDataUnitConsumption() {
  const devices = JSON.parse(
    await fs.readFile(`${uri.filePath}/deviceIds.json`, "utf8")
  );

  await Promise.all(
    devices.map(async (deviceId) => {
      try {
        const contents = await iotInputData(deviceId);
        const result = await basePipeLine(contents, "unitConsumption");

        const dataFile = `${uri.filePath}/unitConsumption_${deviceId}.json`;

        let shouldWrite = true;

        if (fse.existsSync(dataFile)) {
          const existingData = await fs.readFile(dataFile, "utf8");

          // compare content
          if (existingData === result) {
            shouldWrite = false;
          }
        }

        if (shouldWrite) {
          await fs.writeFile(dataFile, result);
          console.log(
            "Unit comsumption updated:",
            `unitConsumption_${deviceId}.json`
          );
        } else {
          console.log(
            "Unit comsumption unchanged:",
            `unitConsumption_${deviceId}.json`
          );
        }
      } catch (err) {
        console.error(`Unit comsumption error for ${deviceId}:`, err.message);
      }
    })
  );
}

//=============================== Master Runner ===============================//
// (async () => {
//   await conn.connect();
//   console.log("MongoDB connected");

//   await getDeviceIds();
//   await getIotDataRoomTemp();
//   await getIotDataHumidity();
//   await getIotDataUnitConsumption();
// })();

(async () => {
  await conn.connect();
  console.log("MongoDB connected");

  while (true) {
    try {
      console.log("=== JOB START ===", new Date().toLocaleString());

      await getDeviceIds();
      await getIotDataRoomTemp();
      await getIotDataHumidity();
      await getIotDataUnitConsumption();

      console.log("=== JOB END ===", new Date().toLocaleString());
      console.log("Sleeping for 2 minutes...\n");

      await sleep(2 * 60 * 1000); // 2 minutes
    } catch (err) {
      console.error("Main loop error:", err);

      // Safety sleep even if something fails
      await sleep(2 * 60 * 1000);
    }
  }
})();

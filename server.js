const fs = require('node:fs/promises');
const fse = require("fs");
const { MongoClient } = require("mongodb");
const jsonDiff = require('json-diff');
const uri = require("./dbconfig");
var cron = require('node-cron');
const { log } = require('node:console');

const conn = new MongoClient(uri.connString);
const database = conn.db('iotDb');
const collection = database.collection('iotCollection');

var device = [];
var deviceIdsFile;

//getDeviceIds from database and save in json file
async function getDeviceIds() {
    try {
      // const file = '/Users/BITS/deviceIds.json';  
      // let fileData = '';
      let requireFile = false;

      let deviceIds = await collection.distinct("Device_ID", {
        $nor: [
          {
            Device_ID: /^A.*/,
          },
          {
            Device_ID: /^0.*/,
          },
        ],
      });

      deviceIds = deviceIds.sort();

      deviceIds = JSON.stringify(deviceIds);

      if(fse.existsSync(uri.filePath + '/deviceIds.json')){
        // fileData = await fs.readFile(deviceIdsFile, { encoding: 'utf8' });
        jsonDiff.diffString(await fs.readFile(uri.filePath + '/deviceIds.json', { encoding: 'utf8' }), deviceIds).length > 0 ? requireFile = false : requireFile = true ;
        if (!requireFile)  fs.unlink(uri.filePath + '/deviceIds.json');
      } else{
        requireFile = false;
      }
      
      if(!requireFile) await fs.writeFile(uri.filePath + '/deviceIds.json', deviceIds);

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

  var dt = new Date(deviceDetails[0].Time_Stamp).toLocaleString("en-US", {
    timeZone: "Asia/Kolkata",
  });
  
  let dtwotime = `${new Date(dt).getFullYear()}-${(
    "0" +
    (new Date(dt).getMonth() + 1)
  ).slice(-2)}-${new Date(dt).getDate()}`;

  let dtMonth = new Date(dt).getMonth() + 1;

  let dtYear = new Date(dt).getFullYear();

  return {
    deviceId: deviceId,
    dtwotime: dtwotime,
    dtMonth: dtMonth,
    dtYear: dtYear,
  };
}

//getIotDataRoomTemp from database and save in json file (to display current month RoomTemp data)
async function getIotDataRoomTemp() {
  let dataFile = '';
  let requireFile = false;

  let devices = JSON.parse(await fs.readFile(uri.filePath + '/deviceIds.json', { encoding: 'utf8' }));
  // devices = devices

  // console.log('devices :: ', devices.reverse());

  await Promise.all(devices.map(async (d) => {
    let contents = await iotInputData(d);
    requireFile = false;

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
          Device_ID: contents.deviceId,
          // maxDate: contents.dtwotime,
          monthDate: contents.dtMonth,
          yearDate: contents.dtYear,
        },
      },
      {
        $group: {
          _id: "$maxDate",
          avgroomtemp: { $avg: "$room_temp" },
        },
      },
      {
        $project: {
          roomTemp: { $round: ["$avgroomtemp", 2] },
        },
      },
      { $sort: { _id: 1 } },
    ];

    let result = JSON.stringify(await collection.aggregate(pipeline, { allowDiskUse: true }).toArray());

    dataFile = uri.filePath + '/roomtemp_' + d + '.json';

    if(fse.existsSync(dataFile)){
      // fileData = await fs.readFile(dataFile, { encoding: 'utf8' });
      jsonDiff.diffString(await fs.readFile(dataFile, { encoding: 'utf8' }), result).length > 0 ? requireFile = false : requireFile = true ;
      if (!requireFile)  fs.unlink(dataFile);
    } else{
      requireFile = false;
    }
    
    if(!requireFile){
      await fs.writeFile(dataFile, result);
      console.log('Room Temp : ',   'roomtemp_' + d );
    } 
  }
  ));
}

//getIotDataHumidity from database and save in json file (to display current month humidity data)
async function getIotDataHumidity() {
  let dataFile = '';
  let requireFile = false;

  let devices = JSON.parse(await fs.readFile(uri.filePath + '/deviceIds.json', { encoding: 'utf8' }));

  await Promise.all(devices.map(async (d) => {
    let contents = await iotInputData(d);
    requireFile = false;

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
          Device_ID: contents.deviceId,
          // maxDate: contents.dtwotime,
          monthDate: contents.dtMonth,
          yearDate: contents.dtYear,
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

    let result = JSON.stringify(await collection.aggregate(pipeline, { allowDiskUse: true }).toArray());

    dataFile = uri.filePath + '/humidity_' + d + '.json';

    if(fse.existsSync(dataFile)){
      // fileData = await fs.readFile(dataFile, { encoding: 'utf8' });
      jsonDiff.diffString(await fs.readFile(dataFile, { encoding: 'utf8' }), result).length > 0 ? requireFile = false : requireFile = true ;
      if (!requireFile)  fs.unlink(dataFile);
    } else{
      requireFile = false;
    }
    
    if(!requireFile) {
      await fs.writeFile(dataFile, result);
      console.log('Humidity : ',   'Humidity_' + d );
    }
  }
  ));
}

//getIotDataUnitConsumption from database and save in json file (to display current month humidity data)
async function getIotDataUnitConsumption() {
  let dataFile = '';
  let requireFile = false;

  let devices = JSON.parse(await fs.readFile(uri.filePath + '/deviceIds.json', { encoding: 'utf8' }));

  await Promise.all(devices.map(async (d) => {
    let contents = await iotInputData(d);
    requireFile = false;

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
          Device_ID: contents.deviceId,
          // maxDate: contents.dtwotime,
          monthDate: contents.dtMonth,
          yearDate: contents.dtYear,
        },
      },
      {
        $group: {
          _id: "$maxDate",
  
          avgUnitConsumption: { $avg: "$unit_consumption" },
        },
      },
      {
        $project: {
          unitConsumption: { $round: ["$avgUnitConsumption", 2] },
        },
      },
      { $sort: { _id: 1 } },
    ];

    let result = JSON.stringify(await collection.aggregate(pipeline, { allowDiskUse: true }).toArray());

    dataFile = uri.filePath + '/unitConsumption_' + d + '.json';

    if(fse.existsSync(dataFile)){
      // fileData = await fs.readFile(dataFile, { encoding: 'utf8' });
      jsonDiff.diffString(await fs.readFile(dataFile, { encoding: 'utf8' }), result).length > 0 ? requireFile = false : requireFile = true ;
      if (!requireFile)  fs.unlink(dataFile);
    } else{
      requireFile = false;
    }
    
    if(!requireFile) {
      await fs.writeFile(dataFile, result);
      console.log('unitConsumption_ : ',   'unitConsumption_' + d );
    }
  }
));
}

getDeviceIds().catch(console.dir);
getIotDataRoomTemp().catch(console.dir);
getIotDataHumidity().catch(console.dir);
getIotDataUnitConsumption().catch(console.dir);

// getDeviceIds().catch(console.dir);
// setInterval(function() {
//   getDeviceIds().catch(console.dir);
// },  4*60*1000);

// var task = cron.schedule('* * * * *', () =>  {
//   console.log('Running task');
//   getDeviceIds().catch(console.dir);
// }, {
//   scheduled: true, timezone: "Asia/Kolkata"
// });

// task.start();

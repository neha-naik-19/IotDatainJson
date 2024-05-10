require("dotenv").config();
const connString = process.env.DATABASE_URL;
const filePath = process.env.FILE_PATH;

module.exports = {connString,filePath};
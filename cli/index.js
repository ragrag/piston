#!/usr/bin/env node
require("dotenv").config();
const axios = require("axios").default;

const axiosHeader = {};
axiosHeader[process.env.API_KEY_HEADER] = process.env.API_KEY;
const axios_instance = function (argv) {
  argv.axios = axios.create({
    baseURL: argv["piston-url"],
    headers: { ...axiosHeader },
  });

  return argv;
};

require("yargs")(process.argv.slice(2))
  .option("piston-url", {
    alias: ["u"],
    default: "http://127.0.0.1:2000",
    desc: "Piston API URL",
    string: true,
  })
  .middleware(axios_instance)
  .scriptName("piston")
  .commandDir("commands")
  .demandCommand()
  .help()
  .wrap(72).argv;

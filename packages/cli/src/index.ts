#!/usr/bin/env node

import chalk from "chalk";
import figlet from "figlet";
import yargs from "yargs";

import build from "./commands/build";
import init from "./commands/init";
import run from "./commands/run";
import exec from "./commands/exec";
import { ValidationError } from "@datayoga-io/blocks-node";

yargs.usage(chalk.white.bold(figlet.textSync("DataYoga CLI")));
try {
  yargs
    .command({
      command: "init <project>",
      describe: "Scaffold a new folder with all the subdirectories",
      handler: async (argv: any) => {
        await init(argv);
      },
    })
    .command(
      "exec <pipeline>",

      "Deploy and execute a pipeline from dist folder against a running datayoga_spark_runner container (dy-runner-spark)",
      (yargs: yargs.Argv) => {
        yargs
          .positional("pipeline", {
            describe: "Pipeline id to run",
            type: "string",
          })
          .option({
            host: {
              describe: "Host",
              default: "localhost",
              type: "string",
            },
            port: {
              describe: "Port",
              default: 8998,
              type: "number",
            },
            runner: {
              describe: "Runner to use",
              default: "js",
              type: "string",
            },
            loglevel: {
              describe: "Logging level",
              default: "INFO",
              choices: ["INFO", "ERROR", "DEBUG", "TRACE", "WARNING"],
              type: "string",
            },
          });
      },
      async (argv: any) => {
        await exec(argv);
      }
    )
    .command(
      "run <pipeline>",

      "Build and execute the pipeline against a running datayoga_spark_runner container (dy-runner-spark)",
      (yargs: yargs.Argv) => {
        yargs
          .positional("pipeline", {
            describe: "pipeline id to run",
            type: "string",
          })
          .option({
            host: {
              describe: "Host",
              default: "localhost",
              type: "string",
            },
            port: {
              describe: "Port",
              default: 8998,
              type: "number",
            },
            local: {
              describe: "whether to run locally or remote",
              default: true,
              type: "boolean",
            },
            loglevel: {
              describe: "Logging level",
              default: "INFO",
              choices: ["INFO", "ERROR", "DEBUG", "TRACE", "WARNING"],
              type: "string",
            },
          });
      },
      async (argv: any) => {
        await run(argv);
      }
    )
    .command({
      command: "validate",
      describe: "Run against a local livy and validate the code is correct",
      builder: {
        host: {
          describe: "Host",
          default: "localhost",
          type: "string",
        },
        port: {
          describe: "Port",
          default: 8998,
          type: "number",
        },
      },
      handler: (argv: any) => {
        console.log(`Validate: ${argv.host}, ${argv.port} \n`);
      },
    })
    .command({
      command: "build [pipeline]",
      describe:
        "Builds a yaml and all its dependencies into a dist folder or tar file",
      builder: {
        clean: {
          describe: "clear the dist folder",
          default: false,
          type: "boolean",
        },
        runner: {
          describe: "runner to use as a runtime target. e.g. pyspark",
          default: "pyspark",
          type: "string",
        },
        console: {
          describe: "print generated code to console",
          default: false,
          type: "boolean",
        },
      },
      handler: async (argv: any) => {
        await build(argv);
      },
    })
    .showHelpOnFail(true)
    .demandCommand(1, "")
    .fail(function (msg, err, yargs) {
      if (err && process.env.NODE_ENV == "development") throw err; // preserve stack
      if (err instanceof ValidationError) {
        if (err.blockId) {
          console.log(`Error while validating step ${err.blockId}:`);
        } else {
          console.log("Validation error:");
        }
        console.error(err.message);
      } else if (err) {
        console.log("Error while running command:");
        console.error(err.message);
      } else if (msg) {
        console.error(msg);
        console.error("Use --help for command reference");
      } else {
        console.log(yargs.help());
      }
      process.exit(1);
    })
    .strict().argv;
} catch (e) {
  console.log(e);
}

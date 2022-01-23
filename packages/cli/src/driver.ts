#!/usr/bin/env node
// this class is needed to spawn node with support for global packages
// needed for dy-js-runner so TypeORM can find globally installed drivers
import * as path from "path";
import * as os from "os";
const { exec } = require("child_process");
const { promisify } = require("util");

async function main() {
  let execAsync = promisify(exec);
  let { stdout: globalPath } = await execAsync("npm root -g");
  // remove trailing newline
  globalPath = globalPath.replace("\n", "");
  const fileDelimiter = os.platform().startsWith("win") ? ";" : ":";
  var spawn = require("child_process").spawn;
  spawn("node", [path.join(__dirname, "index.js"), ...process.argv.slice(2)], {
    env: {
      ...process.env,
      // add the global path for database drivers
      // add the datayoga node modules to find the js-runner without installing globally
      NODE_PATH: `${globalPath}${fileDelimiter}${path.join(
        globalPath,
        "@datayoga-io",
        "datayoga",
        "node_modules"
      )}`,
      NODE_ENV: "production",
    },
    stdio: "inherit",
  });
}
main();

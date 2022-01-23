// this class is needed to spawn node with support for global packages
// needed for dy-js-runner so TypeORM can find globally installed drivers
import * as path from "path";
const { exec } = require("child_process");
const { promisify } = require("util");

async function main() {
  let execAsync = promisify(exec);
  let { stdout: globalPath } = await execAsync("npm root -g");
  var spawn = require("child_process").spawn;
  spawn("node", [path.join(__dirname, "index.js"), ...process.argv.slice(2)], {
    env: {
      ...process.env,
      NODE_PATH: globalPath.replace("\n", ""),
      NODE_ENV: "production",
    },
    stdio: "inherit",
  });
}
main();

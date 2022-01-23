import path from "path";
import * as services from "../services";
import * as utils from "../common/utils";
import Logger from "../common/logger";

async function exec(argv: any) {
  const logger = new Logger();
  const pipelineName = argv.pipeline;
  const distDir = utils.getDistDir();
  const dataDir = utils.toPosix(
    path.join(utils.getDyFolderRoot(path.resolve(".")), "data")
  );
  // fetch extra arguments
  const args = utils.parseExtraArgs();
  if (argv.runner == "js" || argv.runner == "nodejs") {
    // execute locally using js
    await services.executeLocal({
      pipelineName,
      loglevel: argv.loglevel,
      distDir,
      dataDir,
      logger,
      args,
    });
  } else {
    await services.deploy({
      distDir,
      host: argv.host,
    });

    await services.execute({
      pipelineName,
      loglevel: argv.loglevel,
      host: argv.host,
      port: argv.port,
      logger,
    });
  }
}

export default exec;

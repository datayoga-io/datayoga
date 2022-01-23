import path from "path";
import * as services from "../services";
import * as utils from "../common/utils";
import Logger from "../common/logger";

async function run(argv: any) {
  const logger = new Logger();
  const { moduleName, pipelineName } = utils.getPipelineProps(argv.pipeline);
  const distDir = utils.getDistDir();

  // build the catalog
  await services.buildCatalog(logger, distDir);

  // build the pipeline
  const { filename, code, runner } = await services.build(
    moduleName,
    pipelineName,
    distDir
  );

  if (argv.local) {
    // fetch extra arguments
    const args = utils.parseExtraArgs();

    const dataDir = utils.toPosix(
      path.join(utils.getDyFolderRoot(path.resolve(".")), "data")
    );

    await services.executeLocal({
      pipelineName: argv.pipeline,
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
      pipelineName: argv.pipeline,
      loglevel: argv.loglevel,
      host: argv.host,
      port: argv.port,
      logger,
    });
  }
}

export default run;

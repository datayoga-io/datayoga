import fse, { copySync } from "fs-extra";
import path from "path";
import * as services from "../services";
import * as utils from "../common/utils";
import Logger from "../common/logger";
import * as glob from "fast-glob";
import { pipeline } from "form-data";

async function build(argv: any) {
  const logger = new Logger();
  const distDir = utils.getDistDir();
  const pipelinesDir = path.join(distDir, "pipelines");
  if (argv.clean && fse.existsSync(pipelinesDir))
    fse.emptyDirSync(pipelinesDir);

  // copy the catalog
  await services.buildCatalog(logger, distDir);
  if (argv.pipeline) {
    await buildPipeline(argv.pipeline, logger, distDir, argv.console);
  } else {
    logger.info("building all pipelines");
    const rootDir = utils.getDyFolderRoot(path.resolve("."));
    const pipelinesDir = utils.toPosix(path.join(rootDir, "src", "pipelines"));
    const ymlFiles = glob.sync(
      `${utils.toPosix(pipelinesDir)}/**/*.+(yaml|yml)`,
      {
        caseSensitiveMatch: false,
      }
    );

    let builtFiles = 0,
      errorFiles = 0;
    for (const ymlFile of ymlFiles) {
      const pipeline = ymlFile
        .replace(pipelinesDir, "")
        .replace(/\//g, ".")
        .substring(1)
        .split(".")
        .slice(0, -1)
        .join(".");
      try {
        await buildPipeline(pipeline, logger, distDir, argv.console);
        builtFiles++;
      } catch (e) {
        logger.error(`${pipeline} failed: ${e}`);
        errorFiles++;
      }
    }

    logger.info(
      `built ${builtFiles} files successfully, ${errorFiles} files skipped with errors`
    );
  }
}

async function buildPipeline(
  pipeline: string,
  logger: Logger,
  distDir: string,
  toConsole: boolean = false
) {
  // copy the catalog
  await services.buildCatalog(logger, distDir);
  process.stdout.write(`building ${pipeline}...`);
  const { moduleName, pipelineName } = utils.getPipelineProps(pipeline);
  try {
    // build the pipeline
    const { filename, code } = await services.build(
      moduleName,
      pipelineName,
      distDir
    );
    logger.success("done");
    logger.info(`${pipeline} - code rendered to ${filename}`);
    if (toConsole) {
      logger.info(`Code of ${pipeline}:`);
      logger.info(`${code}`);
    }
  } catch (e) {
    logger.error("failed");
    throw e;
  }
}

export default build;

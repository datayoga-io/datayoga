import fse, { copySync } from "fs-extra";
import delay from "delay";
import Logger from "./common/logger";
import axios, { AxiosError } from "axios";
import * as utils from "./common/utils";
import fs from "fs";
import path from "path";
import yaml from "js-yaml";
import { Runner } from "@datayoga-io/dy-js-runner";

export async function build(
  moduleName: string,
  pipelineName: string,
  distDir: string
): Promise<{
  filename: string;
  code: string;
  runner: string;
}> {
  const { code, runner } = utils.renderPipeline(moduleName, pipelineName);
  const pipelineDir = path.join(distDir, "pipelines", ...moduleName.split("."));
  fs.mkdirSync(pipelineDir, { recursive: true });

  // find the extension based on the runner properties
  const runnerInstance = utils.getRunner(runner);
  const extension = runnerInstance.properties.extension;
  const pipelineFileName = path.join(
    pipelineDir,
    pipelineName + "." + extension
  );
  fs.writeFileSync(pipelineFileName, code);
  fs.closeSync(fs.openSync(path.join(pipelineDir, "__init__.py"), "w"));

  return { filename: pipelineFileName, code: code, runner: runner };
}

export async function buildCatalog(logger: Logger, distDir: string) {
  // building of the catalog is essentially copying all the catalog files to dist
  // TODO: run validations on the catalog yaml
  const catalogDir = utils.getCatalogDir();
  process.stdout.write(`building catalog...`);
  const targetDir = path.join(distDir, "catalog");
  await copySync(catalogDir, targetDir);
  logger.success("done");
}

export async function deploy({
  distDir,
  host,
}: {
  distDir: string;
  host: string;
}) {
  // create zip bundles
  const pipelineDir = utils.toPosix(path.join(distDir, "pipelines"));
  const catalogDir = utils.toPosix(path.join(distDir, "catalog"));
  const pipelineZipFile = await utils.zipDir(pipelineDir, distDir);
  const catalogZipFile = await utils.zipDir(catalogDir, distDir);

  // deploy jobs artifact
  process.stdout.write("deploying pipelines...");
  await utils.uploadFile(pipelineZipFile, host);
  console.log("done");

  // deploy catalog
  process.stdout.write("deploying catalog...");
  await utils.uploadFile(catalogZipFile, host);
  console.log("done");

  // deploy env file
  process.stdout.write("deploying env...");
  await utils.uploadFile(path.join(distDir, "..", "env.yaml"), host);
  console.log("done");

  // deploy blocks python lib
  process.stdout.write("deploying dy libs...");
  await utils.uploadFile(
    require.resolve("@datayoga-io/blocks-node/assets/pyspark_dy_lib.zip"),
    host
  );
  console.log("done");
}

export async function execute({
  pipelineName,
  loglevel,
  host,
  port = 8998,
  logger,
}: {
  pipelineName: string;
  loglevel: string;
  host: string;
  port?: number;
  logger: Logger;
}) {
  try {
    const { data: batchResponse } = await axios.post(
      `http://${host}:${port}/batches`,
      {
        file: "/opt/dy/lib/job_runner.py",
        args: [
          "--job",
          pipelineName.replace(/\./g, "/"),
          "--log-level",
          loglevel,
        ],
        pyFiles: ["/opt/dy/lib/packages/*", "/opt/dy/lib/pyspark_dy_lib.zip"],
      }
    );

    const batchId = batchResponse.id;
    let batchState = batchResponse.state;
    let offset = 1;

    // handle ctrl+c to stop the remote job
    // TODO: add a -d flag for daemon
    process.on("SIGINT", async function () {
      console.log("\nstopping job");
      const { data: AxiosResponse } = await axios.delete(
        `http://${host}:${port}/batches/${batchId}`
      );
      process.exit();
    });

    while (batchState === "starting" || batchState === "running") {
      const { data: batchData } = await axios.get(
        `http://${host}:${port}/batches/${batchId}/state`
      );
      batchState = batchData.state;

      offset += await utils.fetchLogs(host, port, batchId, offset, logger);
      await delay(100);
    }
    // fetch logs one final time
    await utils.fetchLogs(host, port, batchId, offset, logger);

    logger.info(`Batch ${batchId}: ${batchState}`);
    if (batchState !== "success") throw new Error("Batch failed");
  } catch (err) {
    if (axios.isAxiosError(err)) {
      // handle specific errors
      if ((err as AxiosError).code == "ECONNREFUSED") {
        throw new Error(
          `can not connect to runner at ${host}:${port}. Make sure the container or remote service is running`
        );
      }
    }
    throw err;
  }
}
export async function executeLocal({
  pipelineName,
  loglevel,
  distDir,
  dataDir,
  logger,
  args,
}: {
  pipelineName: string;
  loglevel: string;
  distDir: string;
  dataDir: string;
  logger: Logger;
  args: { [name: string]: string };
}) {
  // dynamically load the class
  const extension = "js";
  let pipelineModule = pipelineName.split(".");
  let pipelineFileName = pipelineModule.pop();

  const pipelineDir = utils.toPosix(
    path.join(distDir, "pipelines", ...pipelineModule)
  );
  const pipelineFullFileName = path.join(
    pipelineDir,
    pipelineFileName + "." + extension
  );
  const catalogDir = utils.toPosix(path.join(distDir, "catalog"));

  // load the entire catalog to memory into a dict. TODO: optimize into an JSSQL file?
  let catalogMap = new Map();
  fs.readdirSync(catalogDir)
    .filter((file) => path.extname(file) == ".yaml")
    .map((file) => {
      const doc: { [source: string]: any } = <{ [source: string]: any }>(
        yaml.load(fs.readFileSync(catalogDir + path.sep + file, "utf8"))
      );
      doc.forEach((source: any) =>
        catalogMap.set(
          `${
            path.basename(file, "yaml") == "default"
              ? ""
              : path.basename(file, "yaml")
          }${source.id}`,
          source
        )
      );
    });
  const env: any = yaml.load(
    fs.readFileSync(path.join(distDir, "..", "env.yaml"), "utf8")
  );
  env.folders = {
    data: dataDir,
  };
  const pipeline = require(pipelineFullFileName);
  const runner = new Runner(catalogMap, env);
  pipeline.run(runner, args);
}

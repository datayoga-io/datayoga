import Logger from "./logger";
import axios, { AxiosError } from "axios";
import formdata from "form-data";
import archiver from "archiver";
import {
  jobRenderer,
  runners,
  models,
  ValidationError,
} from "@datayoga-io/blocks-node";
import { Pipeline } from "@datayoga-io/shared";
import yaml from "js-yaml";
import * as glob from "fast-glob";
import fs from "fs";
import path from "path";

export function getPipelineProps(job: string) {
  const parts = job.split(".");

  return {
    moduleName:
      parts.length === 1 ? "." : parts.slice(0, parts.length - 1).join("."),
    pipelineName: parts.length === 1 ? job : parts[parts.length - 1],
  };
}

export function toPosix(inputPath: string) {
  return inputPath.split(path.sep).join(path.posix.sep);
}

export function getDistDir(): string {
  const rootDir = getDyFolderRoot(path.resolve("."));
  const dir = toPosix(path.join(rootDir, "dist"));
  return dir;
}

export function getCatalogDir(): string {
  const rootDir = getDyFolderRoot(path.resolve("."));
  const dir = toPosix(path.join(rootDir, "src", "catalog"));
  return dir;
}

/**
 * Find the root of the DataYoga project
 * @summary find root dir by traversing upwards until we find an .dyrc file or root folder
 * @param {folder} string - Current folder
 * @return {string} Full path of DataYoga project root. Throws Error if not a DataYoga folder
 */
export function getDyFolderRoot(folder: string): string {
  let folderTree = folder.split(path.sep);
  let found = false;
  const pathPrefix = process.platform === "win32" ? "" : path.sep;
  while (!found && folderTree.length > 0) {
    if (fs.existsSync(path.join(pathPrefix, ...folderTree, ".dyrc"))) {
      found = true;
    } else {
      folderTree.pop();
    }
  }
  if (!found) {
    throw new Error(
      "can not find .dyrc or not a datayoga folder. Try running from within a datayoga folder"
    );
  }
  return toPosix(path.join(pathPrefix, ...folderTree));
}

/**
 * Render a pipeline into a runnable code artifact
 * @param {moduleName} string - Name of module (corresponds to subfolder in jobs folder)
 * @param {pipelineName} string - Name of pipeline
 * @param {runner} string - Runner to use to render the code
 */
export function renderPipeline(
  moduleName: string,
  pipelineName: string
): { code: string; runner: string } {
  const src = readPipeline(moduleName, pipelineName);
  jobRenderer.validate(src);
  const pipeline = <Pipeline>src;

  // determine the target runner
  let runner = getRunner(Object.values(pipeline.jobs)[0].runs_on);

  // TODO: handle multi-job pipelies
  const job = Object.values((<Pipeline>src).jobs)[0];
  const { blocks, links } = jobRenderer.jobStepsToGraph(job.steps);
  const formattedCode = jobRenderer.renderCode(
    blocks,
    links,
    runner.properties.name
  );
  return {
    code: formattedCode,
    runner: runner.properties.name,
  };

  /**
   * Read a job yaml file
   * @summary Locates the yaml file based on <module name>.<yaml name> notation. Does not validate.
   * @param {moduleName} string - module name
   * @param {jobName} string - job name
   * @return {object} Parsed JSON as javascript object
   */
}
function readPipeline(moduleName: string, pipelineName: string): object {
  const rootDir = getDyFolderRoot(path.resolve("."));
  const jobsDir = toPosix(path.join(rootDir, "src", "pipelines"));
  const ymlFiles = glob.sync(
    `${toPosix(jobsDir)}/${
      moduleName === "." ? moduleName : moduleName.replace(/\./g, "/")
    }/${pipelineName}.+(yaml|yml)`,
    { caseSensitiveMatch: false }
  );

  if (ymlFiles.length === 0)
    throw new Error(
      `cannot find source yaml file for ${
        moduleName === "." ? "" : moduleName + "."
      }${pipelineName}`
    );

  if (ymlFiles.length > 1)
    console.warn(
      `Found ${ymlFiles.length} matching files, taking the first one`
    );

  return <object>yaml.load(fs.readFileSync(ymlFiles[0], "utf8"));
}

export async function zipDir(directory: string, destination: string) {
  const zipFile = toPosix(
    path.join(destination, `${directory.match(/([^\/]*)\/*$/)![1]}.zip`)
  );
  const archive = archiver("zip");
  archive.pipe(fs.createWriteStream(zipFile));
  archive.directory(directory, false);

  await archive.finalize();
  return zipFile;
}

// upload file using HTTP to embedded upload server in livy
// for non-local deployments, this needs to be adapted to upload artifacts to S3 etc
export async function uploadFile(file: string, host: string, port = 8000) {
  const form = new formdata();
  form.append("files", fs.createReadStream(file), {
    knownLength: fs.statSync(file).size,
  });
  const headers = {
    ...form.getHeaders(),
    "Content-Length": form.getLengthSync(),
  };

  try {
    await axios.post(`http://${host}:${port}/upload`, form, {
      headers,
      timeout: 10000,
    });
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

// fetchLogs, write to logger, and return the number of logs fetched (for offset management)
// TODO: move to a Runner interface
export async function fetchLogs(
  host: string,
  port: number,
  batchId: number,
  offset: number,
  logger: Logger
): Promise<number> {
  const { data: batchLogs } = await axios.get(
    `http://${host}:${port}/batches/${batchId}/log?from=${offset}&size=100000`
  );

  const logs: string[] = batchLogs.log.filter(
    (log: string) => !log.includes("stdout:") && !log.includes("stderr:")
  );
  logs.forEach((log) => {
    if (log.includes("WARN")) logger.warn(log);
    else if (log.includes("ERROR")) logger.error(log);
    else logger.info(log);
  });
  return logs.length;
}

export function getRunner(runnerName: string) {
  // search the runners in case this is an alias
  let runner: models.Runner;
  if (!(runnerName in runners)) {
    for (let runnerInstance of Object.values(runners)) {
      if (runnerInstance.properties.aliases.includes(runnerName)) {
        runner = runnerInstance;
        return runnerInstance;
      }
    }
    // this is an unknown runner
    throw new ValidationError(`Unknown runner ${runnerName}`);
  } else return runners[runnerName];
}

export function parseExtraArgs() {
  // we get an array. and convert to an object where each input is its own name
  const firstIndex = process.argv.findIndex((arg) => arg == "--");
  if (firstIndex == -1) return {};

  return process.argv
    .slice(firstIndex + 1)
    .reduce((map: { [name: string]: string }, obj) => {
      let [key, value] = obj.split("=");
      key = key.replace("--", "");
      map[key] = value;
      return map;
    }, {});
}

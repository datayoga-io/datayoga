import { CodeTemplate } from "@/core/template";
import BlockType from "@/models/BlockType";
import Runner from "@/models/Runner";
import * as glob from "fast-glob";
import fs from "fs";
import path from "path";
import ValidationError from "./core/ValidationError";
import * as yaml from "js-yaml";

const blockDescriptors: { [component: string]: BlockType } = {};
const jsonSchemas: { [component: string]: object } = {};

//
// load default templates
//
const runners: {
  [runner: string]: Runner;
} = {};

const runnersFolder = path.join(module.path, "..", "assets", "runners");
const allRunnerFolders = fs
  .readdirSync(runnersFolder, { withFileTypes: true })
  .filter((dirent) => dirent.isDirectory());

const allRunners = allRunnerFolders.map((f) => f.name);

// load the runners
for (const runnerFolder of allRunnerFolders) {
  const templatePath = path.join(runnersFolder, runnerFolder.name);
  // default template
  const defaultTemplateText = fs.readFileSync(
    path.join(templatePath, "default.code.template"),
    "utf8"
  );
  // init template
  const initTemplateText = fs.readFileSync(
    path.join(templatePath, "init.template"),
    "utf8"
  );
  // alter schema template
  const alterSchemaTemplateText = fs.readFileSync(
    path.join(templatePath, "alter.schema.template"),
    "utf8"
  );
  // trace template
  const traceTemplateText = fs.readFileSync(
    path.join(templatePath, "trace.template"),
    "utf8"
  );
  // default sql template
  const defaultSqlTemplateText = fs.readFileSync(
    path.join(templatePath, "default.sql.template"),
    "utf8"
  );
  // runner properties
  const runnerProps = yaml.load(
    fs.readFileSync(path.join(templatePath, "runner.yaml"), "utf8")
  );

  runners[runnerFolder.name] = {
    defaultBlockTemplate: new CodeTemplate(defaultTemplateText),
    defaultSqlTemplate: new CodeTemplate(defaultSqlTemplateText),
    traceTemplate: new CodeTemplate(traceTemplateText),
    initTemplate: new CodeTemplate(initTemplateText),
    alterSchemaTemplate: new CodeTemplate(alterSchemaTemplateText),
    properties: runnerProps,
  };
}

//
// traverse the block type tree and load the components
// .json - config file
// .template - render template for python code
//
const assetsDir = path.join(module.path, "..", "assets", "blocks");
const allBlockFolders = fs
  .readdirSync(assetsDir, { withFileTypes: true })
  .filter((dirent) => dirent.isDirectory());

// loop over the block folders
for (const blockFolder of allBlockFolders) {
  // read descriptor.json
  const componentConfig = JSON.parse(
    fs.readFileSync(
      path.join(assetsDir, blockFolder.name, "descriptor.json"),
      "utf8"
    )
  );
  const blockType = componentConfig["type"];
  // TODO: validate against a json schema
  blockDescriptors[blockType] = new BlockType(componentConfig);

  //
  // load the template files from the blocks folder and add to the dict
  //
  for (let runner of allRunners) {
    // check if this runner uses a code.template
    const templatePath = path.join(
      assetsDir,
      blockFolder.name,
      runner,
      "code.template"
    );
    if (fs.existsSync(templatePath)) {
      const templateText = fs.readFileSync(templatePath, "utf8");
      blockDescriptors[blockType].codeTemplates[runner] = new CodeTemplate(
        templateText
      );
    } else {
      // find all of the sql templates. there may be more than one for blocks with multiple outputs
      const sqlTemplates = fs
        .readdirSync(path.join(assetsDir, blockFolder.name))
        .filter((dirent) => dirent.indexOf("sql.template") > -1);

      // check if there are sql templates
      if (sqlTemplates.length > 0) {
        for (let sqlTemplate of sqlTemplates) {
          const templateText = fs.readFileSync(
            path.join(assetsDir, blockFolder.name, sqlTemplate),
            "utf8"
          );
          // name of the output is the prefix before sql.template
          const outputName = path.basename(sqlTemplate).split(".")[0];
          blockDescriptors[blockType].sqlCodeTemplates[
            outputName
          ] = new CodeTemplate(templateText);
        }
        blockDescriptors[blockType].codeTemplates[runner] =
          runners[runner].defaultSqlTemplate;
      } else {
        // use the default template
        blockDescriptors[blockType].codeTemplates[runner] =
          runners[runner].defaultBlockTemplate;
      }
    }
  }

  //
  // load json schema
  //
  const jsonSchemaPath = path.join(
    assetsDir,
    blockFolder.name,
    "block.schema.json"
  );
  if (fs.existsSync(jsonSchemaPath)) {
    const jsonSchema = JSON.parse(
      fs.readFileSync(
        path.join(assetsDir, blockFolder.name, "descriptor.json"),
        "utf8"
      )
    );
    jsonSchemas[blockType] = jsonSchema;
  }
}

export { blockDescriptors as blockTypes };
export { runners };
export { jsonSchemas };
export { default as jobRenderer } from "./core/jobRenderer/index";
import * as models from "./models/index";
export { models };
export { ValidationError };

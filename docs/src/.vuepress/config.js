const path = require("path");
const { description } = require("../../package");
const { readdirSync, existsSync } = require("fs-extra");
const blocksFolder = "../blocks";
blockFiles = {};
readdirSync(blocksFolder).forEach((blockFolder) => {
  // first we load all of the descriptor.json files
  const componentConfig = require(path.resolve(
    blocksFolder,
    blockFolder,
    "descriptor.json"
  ));
  blockFiles[componentConfig.type] = path.resolve(
    blocksFolder,
    blockFolder,
    "block.schema.json"
  );
});

let config = {
  /**
   * Ref：https://v1.vuepress.vuejs.org/config/#title
   */
  title: "DataYoga documentation",
  /**
   * Ref：https://v1.vuepress.vuejs.org/config/#description
   */
  base: "/docs/",
  description: description,
  dest: "src/.vuepress/dist/docs",
  /**
   * Extra tags to be injected to the page HTML `<head>`
   *
   * ref：https://v1.vuepress.vuejs.org/config/#head
   */
  head: [
    ["meta", { name: "theme-color", content: "#3eaf7c" }],
    ["meta", { name: "apple-mobile-web-app-capable", content: "yes" }],
    [
      "meta",
      { name: "apple-mobile-web-app-status-bar-style", content: "black" },
    ],
  ],

  /**
   * Theme configuration, here is the default theme configuration for VuePress.
   *
   * ref：https://v1.vuepress.vuejs.org/theme/default-theme-config.html
   */

  themeConfig: {
    repo: "",
    editLinks: false,
    docsDir: "",
    editLinkText: "",
    lastUpdated: false,
    nav: [
      {
        text: "Guide",
        link: "/guide/",
      },
      {
        text: "Reference",
        link: "/reference/",
      },
      {
        text: "Github",
        link: "https://github.com/datayoga-io/datayoga",
      },
    ],
    // displayAllHeaders: true,
    sidebar: {
      "/guide/": [
        {
          title: "Guide",
          collapsable: false,
          children: [
            "",
            "installation",
            "folder-structure",
            "deploying",
            // { type: "group", title: "children1", children: ["using-vue"] },
          ],
        },
      ],
      "/reference/": [
        {
          title: "Commands",
          collapsable: false,
          children: ["CLI.md"],
        },
        {
          title: "Job file reference",
          collapsable: false,
          children: ["schemas/job", "schemas/block"],
        },
      ],
    },
  },

  /**
   * Apply plugins，ref：https://v1.vuepress.vuejs.org/zh/plugin/
   */
  plugins: ["@vuepress/plugin-back-to-top", "@vuepress/plugin-medium-zoom"],
};

let schema2mdPages = {
  "/reference/schemas/job.html": {
    schemaPath: "../packages/shared/jsonschema/job.schema.json",
    schemaMarkdown: "./src/config/job_schema.md",
    outputPath: "./src/reference/schemas/job.md",
  },
  "/reference/schemas/block.html": {
    schemaPath: "../packages/shared/jsonschema/step.schema.json",
    schemaMarkdown: "./src/config/job_schema.md",
    outputPath: "./src/reference/schemas/block.md",
  },
};

//
// add the blocks reference sidebar
//
blockPages = [];
for (const [key, value] of Object.entries(blockFiles)) {
  if (existsSync(value)) {
    const blockPage = `/reference/schemas/blocks/${key}.html`;
    schema2mdPages[blockPage] = {
      schemaPath: value,
      outputPath: `./src/reference/schemas/blocks/${key}.md`, // You shouldn't commit this file.
    };
    blockPages.push(blockPage);
  }
}
config.plugins.push(["schema2md", { write: false, pages: schema2mdPages }]);

config.themeConfig.sidebar["/reference/"].push({
  title: "Blocks",
  collapsable: true,
  sidebarDepth: 1,
  children: [...blockPages],
});
module.exports = config;

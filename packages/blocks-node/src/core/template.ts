import * as Handlebars from "handlebars";

export class CodeTemplate {
  template: Handlebars.TemplateDelegate;
  constructor(template: string) {
    // register template helpers
    Handlebars.registerHelper(
      "input",
      function (id: string, options: Handlebars.HelperOptions) {
        return options.data.root.inputs[id];
      }
    );

    Handlebars.registerHelper(
      "output",
      function (id: string, options: Handlebars.HelperOptions) {
        return options.data.root.outputs[id];
      }
    );

    Handlebars.registerHelper(
      "sqloutput",
      function (id: string, options: Handlebars.HelperOptions) {
        return options.data.root.sqls[id];
      }
    );

    // register template helpers
    Handlebars.registerHelper(
      "sqlinput",
      function (id: string, options: Handlebars.HelperOptions) {
        return `({${options.data.root.inputs[id]}}) ${options.data.root.inputs[id]}`;
      }
    );

    Handlebars.registerHelper(
      "indent",
      function (this: Handlebars.HelperDelegate, text: string) {
        const indent = "    ";
        if (text) {
          const lines = text.split(/\r?\n/);
          return lines.map((l) => indent + l).join("\n");
        } else return null;
      }
    );

    Handlebars.registerHelper(
      "columnNames",
      function (this: Handlebars.HelperDelegate, obj: any) {
        const val = obj;
        const retVal: string = [...val.matchAll(/F\.col\(['"](.*?)['"]\)/g)]
          .map((x) => x[1])
          .join(", ");
        return new Handlebars.SafeString(retVal);
      }
    );

    Handlebars.registerHelper("switch", (value: string, options: any) => {
      (this as any).switch_value = value;
      return options.fn(this);
    });

    Handlebars.registerHelper("case", (value: string, options: any) => {
      if (value == (this as any).switch_value) {
        return options.fn(this);
      }
    });

    Handlebars.registerHelper(
      "coalesce",
      function (
        val: string | null,
        defaultValue: string,
        options: Handlebars.HelperOptions
      ) {
        return val || defaultValue;
      }
    );

    Handlebars.registerHelper(
      "default",
      function (value: string, options: any) {
        return true; ///We can add condition if needs
      }
    );

    Handlebars.registerHelper(
      "comment",
      function (
        this: Handlebars.HelperDelegate,
        text: string,
        language: string
      ) {
        if (language == null || typeof language == "undefined") {
          language = "python";
        }
        const commentPrefix = language == "python" ? `# ` : `// `;
        return text ? text.replace(/^/gm, commentPrefix) : "";
      }
    );

    Handlebars.registerHelper(
      "str",
      function (
        this: Handlebars.HelperDelegate,
        text: string | number,
        language: string | undefined
      ) {
        if (language == null || typeof language == "undefined") {
          language = "python";
        }
        const doubleQuot = language == "python" ? `"""` : "`";
        if (typeof text == "undefined") return `""`;

        return typeof text == "string" && text.match(/(\r\n|\n|\r)/)
          ? `${doubleQuot}${text}${doubleQuot}`
          : `"${text}"`;
      }
    );

    Handlebars.registerHelper(
      "columnList",
      function (
        this: Handlebars.HelperDelegate,
        columns: { name: string; rename: string }[]
      ) {
        return columns
          .map(
            (column) =>
              column.name +
              (column.rename !== null && column.rename !== undefined
                ? ` as "${column.rename}"`
                : "")
          )
          .join(", ");
      }
    );

    Handlebars.registerHelper(
      "fstr",
      function (this: Handlebars.HelperDelegate, text: string) {
        return text == null || typeof text == "undefined"
          ? '""'
          : typeof text == "string" && text.match(/(\r\n|\n|\r)/)
          ? `f"""${text}"""`
          : `f"${text}"`;
      }
    );

    Handlebars.registerHelper(
      "list",
      function (this: Handlebars.HelperDelegate, obj: any, defaultValue: any) {
        return obj.join(",");
      }
    );
    Handlebars.registerHelper(
      "json",
      function (this: Handlebars.HelperDelegate, obj: any, defaultValue: any) {
        if (!obj)
          return defaultValue && typeof defaultValue == "string"
            ? defaultValue
            : "{}";
        // pretty print and change false to False to match the python dict format
        return new Handlebars.SafeString(
          JSON.stringify(
            obj,
            (k, v) =>
              typeof v === "boolean" ? (v ? "__TRUE__" : "__FALSE__") : v,
            2
          )
            .replace(/"__TRUE__"/g, "True")
            .replace(/"__FALSE__"/g, "False")
        );
      }
    );

    this.template = Handlebars.compile(template, {
      noEscape: true,
      strict: false,
    });
  }
  render(values = {}) {
    return this.template(values);
  }
}

export class SqlTemplate {
  template: Handlebars.TemplateDelegate;
  constructor(template: string) {
    // register template helpers
    Handlebars.registerHelper(
      "step",
      function (id: string, options: Handlebars.HelperOptions) {
        return options.data.root.inputs[id];
      }
    );

    Handlebars.registerHelper(
      "args",
      function (id: string, options: Handlebars.HelperOptions) {
        return ":" + id;
      }
    );

    Handlebars.registerHelper(
      "arg",
      function (id: string, options: Handlebars.HelperOptions) {
        return ":" + id;
      }
    );

    Handlebars.registerHelper("switch", (value: string, options: any) => {
      (this as any).switch_value = value;
      return options.fn(this);
    });

    Handlebars.registerHelper("case", (value: string, options: any) => {
      if (value == (this as any).switch_value) {
        return options.fn(this);
      }
    });

    Handlebars.registerHelper(
      "default",
      function (value: string, options: any) {
        return true; ///We can add condition if needs
      }
    );

    this.template = Handlebars.compile(template, {
      noEscape: true,
      strict: false,
    });
  }
  render(values = {}) {
    return this.template(values);
  }
}

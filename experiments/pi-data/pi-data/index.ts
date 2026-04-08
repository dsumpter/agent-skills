import type { Extension } from "@mariozechner/pi-agent-core";

import { dataDescribe, dataListTables, dataSearch } from "./src/tools/catalog.js";
import { dataAggregate, dataExport, dataPreview, dataProfile } from "./src/tools/results.js";
import { dataExecute } from "./src/tools/sql.js";
import { dataSession } from "./src/tools/session.js";
import { dataValidate } from "./src/tools/validate.js";
import { dataWriteSql } from "./src/tools/write-sql.js";
import { dataDashboard, dataVizHtml, dataVizInline, dataVizSuggest } from "./src/tools/viz.js";
import { dataNbAddCell, dataNbCreate, dataNbExport, dataNbRunCell } from "./src/tools/notebook.js";
import { dataAnswer, dataPlan, dataSummarize } from "./src/tools/rlm.js";

const extension: Extension = {
  name: "pi-data",
  version: "0.1.0",
  tools: [
    dataSession,
    dataSearch,
    dataListTables,
    dataDescribe,
    dataWriteSql,
    dataValidate,
    dataExecute,
    dataPreview,
    dataProfile,
    dataAggregate,
    dataExport,
    dataVizSuggest,
    dataVizInline,
    dataVizHtml,
    dataDashboard,
    dataNbCreate,
    dataNbAddCell,
    dataNbRunCell,
    dataNbExport,
    dataPlan,
    dataSummarize,
    dataAnswer,
  ],
};

export default function createExtension(): Extension {
  return extension;
}

const getSqlEventParam = (
  paramName,
  paramType = "string",
  columnName = false
) => getSqlUnnestParam(paramName, paramType, columnName);

const getSqlUserProperty = (
  paramName,
  paramType = "string",
  columnName = false
) => getSqlUnnestParam(paramName, paramType, columnName, "user_properties");

const getSqlUnnestParam = (
  paramName,
  paramType = "string",
  resultColumnName = false,
  unnestColumnName = "event_params"
) => {
  let paramTypeName;
  const alias =
    resultColumnName === null
      ? ""
      : `AS ${resultColumnName ? resultColumnName : paramName} `;
  if (paramType.toLowerCase() === "coalesce") {
    return `(SELECT COALESCE(ep.value.string_value, SAFE_CAST(ep.value.int_value AS STRING), SAFE_CAST(ep.value.double_value AS STRING), SAFE_CAST(ep.value.float_value AS STRING)) FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}' LIMIT 1) ${alias}`;
  }
  switch (paramType.toLowerCase()) {
    case "string":
      paramTypeName = "string_value";
      break;
    case "int":
      paramTypeName = "int_value";
      break;
    case "double":
      paramTypeName = "double_value";
      break;
    case "float":
      paramTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT ep.value.${paramTypeName} FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}' LIMIT 1) ${alias}`;
};

const getSqlEventParams = (eventParams) => {
  const sql = eventParams.map((eventParam) =>
    getSqlEventParam(eventParam.name, eventParam.type, eventParam.columnName)
  );
  return eventParams.length > 0 ? sql.join(", ") : "";
};

const getSqlUserProperties = (userProperties) => {
  const sql = userProperties.map((userProperty) =>
    getSqlUserProperty(
      userProperty.name,
      userProperty.type,
      userProperty.columnName
    )
  );
  return userProperties.length > 0 ? sql.join(", ") : "";
};

const getSqlGetFirstNotNullValue = (
  paramName,
  columnName = false,
  orderBy = "event_timestamp"
) => {
  const alias =
    columnName === null ? "" : `AS ${columnName ? columnName : paramName} `;
  return `ARRAY_AGG(${paramName} IGNORE NULLS ORDER BY ${orderBy} LIMIT 1)[SAFE_OFFSET(0)] ${alias}`;
};

const getSqlGetFirstNotNullValues = (columns) => {
  const sql = columns.map((column) =>
    getSqlGetFirstNotNullValue(
      column.columnName ? column.columnName : column.name
    )
  );
  return columns.length > 0 ? sql.join(",") : "";
};

const getSqlColumns = (params) => {
  const sql = params.map(
    (param) =>
      `${param.name} as ${param.columnName ? param.columnName : param.name}`
  );
  return params.length > 0 ? sql.join(", ") : "";
};

const getSqlQueryParameter = (url, param) => {
  const sql = `REGEXP_EXTRACT(${url}, r'(?i).*[?&#]${param.name.toLowerCase()}=([^&#\?]*)') as ${
    param.columnName ? param.columnName : param.name
  }`;
  return sql;
};

const getSqlQueryParameters = (url, params) => {
  const sql = params.map((param) => getSqlQueryParameter(url, param));
  return params.length > 0 ? sql.join(", ") : "";
};

const getDateFromTableName = (tblName) => {
  return tblName.substring(7);
};

const getFormattedDateFromTableName = (tblName) => {
  return `${tblName.substring(7, 11)}-${tblName.substring(
    11,
    13
  )}-${tblName.substring(13)}`;
};

const getSqlList = (list) => {
  return `('${list.join("','")}')`;
};
const getSqlEventId = (timestampEventParamName) => {
  return `FARM_FINGERPRINT(CONCAT(ifnull((SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = '${timestampEventParamName}'),event_timestamp), event_name, user_pseudo_id)) as event_id`;
};

const getSqlSessionId = () => {
  return `FARM_FINGERPRINT(CONCAT((select value.int_value from unnest(event_params) where key = 'ga_session_id'), user_pseudo_id)) as session_id`;
};

const getSqlDate = (timezone = "Europe/London") =>
  `DATE(TIMESTAMP_MICROS(event_timestamp), "${timezone}") as date`;

function isStringInteger(str) {
  const num = Number(str);
  return Number.isInteger(num);
}

const getSqlSelectFromRow = (config) => {
  return Object.entries(config)
    .map(([key, value]) => {
      if (typeof value === "number") {
        return `${value} AS ${key}`;
      } else if (key === "date") {
        return `DATE '${value}' AS ${key}`;
      } else if (key === "session_start") {
        return `TIMESTAMP '${value}' AS ${key}`;
      } else if (key === "session_end") {
        return `TIMESTAMP '${value}' AS ${key}`;
      } else if (typeof value === "string") {
        if (key === "int_value") return `${parseInt(value)} AS ${key}`;
        if (key.indexOf("timestamp") > -1)
          return `${parseInt(value)} AS ${key}`;
        if (key === "float_value" || key === "double_value")
          return `${parseFloat(value)} AS ${key}`;
        return `'${value}' AS ${key}`;
      } else if (value === null) {
        return `${value} AS ${key}`;
      } else if (value instanceof Array) {
        return `[${getSqlSelectFromRow(value)}] AS ${key}`;
      } else {
        if (isStringInteger(key))
          return `STRUCT(${getSqlSelectFromRow(value)})`;
        else return `STRUCT(${getSqlSelectFromRow(value)}) AS ${key}`;
      }
    })
    .join(", ");
};

const getSqlUnionAllFromRows = (rows) => {
  try {
    const selectStatements = rows
      .map((data) => "SELECT " + getSqlSelectFromRow(data))
      .join("\nUNION ALL\n ");
    return selectStatements;
  } catch (err) {
    console.error("Error reading or parsing the file", err);
  }
};

const declareSources = ({
  database = dataform.projectConfig.defaultDatabase,
  dataset,
  incrementalTableName,
  nonIncrementalTableName = "events_*",
}) => {
  declare({
    database,
    schema: dataset,
    name: incrementalTableName,
  });

  declare({
    database,
    schema: dataset,
    name: nonIncrementalTableName,
  });
};

module.exports = {
  getDateFromTableName,
  getFormattedDateFromTableName,
  getSqlEventParam,
  getSqlEventParams,
  getSqlUserProperty,
  getSqlUserProperties,
  getSqlList,
  getSqlEventId,
  getSqlSessionId,
  getSqlDate,
  getSqlGetFirstNotNullValue,
  getSqlGetFirstNotNullValues,
  getSqlColumns,
  getSqlQueryParameter,
  getSqlQueryParameters,
  getSqlUnionAllFromRows,
  declareSources,
};

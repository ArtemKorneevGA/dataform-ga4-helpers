const getSqlEventParam = (
  paramName,
  paramType = "string",
  columnName = false
) => getUnnestParam(paramName, paramType, columnName);

const getSqlUserProperty = (
  paramName,
  paramType = "string",
  columnName = false
) => getUnnestParam(paramName, paramType, columnName, "user_properties");

const getUnnestParam = (
  paramName,
  paramType = "string",
  resultColumnName = false,
  unnestColumnName = "event_params"
) => {
  let paramTypeName = "";
  switch (paramType) {
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
  return `(SELECT ep.value.${paramTypeName} AS ${paramName} FROM UNNEST(${unnestColumnName}) ep WHERE ep.key = '${paramName}') AS ${
    resultColumnName ? resultColumnName : paramName
  }`;
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

module.exports = {
  getDateFromTableName,
  getFormattedDateFromTableName,
  getSqlEventParam,
  getSqlUserProperty,
  getSqlList,
  getSqlEventId,
  getSqlSessionId,
  getSqlDate,
};

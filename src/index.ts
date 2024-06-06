import mysql, {RowDataPacket} from 'mysql2/promise'; 
import { resolve } from 'path';
import { readFile } from 'fs/promises';
import { BadRequest, CapabilitiesResponse, CollectionInfo, ComparisonTarget, ComparisonValue, Connector, ConnectorError, ExplainResponse, Expression, MutationRequest, MutationResponse, NotSupported, ObjectType, OrderByElement, QueryRequest, QueryResponse, RowFieldValue, ScalarType, SchemaResponse, start } from "@hasura/ndc-sdk-typescript";

type Configuration = {
  host: string;
  port: number;
  db: string;
  user: string;
  password: string;
  schema: Schema;
  nativeQueries: { [k: string]: any };
};

type Schema = {
  scalar_types: { [typeName: string]: ScalarType };
  object_types: { [k: string]: ObjectType };
  collections: CollectionInfo[];
  functions: any[];
  procedures: any[];
};

type State = {
  db: mysql.Connection;
};


async function parseConfiguration(configurationDir: string): Promise<Configuration> {
    const configuration_file = resolve(configurationDir, 'config.json');
    const configuration_data = await readFile(configuration_file);
    const configuration = JSON.parse(configuration_data.toString());
    return {
        ...configuration
    };
}

async function fetchMetrics(configuration: Configuration, state: State): Promise<undefined> {
  throw new Error("Function not implemented.");
}

async function healthCheck(configuration: Configuration, state: State): Promise<undefined> {
  try {
    await state.db.query("SELECT 1");
  } catch (x) {
    throw new ConnectorError(503, "Service Unavailable");
  }
  // throw new Error("Function not implemented.");
}

async function queryExplain(configuration: Configuration, state: State, request: QueryRequest): Promise<ExplainResponse> {
  throw new Error("Function not implemented.");
}

async function mutationExplain(configuration: Configuration, state: State, request: MutationRequest): Promise<ExplainResponse> {
  throw new Error("Function not implemented.");
}

async function mutation(configuration: Configuration, state: State, request: MutationRequest): Promise<MutationResponse> {
  throw new Error("Function not implemented.");
}

async function tryInitState(configuration: Configuration, metrics: unknown): Promise<State> {
  // creates a connection to the MySQL database and stores it in the state
  const db = await mysql.createConnection({
    host: configuration.host,
    port: configuration.port,
    user: configuration.user,
    password: configuration.password,
    database: configuration.db
  })

  return {db};

}

function getCapabilities(configuration: Configuration): CapabilitiesResponse {
  throw new Error("Function not implemented.");
}

async function getSchema(configuration: Configuration): Promise<SchemaResponse> {
  const collections = configuration.schema.collections
  const functions = configuration.schema.functions
  const object_types = configuration.schema.object_types
  const procedures = configuration.schema.procedures
  const scalar_types = configuration.schema.scalar_types
  return {
        collections,
        functions,
        object_types,
        procedures,
        scalar_types,
    };
  // throw new Error("Function not implemented.");
}

async function query(configuration: Configuration, state: State, request: QueryRequest): Promise<QueryResponse> {
  // takes in the queryRequest and returns the rows from the database
  console.log(JSON.stringify(request, null, 2));

  if (!request.query.fields && !request.query.aggregates){
    throw new BadRequest("No fields or aggregates specified");
  }
  if (request.query.fields && request.query.aggregates){
    throw new BadRequest("Both fields and aggregates specified. Currently GroupBy is not supported. Please specify only one of them.");
  }
  
  const rows = await fetch_rows(state, request);
  return [{ rows }];

}

async function fetch_sql_query(request: QueryRequest, parameters: any[]): Promise<string> {
  // takes in the query details and parses the information to form and return a SQL query
  const fields = [];
  const collection_list = [];
  collection_list.push(request.collection);
  const additional_predicates: string[] = [];
  for (const fieldName in request.query.fields) {
    if (Object.prototype.hasOwnProperty.call(request.query.fields, fieldName)) {
      const field = request.query.fields[fieldName];
      switch (field.type) {
        case 'column':
          fields.push(`${request.collection}.${field.column} AS ${request.collection}_${fieldName}`);
          break;
        case 'relationship':
          const relationship = request.collection_relationships[field.relationship];
          if (relationship === undefined) {
              throw new BadRequest("Undefined relationship");
          }
          collection_list.push(relationship.target_collection);
          for (const relationshipfieldName in field.query.fields) {
            if (Object.prototype.hasOwnProperty.call(field.query.fields, relationshipfieldName)) {
              const relationshipfield = field.query.fields[relationshipfieldName];
              switch (relationshipfield.type) {
                case 'column':
                  fields.push(`${relationship.target_collection}.${relationshipfield.column} AS ${fieldName}_${relationshipfieldName}`);
                  break;
                default:
                  throw new Error("Not supported");
              }
            }
          }
          for (const src_column in relationship.column_mapping) {
            const tgt_column = relationship.column_mapping[src_column];
            additional_predicates.push(`${request.collection}.${src_column} = ${relationship.target_collection}.${tgt_column}`);
          }
          break;
        default:
          throw new Error("Not supported");
      }
    }
  }

  const target_list = [];
  for (const aggregateName in request.query.aggregates) {
    if (Object.prototype.hasOwnProperty.call(request.query.aggregates, aggregateName)) {
      const aggregate = request.query.aggregates[aggregateName];
      switch (aggregate.type) {
        case 'star_count':
          target_list.push(`COUNT(1) AS ${aggregateName}`);
          break;
        case 'column_count':
          target_list.push(`COUNT(${aggregate.distinct ? 'DISTINCT ' : ''}${aggregate.column}) AS ${aggregateName}`);
          break;
        case 'single_column':
          switch (aggregate.function) {
            case 'sum':
              target_list.push(`SUM(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'avg':
              target_list.push(`AVG(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'min':
              target_list.push(`MIN(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'max':
              target_list.push(`MAX(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'stddev_pop':
              target_list.push(`STDDEV_POP(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'stddev_samp':
              target_list.push(`STDDEV_SAMP(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'var_pop':
              target_list.push(`VAR_POP(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'var_samp':
              target_list.push(`VAR_SAMP(${aggregate.column}) AS ${aggregateName}`);
              break;
            case 'csv':
              target_list.push(`GROUP_CONCAT(${aggregate.column}) AS ${aggregateName}`);
              break;
            default:
              throw new BadRequest("Unknown aggregate function");
          }
      }
    }
  }

  // console.log(JSON.stringify({ fields, collection_list, additional_predicates, target_list }, null, 2));

  const limit_clause = request.query.limit == null ? "" : `LIMIT ${request.query.limit}`;
  const offset_clause = request.query.offset == null ? "" : `OFFSET ${request.query.offset}`;
  const where_clause = request.query.predicate == null ? "" : `WHERE ${visit_expression(parameters, request.query.predicate)} ${additional_predicates.length ? `AND ${additional_predicates.join(" AND ")}` : ''}`;
  const order_by_clause = request.query.order_by == null ? "" : `ORDER BY ${visit_order_by_elements(request.query.order_by.elements)}`;

  let sql = `SELECT ${fields.length ? fields.join(", ") : '1 AS __empty'}, ${target_list.length ? target_list.join(", ") : "1 AS __empty"} FROM ${collection_list.join(", ")} ${where_clause} ${order_by_clause} ${limit_clause} ${offset_clause}`;

  return sql;
}

async function fetch_rows(state: State, request: QueryRequest): Promise<{[k: string]: RowFieldValue}[]> {
  // takes in the queryRequest and forms a SQL query using 'fetch_sql_query' funciton, before sending it to the DBMS for execution
  // returns back the rows of the created View

  const parameters: any[] = [];
  let sql = await fetch_sql_query(request,parameters);

  // console.log(JSON.stringify({ sql, parameters }, null, 2));

  parameters.forEach((value) => {
    if (typeof value === 'string') {
      sql = sql.replace('?', `"${value}"`);
    }
    if (typeof value === 'object') {
      if (Array.isArray(value)) {
        sql = sql.replace(
          '?',
          `(${value.map((element) => (typeof element === 'string' ? `"${element}"` : element)).join(',')})`,
        );
      } else {
        sql = sql.replace('?', value);
      }
    }
    if (['number', 'boolean'].includes(typeof value)) {
      sql = sql.replace('?', value.toString());
    }
  }); 

  // console.log(JSON.stringify({ sql}, null, 2));

  const [rows] = await state.db.query<RowDataPacket[]>(sql,[]);

  // console.log(JSON.stringify({ rows}, null, 2));


  return rows.map((row) => { delete row.__empty; return row; });
}

function visit_order_by_elements(elements: OrderByElement[]): String {
  //handles nested ordering incase of order by multiple columns
  // joins "COLUMN_NAME ASC/DESC" in order
  if (elements.length > 0) {
      return elements.map(visit_order_by_element).join(", ");
  } else {
      return "1";
  }
}

function visit_order_by_element(element: OrderByElement): String {
  // returns "COLUMN_NAME ASC/DESC"
  const direction = element.order_direction === 'asc' ? 'ASC' : 'DESC';

  switch (element.target.type) {
      case 'column':
          if (element.target.path.length > 0) {
              throw new NotSupported("Not supported");
          }
          return `${element.target.name} ${direction}`;
      default:
          throw new NotSupported("Not supported");
  }
}

function visit_expression_with_parens(
  parameters: any[],
  expr: Expression
): string {
  // returns the expression with parentheses
  return `(${visit_expression(parameters, expr)})`;
}

function visit_expression(
  parameters: any[],
  expr: Expression
): string {
  // returns the expression in the form of a string after processing and making it SQL compatible
  switch (expr.type) {
      case "and":
          if (expr.expressions.length > 0) {
              return expr.expressions.map(e => visit_expression_with_parens(parameters, e)).join(" AND ");
          } else {
              return "TRUE";
          }
      case "or":
          if (expr.expressions.length > 0) {
              return expr.expressions.map(e => visit_expression_with_parens(parameters, e)).join(" OR ");
          } else {
              return "TRUE";
          }
      case "binary_comparison_operator":
          switch (expr.operator) {
              case '_lte':
                  return `${visit_comparison_target(expr.column)} <= ${visit_comparison_value(parameters, expr.value)}`
              case '_lt':
                  return `${visit_comparison_target(expr.column)} < ${visit_comparison_value(parameters, expr.value)}`
              case '_gte':
                  return `${visit_comparison_target(expr.column)} >= ${visit_comparison_value(parameters, expr.value)}`
              case '_gt':
                  return `${visit_comparison_target(expr.column)} > ${visit_comparison_value(parameters, expr.value)}`
              case '_eq':
                  return `${visit_comparison_target(expr.column)} = ${visit_comparison_value(parameters, expr.value)}`
              case '_like':
                  return `${visit_comparison_target(expr.column)} LIKE ${visit_comparison_value(parameters, expr.value)}`
              case '_contains':
                  // console.log("%"+expr.value.value+"%");
                  let temp = expr
                  temp.value.value = "%"+expr.value.value+"%"
                  return `${visit_comparison_target(expr.column)} LIKE ${visit_comparison_value(parameters, temp.value)}`
              case '_in':
                  return `${visit_comparison_target(expr.column)} IN ${visit_comparison_value(parameters, expr.value)}`
              default:
                  throw new BadRequest("Unknown comparison operator");
          }
      default:
          throw new BadRequest("Unknown expression type");
  }
}

function visit_comparison_target(target: ComparisonTarget) {
  // makes sure that the comparison condition's target is a column
  switch (target.type) {
      case 'column':
          if (target.path.length > 0) {
              throw new NotSupported("Not supported");
          }
          return target.name;
      default:
          throw new NotSupported("Not supported");
  }
}

function visit_comparison_value(parameters: any[], target: ComparisonValue) {
  // makes sure that the comparison condition's value is a scalar
  // appends it to the parameters array and returns a "?" to be used in the SQL query
  switch (target.type) {
      case 'scalar':
          parameters.push(target.value);
          return "?";
      default:
          throw new NotSupported("Option not supported yet");
  }
}

const connector: Connector<Configuration, State> = {
  parseConfiguration,
  tryInitState,
  fetchMetrics,
  healthCheck,
  getCapabilities,
  getSchema,
  queryExplain,
  mutationExplain,
  mutation,
  query
};

start(connector);


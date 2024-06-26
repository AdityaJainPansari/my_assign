"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const promise_1 = __importDefault(require("mysql2/promise"));
const path_1 = require("path");
const promises_1 = require("fs/promises");
const ndc_sdk_typescript_1 = require("@hasura/ndc-sdk-typescript");
function parseConfiguration(configurationDir) {
    return __awaiter(this, void 0, void 0, function* () {
        const configuration_file = (0, path_1.resolve)(configurationDir, 'config.json');
        const configuration_data = yield (0, promises_1.readFile)(configuration_file);
        const configuration = JSON.parse(configuration_data.toString());
        return Object.assign({}, configuration);
    });
}
function fetchMetrics(configuration, state) {
    return __awaiter(this, void 0, void 0, function* () {
        throw new Error("Function not implemented.");
    });
}
function healthCheck(configuration, state) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            yield state.db.query("SELECT 1");
        }
        catch (x) {
            throw new ndc_sdk_typescript_1.ConnectorError(503, "Service Unavailable");
        }
        // throw new Error("Function not implemented.");
    });
}
function queryExplain(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function* () {
        throw new Error("Function not implemented.");
    });
}
function mutationExplain(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function* () {
        throw new Error("Function not implemented.");
    });
}
function mutation(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function* () {
        throw new Error("Function not implemented.");
    });
}
function tryInitState(configuration, metrics) {
    return __awaiter(this, void 0, void 0, function* () {
        // creates a connection to the MySQL database and stores it in the state
        const db = yield promise_1.default.createConnection({
            host: configuration.host,
            port: configuration.port,
            user: configuration.user,
            password: configuration.password,
            database: configuration.db
        });
        return { db };
    });
}
function getCapabilities(configuration) {
    throw new Error("Function not implemented.");
}
function getSchema(configuration) {
    return __awaiter(this, void 0, void 0, function* () {
        const collections = configuration.schema.collections;
        const functions = configuration.schema.functions;
        const object_types = configuration.schema.object_types;
        const procedures = configuration.schema.procedures;
        const scalar_types = configuration.schema.scalar_types;
        return {
            collections,
            functions,
            object_types,
            procedures,
            scalar_types,
        };
        // throw new Error("Function not implemented.");
    });
}
function query(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function* () {
        // takes in the queryRequest and returns the rows from the database
        console.log(JSON.stringify(request, null, 2));
        if (!request.query.fields && !request.query.aggregates) {
            throw new ndc_sdk_typescript_1.BadRequest("No fields or aggregates specified");
        }
        if (request.query.fields && request.query.aggregates) {
            throw new ndc_sdk_typescript_1.BadRequest("Both fields and aggregates specified. Currently GroupBy is not supported. Please specify only one of them.");
        }
        const rows = yield fetch_rows(state, request);
        return [{ rows }];
    });
}
function fetch_sql_query(request, parameters) {
    return __awaiter(this, void 0, void 0, function* () {
        // takes in the query details and parses the information to form and return a SQL query
        const fields = [];
        const collection_list = [];
        collection_list.push(request.collection);
        const additional_predicates = [];
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
                            throw new ndc_sdk_typescript_1.BadRequest("Undefined relationship");
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
                                throw new ndc_sdk_typescript_1.BadRequest("Unknown aggregate function");
                        }
                }
            }
        }
        console.log(JSON.stringify({ fields, collection_list, additional_predicates, target_list }, null, 2));
        const limit_clause = request.query.limit == null ? "" : `LIMIT ${request.query.limit}`;
        const offset_clause = request.query.offset == null ? "" : `OFFSET ${request.query.offset}`;
        const where_clause = request.query.predicate == null ? "" : `WHERE ${visit_expression(parameters, request.query.predicate)} ${additional_predicates.length ? `AND ${additional_predicates.join(" AND ")}` : ''}`;
        const order_by_clause = request.query.order_by == null ? "" : `ORDER BY ${visit_order_by_elements(request.query.order_by.elements)}`;
        let sql = `SELECT ${fields.length ? fields.join(", ") : '1 AS __empty'}, ${target_list.length ? target_list.join(", ") : "1 AS __empty"} FROM ${collection_list.join(", ")} ${where_clause} ${order_by_clause} ${limit_clause} ${offset_clause}`;
        return sql;
    });
}
function fetch_rows(state, request) {
    return __awaiter(this, void 0, void 0, function* () {
        // takes in the queryRequest and forms a SQL query using 'fetch_sql_query' funciton, before sending it to the DBMS for execution
        // returns back the rows of the created View
        const parameters = [];
        let sql = yield fetch_sql_query(request, parameters);
        // console.log(JSON.stringify({ sql, parameters }, null, 2));
        parameters.forEach((value) => {
            if (typeof value === 'string') {
                sql = sql.replace('?', `"${value}"`);
            }
            if (typeof value === 'object') {
                if (Array.isArray(value)) {
                    sql = sql.replace('?', `(${value.map((element) => (typeof element === 'string' ? `"${element}"` : element)).join(',')})`);
                }
                else {
                    sql = sql.replace('?', value);
                }
            }
            if (['number', 'boolean'].includes(typeof value)) {
                sql = sql.replace('?', value.toString());
            }
        });
        console.log(JSON.stringify({ sql }, null, 2));
        const [rows] = yield state.db.query(sql, []);
        // console.log(JSON.stringify({ rows}, null, 2));
        return rows.map((row) => { delete row.__empty; return row; });
    });
}
function visit_order_by_elements(elements) {
    //handles nested ordering incase of order by multiple columns
    // joins "COLUMN_NAME ASC/DESC" in order
    if (elements.length > 0) {
        return elements.map(visit_order_by_element).join(", ");
    }
    else {
        return "1";
    }
}
function visit_order_by_element(element) {
    // returns "COLUMN_NAME ASC/DESC"
    const direction = element.order_direction === 'asc' ? 'ASC' : 'DESC';
    switch (element.target.type) {
        case 'column':
            if (element.target.path.length > 0) {
                throw new ndc_sdk_typescript_1.NotSupported("Not supported");
            }
            return `${element.target.name} ${direction}`;
        default:
            throw new ndc_sdk_typescript_1.NotSupported("Not supported");
    }
}
function visit_expression_with_parens(parameters, expr) {
    // returns the expression with parentheses
    return `(${visit_expression(parameters, expr)})`;
}
function visit_expression(parameters, expr) {
    // returns the expression in the form of a string after processing and making it SQL compatible
    switch (expr.type) {
        case "and":
            if (expr.expressions.length > 0) {
                return expr.expressions.map(e => visit_expression_with_parens(parameters, e)).join(" AND ");
            }
            else {
                return "TRUE";
            }
        case "or":
            if (expr.expressions.length > 0) {
                return expr.expressions.map(e => visit_expression_with_parens(parameters, e)).join(" OR ");
            }
            else {
                return "TRUE";
            }
        case "binary_comparison_operator":
            switch (expr.operator) {
                case '_lte':
                    return `${visit_comparison_target(expr.column)} <= ${visit_comparison_value(parameters, expr.value)}`;
                case '_lt':
                    return `${visit_comparison_target(expr.column)} < ${visit_comparison_value(parameters, expr.value)}`;
                case '_gte':
                    return `${visit_comparison_target(expr.column)} >= ${visit_comparison_value(parameters, expr.value)}`;
                case '_gt':
                    return `${visit_comparison_target(expr.column)} > ${visit_comparison_value(parameters, expr.value)}`;
                case '_eq':
                    return `${visit_comparison_target(expr.column)} = ${visit_comparison_value(parameters, expr.value)}`;
                case '_like':
                    return `${visit_comparison_target(expr.column)} LIKE ${visit_comparison_value(parameters, expr.value)}`;
                case '_contains':
                    // console.log("%"+expr.value.value+"%");
                    let temp = expr;
                    temp.value.value = "%" + expr.value.value + "%";
                    return `${visit_comparison_target(expr.column)} LIKE ${visit_comparison_value(parameters, temp.value)}`;
                case '_in':
                    return `${visit_comparison_target(expr.column)} IN ${visit_comparison_value(parameters, expr.value)}`;
                default:
                    throw new ndc_sdk_typescript_1.BadRequest("Unknown comparison operator");
            }
        default:
            throw new ndc_sdk_typescript_1.BadRequest("Unknown expression type");
    }
}
function visit_comparison_target(target) {
    // makes sure that the comparison condition's target is a column
    switch (target.type) {
        case 'column':
            if (target.path.length > 0) {
                throw new ndc_sdk_typescript_1.NotSupported("Not supported");
            }
            return target.name;
        default:
            throw new ndc_sdk_typescript_1.NotSupported("Not supported");
    }
}
function visit_comparison_value(parameters, target) {
    // makes sure that the comparison condition's value is a scalar
    // appends it to the parameters array and returns a "?" to be used in the SQL query
    switch (target.type) {
        case 'scalar':
            parameters.push(target.value);
            return "?";
        default:
            throw new ndc_sdk_typescript_1.NotSupported("Option not supported yet");
    }
}
const connector = {
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
(0, ndc_sdk_typescript_1.start)(connector);

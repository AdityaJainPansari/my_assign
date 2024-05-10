"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var promise_1 = require("mysql2/promise");
var path_1 = require("path");
var promises_1 = require("fs/promises");
var ndc_sdk_typescript_1 = require("@hasura/ndc-sdk-typescript");
function parseConfiguration(configurationDir) {
    return __awaiter(this, void 0, void 0, function () {
        var configuration_file, configuration_data, configuration;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    configuration_file = (0, path_1.resolve)(configurationDir, 'config.json');
                    return [4 /*yield*/, (0, promises_1.readFile)(configuration_file)];
                case 1:
                    configuration_data = _a.sent();
                    configuration = JSON.parse(configuration_data.toString());
                    return [2 /*return*/, __assign({}, configuration)];
            }
        });
    });
}
function fetchMetrics(configuration, state) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            throw new Error("Function not implemented.");
        });
    });
}
function healthCheck(configuration, state) {
    return __awaiter(this, void 0, void 0, function () {
        var x_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 2, , 3]);
                    return [4 /*yield*/, state.db.query("SELECT 1")];
                case 1:
                    _a.sent();
                    return [3 /*break*/, 3];
                case 2:
                    x_1 = _a.sent();
                    throw new ndc_sdk_typescript_1.ConnectorError(503, "Service Unavailable");
                case 3: return [2 /*return*/];
            }
        });
    });
}
function queryExplain(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            throw new Error("Function not implemented.");
        });
    });
}
function mutationExplain(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            throw new Error("Function not implemented.");
        });
    });
}
function mutation(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            throw new Error("Function not implemented.");
        });
    });
}
function tryInitState(configuration, metrics) {
    return __awaiter(this, void 0, void 0, function () {
        var db;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, promise_1.default.createConnection({
                        host: configuration.host,
                        port: configuration.port,
                        user: configuration.user,
                        password: configuration.password,
                        database: configuration.db
                    })];
                case 1:
                    db = _a.sent();
                    return [2 /*return*/, { db: db }];
            }
        });
    });
}
function getCapabilities(configuration) {
    throw new Error("Function not implemented.");
}
function getSchema(configuration) {
    return __awaiter(this, void 0, void 0, function () {
        var collections, functions, object_types, procedures, scalar_types;
        return __generator(this, function (_a) {
            collections = configuration.schema.collections;
            functions = configuration.schema.functions;
            object_types = configuration.schema.object_types;
            procedures = configuration.schema.procedures;
            scalar_types = configuration.schema.scalar_types;
            return [2 /*return*/, {
                    collections: collections,
                    functions: functions,
                    object_types: object_types,
                    procedures: procedures,
                    scalar_types: scalar_types,
                }];
        });
    });
}
function query(configuration, state, request) {
    return __awaiter(this, void 0, void 0, function () {
        var rows, _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    console.log(JSON.stringify(request, null, 2));
                    _a = request.query.fields;
                    if (!_a) return [3 /*break*/, 2];
                    return [4 /*yield*/, fetch_rows(state, request)];
                case 1:
                    _a = (_b.sent());
                    _b.label = 2;
                case 2:
                    rows = _a;
                    return [2 /*return*/, [{ rows: rows }]];
            }
        });
    });
}
function fetch_rows(state, request) {
    return __awaiter(this, void 0, void 0, function () {
        var fields, fieldName, field, parameters, limit_clause, offset_clause, where_clause, order_by_clause, sql, rows;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    fields = [];
                    for (fieldName in request.query.fields) {
                        if (Object.prototype.hasOwnProperty.call(request.query.fields, fieldName)) {
                            field = request.query.fields[fieldName];
                            switch (field.type) {
                                case 'column':
                                    fields.push("".concat(field.column, " AS ").concat(fieldName));
                                    break;
                                case 'relationship':
                                    throw new Error("Relationships are not supported");
                            }
                        }
                    }
                    parameters = [];
                    limit_clause = request.query.limit == null ? "" : "LIMIT ".concat(request.query.limit);
                    offset_clause = request.query.offset == null ? "" : "OFFSET ".concat(request.query.offset);
                    where_clause = request.query.predicate == null ? "" : "WHERE ".concat(visit_expression(parameters, request.query.predicate));
                    order_by_clause = request.query.order_by == null ? "" : "ORDER BY ".concat(visit_order_by_elements(request.query.order_by.elements));
                    sql = "SELECT ".concat(fields.length ? fields.join(", ") : '1 AS __empty', " FROM ").concat(request.collection, " ").concat(where_clause, " ").concat(order_by_clause, " ").concat(limit_clause, " ").concat(offset_clause);
                    console.log(JSON.stringify({ sql: sql, parameters: parameters }, null, 2));
                    parameters.forEach(function (value) {
                        if (typeof value === 'string') {
                            sql = sql.replace('?', "\"".concat(value, "\""));
                        }
                        if (typeof value === 'object') {
                            if (Array.isArray(value)) {
                                sql = sql.replace('?', "(".concat(value.map(function (element) { return (typeof element === 'string' ? "\"".concat(element, "\"") : element); }).join(','), ")"));
                            }
                            else {
                                sql = sql.replace('?', value);
                            }
                        }
                        if (['number', 'boolean'].includes(typeof value)) {
                            sql = sql.replace('?', value.toString());
                        }
                    });
                    console.log(JSON.stringify({ sql: sql }, null, 2));
                    return [4 /*yield*/, state.db.query(sql, [])];
                case 1:
                    rows = (_a.sent())[0];
                    console.log(JSON.stringify({ rows: rows }, null, 2));
                    return [2 /*return*/, rows.map(function (row) { delete row.__empty; return row; })];
            }
        });
    });
}
function visit_order_by_elements(elements) {
    if (elements.length > 0) {
        return elements.map(visit_order_by_element).join(", ");
    }
    else {
        return "1";
    }
}
function visit_order_by_element(element) {
    var direction = element.order_direction === 'asc' ? 'ASC' : 'DESC';
    switch (element.target.type) {
        case 'column':
            if (element.target.path.length > 0) {
                throw new ndc_sdk_typescript_1.NotSupported("Not supported");
            }
            return "".concat(element.target.name, " ").concat(direction);
        default:
            throw new ndc_sdk_typescript_1.NotSupported("Not supported");
    }
}
function visit_expression_with_parens(parameters, expr) {
    return "(".concat(visit_expression(parameters, expr), ")");
}
function visit_expression(parameters, expr) {
    switch (expr.type) {
        case "and":
            if (expr.expressions.length > 0) {
                return expr.expressions.map(function (e) { return visit_expression_with_parens(parameters, e); }).join(" AND ");
            }
            else {
                return "TRUE";
            }
        case "binary_comparison_operator":
            switch (expr.operator) {
                case '_lte':
                    return "".concat(visit_comparison_target(expr.column), " <= ").concat(visit_comparison_value(parameters, expr.value));
                case '_eq':
                    return "".concat(visit_comparison_target(expr.column), " = ").concat(visit_comparison_value(parameters, expr.value));
                case '_like':
                    return "".concat(visit_comparison_target(expr.column), " LIKE ").concat(visit_comparison_value(parameters, expr.value));
                case '_in':
                    return "".concat(visit_comparison_target(expr.column), " IN ").concat(visit_comparison_value(parameters, expr.value));
                default:
                    throw new ndc_sdk_typescript_1.BadRequest("Unknown comparison operator");
            }
        default:
            throw new ndc_sdk_typescript_1.BadRequest("Unknown expression type");
    }
}
function visit_comparison_target(target) {
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
    switch (target.type) {
        case 'scalar':
            parameters.push(target.value);
            return "?";
        default:
            throw new ndc_sdk_typescript_1.NotSupported("Option not supported yet");
    }
}
var connector = {
    parseConfiguration: parseConfiguration,
    tryInitState: tryInitState,
    fetchMetrics: fetchMetrics,
    healthCheck: healthCheck,
    getCapabilities: getCapabilities,
    getSchema: getSchema,
    queryExplain: queryExplain,
    mutationExplain: mutationExplain,
    mutation: mutation,
    query: query
};
(0, ndc_sdk_typescript_1.start)(connector);

import * as gq from 'graphql';


//////// CONFIGURATION

type MaybePromise<T> = T | Promise<T>

export type JSONObject = { [key: string]: undefined }

/** Named arguments to a function call. */
export type Args = { [key:string]: any};

/** JavaScript function. May return a Promise. */
export type JSFn = (context: Context, args?: Args) => unknown;

/** JavaScript GraphQL resolver function. */
export type ResolverFn = (source: any,
                          args: Args,
                          context: Context,
                          info: ResolveInfo) => undefined;

/** GraphQL resolver `info` parameter. */
export interface ResolveInfo extends gq.GraphQLResolveInfo {
    /** The names of the fields of the returned object that will be used.
     *  The resolver implementation may omit any fields not included in this list;
     *  this can be a significant optimization. */
    readonly selectedFieldNames : string[];
};

/** Authorization for a function. */
export type AllowConfig = {
    users?:    string[],    // Names of allowed users
    roles?:    string[],    // Allowed roles
    channels?: string[],    // Allowed channels
};

/** Defines a function or GraphQL resolver. */
export type FunctionConfig = {
    type:       "query" | "javascript", // Language the 'code' is in
    code:       string,                 // The function's JavaScript code or N1QL query
    args?:      string[],               // Names of parameters (not used by GraphQL)
    mutating?:  boolean,                // Is function allowed to modify the database?
    allow?:     AllowConfig,            // Who's allowed to call this
};

/** Functions configuration: maps function name to its config. */
export type FunctionsConfig = {
    definitions:         Record<string,FunctionConfig>  // Names & definitions of the functions
    max_function_count?: number;                        // Maximum number of functions
    max_code_size?:      number;                        // Maximum size in bytes of a fn's `code`
    max_request_size?:   number;                        // Maximum size in bytes of a request
};

export type FieldMap = Record<string,FunctionConfig>;
export type ResolverMap = Record<string,FieldMap>;

/** GraphQL configuration. */
export type GraphQLConfig = {
    schema?:             string,        // The schema itself
    schemaFile?:         string,        // Path to schema file (only if schema is not given)
    resolvers:           ResolverMap,   // GraphQL resolver functions
    max_code_size?:      number;        // Maximum size in bytes of a function's `code`
    max_request_size?:   number;        // Maximum size in bytes of an incoming request
    max_resolver_count?: number;        // Maximum number of resolvers
    max_schema_size?:    number;        // Maximum size in bytes of the schema
};


/** Top-level configuration. */
export type Config = {
    functions?:     FunctionsConfig;
    graphql?:       GraphQLConfig;
}


//////// RUNTIME CONTEXT


/** Type of the `context` parameter passed to all functions. */
export interface Context {
    readonly user: User;
    readonly admin: User;

    checkUser(name: string | string[]) : boolean;
    requireUser(name: string | string[]) : void;
    checkAdmin() : boolean;
    requireAdmin() : void;
    checkRole(role: string | string[]) : boolean;
    requireRole(role: string | string[]) : void;
    checkAccess(channel: string | string[]) : boolean;
    requireAccess(channel: string | string[]) : void;
    checkMutating() : boolean;
    requireMutating() : void;
}


/** The type of the `context.user` and `context.admin` objects.
 *  Exposes auth and APIs scoped to either the current user, or to an admin. */
 export interface User {
    readonly name?: string;
    readonly roles?: string[];
    readonly channels?: string[];

    readonly isAdmin : boolean;         // This is an admin user
    readonly isSuperUser : boolean;     // This is the magic "context.admin" user

    readonly canMutate : boolean;

    readonly defaultCollection: CRUD;

    function(name: string, args?: Args) : unknown;
    graphql(query: string, args?: Args) : Promise<JSONObject | null | undefined>;
};


/** Shape of a Couchbase document, used in the CRUD API. */
export interface Document {
    _id? : string;      // Document ID (primary key)
    _rev? : string;     // Revision ID (used for MVCC)
};


/** The type of the `User.defaultCollection` object. Exposes database CRUD APIs. */
export interface CRUD {
    get(docID: string) : Document | null;
    save(doc: Document, docID?: string) : string | null;
    delete(docID: string) : boolean;
    delete(doc: Document) : boolean;
}


//////// UTILITIES


/** An exception that conveys an HTTP status. */
export class HTTPError extends Error {
    constructor(readonly status: number,
                readonly baseMessage: string) {
        super(`[${status}] ${baseMessage}`);    // unpackJSError() in evaluator.go parses this
    }
}


//////// DATABASE


/** User credentials: tuple of [username, roles, channels] */
export type Credentials = [string, string[], string[]];


/** Top-level object that stores the compiled state for a database. */
export interface Database {
    /** Sets the configuration. Returns all errors found. */
    configure(functions: FunctionsConfig | undefined,
              graphql: GraphQLConfig | undefined) : string[] | null;

    /** Creates an execution context given a user's name, roles and channels. */
    makeContext(credentials: Credentials | null,
                mutationAllowed: boolean) : Context;

    /** Calls a named function. */
    callFunction(context: Context,
                 name: string,
                 args?: Args) : MaybePromise<unknown>;

    /** Runs a N1QL query. Called by functions of type "query". */
    query(context: Context,
          fnName: string,
          n1ql: string,
          args?: Args) : JSONObject[];

    /** Runs a GraphQL query. */
    graphql(context: Context,
            query: string,
            variableValues?: Args,
            operationName?: string) : Promise<gq.ExecutionResult>;
}

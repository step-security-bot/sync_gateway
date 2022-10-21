import { Args, User, Config, Database, Context, Credentials, Document, JSONObject } from './types'
import { MakeDatabase, Upstream } from "./impl";


/** The interface the native code needs to implement. */
export interface NativeAPI {
    query(fnName: string,
          n1ql: string,
          argsJSON: string | undefined,
          asAdmin: boolean) : string;
    get(docID: string,
        asAdmin: boolean) : string | null;
    save(docJSON: string,
         docID: string | undefined,
         asAdmin: boolean) : string | null;
    delete(docID: string,
           revID: string | undefined,
           asAdmin: boolean) : boolean;
    log(sgLogLevel: number, ...args: any) : void;
}


// Wraps a NativeAPI and exposes it as an Upstream for a Database to use
class UpstreamNativeImpl implements Upstream {
    constructor(private native: NativeAPI) { }

    query(fnName: string, n1ql: string, args: Args | undefined, user: User) : JSONObject[] {
        let result = this.native.query(fnName, n1ql, this.stringify(args), user.isAdmin);
        return JSON.parse(result);
    }

    get(docID: string, user: User) : Document | null {
        let jresult = this.native.get(docID, user.isAdmin);
        if (jresult === null) return jresult;
        return this.parseDoc(jresult)
    }

    save(doc: object, docID: string | undefined, user: User) : string | null {
        return this.native.save(JSON.stringify(doc), docID, user.isAdmin);
    }

    delete(docID: string, revID: string | undefined, user: User) : boolean {
        return this.native.delete(docID, revID, user.isAdmin);
    }

    private stringify(obj: object | undefined) : string | undefined {
        return obj ? JSON.stringify(obj) : undefined;
    }

    private parseDoc(json: string) : Document {
        let result = JSON.parse(json)
        if (typeof(result) !== "object")
            throw Error("NativeAPI returned JSON that's not an Object");
        return result as Document
    }
}


/** The API this module implements, and the native code calls. */
export class API {
    constructor(configJSON: string, native: NativeAPI) {
        console.debug = (...args: any) => native.log(4, ...args);
        console.log   = (...args: any) => native.log(3, ...args);
        console.warn  = (...args: any) => native.log(2, ...args);
        console.error = (...args: any) => native.log(1, ...args);

        let config = JSON.parse(configJSON) as Config;
        this.db = MakeDatabase(config.functions, config.graphql, new UpstreamNativeImpl(native));
    }

    /** Calls a named function. */
    callFunction(name: string,
                 argsJSON: string | undefined,
                 user: string | undefined,
                 roles: string | undefined,
                 channels: string | undefined,
                 mutationAllowed: boolean) : string | Promise<string>
    {
        console.debug(`>>>> callFunction(${name}) user=${user} mutationAllowed=${mutationAllowed}`);
        let args = argsJSON ? JSON.parse(argsJSON) : undefined;
        let context = this.makeContext(user, roles, channels, mutationAllowed);
        let result = this.db.callFunction(context, name, args);
        if (result instanceof Promise) {
            return result.then( result => JSON.stringify(result) );
        } else {
            return JSON.stringify(result);
        }
    }

    /** Runs a GraphQL query. */
    graphql(query: string,
            operationName: string | undefined,
            variablesJSON: string | undefined,
            user: string | undefined,
            roles: string | undefined,
            channels: string | undefined,
            mutationAllowed: boolean) : Promise<string>
    {
        if (operationName === "") operationName = undefined;
        console.debug(`>>>> graphql("${query}") user=${user} mutationAllowed=${mutationAllowed}`);
        let vars = variablesJSON ? JSON.parse(variablesJSON) : undefined;
        let context = this.makeContext(user, roles, channels, mutationAllowed);
        return this.db.graphql(context, query, vars, operationName)
            .then( result => JSON.stringify(result) );
    }

    private makeContext(user: string | undefined,
                        roles: string | undefined,
                        channels: string | undefined,
                        mutationAllowed: boolean) : Context
    {
        var credentials: Credentials | null = null;
        if (user !== undefined) {
            credentials = [user,
                           roles?.split(',') ?? [],
                           channels?.split(',') ?? []];
        }
        return this.db.makeContext(credentials, mutationAllowed)
    }

    private db: Database;
};


export function main(configJSON: string, native: NativeAPI) : API {
    return new API(configJSON, native);
}

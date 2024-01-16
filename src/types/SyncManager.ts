import { InferSchemaTypeFromCollection } from "../Collection";
import { Cursor } from "../InternalSchema";
import { AnyDatabaseSchema } from "./Database";
import { StorageEngineValidKey } from "./StorageEngine";
import { MaybePromise } from "./types";

export type PullResponse<TSchema extends AnyDatabaseSchema> = {
  change: {
    [K in keyof TSchema]: Array<
      | { action: "CLEAR" }
      | {
          action: "CREATED";
          value: InferSchemaTypeFromCollection<TSchema[K]>;
          key: StorageEngineValidKey;
        }
      | {
          action: "UPDATED";
          value: InferSchemaTypeFromCollection<TSchema[K]>;
          key: StorageEngineValidKey;
        }
      | { action: "DELETED"; key: StorageEngineValidKey }
    >;
  };
  cursor?: Cursor;
  lastProcessedMutationId: number;
};

export type Puller<TDatabaseSchema extends AnyDatabaseSchema> = (
  cursor?: Cursor
) => MaybePromise<PullResponse<TDatabaseSchema>>;

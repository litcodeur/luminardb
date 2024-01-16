import { Collection, InferSchemaTypeFromCollection } from "../Collection";
import { Mutators } from "./Mutators";
import { StorageEngine, StorageEngineValidKey } from "./StorageEngine";
import { Puller } from "./SyncManager";

export type AnyDatabaseSchema = Record<string, Collection<any, any>>;

export type DatabaseConfig<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema,
  TMutators extends Mutators<TDatabaseSchema> = Mutators<TDatabaseSchema>
> = {
  storageEngine: StorageEngine<TDatabaseSchema>;
  mutators?: TMutators;
  puller?: Puller<TDatabaseSchema>;
  initialize?: boolean;
};

export type DatabaseMutators<TMutators extends Mutators<any> = Mutators<any>> =
  {
    [K in keyof TMutators]: (
      ...args: Parameters<TMutators[K]>
    ) => Promise<ReturnType<ReturnType<TMutators[K]>["localResolver"]>>;
  };

export type DatabaseCDCEvents<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> =
  | {
      [K in keyof TDatabaseSchema]:
        | {
            action: "DELETE";
            collection: K;
            timestamp: number;
            value: InferSchemaTypeFromCollection<TDatabaseSchema[K]>;
            key: StorageEngineValidKey;
          }
        | {
            action: "INSERT";
            collection: K;
            timestamp: number;
            value: InferSchemaTypeFromCollection<TDatabaseSchema[K]>;
            key: StorageEngineValidKey;
          }
        | {
            action: "UPDATE";
            collection: K;
            timestamp: number;
            delta: Partial<InferSchemaTypeFromCollection<TDatabaseSchema[K]>>;
            postUpdateValue: InferSchemaTypeFromCollection<TDatabaseSchema[K]>;
            key: StorageEngineValidKey;
            preUpdateValue: InferSchemaTypeFromCollection<TDatabaseSchema[K]>;
          }
        | { action: "CLEAR"; collection: K; timestamp: number };
    }[keyof TDatabaseSchema][];

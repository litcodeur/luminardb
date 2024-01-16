import { IDBStorageEngine } from "./IDBStorageEngine/IDBStorageEngine";
import {
  INTERNAL_SCHEMA,
  INTERNAL_SCHEMA_COLLECTION_NAMES,
} from "./InternalSchema";
import { AnyDatabaseSchema } from "./types/Database";

function validateSchema(schema: AnyDatabaseSchema) {
  Object.entries(schema).forEach(([_, collection]) => {
    if (INTERNAL_SCHEMA_COLLECTION_NAMES.includes(collection.name)) {
      throw new Error(
        `Cannot have a collection with name: ${collection.name} as it is reserved`
      );
    }
  });
}

export function createIDBStorageEngine<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
>(config: { name: string; version: number; schema: TDatabaseSchema }) {
  validateSchema(config.schema);

  let channelConstructor: (new (name: string) => BroadcastChannel) | undefined =
    undefined;

  if (typeof BroadcastChannel !== "undefined") {
    channelConstructor = BroadcastChannel;
  }

  return new IDBStorageEngine<TDatabaseSchema>(
    config.name,
    config.version,
    {
      ...config.schema,
      ...INTERNAL_SCHEMA,
    },
    channelConstructor
  );
}

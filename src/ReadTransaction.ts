import {
  AnyDatabaseSchema,
  InferMetadataTypeFromCollection,
  InferSchemaTypeFromCollection,
} from ".";
import { ReadOnlyCollectionReference } from "./CollectionReference";
import { EnhancedStorageEngineTransaction } from "./EnhancedStorageEngineTransaction";

export class ReadTransaction<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> {
  #storageEngineTransaction: EnhancedStorageEngineTransaction;
  #schema: TDatabaseSchema;

  constructor(
    storageEngine: EnhancedStorageEngineTransaction,
    schema: TDatabaseSchema
  ) {
    this.#storageEngineTransaction = storageEngine;
    this.#schema = schema;
  }

  collection<TCollectionIdentifer extends keyof TDatabaseSchema>(
    collectionIdentifier: TCollectionIdentifer
  ) {
    return new ReadOnlyCollectionReference<
      InferSchemaTypeFromCollection<TDatabaseSchema[TCollectionIdentifer]>,
      InferMetadataTypeFromCollection<TDatabaseSchema[TCollectionIdentifer]>
    >(this.#schema[collectionIdentifier]!, this.#storageEngineTransaction);
  }
}

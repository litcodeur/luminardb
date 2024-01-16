import {
  InferMetadataTypeFromCollection,
  InferSchemaTypeFromCollection,
} from ".";
import { CollectionReference } from "./CollectionReference";
import { DatabaseMutation, INTERNAL_SCHEMA, MUTATION } from "./InternalSchema";
import { EnhancedStorageEngineTransaction } from "./EnhancedStorageEngineTransaction";
import { AnyDatabaseSchema } from "./types/Database";
import { StorableJSONObject } from "./types/types";
import { resolver } from "./utils";

/**
 * An write transaction reserved for internal use as to not expose initialization methods and constructor to
 * the user.
 */
export class InternalWriteTransaction<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> {
  #mutationId: number | null = null;
  #initializationPromise: Promise<void> | null = null;

  #mutationName: string;
  #mutationArgs: StorableJSONObject;
  #tx: EnhancedStorageEngineTransaction;
  #schema: TDatabaseSchema;

  constructor(
    schema: TDatabaseSchema,
    tx: EnhancedStorageEngineTransaction,
    mutationName: string,
    mutationArgs: StorableJSONObject
  ) {
    this.#mutationName = mutationName;
    this.#mutationArgs = mutationArgs;
    this.#tx = tx;
    this.#schema = schema;
  }

  async initialize() {
    if (this.#initializationPromise) {
      return this.#initializationPromise;
    }

    if (this.#mutationId) {
      return;
    }

    const r = resolver();

    this.#initializationPromise = r.promise;

    const mutation = {
      mutationArgs: this.#mutationArgs,
      mutationName: this.#mutationName,
      changes: [],
      collectionsAffected: [],
      isCompleted: false,
      isPushed: false,
    } satisfies Omit<DatabaseMutation, "id">;

    const { key } = await this.#tx.insert<DatabaseMutation>({
      collectionName: INTERNAL_SCHEMA[MUTATION].name,
      value: mutation as any,
    });

    await this.#tx.update<DatabaseMutation>({
      collectionName: INTERNAL_SCHEMA[MUTATION].name,
      key,
      value: { id: key as number },
    });

    this.#mutationId = key as number;

    r.resolve();

    this.#initializationPromise = null;
  }

  get mutationId() {
    if (!this.#mutationId) {
      throw new Error(
        "WriteTransaction not initialized before accessing mutationId"
      );
    }

    return this.#mutationId;
  }

  isInitialzed() {
    return this.#mutationId !== null;
  }

  get schema() {
    return this.#schema;
  }

  get storageEngineTransaction() {
    return this.#tx;
  }
}

export class WriteTransaction<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> {
  #internalWriteTransaction: InternalWriteTransaction;

  constructor(
    internalWriteTransaction: InternalWriteTransaction<TDatabaseSchema>
  ) {
    this.#internalWriteTransaction = internalWriteTransaction;
  }

  get mutationId() {
    return this.#internalWriteTransaction.mutationId;
  }

  collection<
    TCollectionIdentifier extends keyof TDatabaseSchema = keyof TDatabaseSchema,
    TCollection extends TDatabaseSchema[TCollectionIdentifier] = TDatabaseSchema[TCollectionIdentifier]
  >(collectionIdentifier: TCollectionIdentifier) {
    const schema = this.#internalWriteTransaction.schema;

    const storageEngineTransaction =
      this.#internalWriteTransaction.storageEngineTransaction;

    const mutationId = this.#internalWriteTransaction.mutationId;

    const collection = schema[collectionIdentifier as string] as TCollection;

    return new CollectionReference<
      InferSchemaTypeFromCollection<TCollection>,
      InferMetadataTypeFromCollection<TCollection>
    >(collection, storageEngineTransaction, mutationId);
  }
}

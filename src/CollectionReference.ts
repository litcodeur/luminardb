import { Collection, CollectionMetadata } from ".";
import { Condition } from "./Condition";
import {
  DatabaseMutation,
  INTERNAL_SCHEMA,
  MUTATION,
  PendingDeleteMutationChange,
  PendingInsertMutationChange,
  PendingMutationChange,
  PendingUpdateMutationChange,
} from "./InternalSchema";
import { EnhancedStorageEngineTransaction } from "./EnhancedStorageEngineTransaction";
import { FilterOption } from "./types/Query";
import { StorageEngineValidKey } from "./types/StorageEngine";
import { StorableJSONObject } from "./types/types";
import { getIncrementingTimestamp, isUndefined } from "./utils";

export class ReadOnlyCollectionReference<
  TCollectionSchema extends StorableJSONObject = StorableJSONObject,
  TCollectionMetadata extends CollectionMetadata<TCollectionSchema> = CollectionMetadata<TCollectionSchema>
> {
  #collection: Collection<TCollectionSchema, TCollectionMetadata>;
  #storageEngineTransaction: EnhancedStorageEngineTransaction;

  constructor(
    collection: Collection<TCollectionSchema, TCollectionMetadata>,
    tx: EnhancedStorageEngineTransaction
  ) {
    this.#collection = collection;
    this.#storageEngineTransaction = tx;
  }

  async get<TValue extends TCollectionSchema = TCollectionSchema>(
    key: StorageEngineValidKey
  ): Promise<TValue | undefined> {
    const queryResult = await this.#storageEngineTransaction.queryByKey<TValue>(
      this.#collection.name,
      key
    );

    return queryResult.get(key);
  }

  async getAll<TValue extends TCollectionSchema = TCollectionSchema>(
    option?: FilterOption<Collection<TCollectionSchema, TCollectionMetadata>>
  ): Promise<Array<TValue>> {
    if (isUndefined(option)) {
      const queryResult = await this.#storageEngineTransaction.queryAll<TValue>(
        this.#collection.name
      );

      return Array.from(queryResult.values());
    }

    const condition = new Condition(option as any);

    const queryResult =
      await this.#storageEngineTransaction.queryByCondition<TValue>(
        this.#collection.name,
        condition
      );

    return Array.from(queryResult.values());
  }
}
export class CollectionReference<
  TCollectionSchema extends StorableJSONObject = StorableJSONObject,
  TCollectionMetadata extends CollectionMetadata<TCollectionSchema> = CollectionMetadata<TCollectionSchema>
> extends ReadOnlyCollectionReference<TCollectionSchema, TCollectionMetadata> {
  #collection: Collection<TCollectionSchema, TCollectionMetadata>;
  #storageEngineTransaction: EnhancedStorageEngineTransaction;
  #mutationId: number;

  constructor(
    collection: Collection<TCollectionSchema, TCollectionMetadata>,
    tx: EnhancedStorageEngineTransaction,
    mutationId: number
  ) {
    super(collection, tx);
    this.#collection = collection;
    this.#storageEngineTransaction = tx;
    this.#mutationId = mutationId;
  }

  async #getMutation() {
    const result =
      await this.#storageEngineTransaction.queryByKey<DatabaseMutation>(
        INTERNAL_SCHEMA[MUTATION].name,
        this.#mutationId
      );

    if (!result.has(this.#mutationId)) {
      throw new Error("Mutation not found!");
    }

    return result.get(this.#mutationId)!;
  }

  async #addChangeToDatabaseMutation(
    change:
      | Omit<PendingInsertMutationChange, "id" | "timestamp" | "collectionName">
      | Omit<PendingUpdateMutationChange, "id" | "timestamp" | "collectionName">
      | Omit<PendingDeleteMutationChange, "id" | "timestamp" | "collectionName">
  ) {
    const collectionName = this.#collection.name;

    const mutation = await this.#getMutation();

    const collectionsAffected = Array.from(
      new Set([...mutation.collectionsAffected, collectionName])
    );

    const changes = mutation.changes;

    const timestamp = getIncrementingTimestamp();

    const finalChangeState = {
      ...change,
      collectionName: this.#collection.name,
      id: `${this.#mutationId}-${timestamp}`,
      timestamp,
    } satisfies PendingMutationChange;

    changes.push(finalChangeState);

    const result =
      await this.#storageEngineTransaction.update<DatabaseMutation>({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key: this.#mutationId,
        value: { collectionsAffected, changes },
      });

    return result!.value;
  }

  async insert<TValue extends TCollectionSchema = TCollectionSchema>(
    key: StorageEngineValidKey,
    value: TValue
  ): Promise<TValue> {
    const queryResult = await this.get<TValue>(key);

    if (queryResult) {
      throw new Error(
        `Unable to insert document as a document with key: ${key} already exists in collection: ${
          this.#collection.name
        }`
      );
    }

    await this.#addChangeToDatabaseMutation({
      action: "INSERT",
      key,
      value,
    });

    return value;
  }

  async update<TValue extends TCollectionSchema = TCollectionSchema>(
    key: StorageEngineValidKey,
    delta: Partial<TValue>
  ): Promise<TValue> {
    const queryResult = await this.get<TValue>(key);

    if (!queryResult) {
      throw new Error(
        `Unable to update document as a document with key: ${key} does not exist in collection: ${
          this.#collection.name
        }`
      );
    }

    const data = queryResult;

    const preUpdateValue = data;

    const postUpdateValue = { ...data, ...delta };

    await this.#addChangeToDatabaseMutation({
      action: "UPDATE",
      key,
      delta,
      preUpdateValue,
      postUpdateValue: postUpdateValue,
    });

    return postUpdateValue;
  }

  async delete<TValue extends TCollectionSchema = TCollectionSchema>(
    key: StorageEngineValidKey
  ): Promise<TValue> {
    const queryResult = await this.get<TValue>(key);

    if (!queryResult) {
      throw new Error(
        `Unable to delete document as a document with key: ${key} does not exist in collection: ${
          this.#collection.name
        }`
      );
    }

    const data = queryResult;

    await this.#addChangeToDatabaseMutation({
      action: "DELETE",
      key,
      value: data,
    });

    return data;
  }
}

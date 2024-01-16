import { IDBPDatabase, openDB } from "idb";
import {
  AutoIncrementingCollection,
  Collection,
  IndexOptions,
} from "../Collection";
import { Condition } from "../Condition";
import { INTERNAL_SCHEMA_COLLECTION_NAMES } from "../InternalSchema";
import { EnhancedStorageEngineTransaction } from "../EnhancedStorageEngineTransaction";
import { AnyDatabaseSchema } from "../types/Database";
import {
  InsertOptionWithKey,
  InsertOptionWithoutKey,
  StorageEngine,
  StorageEngineCDCEvent,
  StorageEngineCDCEventSubscriber,
  StorageEngineQueryResult,
  StorageEngineStoredValue,
  StorageEngineTransactionMode,
  StorageEngineValidKey,
} from "../types/StorageEngine";
import { StorableJSONObject } from "../types/types";
import { resolver } from "../utils";
import { IDBStorageEngineTransaction } from "./IDBStorageEngineTransaction";

export type BroadcastChannelMessages = {
  type: "StorageEngineCDCEvent";
  data: StorageEngineCDCEvent;
};

export class IDBStorageEngine<
  TSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> implements StorageEngine<TSchema>
{
  #initialzationPromise: Promise<void> | null = null;
  #isInitialized = false;

  #name: string;
  #version: number;
  #schema: TSchema;
  #idb: IDBPDatabase | null = null;
  #allCollectionNames: Set<string> = new Set();

  #subscribers = new Set<StorageEngineCDCEventSubscriber>();
  #channel?: BroadcastChannel;

  constructor(
    name: string,
    version: number,
    schema: TSchema,
    ChannelConstructor?: new (name: string) => BroadcastChannel | undefined
  ) {
    this.#name = name;
    this.#version = version;
    this.#schema = schema;

    if (ChannelConstructor) {
      this.#channel = new ChannelConstructor(`IDBStorageEngine:${name}`);
    }
  }

  #getIDB() {
    if (!this.#idb) {
      throw new Error("IDBStorageEngine is not initialized");
    }
    return this.#idb;
  }

  startTransaction(
    collectionNames: string[] | "ALL",
    mode: StorageEngineTransactionMode
  ) {
    let collectionNamesForTransaction: Array<string> = [];

    if (collectionNames === "ALL") {
      collectionNamesForTransaction = Array.from(this.#allCollectionNames);
    } else {
      const missingInternalCollectionNames = [];

      for (let name of INTERNAL_SCHEMA_COLLECTION_NAMES) {
        if (!collectionNames.includes(name)) {
          missingInternalCollectionNames.push(name);
        }
      }

      collectionNamesForTransaction = [
        ...collectionNames,
        ...missingInternalCollectionNames,
      ];
    }

    const idbTX = new IDBStorageEngineTransaction(
      this.#getIDB(),
      collectionNamesForTransaction,
      mode
    );

    const tx = new EnhancedStorageEngineTransaction(idbTX);

    tx.onComplete((events) => this.#notifyCDCSubscribers({ events }));

    return tx;
  }

  #handleBroadcastChannelSubscription() {
    if (!this.#channel) return;

    const notifyCDCSubscribers = this.#notifyCDCSubscribers.bind(this);

    this.#channel.onmessage = function (ev) {
      const { data, type } = ev.data as BroadcastChannelMessages;

      if (ev.data && type === "StorageEngineCDCEvent") {
        notifyCDCSubscribers(data, false);
      }
    };
  }

  #handleIDBUpgradeEvent(
    event: IDBVersionChangeEvent,
    schema: AnyDatabaseSchema
  ) {
    const collections = Object.values({
      ...schema,
    });

    Object.getOwnPropertySymbols(schema).forEach((s) => {
      const collection = schema[s as any];
      if (collection instanceof Collection) {
        collections.push(collection);
      }
    });

    const db = (event.target as any).result as IDBDatabase;

    if (event.oldVersion !== event.newVersion) {
      const existingObjectStores = db.objectStoreNames;

      for (const objectStore of existingObjectStores) {
        db.deleteObjectStore(objectStore);
      }
    }

    collections.forEach((collection) => {
      if (collection instanceof AutoIncrementingCollection) {
        const typedCollection = collection as AutoIncrementingCollection<
          any,
          any
        >;
        db.createObjectStore(typedCollection.name, {
          keyPath: "key",
          autoIncrement: true,
        });
        return;
      }

      const store = db.createObjectStore(collection.name);

      Object.entries<IndexOptions>(collection.metadata.indexes ?? {}).forEach(
        ([indexPath, index]) => {
          const storedIndexName = indexPath;
          const storedIndexPath = `value.${indexPath}`;

          if (typeof index === "boolean") {
            store.createIndex(storedIndexName, storedIndexPath);
          } else {
            store.createIndex(storedIndexName, storedIndexPath, {
              unique: index?.unique,
              multiEntry: index?.multiEntry,
            });
          }
        }
      );
    });
  }

  async initialize(): Promise<void> {
    if (this.#isInitialized) {
      return;
    }

    if (this.#initialzationPromise) {
      return this.#initialzationPromise;
    }

    const { promise, resolve } = resolver();

    this.#initialzationPromise = promise;

    const handleIDBUpgradeEvent = this.#handleIDBUpgradeEvent.bind(this);

    const schema = this.#schema;

    const idb = await openDB(this.#name, this.#version, {
      upgrade(_, _1, _2, _3, event) {
        handleIDBUpgradeEvent(event, schema);
      },
    });

    Object.values(this.#schema).forEach((c) => {
      this.#allCollectionNames.add(c.name);
    });

    Object.getOwnPropertySymbols(this.#schema).forEach((s) => {
      const collection = this.#schema[s as any];
      if (collection instanceof Collection) {
        this.#allCollectionNames.add(collection.name);
      }
    });

    this.#idb = idb;
    this.#handleBroadcastChannelSubscription();

    this.#isInitialized = true;

    resolve();

    this.#initialzationPromise = null;
  }

  subscribeToCDC(subscriber: StorageEngineCDCEventSubscriber): () => void {
    this.#subscribers.add(subscriber);
    return () => {
      this.#subscribers.delete(subscriber);
    };
  }

  #notifyCDCSubscribers(
    event: StorageEngineCDCEvent,
    publishToBroadcastChannel = true
  ) {
    if (event.events.length === 0) {
      return;
    }

    for (const subscriber of this.#subscribers) {
      subscriber(event);
    }

    if (this.#channel && publishToBroadcastChannel) {
      this.#channel.postMessage({
        data: event,
        type: "StorageEngineCDCEvent",
      } as BroadcastChannelMessages);
    }
  }

  get schema(): TSchema {
    return this.#schema;
  }

  async queryByKey<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ): Promise<StorageEngineQueryResult<TValue>> {
    await this.initialize();
    const tx = this.startTransaction([collectionName], "readonly");
    return await tx.queryByKey<TValue>(collectionName, key);
  }

  async queryByCondition<
    TValue extends StorableJSONObject = StorableJSONObject
  >(
    collectionName: string,
    condition: Condition
  ): Promise<StorageEngineQueryResult<TValue>> {
    await this.initialize();
    const tx = this.startTransaction([collectionName], "readonly");
    return await tx.queryByCondition<TValue>(collectionName, condition);
  }

  async queryAll<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string
  ): Promise<StorageEngineQueryResult<TValue>> {
    await this.initialize();
    const tx = this.startTransaction([collectionName], "readonly");
    return await tx.queryAll<TValue>(collectionName);
  }

  async insert<TValue extends StorableJSONObject = StorableJSONObject>(
    option: InsertOptionWithKey<TValue> | InsertOptionWithoutKey<TValue>
  ): Promise<StorageEngineStoredValue<TValue>> {
    await this.initialize();
    const tx = this.startTransaction([option.collectionName], "readwrite");
    return await tx.insert<TValue>(option);
  }

  async update<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: Partial<TValue>;
    emitCDCEvent?: boolean | undefined;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    await this.initialize();
    const tx = this.startTransaction([option.collectionName], "readwrite");
    return await tx.update<TValue>(option);
  }

  async delete<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    emitCDCEvent?: boolean | undefined;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    await this.initialize();
    const tx = this.startTransaction([option.collectionName], "readwrite");
    return await tx.delete<TValue>(option);
  }

  async upsert<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: TValue;
    emitCDCEvent?: boolean | undefined;
  }): Promise<StorageEngineStoredValue<TValue>> {
    await this.initialize();
    const tx = this.startTransaction([option.collectionName], "readwrite");
    return await tx.upsert<TValue>(option);
  }

  async clear(option: {
    collectionName: string;
    emitCDCEvent?: boolean | undefined;
  }): Promise<void> {
    await this.initialize();
    const tx = this.startTransaction([option.collectionName], "readwrite");
    return await tx.clear(option);
  }

  get name() {
    return this.#name;
  }
}

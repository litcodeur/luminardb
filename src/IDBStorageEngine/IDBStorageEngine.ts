import { IDBPDatabase, openDB } from "idb";
import { Collection, IndexOptions } from "../Collection";
import { EnhancedStorageEngineTransaction } from "../EnhancedStorageEngineTransaction";
import { AnyDatabaseSchema } from "../types/Database";
import {
  StorageEngine,
  StorageEngineCDCEvent,
  StorageEngineCDCEventSubscriber,
} from "../types/StorageEngine";
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
  #transactionQueue: Array<{ resolve: () => void }> = [];
  #subscribers = new Set<StorageEngineCDCEventSubscriber>();
  #channel: BroadcastChannel;

  constructor(name: string, version: number, schema: TSchema) {
    this.#name = name;
    this.#version = version;
    this.#schema = schema;
    this.#channel = new BroadcastChannel(`IDBStorageEngine:${name}:${version}`);
  }

  #getIDB() {
    if (!this.#idb) {
      throw new Error("IDBStorageEngine is not initialized");
    }
    return this.#idb;
  }

  startTransaction() {
    let collectionNamesForTransaction = Array.from(this.#allCollectionNames);

    const { promise, resolve } = resolver();

    this.#transactionQueue.push({ resolve });

    const idbTX = new IDBStorageEngineTransaction(
      this.#getIDB(),
      collectionNamesForTransaction,
      "readwrite",
      promise
    );

    const tx = new EnhancedStorageEngineTransaction(idbTX);

    tx.onComplete((events) => {
      this.#notifyCDCSubscribers({ events });
      this.#transactionQueue.shift();
      if (this.#transactionQueue.length > 0) {
        this.#transactionQueue[0]!.resolve();
      }
    });

    if (this.#transactionQueue.length === 1) {
      resolve();
    }

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

  get name() {
    return this.#name;
  }
}

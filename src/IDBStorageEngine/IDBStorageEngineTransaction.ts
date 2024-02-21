import { IDBPDatabase, IDBPIndex, IDBPObjectStore, IDBPTransaction } from "idb";
import { Condition } from "../Condition";
import {
  InsertOptionWithKey,
  StorageEngineQueryResult,
  StorageEngineStoredValue,
  StorageEngineTransaction,
  StorageEngineTransactionMode,
  StorageEngineValidKey,
} from "../types/StorageEngine";
import { StorableJSONObject } from "../types/types";
import { Log } from "./IDBStorageEngine";

export class IDBStorageEngineTransaction implements StorageEngineTransaction {
  #tx: IDBPTransaction<unknown, string[], StorageEngineTransactionMode> | null =
    null;
  #isActive: boolean = false;
  #txOptions: {
    idb: IDBPDatabase;
    collectionNames: Array<string>;
    mode: StorageEngineTransactionMode;
    queuePromise: Promise<void>;
  };

  #onCompleteCallbacks: Set<(transactionLogs: Array<Log>) => void> = new Set();
  #onErrorCallbacks: Set<(error: Event) => void> = new Set();
  #normalizedBufferedLogs: Array<Log> = [];
  #durability: "memory" | "disk" = "disk";
  #txLogs: Array<Log> = [];

  public logStuff() {
    console.log(
      "logStuff",
      this.#normalizedBufferedLogs,
      this.#txLogs,
      this.#durability
    );
  }

  async #getTX() {
    if (this.#tx) {
      return this.#tx;
    }

    const { collectionNames, mode, idb, queuePromise } = this.#txOptions;

    await queuePromise;

    this.#tx = idb.transaction(collectionNames, mode);

    this.#isActive = true;

    this.#tx.oncomplete = () => {
      this.#isActive = false;
      for (let callback of this.#onCompleteCallbacks) {
        callback(this.#txLogs);
      }
    };

    this.#tx.onerror = (event) => {
      this.#onErrorCallbacks.forEach((c) => c(event));
      this.#isActive = false;
    };

    return this.#tx;
  }

  constructor(
    idb: IDBPDatabase,
    collectionNames: Array<string>,
    mode: StorageEngineTransactionMode,
    queuePromise: Promise<void>,
    bufferedLogs: Array<Array<Log>>,
    durability: "memory" | "disk" = "disk"
  ) {
    this.#txOptions = { idb, collectionNames, mode, queuePromise };
    this.#normalizedBufferedLogs = bufferedLogs.flat();
    this.#durability = durability;
  }

  onComplete(callback: (logs: Array<Log>) => void): void {
    this.#onCompleteCallbacks.add(callback);
  }

  onError(callback: (error: Event) => void): void {
    this.#onErrorCallbacks.add(callback);
  }

  #getStoreOrIndexAndKeyRange<
    ObjectStore extends IDBPObjectStore<
      unknown,
      string[],
      string,
      "readonly" | "readwrite"
    >
  >(store: ObjectStore, condition?: Condition) {
    if (!condition) {
      return { storeOrIndex: store, keyRange: undefined };
    }

    const filterByField = condition.key;

    if (!filterByField) {
      return { storeOrIndex: store, keyRange: undefined };
    }

    const keyRange = condition.generateIDBKeyRange();

    let storeOrIndex:
      | ObjectStore
      | IDBPIndex<unknown, [string], string, string, "readonly" | "readwrite"> =
      store;

    const keyPath = store.keyPath as string;

    if (!!filterByField && keyPath !== filterByField) {
      storeOrIndex = store.index(filterByField) as IDBPIndex<
        unknown,
        [string],
        string,
        string,
        "readonly" | "readwrite"
      >;
    }

    return { storeOrIndex, keyRange };
  }

  async commit(): Promise<void> {
    const tx = await this.#getTX();
    /**
     * Not explicity handling handleOnCompleteCallBacks here
     * Because they would be handled in the tx.oncomplete fn
     * initially passed to the constructor
     */
    await tx.done;
  }

  async rollback(): Promise<void> {
    const tx = await this.#getTX();
    tx.abort();
    return;
  }

  isActive(): boolean {
    return this.#isActive;
  }

  async queryByKey<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ): Promise<StorageEngineQueryResult<TValue>> {
    const tx = await this.#getTX();

    const store = tx.objectStore(collectionName);

    const storedData = (await store.get(key)) as
      | StorageEngineStoredValue<TValue>
      | undefined;

    const result = new Map() as StorageEngineQueryResult<TValue>;

    if (storedData) {
      const { key, value } = storedData;
      result.set(key, value);
    }

    return result;
  }

  async queryByCondition<
    TValue extends StorableJSONObject = StorableJSONObject
  >(
    collectionName: string,
    condition: Condition
  ): Promise<StorageEngineQueryResult<TValue>> {
    const tx = await this.#getTX();

    const store = tx.objectStore(collectionName);

    const { storeOrIndex, keyRange } = this.#getStoreOrIndexAndKeyRange(
      store,
      condition
    );

    const storedValues = (await storeOrIndex.getAll(keyRange)) as Array<
      StorageEngineStoredValue<TValue>
    >;

    const result = new Map() as StorageEngineQueryResult<TValue>;

    storedValues.forEach(({ key, value }) => {
      result.set(key, value);
    });

    return result;
  }

  async queryAll<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string
  ): Promise<StorageEngineQueryResult<TValue>> {
    const tx = await this.#getTX();
    const store = tx.objectStore(collectionName);

    const storedValues = (await store.getAll()) as Array<
      StorageEngineStoredValue<TValue>
    >;

    const result = new Map() as StorageEngineQueryResult<TValue>;

    storedValues.forEach(({ key, value }) => {
      result.set(key, value);
    });

    return result;
  }

  async clear(option: { collectionName: string }): Promise<void> {
    const tx = await this.#getTX();

    const { collectionName } = option;
    const store = tx.objectStore(collectionName);

    if (!store.clear) {
      throw new Error("Invalid collection name");
    }

    await store.clear();

    return;
  }

  async insert<TValue extends StorableJSONObject = StorableJSONObject>(
    option: InsertOptionWithKey<TValue>
  ): Promise<StorageEngineStoredValue<TValue>> {
    const tx = await this.#getTX();

    const typedOption = option as InsertOptionWithKey<TValue>;

    const { collectionName, key, value } = typedOption;

    const store = tx.objectStore(collectionName);

    if (typeof store.add === "undefined") {
      throw new Error("Invalid collection name");
    }

    const ts = Date.now();

    await store.add({ key, value }, key);

    return { key, value, ts };
  }

  async update<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: Partial<TValue>;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    const tx = await this.#getTX();

    const { collectionName, key, value } = option;

    const documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (!documentResult.has(key)) return null;

    const currentValue = documentResult.get(key) as TValue;

    const store = tx.objectStore(collectionName);

    const valueToInsert = { ...currentValue, ...value };

    if (typeof store.put === "undefined") {
      throw new Error("Invalid collection name");
    }

    const putKey = store.autoIncrement ? undefined : key;

    const ts = Date.now();

    await store.put({ key, value: valueToInsert, ts }, putKey);

    return { key, value: valueToInsert, ts };
  }

  async delete<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
  }): Promise<Omit<StorageEngineStoredValue<TValue>, "ts"> | null> {
    const tx = await this.#getTX();

    const { collectionName, key } = option;

    const documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (!documentResult.has(key)) return null;

    const storedValue = documentResult.get(key) as TValue;

    const store = tx.objectStore(collectionName);

    if (typeof store.delete === "undefined") {
      throw new Error("Invalid collection name");
    }

    await store.delete(key);

    return { key, value: storedValue };
  }

  async upsert<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: TValue;
  }): Promise<StorageEngineStoredValue<TValue>> {
    const { collectionName, key, value } = option;

    const documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (documentResult.has(key)) {
      return this.update<TValue>({
        collectionName,
        key,
        value,
      }) as Promise<StorageEngineStoredValue<TValue>>;
    }

    return this.insert<TValue>({ collectionName, key, value });
  }
}

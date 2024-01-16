import { IDBPDatabase, IDBPIndex, IDBPObjectStore, IDBPTransaction } from "idb";
import { Condition } from "../Condition";
import {
  InsertOptionWithKey,
  InsertOptionWithoutKey,
  StorageEngineQueryResult,
  StorageEngineStoredValue,
  StorageEngineTransaction,
  StorageEngineTransactionMode,
  StorageEngineValidKey,
} from "../types/StorageEngine";
import { StorableJSONObject } from "../types/types";
import { isUndefined } from "../utils";

export class IDBStorageEngineTransaction implements StorageEngineTransaction {
  #tx: IDBPTransaction<unknown, string[], StorageEngineTransactionMode>;
  #isActive: boolean = false;

  #onCompleteCallbacks: Set<() => void> = new Set();
  #onErrorCallbacks: Set<(error: Event) => void> = new Set();

  constructor(
    protected idb: IDBPDatabase,
    protected collectionNames: Array<string>,
    protected mode: StorageEngineTransactionMode
  ) {
    this.#tx = idb.transaction(collectionNames, mode);

    this.#isActive = true;

    this.#tx.oncomplete = () => {
      this.#isActive = false;
      for (let callback of this.#onCompleteCallbacks) {
        callback();
      }
    };

    this.#tx.onerror = (event) => {
      this.#onErrorCallbacks.forEach((c) => c(event));
      this.#isActive = false;
    };
  }

  onComplete(callback: () => void): void {
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
    /**
     * Not explicity handling handleOnCompleteCallBacks here
     * Because they would be handled in the tx.oncomplete fn
     * initially passed to the constructor
     */
    await this.#tx.done;
  }

  rollback(): void {
    this.#tx.abort();
    return;
  }

  isActive(): boolean {
    return this.#isActive;
  }

  async queryByKey<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ): Promise<StorageEngineQueryResult<TValue>> {
    const store = this.#tx.objectStore(collectionName);

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
    const store = this.#tx.objectStore(collectionName);

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
    const store = this.#tx.objectStore(collectionName);

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
    const { collectionName } = option;
    const store = this.#tx.objectStore(collectionName);

    if (!store.clear) {
      throw new Error("Invalid collection name");
    }

    await store.clear();

    return;
  }

  async insert<TValue extends StorableJSONObject = StorableJSONObject>(
    option: InsertOptionWithKey<TValue> | InsertOptionWithoutKey<TValue>
  ): Promise<StorageEngineStoredValue<TValue>> {
    if (isUndefined((option as InsertOptionWithKey<TValue>).key)) {
      const typedOption = option as InsertOptionWithoutKey<TValue>;

      const { collectionName, value } = typedOption;
      const store = this.#tx.objectStore(collectionName);

      if (typeof store.add === "undefined") {
        throw new Error("Invalid collection name");
      }

      const key = (await store.add({ value })) as StorageEngineValidKey;

      return { key, value };
    }

    const typedOption = option as InsertOptionWithKey<TValue>;

    const { collectionName, key, value } = typedOption;

    const store = this.#tx.objectStore(collectionName);

    if (typeof store.add === "undefined") {
      throw new Error("Invalid collection name");
    }

    await store.add({ key, value }, key);

    return { key, value };
  }

  async update<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: Partial<TValue>;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    const { collectionName, key, value } = option;

    const documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (!documentResult.has(key)) return null;

    const currentValue = documentResult.get(key) as TValue;

    const store = this.#tx.objectStore(collectionName);

    const valueToInsert = { ...currentValue, ...value };

    if (typeof store.put === "undefined") {
      throw new Error("Invalid collection name");
    }

    const putKey = store.autoIncrement ? undefined : key;

    await store.put({ key, value: valueToInsert }, putKey);

    return { key, value: valueToInsert };
  }

  async delete<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    const { collectionName, key } = option;

    const documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (!documentResult.has(key)) return null;

    const storedValue = documentResult.get(key) as TValue;

    const store = this.#tx.objectStore(collectionName);

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

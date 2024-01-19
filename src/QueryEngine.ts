import { Collection } from ".";
import { Condition } from "./Condition";
import { Query, isQueryOptionDocumentQueryOption } from "./Query";
import { FilterOption, QueryOption } from "./types/Query";
import {
  StorageEngine,
  StorageEngineCDCEvent,
  StorageEngineQueryResult,
  StorageEngineValidKey,
} from "./types/StorageEngine";
import { StorableJSONObject } from "./types/types";
import { hashObject, isNotUndefined, resolver } from "./utils";

export class QueryEngine {
  #storageEngine: StorageEngine;
  #cache = new Map<string, Query<any>>();
  #initializationPromise: Promise<void> | null = null;

  #queue = new Map<
    string,
    {
      option: QueryOption;
      resolve: (result: StorageEngineQueryResult<any>) => void;
      reject: (error: Error) => void;
      promise: Promise<StorageEngineQueryResult<any>>;
    }
  >();

  timeout: ReturnType<typeof setTimeout> | null = null;

  async #processQueue() {
    try {
      const tx = this.#storageEngine.startTransaction("ALL", "readonly");

      for (let [_, { option, resolve }] of this.#queue) {
        if (isQueryOptionDocumentQueryOption(option)) {
          const { collectionName, key } = option;
          const result = await tx.queryByKey(collectionName, key);
          resolve(result);
          continue;
        }

        if (isNotUndefined(option.filterOption)) {
          const condition = new Condition(option.filterOption as any);
          const result = await tx.queryByCondition(
            option.collectionName,
            condition
          );
          resolve(result);
          continue;
        }

        const result = await tx.queryAll(option.collectionName);
        resolve(result);
      }
    } catch (error) {
      for (let [_, { reject }] of this.#queue) {
        reject(error as any);
      }
    } finally {
      this.#queue.clear();
      clearTimeout(this.timeout!);
      this.timeout = null;
    }
  }

  constructor(
    storageEngine: StorageEngine,
    initializationPromise: Promise<void> | null
  ) {
    this.#storageEngine = storageEngine;
    this.#initializationPromise = initializationPromise;
  }

  handleCDCEvents(event: StorageEngineCDCEvent) {
    const queriesAffected = new Set<Query<any>>();
    for (let [_, query] of this.#cache) {
      for (let e of event.events) {
        if (query.doesCDCEventAffectResult(e)) {
          queriesAffected.add(query);
          break;
        }
      }
    }

    for (let query of queriesAffected) {
      query.updateResultFromCDCEvents(event.events);
    }
  }

  #getResultFromStorage(option: QueryOption) {
    const { promise, resolve, reject } = resolver<StorageEngineQueryResult>();

    const hash = this.#hashQueryOption(option);

    if (this.#queue.has(hash)) {
      const { promise } = this.#queue.get(hash)!;
      return promise;
    }

    this.#queue.set(hash, {
      option,
      resolve,
      reject,
      promise,
    });

    if (!this.timeout) {
      this.timeout = setTimeout(() => this.#processQueue(), 5);
    }

    return promise;
  }

  #hashQueryOption = (option: QueryOption) => {
    return hashObject(option);
  };

  get<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ) {
    const option = {
      method: "get",
      collectionName,
      key,
    } satisfies QueryOption;

    const hash = this.#hashQueryOption(option);

    if (!this.#cache.has(hash)) {
      const query = new Query<TValue>({
        option,
        getResultFromReadTransaction: async () => {
          if (this.#initializationPromise) {
            await this.#initializationPromise;
          }
          return this.#getResultFromStorage(option);
        },
      });

      this.#cache.set(hash, query);
    }

    return this.#cache.get(hash)! as Query<TValue>;
  }

  getAll<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    filterOption?: FilterOption<Collection<any, any>>
  ) {
    const option = {
      method: "getAll",
      collectionName,
      filterOption,
    } satisfies QueryOption;

    const hash = this.#hashQueryOption(option);

    if (!this.#cache.has(hash)) {
      const query = new Query({
        option,
        getResultFromReadTransaction: async () => {
          if (this.#initializationPromise) {
            await this.#initializationPromise;
          }

          return this.#getResultFromStorage(option);
        },
      });

      this.#cache.set(hash, query);
    }

    return this.#cache.get(hash)! as Query<Array<TValue>>;
  }
}

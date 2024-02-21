import { IDBStorageEngine } from "./IDBStorageEngine/IDBStorageEngine";
import { INTERNAL_SCHEMA, LockMeta, META } from "./InternalSchema";
import { MaybePromise } from "./types/types";
import { ONE_MINUTE_IN_MS, ONE_SECOND_IN_MS, sleep } from "./utils";

export interface PersistentStorage {
  getItem<T>(key: string): MaybePromise<T | undefined>;
  setItem<T>(key: string, value: T): MaybePromise<T>;
  removeItem<T>(key: string): MaybePromise<T | undefined>;
}

export function createIDBStorageEnginePersistentStorage(
  engine: IDBStorageEngine
): PersistentStorage {
  return {
    async getItem<T>(key: string) {
      const tx = engine.startTransaction();
      const documentResult = await tx.queryByKey(
        INTERNAL_SCHEMA[META].name,
        key
      );
      return documentResult.get(key) as T | undefined;
    },
    async setItem<T>(key: string, value: T) {
      const tx = engine.startTransaction();
      const result = await tx.update({
        collectionName: INTERNAL_SCHEMA[META].name,
        key,
        value: value as any,
      });

      return result?.value as T;
    },
    async removeItem<T>(key: string) {
      const tx = engine.startTransaction();
      const result = await tx.delete({
        collectionName: INTERNAL_SCHEMA[META].name,
        key,
      });

      return result?.value as T;
    },
  };
}

export const createLocalStoragePersistentStorage = (
  prefix: string
): PersistentStorage => {
  const getPrefixedKey = (key: string) => `${prefix}:${key}`;
  return {
    getItem<T>(key: string) {
      const storedString = localStorage.getItem(getPrefixedKey(key));

      if (!storedString) {
        return undefined;
      }

      return JSON.parse(storedString) as T;
    },
    setItem<T>(key: string, value: T) {
      localStorage.setItem(getPrefixedKey(key), JSON.stringify(value));
      return value;
    },
    removeItem(key) {
      const storedString = localStorage.getItem(getPrefixedKey(key));

      if (!storedString) {
        return undefined;
      }

      const value = JSON.parse(storedString);

      localStorage.removeItem(getPrefixedKey(key));

      return value;
    },
  };
};

export class LockController {
  #persistentStorage: PersistentStorage;
  #id = Math.random().toString();

  constructor(persistentStorage: PersistentStorage) {
    this.#persistentStorage = persistentStorage;
  }

  async request(
    lockName: string,
    callback: () => MaybePromise<void>,
    timeoutInMs = 5 * ONE_MINUTE_IN_MS
  ) {
    let isLocked = false;

    let that = this;

    async function checkLock() {
      const documentResult = await that.#persistentStorage.getItem<LockMeta>(
        lockName
      );

      if (documentResult) {
        if (documentResult.id === that.#id) {
          isLocked = false;
          return;
        }
        isLocked = documentResult.locked;
      } else {
        isLocked = false;
      }
    }

    let totalTimeOut = 0;

    await checkLock();

    while (isLocked) {
      await sleep(ONE_SECOND_IN_MS);
      await checkLock();
      totalTimeOut += ONE_SECOND_IN_MS;
      if (totalTimeOut > timeoutInMs) {
        await this.#persistentStorage.removeItem(lockName);
      }
    }

    await this.#persistentStorage.setItem<LockMeta>(lockName, {
      locked: true,
      id: this.#id,
    });

    await callback();

    await this.#persistentStorage.removeItem(lockName);
  }
}

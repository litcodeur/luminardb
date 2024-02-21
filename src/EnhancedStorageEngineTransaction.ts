import { Condition } from "./Condition";
import {
  DatabaseMutation,
  INTERNAL_SCHEMA,
  INTERNAL_SCHEMA_COLLECTION_NAMES,
  MUTATION,
} from "./InternalSchema";
import { CDCEvent } from "./types/CDCEvent";
import {
  StorageEngineQueryResult,
  StorageEngineStoredValue,
  StorageEngineTransaction,
  StorageEngineValidKey,
} from "./types/StorageEngine";
import { StorableJSONObject } from "./types/types";
import { getIncrementingTimestamp, isUndefined } from "./utils";

export type PendingDocumentState =
  | {
      state: "INSERTED" | "DELETED";
      key: StorageEngineValidKey;
      value: StorableJSONObject;
    }
  | {
      state: "UPDATED";
      value: StorableJSONObject;
      delta: StorableJSONObject;
      key: StorageEngineValidKey;
    }
  | {
      state: "UPDATE_POST_INSERT";
      delta: StorableJSONObject;
      key: StorageEngineValidKey;
      value: StorableJSONObject;
    };

export class EnhancedStorageEngineTransaction {
  #tx: StorageEngineTransaction;
  #isActive: boolean = false;
  #shouldPublishCDCEvents = true;
  #cdcEvents: Array<CDCEvent> = [];

  #onCompleteCallbacks: Set<(cdcEvents: Array<CDCEvent>) => void> = new Set();
  #onErrorCallbacks: Set<(error: Event) => void> = new Set();

  constructor(storageEngineTransaction: StorageEngineTransaction) {
    this.#tx = storageEngineTransaction;
    this.#isActive = true;

    this.#tx.onComplete(() => {
      if (this.#shouldPublishCDCEvents) {
        this.#onCompleteCallbacks.forEach((c) => c(this.#cdcEvents));
      }
    });
  }

  onComplete(callback: (cdcEvents: CDCEvent[]) => void): void {
    this.#onCompleteCallbacks.add(callback);
  }

  onError(callback: (error: Event) => void): void {
    this.#onErrorCallbacks.add(callback);
  }

  async commit(): Promise<CDCEvent[]> {
    /**
     * Not explicity handling handleOnCompleteCallBacks here
     * Because they would be handled in the tx.oncomplete fn
     * initially passed to the constructor
     */
    await this.#tx.commit();
    return this.#cdcEvents;
  }

  rollback(): void {
    this.#shouldPublishCDCEvents = false;
    this.#tx.rollback();
    return;
  }

  isActive(): boolean {
    return this.#isActive;
  }

  async #getPendingDocumentChangesCollectionMap() {
    const collectionDocumentStateMap = new Map<
      string,
      Map<StorageEngineValidKey, PendingDocumentState>
    >();

    const getDocumentStateMapForCollection = (collectionName: string) => {
      if (!collectionDocumentStateMap.has(collectionName)) {
        collectionDocumentStateMap.set(collectionName, new Map());
      }
      return collectionDocumentStateMap.get(collectionName)!;
    };

    const mutationsQueryResult = await this.#tx.queryAll<DatabaseMutation>(
      INTERNAL_SCHEMA[MUTATION].name
    );

    const mutations = Array.from(mutationsQueryResult.values());

    if (mutations.length === 0) {
      return collectionDocumentStateMap;
    }

    const sortedChanges = mutations
      .filter((m) => m.isCompleted)
      .flatMap((m) => m.changes)
      .sort(function (a, b) {
        const [aMutationId, aTimestamp] = a.id.split("-");
        const [bMutationId, bTimestamp] = b.id.split("-");

        const aMutationIdAsNumber = parseInt(aMutationId!);
        const bMutationIdAsNumber = parseInt(bMutationId!);

        const aTimestampAsNumber = parseInt(aTimestamp!);
        const bTimestampAsNumber = parseInt(bTimestamp!);

        if (aMutationIdAsNumber === bMutationIdAsNumber) {
          return aTimestampAsNumber - bTimestampAsNumber;
        }

        return aMutationIdAsNumber - bMutationIdAsNumber;
      });

    for (const change of sortedChanges) {
      const documentStateMap = getDocumentStateMapForCollection(
        change.collectionName
      );

      if (change.action === "INSERT" || change.action === "DELETE") {
        documentStateMap.set(change.key, {
          state: change.action === "INSERT" ? "INSERTED" : "DELETED",
          key: change.key,
          value: change.value,
        });
        continue;
      }

      const existingDocumentState = documentStateMap.get(change.key);

      if (isUndefined(existingDocumentState)) {
        documentStateMap.set(change.key, {
          state: "UPDATED",
          delta: change.delta,
          key: change.key,
          value: change.postUpdateValue,
        });
        continue;
      }

      const isDocumentInPendingDeleteState =
        existingDocumentState.state === "DELETED";

      if (isDocumentInPendingDeleteState) {
        // TODO: warn the user that something might be wrong as they tried to perform an update on a deleted document.
        continue;
      }

      const isDocumentInPendingInsertState =
        existingDocumentState.state === "INSERTED";

      if (isDocumentInPendingInsertState) {
        const mergedValue = { ...existingDocumentState.value, ...change.delta };
        documentStateMap.set(change.key, {
          state: "UPDATE_POST_INSERT",
          key: change.key,
          delta: change.delta,
          value: mergedValue,
        });
        continue;
      }

      const isDocumentInPendingUpdatePostInsertState =
        existingDocumentState.state === "UPDATE_POST_INSERT";

      if (isDocumentInPendingUpdatePostInsertState) {
        const mergedValue = {
          ...existingDocumentState.value,
          ...change.delta,
        };
        const mergedDelta = { ...existingDocumentState.delta, ...change.delta };

        documentStateMap.set(change.key, {
          state: "UPDATE_POST_INSERT",
          key: change.key,
          delta: mergedDelta,
          value: mergedValue,
        });
        continue;
      }

      const isDocumentInPendingUpdateState =
        existingDocumentState.state === "UPDATED";

      if (isDocumentInPendingUpdateState) {
        const mergedValue = {
          ...existingDocumentState.value,
          ...change.delta,
        };
        const mergedDelta = { ...existingDocumentState.delta, ...change.delta };

        documentStateMap.set(change.key, {
          state: "UPDATED",
          key: change.key,
          delta: mergedDelta,
          value: mergedValue,
        });
        continue;
      }
    }

    return collectionDocumentStateMap;
  }

  async #getPendingDocumentChanges(
    collectionName: string
  ): Promise<Map<StorageEngineValidKey, PendingDocumentState>> {
    const collectionDocumentStateMap =
      await this.#getPendingDocumentChangesCollectionMap();

    if (!collectionDocumentStateMap.has(collectionName)) {
      return new Map();
    }

    return collectionDocumentStateMap.get(collectionName)!;
  }

  async queryByKey<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ): Promise<StorageEngineQueryResult<TValue>> {
    const storageEngineResult = await this.#tx.queryByKey<TValue>(
      collectionName,
      key
    );

    const pendingDocumentStateMap = await this.#getPendingDocumentChanges(
      collectionName
    );

    if (pendingDocumentStateMap.size === 0) {
      return storageEngineResult;
    }

    const pendingDocumentState = pendingDocumentStateMap.get(key);

    if (!pendingDocumentState) {
      return storageEngineResult;
    }

    if (
      pendingDocumentState.state === "INSERTED" ||
      pendingDocumentState.state === "UPDATE_POST_INSERT"
    ) {
      storageEngineResult.set(key, pendingDocumentState.value as TValue);
      return storageEngineResult;
    }

    if (pendingDocumentState.state === "DELETED") {
      storageEngineResult.delete(key);
      return storageEngineResult;
    }

    const storageEngineData = storageEngineResult.get(key) as TValue;
    const delta = (pendingDocumentState as any).delta;

    if (!storageEngineData) {
      return storageEngineResult;
    }

    const mergedValue = {
      ...storageEngineData,
      ...delta,
    };

    storageEngineResult.set(key, mergedValue);
    return storageEngineResult;
  }

  async queryByCondition<
    TValue extends StorableJSONObject = StorableJSONObject
  >(
    collectionName: string,
    condition: Condition
  ): Promise<StorageEngineQueryResult<TValue>> {
    const storageEngineResult = await this.#tx.queryByCondition<TValue>(
      collectionName,
      condition
    );

    const pendingDocumentStateMap = await this.#getPendingDocumentChanges(
      collectionName
    );

    const result = new Map() as StorageEngineQueryResult<TValue>;

    storageEngineResult.forEach((value, key) => {
      result.set(key, value);
    });

    for (let [key, pendingDocumentState] of pendingDocumentStateMap) {
      if (
        (pendingDocumentState.state === "INSERTED" &&
          condition.doesDataSatisfyCondition(pendingDocumentState.value)) ||
        (pendingDocumentState.state === "UPDATE_POST_INSERT" &&
          condition.doesDataSatisfyCondition(pendingDocumentState.value))
      ) {
        result.set(key, pendingDocumentState.value as TValue);
        continue;
      }

      if (
        pendingDocumentState.state === "DELETED" &&
        condition.doesDataSatisfyCondition(pendingDocumentState.value)
      ) {
        result.delete(key);
        continue;
      }

      if (pendingDocumentState.state === "UPDATED") {
        let existingDocumentData = result.get(key) as TValue;

        if (
          !existingDocumentData &&
          condition.doesDataSatisfyCondition(pendingDocumentState.delta)
        ) {
          const queryResult = await this.queryByKey(collectionName, key);
          existingDocumentData = queryResult.get(key) as TValue;
        }

        if (!existingDocumentData) {
          continue;
        }

        const mergedValue = {
          ...existingDocumentData,
          ...pendingDocumentState.delta,
        };

        if (condition.doesDataSatisfyCondition(mergedValue)) {
          result.set(key, mergedValue as TValue);
        }
      }
    }

    return result;
  }

  async queryAll<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string
  ): Promise<StorageEngineQueryResult<TValue>> {
    const storageEngineResult = await this.#tx.queryAll<TValue>(collectionName);

    const pendingDocumentStateMap = await this.#getPendingDocumentChanges(
      collectionName
    );

    const result = new Map() as StorageEngineQueryResult<TValue>;

    storageEngineResult.forEach((value, key) => {
      result.set(key, value);
    });

    for (let [key, pendingDocumentState] of pendingDocumentStateMap) {
      if (pendingDocumentState.state === "INSERTED") {
        result.set(key, pendingDocumentState.value as TValue);
        continue;
      }

      if (pendingDocumentState.state === "UPDATE_POST_INSERT") {
        const existingDocumentData = result.get(key) as TValue;
        if (existingDocumentData) {
          result.set(key, {
            ...existingDocumentData,
            ...pendingDocumentState.delta,
          });
          continue;
        }
        result.set(key, pendingDocumentState.value as TValue);
        continue;
      }

      if (pendingDocumentState.state === "DELETED") {
        result.delete(key);
        continue;
      }

      if (pendingDocumentState.state === "UPDATED") {
        let existingDocumentData = result.get(key) as TValue;

        if (!existingDocumentData) {
          const queryResult = await this.queryByKey(collectionName, key);
          existingDocumentData = queryResult.get(key) as TValue;
        }

        if (!existingDocumentData) {
          continue;
        }

        const mergedValue = {
          ...existingDocumentData,
          ...pendingDocumentState.delta,
        };

        result.set(key, mergedValue as TValue);
        continue;
      }
    }

    return result;
  }

  async clear(
    option: Parameters<StorageEngineTransaction["clear"]>[0]
  ): Promise<void> {
    const { collectionName, emitCDCEvent = true } = option;

    await this.#tx.clear({ collectionName, emitCDCEvent: false });

    if (emitCDCEvent) {
      this.#cdcEvents.push({
        action: "CLEAR",
        collectionName,
        timestamp: getIncrementingTimestamp(),
      });
    }

    return;
  }

  async insert<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: TValue;
    emitCDCEvent?: boolean;
    skipOptimisticDataCheck?: boolean;
  }): Promise<StorageEngineStoredValue<TValue>> {
    const typedOption = option as {
      collectionName: string;
      value: TValue;
      key: StorageEngineValidKey;
      skipOptimisticDataCheck?: boolean;
      emitCDCEvent?: boolean;
    };

    const {
      collectionName,
      key,
      value,
      emitCDCEvent = true,
      skipOptimisticDataCheck = false,
    } = typedOption;

    let queryResult = await this.queryByKey<TValue>(collectionName, key);

    if (skipOptimisticDataCheck) {
      queryResult = await this.#tx.queryByKey<TValue>(collectionName, key);
    }

    if (queryResult.has(key)) {
      throw new Error(
        `Document with key: ${key} already exists in collection: ${collectionName}`
      );
    }

    const { ts } = await this.#tx.insert({ collectionName, key, value });

    if (emitCDCEvent) {
      await this.#handlePushCDC({
        action: "INSERT",
        collectionName,
        key,
        value,
        timestamp: getIncrementingTimestamp(),
      });
    }

    return { key, value, ts };
  }

  async update<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: Partial<TValue>;
    emitCDCEvent?: boolean;
    skipOptimisticDataCheck?: boolean;
  }): Promise<StorageEngineStoredValue<TValue> | null> {
    const {
      collectionName,
      key,
      value,
      emitCDCEvent = true,
      skipOptimisticDataCheck = false,
    } = option;

    let queryResult = await this.queryByKey<TValue>(collectionName, key);

    if (skipOptimisticDataCheck) {
      queryResult = await this.#tx.queryByKey<TValue>(collectionName, key);
    }

    if (!queryResult.has(key)) return null;

    const currentValue = queryResult.get(key) as TValue;

    const isUpdateCommittingDatabaseMutationToDisk =
      collectionName === INTERNAL_SCHEMA[MUTATION].name && !!value.isCompleted;

    if (isUpdateCommittingDatabaseMutationToDisk && emitCDCEvent) {
      const typedValue = currentValue as any as DatabaseMutation;

      const changes = typedValue.changes;

      await this.#handlePushCDC(changes, true);
    }

    const valueToInsert = { ...currentValue, ...value };

    const result = await this.#tx.update({
      collectionName,
      key,
      value: valueToInsert,
    });

    if (emitCDCEvent) {
      await this.#handlePushCDC({
        action: "UPDATE",
        collectionName,
        key,
        postUpdateValue: valueToInsert,
        preUpdateValue: currentValue,
        delta: value,
        timestamp: getIncrementingTimestamp(),
      });
    }

    return { key, value: valueToInsert, ts: result!.ts };
  }

  async delete<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    emitCDCEvent?: boolean;
    skipOptimisticDataCheck?: boolean;
  }): Promise<Omit<StorageEngineStoredValue<TValue>, "ts"> | null> {
    const {
      collectionName,
      key,
      emitCDCEvent = true,
      skipOptimisticDataCheck = false,
    } = option;

    const queryResultWithOptimisticData = await this.queryByKey<TValue>(
      collectionName,
      key
    );

    let queryResult = queryResultWithOptimisticData;

    if (skipOptimisticDataCheck) {
      queryResult = await this.#tx.queryByKey<TValue>(collectionName, key);
    }

    if (!queryResult.has(key)) return null;

    const storedValue = queryResult.get(key) as TValue;

    await this.#tx.delete({ collectionName, key });

    const isOperationDeletingDatabaseMutationFromDisk =
      collectionName === INTERNAL_SCHEMA[MUTATION].name;

    if (isOperationDeletingDatabaseMutationFromDisk && emitCDCEvent) {
      await this.#handleDeleteDatabaseMutation(
        storedValue as any as DatabaseMutation
      );
    }

    if (emitCDCEvent && queryResultWithOptimisticData.has(key)) {
      await this.#handlePushCDC({
        action: "DELETE",
        collectionName,
        key: key,
        value: storedValue,
        timestamp: getIncrementingTimestamp(),
      });
    }

    return { key, value: storedValue };
  }

  async upsert<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: TValue;
    emitCDCEvent?: boolean;
    skipOptimisticDataCheck?: boolean;
  }): Promise<StorageEngineStoredValue<TValue>> {
    const {
      collectionName,
      key,
      value,
      emitCDCEvent = true,
      skipOptimisticDataCheck = false,
    } = option;

    let documentResult = await this.queryByKey<TValue>(collectionName, key);

    if (skipOptimisticDataCheck) {
      documentResult = await this.#tx.queryByKey<TValue>(collectionName, key);
    }

    if (documentResult.has(key)) {
      return this.update<TValue>({
        collectionName,
        key,
        value: value as Partial<TValue>,
        emitCDCEvent,
        skipOptimisticDataCheck,
      }) as Promise<StorageEngineStoredValue<TValue>>;
    }

    return this.insert<TValue>({
      collectionName,
      key,
      value: value as TValue,
      emitCDCEvent,
      skipOptimisticDataCheck,
    });
  }

  async #handleDeleteDatabaseMutation(mutation: DatabaseMutation) {
    if (!mutation.isCompleted) return;

    const changes = mutation.changes;

    const collectionDocumentStateMap =
      await this.#getPendingDocumentChangesCollectionMap();

    for (let change of changes) {
      const queryResult = await this.queryByKey(
        change.collectionName,
        change.key
      );

      const documentStateMap = collectionDocumentStateMap.get(
        change.collectionName
      );

      const pendingDocumentState = documentStateMap?.get(change.key);

      const document = queryResult.get(change.key);

      if (change.action === "DELETE") {
        if (!document) {
          continue;
        }

        /**
         * Okay so here's the example
         * | Optimistic DATA | Pending DATA|
         * | {foo: bar}      | {action:"DELETE", key: "foo"} |
         * |                 | {action:"INSERT",key: "foo", value:"baz"}  |
         * In this case if the first pending delete is deleted the final state of key is still an insert.
         * and assuming insert cdc worked correctly there isn't a need to emit a correctional cdc
         * to delete the key.
         */
        if (pendingDocumentState) {
          continue;
        }

        this.#cdcEvents.push({
          action: "INSERT",
          collectionName: change.collectionName,
          key: change.key,
          value: document,
          timestamp: getIncrementingTimestamp(),
        });
        continue;
      }

      if (change.action === "INSERT") {
        if (document) {
          continue;
        }

        this.#cdcEvents.push({
          action: "DELETE",
          collectionName: change.collectionName,
          key: change.key,
          value: change.value,
          timestamp: getIncrementingTimestamp(),
        });
        continue;
      }

      if (change.action === "UPDATE") {
        if (!queryResult.has(change.key)) {
          continue;
        }

        const preUpdateValue = queryResult.get(
          change.key
        ) as StorableJSONObject;

        const preUpdateDelta = Object.keys(change.delta).reduce((acc, key) => {
          acc[key] = preUpdateValue[key];
          return acc;
        }, {} as typeof change.delta);

        const modifiedEvent = {
          action: "UPDATE",
          collectionName: change.collectionName,
          key: change.key,
          timestamp: getIncrementingTimestamp(),
          delta: preUpdateDelta,
          postUpdateValue: change.preUpdateValue,
          preUpdateValue: change.postUpdateValue,
        } satisfies CDCEvent;

        this.#cdcEvents.push(modifiedEvent);

        continue;
      }
    }
  }

  async #handlePushCDC(
    event: Array<CDCEvent> | CDCEvent,
    isOptimistic = false
  ) {
    const events = Array.isArray(event) ? event : [event];

    const eventsToPush: Array<CDCEvent> = [];

    const collectionDocumentStateMap =
      await this.#getPendingDocumentChangesCollectionMap();

    for (let event of events) {
      if (INTERNAL_SCHEMA_COLLECTION_NAMES.includes(event.collectionName)) {
        eventsToPush.push(event);
        continue;
      }

      const pendingDocumentStateMap = collectionDocumentStateMap.get(
        event.collectionName
      );

      if (pendingDocumentStateMap?.size === 0) {
        eventsToPush.push(event);
        continue;
      }

      if (event.action === "CLEAR") {
        eventsToPush.push(event);
        continue;
      }

      const pendingDocumentState = pendingDocumentStateMap?.get(event.key);

      if (!pendingDocumentState) {
        eventsToPush.push(event);
        continue;
      }

      if (event.action === "INSERT") {
        if (pendingDocumentState.state === "INSERTED") {
          const preUpdateValue = event.value;
          const delta = pendingDocumentState.value;
          const postUpdateValue = { ...preUpdateValue, ...delta };

          eventsToPush.push({
            action: "UPDATE",
            collectionName: event.collectionName,
            key: event.key,
            timestamp: event.timestamp,
            preUpdateValue,
            postUpdateValue,
            delta,
          });
          continue;
        }

        if (pendingDocumentState.state === "UPDATED") {
          if (isOptimistic) {
            // TODO: warn the user that something might be wrong as they tried to perform an insert on an updated document.
          }

          const valueWithUpdates = {
            ...event.value,
            ...pendingDocumentState.delta,
          };

          eventsToPush.push({
            action: "INSERT",
            collectionName: event.collectionName,
            key: event.key,
            timestamp: event.timestamp,
            value: valueWithUpdates,
          });
          continue;
        }

        if (pendingDocumentState.state === "UPDATE_POST_INSERT") {
          if (isOptimistic) {
            // TODO: warn the user that something might be wrong as they tried to perform an insert on an updated document.
          }

          const preUpdateValue = event.value;
          const delta = pendingDocumentState.value;
          const postUpdateValue = { ...preUpdateValue, ...delta };

          eventsToPush.push({
            action: "UPDATE",
            collectionName: event.collectionName,
            key: event.key,
            timestamp: event.timestamp,
            preUpdateValue,
            postUpdateValue,
            delta,
          });
          continue;
        }

        if (pendingDocumentState.state === "DELETED") {
          if (!isOptimistic) continue;
          eventsToPush.push(event);
        }
      }

      if (event.action === "UPDATE") {
        if (
          pendingDocumentState.state === "INSERTED" ||
          pendingDocumentState.state === "UPDATE_POST_INSERT"
        ) {
          const preUpdateValue = isOptimistic
            ? pendingDocumentState.value
            : event.postUpdateValue;

          const delta = isOptimistic ? event.delta : {};

          const postUpdateValue = { ...preUpdateValue, ...delta };

          eventsToPush.push({
            action: "UPDATE",
            collectionName: event.collectionName,
            key: event.key,
            timestamp: event.timestamp,
            preUpdateValue,
            postUpdateValue,
            delta,
          });
          continue;
        }

        if (pendingDocumentState.state === "UPDATED") {
          const preUpdateValue = isOptimistic
            ? pendingDocumentState.value
            : event.postUpdateValue;

          const delta = isOptimistic
            ? { ...pendingDocumentState.delta, ...event.delta }
            : { ...event.delta, ...pendingDocumentState.delta };

          const postUpdateValue = { ...preUpdateValue, ...delta };

          const modifiedEvent = {
            action: "UPDATE",
            collectionName: event.collectionName,
            key: event.key,
            timestamp: event.timestamp,
            preUpdateValue,
            postUpdateValue,
            delta,
          } satisfies CDCEvent;

          eventsToPush.push(modifiedEvent);
          continue;
        }

        continue;
      }

      if (event.action === "DELETE") {
        if (isOptimistic) {
          eventsToPush.push(event);
          continue;
        }

        if (
          pendingDocumentState.state === "INSERTED" ||
          pendingDocumentState.state === "UPDATE_POST_INSERT"
        ) {
          continue;
        }

        if (pendingDocumentState.state === "UPDATED") {
          eventsToPush.push(event);
          continue;
        }

        continue;
      }
    }

    for (let event of eventsToPush) {
      this.#cdcEvents.push(event);
    }
  }
}

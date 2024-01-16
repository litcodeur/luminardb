import { Condition } from "./Condition";
import { Store } from "./Store";
import { CDCEvent } from "./types/CDCEvent";
import {
  CollectionQueryOption,
  DocumentQueryOption,
  QueryConfig,
  QueryOption,
  QueryResultChange,
  QueryState,
} from "./types/Query";
import { StorageEngineQueryResult } from "./types/StorageEngine";
import { StorableJSONValue } from "./types/types";
import { resolver } from "./utils";

export function isQueryOptionDocumentQueryOption(
  option: QueryOption,
  validate = false
): option is DocumentQueryOption {
  if (validate) {
    if (option.method !== "get") {
      throw new Error("Invalid query option, method must be get.");
    }
    if (!option.collectionName) {
      throw new Error("Invalid query option, collectionName must be present.");
    }
    if (!option.key) {
      throw new Error("Invalid query option, key must be present.");
    }
  }
  return option.method === "get";
}

export function isQueryOptionCollectionQueryOption(
  option: QueryOption,
  validate = false
): option is CollectionQueryOption {
  if (validate) {
    if (option.method !== "getAll") {
      throw new Error("Invalid query option, method must be getAll.");
    }
    if (!option.collectionName) {
      throw new Error("Invalid query option, collectionName must be present.");
    }
  }
  return option.method === "getAll";
}

export class Query<TData extends StorableJSONValue = StorableJSONValue> {
  #state: Store<QueryState<TData>>;

  #option: QueryConfig["option"];
  #readDataFn: () => Promise<StorageEngineQueryResult>;
  #storageEngineResult: StorageEngineQueryResult | undefined = undefined;

  #resolvingPromise: Promise<void> | null = null;

  constructor(config: QueryConfig) {
    this.#readDataFn = config.getResultFromReadTransaction;
    this.#option = config.option;
    this.#state = new Store<QueryState<TData>>({
      status: "idle",
    });
    this.#generateInitialState();
  }

  async #generateInitialState() {
    if (this.#resolvingPromise) {
      return this.#resolvingPromise;
    }

    const { promise, resolve, reject } = resolver<void>();

    this.#resolvingPromise = promise;

    this.#state.set({
      status: "reading",
    });

    try {
      const data = await this.#readDataFn();
      this.#storageEngineResult = data;
    } catch (error) {
      this.#state.set({
        status: "error",
        error,
      });
      reject(error);
      return;
    }

    if (isQueryOptionDocumentQueryOption(this.#option)) {
      this.#state.set({
        status: "success",
        data: this.#storageEngineResult!.get(this.#option.key) as any,
      });
    } else {
      const resultData: any = [];

      const changes: Array<QueryResultChange> = [];

      this.#storageEngineResult.forEach((value, key) => {
        resultData.push(value);
        changes.push({
          action: "INSERT",
          key,
          value,
        });
      });

      this.#state.set({
        status: "success",
        data: resultData as any,
        changes: changes,
      });
    }

    this.#resolvingPromise = null;

    resolve();
  }

  async getResult() {
    await this.#generateInitialState();
    return this.#state.get().data;
  }

  subscribe(callback: (state: QueryState<TData>) => void): () => void {
    callback(this.#state.get());
    return this.#state.subscribe(callback);
  }

  get option() {
    return this.#option;
  }

  private get condition() {
    if (this.#option.method !== "getAll") {
      return;
    }

    const filterOption = this.#option.filterOption;

    if (!filterOption) return;

    return new Condition(filterOption as any);
  }

  #doesCollectionCDCEventAffectResult(event: CDCEvent) {
    const collectionName = this.#option.collectionName;

    if (event.collectionName !== collectionName) {
      return false;
    }

    if (event.action === "CLEAR") {
      return true;
    }

    if (isQueryOptionDocumentQueryOption(this.#option)) {
      return this.#option.key === event.key;
    }

    const isQueryFetchingAllDocuments = !this.condition;

    if (isQueryFetchingAllDocuments) {
      return true;
    }

    if (event.action === "UPDATE") {
      return this.condition.doesDataSatisfyCondition(event.postUpdateValue);
    }

    return this.condition.doesDataSatisfyCondition(event.value);
  }

  doesCDCEventAffectResult(event: CDCEvent) {
    if (event.collectionName !== this.#option.collectionName) {
      return false;
    }

    return this.#doesCollectionCDCEventAffectResult(event);
  }

  #handleCDCEventForCollection(event: CDCEvent): Array<QueryResultChange> {
    if (!this.#storageEngineResult) {
      throw new Error(
        "Cannot handle cdc event while storage engine hasn't returned a value. This is likely a bug please report."
      );
    }

    const changes: Array<QueryResultChange> = [];

    if (event.collectionName !== this.#option.collectionName) {
      return changes;
    }

    if (event.action === "CLEAR") {
      for (let [key, value] of this.#storageEngineResult) {
        changes.push({
          action: "DELETE",
          key,
          value,
        });
        this.#storageEngineResult.delete(key);
      }
      return changes;
    }

    if (event.action === "DELETE") {
      if (
        isQueryOptionDocumentQueryOption(this.#option) &&
        this.#option.key === event.key
      ) {
        changes.push({
          action: "DELETE",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.delete(event.key);
        return changes;
      }

      const isQueryFetchingAllDocuments = !this.condition;

      if (isQueryFetchingAllDocuments) {
        changes.push({
          action: "DELETE",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.delete(event.key);
        return changes;
      }

      if (this.condition.doesDataSatisfyCondition(event.value)) {
        changes.push({
          action: "DELETE",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.delete(event.key);
        return changes;
      }

      return changes;
    }

    if (event.action === "INSERT") {
      if (
        isQueryOptionDocumentQueryOption(this.#option) &&
        this.#option.key === event.key
      ) {
        changes.push({
          action: "INSERT",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.set(event.key, event.value);
        return changes;
      }

      const isQueryFetchingAllDocuments = !this.condition;

      if (isQueryFetchingAllDocuments) {
        changes.push({
          action: "INSERT",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.set(event.key, event.value);
        return changes;
      }

      if (this.condition.doesDataSatisfyCondition(event.value)) {
        changes.push({
          action: "INSERT",
          key: event.key,
          value: event.value,
        });
        this.#storageEngineResult.set(event.key, event.value);
        return changes;
      }
      return changes;
    }

    const storageEngineValue = this.#storageEngineResult.get(event.key);

    if (!storageEngineValue) {
      return changes;
    }

    const postUpdateValue = { ...storageEngineValue, ...event.delta };

    if (
      isQueryOptionDocumentQueryOption(this.#option) &&
      event.key === this.#option.key
    ) {
      changes.push({
        action: "UPDATE",
        key: event.key,
        delta: event.delta,
        postUpdateValue: event.postUpdateValue,
        preUpdateValue: event.postUpdateValue,
      });
      this.#storageEngineResult.set(event.key, postUpdateValue);
      return changes;
    }

    const isQueryFetchingAllDocuments = !this.condition;

    if (isQueryFetchingAllDocuments) {
      changes.push({
        action: "UPDATE",
        key: event.key,
        delta: event.delta,
        postUpdateValue: event.postUpdateValue,
        preUpdateValue: event.postUpdateValue,
      });
      this.#storageEngineResult.set(event.key, postUpdateValue);
      return changes;
    }

    if (this.condition.doesDataSatisfyCondition(postUpdateValue)) {
      changes.push({
        action: "UPDATE",
        key: event.key,
        delta: event.delta,
        postUpdateValue: event.postUpdateValue,
        preUpdateValue: event.postUpdateValue,
      });
      this.#storageEngineResult.set(event.key, postUpdateValue);
      return changes;
    }

    return changes;
  }

  updateResultFromCDCEvents(events: Array<CDCEvent>) {
    const changes: Array<QueryResultChange> = [];

    for (let event of events) {
      if (event.collectionName !== this.#option.collectionName) {
        continue;
      }

      if (!this.#doesCollectionCDCEventAffectResult(event)) {
        continue;
      }

      const eventChanges = this.#handleCDCEventForCollection(event);

      for (let change of eventChanges) {
        changes.push(change);
      }
    }

    if (isQueryOptionDocumentQueryOption(this.#option)) {
      return this.#state.set({
        status: "success",
        data: this.#storageEngineResult!.get(this.#option.key) as any,
      });
    }

    return this.#state.set({
      status: "success",
      data: Array.from(this.#storageEngineResult!.values()) as any,
      changes,
    });
  }
}

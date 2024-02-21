import { AnyDatabaseSchema } from "..";
import { Condition } from "../Condition";
import { EnhancedStorageEngineTransaction } from "../EnhancedStorageEngineTransaction";
import { CDCEvent } from "./CDCEvent";
import { StorableJSONObject } from "./types";

export type StorageEngineTransactionMode = "readwrite" | "readonly";

export type StorageEngineValidKey = string | number;

export type StorageEngineStoredValue<
  T extends StorableJSONObject = StorableJSONObject
> = {
  key: StorageEngineValidKey;
  value: T;
};

export type StorageEngineQueryResult<
  T extends StorableJSONObject = StorableJSONObject
> = Map<StorageEngineValidKey, T>;

export type InsertOptionWithKey<TValue extends StorableJSONObject> = {
  collectionName: string;
  key: StorageEngineValidKey;
  value: TValue;
};

export type InsertOptionWithoutKey<TValue extends StorableJSONObject> = {
  collectionName: string;
  value: TValue;
};

export interface StorageEngineTransaction {
  onComplete(callback: () => void): void;

  onError(callback: (error: Event) => void): void;

  commit(): Promise<void>;

  rollback(): void;

  isActive(): boolean;

  queryByKey<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    key: StorageEngineValidKey
  ): Promise<StorageEngineQueryResult<TValue>>;

  queryAll<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string
  ): Promise<StorageEngineQueryResult<TValue>>;

  queryByCondition<TValue extends StorableJSONObject = StorableJSONObject>(
    collectionName: string,
    condition: Condition
  ): Promise<StorageEngineQueryResult<TValue>>;

  insert<TValue extends StorableJSONObject = StorableJSONObject>(
    option: InsertOptionWithKey<TValue> | InsertOptionWithoutKey<TValue>
  ): Promise<StorageEngineStoredValue<TValue>>;

  update<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: Partial<TValue>;
  }): Promise<StorageEngineStoredValue<TValue> | null>;

  delete<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
  }): Promise<StorageEngineStoredValue<TValue> | null>;

  upsert<TValue extends StorableJSONObject = StorableJSONObject>(option: {
    collectionName: string;
    key: StorageEngineValidKey;
    value: TValue;
  }): Promise<StorageEngineStoredValue<TValue>>;

  clear(option: {
    collectionName: string;
    emitCDCEvent?: boolean;
  }): Promise<void>;
}

export type StorageEngineCDCEvent = {
  events: Array<CDCEvent>;
};

export type StorageEngineCDCEventSubscriber = (
  cdcEvent: StorageEngineCDCEvent
) => void;

export interface StorageEngine<
  TSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> extends Pick<
    StorageEngineTransaction,
    | "queryAll"
    | "queryByCondition"
    | "queryByKey"
    | "insert"
    | "update"
    | "upsert"
    | "delete"
    | "clear"
  > {
  initialize(): Promise<void>;

  get schema(): TSchema;

  subscribeToCDC(subscriber: StorageEngineCDCEventSubscriber): () => void;

  startTransaction(): EnhancedStorageEngineTransaction;

  get name(): string;
}

import { StorageEngineValidKey } from "./StorageEngine";
import { StorableJSONObject } from "./types";

export type InsertCDCEvent<T = StorableJSONObject, C = string> = {
  action: "INSERT";
  collectionName: C;
  key: StorageEngineValidKey;
  value: T;
  timestamp: number;
};

export type UpdateCDCEvent<T = StorableJSONObject, C = string> = {
  action: "UPDATE";
  collectionName: C;
  key: StorageEngineValidKey;
  postUpdateValue: T;
  preUpdateValue: T;
  delta: Partial<T>;
  timestamp: number;
};

export type DeleteCDCEvent<T = StorableJSONObject, C = string> = {
  action: "DELETE";
  collectionName: C;
  key: StorageEngineValidKey;
  value: T;
  timestamp: number;
};

export type ClearCDCEvent<C = string> = {
  action: "CLEAR";
  collectionName: C;
  timestamp: number;
};

export type CDCEvent<T = StorableJSONObject, C = string> =
  | InsertCDCEvent<T, C>
  | UpdateCDCEvent<T, C>
  | DeleteCDCEvent<T, C>
  | ClearCDCEvent<C>;

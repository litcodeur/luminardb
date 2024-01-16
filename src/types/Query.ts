import {
  Collection,
  InferMetadataTypeFromCollection,
  InferSchemaTypeFromCollection,
} from "..";
import { ConditionComparators } from "../Condition";
import {
  StorageEngineQueryResult,
  StorageEngineValidKey,
} from "./StorageEngine";
import { ExactlyOne, StorableJSONObject } from "./types";

export type DocumentQueryOption = {
  method: "get";
  key: StorageEngineValidKey;
  collectionName: string;
};

export type CollectionQueryOption = {
  method: "getAll";
  collectionName: string;
  filterOption?: FilterOption<Collection<any, any>>;
};

export type QueryOption = DocumentQueryOption | CollectionQueryOption;

export type FilterOption<TCollection extends Collection<any, any>> = {
  where?: ExactlyOne<{
    [TFilterKey in keyof InferMetadataTypeFromCollection<TCollection>["indexes"]]?: ExactlyOne<
      Record<
        ConditionComparators,
        InferSchemaTypeFromCollection<TCollection>[TFilterKey]
      >
    >;
  }>;
};

export type QueryConfig<T extends StorableJSONObject = StorableJSONObject> = {
  option: QueryOption;
  getResultFromReadTransaction: () => Promise<StorageEngineQueryResult<T>>;
};

export type QueryStatus = "idle" | "reading" | "success" | "error";

export type QueryState<TData = unknown, TError extends any = unknown> = {
  data?: TData;
  error?: TError;
  status: QueryStatus;
  changes?: Array<QueryResultChange>;
};

type InsertQueryResultChange<
  T extends StorableJSONObject = StorableJSONObject
> = {
  action: "INSERT";
  key: StorageEngineValidKey;
  value: T;
};

type UpdateQueryResultChange<
  T extends StorableJSONObject = StorableJSONObject
> = {
  action: "UPDATE";
  key: StorageEngineValidKey;
  postUpdateValue: T;
  preUpdateValue: T;
  delta: Partial<T>;
};

type DeleteQueryResultChange<
  T extends StorableJSONObject = StorableJSONObject
> = {
  action: "DELETE";
  key: StorageEngineValidKey;
  value: T;
};

export type QueryResultChange<
  T extends StorableJSONObject = StorableJSONObject
> =
  | InsertQueryResultChange<T>
  | UpdateQueryResultChange<T>
  | DeleteQueryResultChange<T>;

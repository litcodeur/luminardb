import { Collection, CollectionMetadata } from "./Collection";
import { StorageEngineValidKey } from "./types/StorageEngine";
import { StorableJSONObject } from "./types/types";

type BasePendingMutationChange<C extends string = string> = {
  collectionName: C;
  timestamp: number;
  id: string;
};

export type PendingInsertMutationChange<
  T extends StorableJSONObject = StorableJSONObject,
  C extends string = string
> = BasePendingMutationChange<C> & {
  action: "INSERT";
  key: StorageEngineValidKey;
  value: T;
};

export type PendingUpdateMutationChange<
  T extends StorableJSONObject = StorableJSONObject,
  C extends string = string
> = BasePendingMutationChange<C> & {
  action: "UPDATE";
  postUpdateValue: T;
  key: StorageEngineValidKey;
  preUpdateValue: T;
  delta: Partial<T>;
};

export type PendingDeleteMutationChange<
  T extends StorableJSONObject = StorableJSONObject,
  C extends string = string
> = BasePendingMutationChange<C> & {
  action: "DELETE";
  value: T;
  key: StorageEngineValidKey;
};

export type PendingMutationChange<
  T extends StorableJSONObject = StorableJSONObject,
  C extends string = string
> =
  | PendingInsertMutationChange<T, C>
  | PendingUpdateMutationChange<T, C>
  | PendingDeleteMutationChange<T, C>;

export type DatabaseMutation = {
  id: number;
  collectionsAffected: Array<string>;
  mutationName: string;
  mutationArgs: StorableJSONObject;
  isCompleted: boolean;
  isPushed: boolean;
  remotePushAttempts?: number;
  changes: Array<PendingMutationChange>;
  localResolverResult?: StorableJSONObject;
  serverMutationId?: number;
};

const databaseMutationCollectionMetadata =
  {} satisfies CollectionMetadata<DatabaseMutation>;

const databaseMutationsCollection = new Collection<
  DatabaseMutation,
  typeof databaseMutationCollectionMetadata
>("__mutations", databaseMutationCollectionMetadata);

export type Cursor = string | number;

export type LockMeta = {
  locked: boolean;
  id: string;
};

export type CursroMeta = { cursor: Cursor };

export type Meta = LockMeta | CursroMeta;

const metaCollectionMetadata = {} satisfies CollectionMetadata<Meta>;

const metaCollection = new Collection<Meta, typeof metaCollectionMetadata>(
  "__meta",
  metaCollectionMetadata
);

export const MUTATION = Symbol("__mutations");

export const META = Symbol("__meta");

export const INTERNAL_SCHEMA = {
  [MUTATION]: databaseMutationsCollection,
  [META]: metaCollection,
} as const;

export const INTERNAL_SCHEMA_COLLECTION_NAMES = Object.getOwnPropertySymbols(
  INTERNAL_SCHEMA
).map((s) => (INTERNAL_SCHEMA as any)[s as any].name as string);

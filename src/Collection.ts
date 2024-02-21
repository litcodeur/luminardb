import { StorableJSONObject } from "./types/types";

export type IndexOptions =
  | true
  | {
      unique?: boolean;
      multiEntry?: boolean;
    };

export type IndexableKeysImpl<T, K extends keyof T> = K extends string
  ? T[K] extends string | number
    ? K
    : never
  : never;

export type IndexableKeys<T> = IndexableKeysImpl<T, keyof T>;

export type CollectionMetadata<
  TCollectionSchema extends StorableJSONObject = StorableJSONObject
> = {
  indexes?: Partial<Record<IndexableKeys<TCollectionSchema>, IndexOptions>>;
};

export type InferSchemaTypeFromCollection<TCollection = Collection<any, any>> =
  TCollection extends Collection<infer U, any> ? U : never;

export type InferMetadataTypeFromCollection<
  TCollection = Collection<any, any>
> = TCollection extends Collection<any, infer U> ? U : never;

export class Collection<
  TCollectionSchema extends StorableJSONObject = StorableJSONObject,
  TCollectionMetadata extends CollectionMetadata<TCollectionSchema> = CollectionMetadata<TCollectionSchema>
> {
  #name: string;
  #metadata: TCollectionMetadata;

  constructor(name: string, metadata: TCollectionMetadata) {
    this.#name = name;
    this.#metadata = metadata;
  }

  get name() {
    return this.#name;
  }

  get metadata() {
    return this.#metadata;
  }
}

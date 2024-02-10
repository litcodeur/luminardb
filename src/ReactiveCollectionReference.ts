import { Collection, CollectionMetadata } from ".";
import { QueryEngine } from "./QueryEngine";
import { FilterOption, QueryResultChange } from "./types/Query";
import { StorageEngineValidKey } from "./types/StorageEngine";
import { StorableJSONObject } from "./types/types";

export class ReactiveCollectionReference<
  TCollectionSchema extends StorableJSONObject = StorableJSONObject,
  TCollectionMetadata extends CollectionMetadata<TCollectionSchema> = CollectionMetadata<TCollectionSchema>
> {
  #collection: Collection<TCollectionSchema, TCollectionMetadata>;
  #queryEngine: QueryEngine;

  constructor(
    collection: Collection<TCollectionSchema, TCollectionMetadata>,
    queryEngine: QueryEngine
  ) {
    this.#collection = collection;
    this.#queryEngine = queryEngine;
  }

  get<TValue extends TCollectionSchema = TCollectionSchema>(
    key: StorageEngineValidKey
  ) {
    const getQuery = () => {
      return this.#queryEngine.get<TValue>(this.#collection.name, key);
    };

    return {
      execute: async () => {
        const query = getQuery();
        const result = await query.getResult();
        return result;
      },
      subscribe: (callback: (value: TValue | undefined) => void) => {
        const query = getQuery();

        return query.subscribe((s) => {
          if (s.status === "error") {
            throw s.error;
          }

          if (s.status !== "success") return;
          callback(s.data);
        });
      },
    };
  }

  getAll<TValue extends TCollectionSchema = TCollectionSchema>(
    option?: FilterOption<Collection<TCollectionSchema, TCollectionMetadata>>
  ) {
    const getQuery = () => {
      return this.#queryEngine.getAll<TValue>(this.#collection.name, option);
    };

    return {
      execute: async () => {
        const query = getQuery();
        const result = await query.getResult();
        return result as Array<TValue>;
      },
      subscribe: (callback: (value: Array<TValue>) => void) => {
        const query = getQuery();

        return query.subscribe((s) => {
          if (s.data) {
            callback(s.data);
          }
        });
      },
      watch: (
        callback: (changes: Array<QueryResultChange<TValue>>) => void
      ) => {
        const query = getQuery();
        return query.subscribe((s) => {
          if (s.changes) {
            callback(s.changes as Array<QueryResultChange<TValue>>);
          }
        });
      },
    };
  }
}

import {
  InferMetadataTypeFromCollection,
  InferSchemaTypeFromCollection,
  PullResponse,
} from ".";
import {
  DatabaseMutation,
  INTERNAL_SCHEMA,
  INTERNAL_SCHEMA_COLLECTION_NAMES,
  MUTATION,
} from "./InternalSchema";
import { QueryEngine } from "./QueryEngine";
import { ReactiveCollectionReference } from "./ReactiveCollectionReference";
import { ReadTransaction } from "./ReadTransaction";
import { SyncManager } from "./SyncManager";
import { InternalWriteTransaction, WriteTransaction } from "./WriteTransaction";
import { CDCEvent } from "./types/CDCEvent";
import {
  AnyDatabaseSchema,
  DatabaseCDCEvents,
  DatabaseConfig,
  DatabaseMutators,
} from "./types/Database";
import { Mutators } from "./types/Mutators";
import { StorageEngine } from "./types/StorageEngine";
import { Prettify } from "./types/types";
import { ONE_SECOND_IN_MS, resolver } from "./utils";

export class Database<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema,
  TMutators extends Mutators<TDatabaseSchema> = Mutators<TDatabaseSchema>
> {
  #storageEngine: StorageEngine<TDatabaseSchema>;
  #queryEngine: QueryEngine;
  #syncManager: SyncManager<TDatabaseSchema, TMutators>;
  #schema: TDatabaseSchema;
  #databaseMutators: DatabaseMutators<TMutators>;

  #initializationPromise: Promise<void> | null = null;
  #isInitialized = false;

  #constructLocalMutators(mutators?: TMutators): DatabaseMutators<TMutators> {
    if (!mutators) {
      return {} as DatabaseMutators<TMutators>;
    }

    let localMutators = {} as DatabaseMutators<TMutators>;

    Object.keys(mutators).forEach((key) => {
      const typedKey = key as keyof typeof localMutators;
      localMutators[typedKey] = async (args: any) => {
        if (this.#initializationPromise) {
          await this.#initializationPromise;
        }

        const storageEngineTransaction = this.#storageEngine.startTransaction(
          "ALL",
          "readwrite"
        );

        const internalWriteTransaction = new InternalWriteTransaction(
          this.#schema,
          storageEngineTransaction,
          key,
          args
        );

        await internalWriteTransaction.initialize();

        const writeTransaction = new WriteTransaction(internalWriteTransaction);

        try {
          const result = await mutators[typedKey]!(args).localResolver(
            writeTransaction
          );

          storageEngineTransaction.update<DatabaseMutation>({
            key: writeTransaction.mutationId,
            collectionName: INTERNAL_SCHEMA[MUTATION].name,
            value: {
              isCompleted: true,
              localResolverResult: result,
            },
          });

          await storageEngineTransaction.commit();

          void this.#syncManager.push();

          return result;
        } catch (error) {
          storageEngineTransaction.rollback();
          throw error;
        }
      };
    });

    return localMutators;
  }

  async #initialize() {
    if (this.#initializationPromise) {
      return this.#initializationPromise;
    }

    if (this.isInitialized) {
      return;
    }

    const r = resolver();

    this.#initializationPromise = r.promise;

    await this.#storageEngine.initialize();

    this.#storageEngine.subscribeToCDC(({ events }) => {
      this.#queryEngine.handleCDCEvents({ events });
    });

    await this.#syncManager.initialize();

    this.#isInitialized = true;

    r.resolve();

    this.#initializationPromise = null;
  }

  public async initialize() {
    await this.#initialize();
  }

  constructor(config: DatabaseConfig<TDatabaseSchema, TMutators>) {
    this.#storageEngine = config.storageEngine;
    this.#schema = config.storageEngine.schema;
    this.#databaseMutators = this.#constructLocalMutators(config.mutators);
    this.#syncManager = new SyncManager({
      storageEngine: this.#storageEngine,
      puller: config.puller,
      schema: this.#schema,
      mutators: config.mutators,
      pullInterval: 30 * ONE_SECOND_IN_MS,
    });
    this.#queryEngine = new QueryEngine(this.#storageEngine, async () => {
      await this.#initialize();
    });
    if (config.initialize) {
      void this.initialize();
    }
  }

  public get isInitialized() {
    return this.#isInitialized;
  }

  public get mutate(): Prettify<DatabaseMutators<TMutators>> {
    return this.#databaseMutators;
  }

  public collection<
    TCollectionIdentifer extends keyof TDatabaseSchema = keyof TDatabaseSchema
  >(collectionIdentifier: TCollectionIdentifer) {
    const collection = this.#schema[collectionIdentifier]!;

    return new ReactiveCollectionReference<
      InferSchemaTypeFromCollection<TDatabaseSchema[TCollectionIdentifer]>,
      InferMetadataTypeFromCollection<TDatabaseSchema[TCollectionIdentifer]>
    >(collection, this.#queryEngine);
  }

  public async pull() {
    await this.#initialize();
    return this.#syncManager.pull();
  }

  #batchReadTimeout: ReturnType<typeof setTimeout> | null = null;
  #batchReadQueue: Array<{
    queryFn: (tx: ReadTransaction<TDatabaseSchema>) => Promise<any>;
    resolve: (res: any) => void;
    reject: (err: any) => void;
  }> = [];

  async #processBatchReadQueue() {
    const tx = new ReadTransaction(
      this.#storageEngine.startTransaction("ALL", "readonly"),
      this.#schema
    );

    while (this.#batchReadQueue.length > 0) {
      const { queryFn, resolve, reject } = this.#batchReadQueue.shift()!;
      try {
        const result = await queryFn(tx);
        resolve(result);
      } catch (error) {
        reject(error);
      }
    }

    if (this.#batchReadTimeout) {
      clearTimeout(this.#batchReadTimeout);
    }
    this.#batchReadTimeout = null;
  }

  public async batchRead<T>(
    queryFn: (tx: ReadTransaction<TDatabaseSchema>) => Promise<T>
  ) {
    const { promise, resolve, reject } = resolver<T>();

    this.#batchReadQueue.push({
      queryFn,
      resolve,
      reject,
    });

    if (!this.#batchReadTimeout) {
      this.#batchReadTimeout = setTimeout(() => {
        void this.#processBatchReadQueue();
      }, 5);
    }

    return promise;
  }

  #convertStorageEngineCDCEventsToDatabaseCDCEvents(
    events: Array<CDCEvent>
  ): DatabaseCDCEvents<TDatabaseSchema> {
    const dbEvents: DatabaseCDCEvents<TDatabaseSchema> = [];

    const collectionNameToCollectionIdentifier = new Map<string, string>();

    for (let collectionIdentifier in this.#schema) {
      const collection = this.#schema[collectionIdentifier]!;
      collectionNameToCollectionIdentifier.set(
        collection.name,
        collectionIdentifier
      );
    }

    for (let event of events) {
      if (INTERNAL_SCHEMA_COLLECTION_NAMES.includes(event.collectionName)) {
        continue;
      }

      const collectionIdentifier = collectionNameToCollectionIdentifier.get(
        event.collectionName
      );

      if (!collectionIdentifier) {
        throw new Error(
          `Collection identifier not found for collection name: ${event.collectionName}`
        );
      }

      if (event.action === "CLEAR") {
        dbEvents.push({
          action: "CLEAR",
          collection: collectionIdentifier,
          timestamp: event.timestamp,
        });
        continue;
      }

      const { collectionName, ...eventProps } = event;

      dbEvents.push({
        ...eventProps,
        collection: collectionIdentifier,
      } as DatabaseCDCEvents[number]);
    }

    return dbEvents;
  }

  public subscribeToCDC(
    callback: (events: DatabaseCDCEvents<TDatabaseSchema>) => void
  ) {
    return this.#storageEngine.subscribeToCDC(({ events }) => {
      callback(this.#convertStorageEngineCDCEventsToDatabaseCDCEvents(events));
    });
  }

  public async getPendingMutationsCount() {
    await this.#initialize();
    return this.#syncManager.getPendingMutationsCount();
  }

  /**
   * This method allows you to apply changes directly to the database,
   * bypassing the default mutators, and pull operations to allow for more complex scenarios.
   * For ex: where you might want to apply changes that come in via websockets before the pull.
   */
  public async applyChange(
    change: Partial<PullResponse<TDatabaseSchema>["change"]>
  ) {
    await this.#initialize();
    return this.#syncManager.applyChange(change);
  }
}

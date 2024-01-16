import { AnyDatabaseSchema, Mutators } from ".";
import { IDBStorageEngine } from "./IDBStorageEngine/IDBStorageEngine";
import {
  Cursor,
  CursroMeta,
  DatabaseMutation,
  INTERNAL_SCHEMA,
  META,
  MUTATION,
} from "./InternalSchema";
import {
  LockController,
  createIDBStorageEnginePersistentStorage,
  createLocalStoragePersistentStorage,
} from "./LockController";
import { EnhancedStorageEngineTransaction } from "./EnhancedStorageEngineTransaction";
import { StorageEngine } from "./types/StorageEngine";
import { PullResponse, Puller } from "./types/SyncManager";
import {
  ONE_MINUTE_IN_MS,
  ONE_SECOND_IN_MS,
  isNotUndefined,
  resolver,
  retry,
} from "./utils";

export class SyncManager<
  TDatabaseSchema extends AnyDatabaseSchema,
  TMutators extends Mutators<TDatabaseSchema> = Mutators<TDatabaseSchema>
> {
  #puller?: Puller<TDatabaseSchema>;
  #storageEngine: StorageEngine;
  #mutators?: TMutators;
  #schema: TDatabaseSchema;
  #pullInterval: number;

  #pullPromise: null | Promise<PullResponse<TDatabaseSchema>> = null;
  #pushPromise: null | Promise<void> = null;

  #lockController: LockController;

  #state = {
    initializationPromise: null as null | Promise<void>,
    isInitialized: false,
  };

  #interval: number | null = null;

  #startPullInterval() {
    if (!this.#puller) return;

    this.#interval = setInterval(() => {
      this.pull();
    }, this.#pullInterval);
  }

  async initialize() {
    if (this.#state.isInitialized) return;

    if (this.#state.initializationPromise) {
      return this.#state.initializationPromise;
    }

    const { promise, resolve } = resolver<void>();

    this.#state.initializationPromise = promise;

    this.#startPullInterval();
    this.#processMutationQueue();
    this.pull();
    this.#state.isInitialized = true;

    resolve();

    return;
  }

  destory() {
    if (this.#interval) {
      clearInterval(this.#interval);
    }
  }

  constructor(config: {
    storageEngine: StorageEngine;
    schema: TDatabaseSchema;
    puller?: Puller<TDatabaseSchema>;
    pullInterval?: number;
    mutators?: TMutators;
  }) {
    this.#storageEngine = config.storageEngine;
    this.#puller = config.puller;
    this.#schema = config.schema;
    this.#mutators = config.mutators;
    this.#pullInterval = config.pullInterval ?? 30 * ONE_SECOND_IN_MS;

    const lockControllerStorage =
      config.storageEngine instanceof IDBStorageEngine
        ? createIDBStorageEnginePersistentStorage(config.storageEngine)
        : createLocalStoragePersistentStorage(config.storageEngine.name);

    this.#lockController = new LockController(lockControllerStorage);
  }

  async #getHasUnProcessedMutation() {
    const tx = this.#storageEngine.startTransaction("ALL", "readonly");

    const allMutationsMap = await tx.queryAll<DatabaseMutation>(
      INTERNAL_SCHEMA[MUTATION].name
    );

    const allMutations = Array.from(allMutationsMap.values());

    const mutationsToPush = allMutations.filter((m) => !m.isPushed);

    return mutationsToPush.length > 0;
  }

  async #getNextUnPushedMutation() {
    const tx = this.#storageEngine.startTransaction("ALL", "readonly");

    const allMutationsMap = await tx.queryAll<DatabaseMutation>(
      INTERNAL_SCHEMA[MUTATION].name
    );

    const allMutations = Array.from(allMutationsMap.values());

    const mutationsToPush = allMutations
      .filter((m) => !m.isPushed)
      .sort((a, b) => a.id - b.id);

    return mutationsToPush[0];
  }

  private getMutationDetailsFromArgs(mutationName: string) {
    if (!this.#mutators) {
      throw new Error(
        `Mutation ${mutationName} not found, please initialize the database with the correct mutators`
      );
    }

    const mutation = this.#mutators[mutationName];

    if (!mutation) {
      throw new Error(
        `Mutation ${mutationName} not found, please initialize the database with the correct mutators`
      );
    }

    return mutation;
  }

  async #deleteMutationAndCommitChanges(
    mutation: DatabaseMutation,
    storageEngineTransaction?: EnhancedStorageEngineTransaction
  ) {
    const tx =
      storageEngineTransaction ??
      this.#storageEngine.startTransaction("ALL", "readwrite");
    try {
      await tx.delete<DatabaseMutation>({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key: mutation.id,
        emitCDCEvent: false,
      });
      for (let change of mutation.changes) {
        if (change.action === "INSERT") {
          await tx.insert({
            collectionName: change.collectionName,
            key: change.key,
            value: change.value,
            emitCDCEvent: false,
          });
        } else if (change.action === "UPDATE") {
          await tx.update({
            collectionName: change.collectionName,
            key: change.key,
            value: change.delta,
            emitCDCEvent: false,
          });
        } else if (change.action === "DELETE") {
          await tx.delete({
            collectionName: change.collectionName,
            key: change.key,
            emitCDCEvent: false,
          });
        }
      }
    } catch (error) {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  async #applyRemoteChange(
    change: Partial<PullResponse<AnyDatabaseSchema>["change"]>,
    cursor?: Cursor,
    tx?: EnhancedStorageEngineTransaction
  ) {
    const storageEngineTransaction =
      tx ?? this.#storageEngine.startTransaction("ALL", "readwrite");

    for (let collectionIdentifier in change) {
      const collection = this.#schema[collectionIdentifier as any];

      if (!collection) continue;

      const collectionName = collection.name;

      const operations = change[collectionIdentifier as keyof typeof change];

      if (!operations) continue;

      for (let operation of operations) {
        if (operation.action === "CLEAR") {
          await storageEngineTransaction.clear({ collectionName });
        } else if (
          operation.action === "CREATED" ||
          operation.action === "UPDATED"
        ) {
          await storageEngineTransaction.upsert({
            collectionName,
            key: operation.key,
            value: operation.value,
            skipOptimisticDataCheck: true,
          });
        } else if (operation.action === "DELETED") {
          await storageEngineTransaction.delete({
            collectionName,
            key: operation.key,
            skipOptimisticDataCheck: true,
          });
        }
      }
    }

    if (cursor && (typeof cursor === "string" || typeof cursor === "number")) {
      await storageEngineTransaction.upsert({
        collectionName: INTERNAL_SCHEMA[META].name,
        key: "cursor",
        value: {
          cursor,
        },
      });
    }
  }

  async #handleMutationWithRemoteResolver(
    mutationDetails: ReturnType<
      ReturnType<typeof this.getMutationDetailsFromArgs>
    >,
    mutation: DatabaseMutation
  ) {
    const remoteResolver = mutationDetails.remoteResolver!;

    try {
      const result = await retry(
        async () => {
          return remoteResolver.mutationFn(mutation.localResolverResult);
        },
        remoteResolver.shouldRetry ?? true,
        {
          onRetry: async (retryCount) => {
            const tx = this.#storageEngine.startTransaction("ALL", "readwrite");
            await tx.update<DatabaseMutation>({
              collectionName: INTERNAL_SCHEMA[MUTATION].name,
              key: mutation.id,
              value: {
                remotePushAttempts: retryCount,
              },
            });
          },
          initialRetryCount: mutation.remotePushAttempts,
        }
      );

      const tx = this.#storageEngine.startTransaction("ALL", "readwrite");

      try {
        await tx.update<DatabaseMutation>({
          collectionName: INTERNAL_SCHEMA[MUTATION].name,
          key: mutation.id,
          value: {
            isPushed: true,
            serverMutationId: result.serverMutationId,
          },
        });

        if (isNotUndefined(remoteResolver.onSuccess)) {
          await remoteResolver.onSuccess({
            result,
          });
        }
      } catch (error) {
        tx.rollback();
      }
    } catch (error) {
      const tx = this.#storageEngine.startTransaction("ALL", "readwrite");
      await tx.delete<DatabaseMutation>({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key: mutation.id,
      });
    }
  }

  async #handleMutation(mutation: DatabaseMutation) {
    const mutationFn = this.getMutationDetailsFromArgs(mutation.mutationName);

    const mutationDetails = mutationFn(mutation.mutationArgs);

    if (!mutationDetails.remoteResolver) {
      await this.#deleteMutationAndCommitChanges(mutation);
      return;
    }

    await this.#handleMutationWithRemoteResolver(mutationDetails, mutation);
  }

  async #processMutationQueue() {
    if (this.#pushPromise) {
      return this.#pushPromise;
    }

    const { promise, reject, resolve } = resolver<void>();

    this.#pushPromise = promise;

    try {
      let shouldPull = false;

      while (await this.#getHasUnProcessedMutation()) {
        let shouldBreak = false;

        await this.#lockController.request(
          `push:${this.#storageEngine.name}`,
          async () => {
            const mutation = await this.#getNextUnPushedMutation();

            if (!mutation) {
              shouldBreak = true;
              return;
            }

            await this.#handleMutation(mutation);

            shouldPull = true;
          },
          2 * ONE_MINUTE_IN_MS
        );

        if (shouldBreak) {
          break;
        }
      }

      if (shouldPull) {
        void this.pull();
      }

      resolve();
    } catch (error) {
      reject(error);
    } finally {
      this.#pushPromise = null;
    }

    return promise;
  }

  async push() {
    this.#processMutationQueue();
  }

  async pull() {
    const puller = this.#puller;

    if (!puller) {
      return;
    }

    if (this.#pullPromise !== null) {
      return this.#pullPromise;
    }

    const { promise, reject, resolve } =
      resolver<PullResponse<TDatabaseSchema>>();

    this.#pullPromise = promise;

    let result: PullResponse<TDatabaseSchema> | undefined = undefined;

    try {
      const cursor = await this.#storageEngine.queryByKey<CursroMeta>(
        INTERNAL_SCHEMA[META].name,
        "cursor"
      );

      result = await retry(async () => {
        return await puller(cursor.get("cursor")?.cursor);
      }, true);

      await this.#lockController.request(
        `pull:${this.#storageEngine.name}`,
        async () => {
          const tx = this.#storageEngine.startTransaction("ALL", "readwrite");
          try {
            const allMutationsMap = await tx.queryAll<DatabaseMutation>(
              INTERNAL_SCHEMA[MUTATION].name
            );

            for (let [_, mutation] of allMutationsMap) {
              const shouldRemoveMutation =
                mutation.isPushed &&
                isNotUndefined(mutation.serverMutationId) &&
                mutation.serverMutationId <= result!.lastProcessedMutationId;

              if (!shouldRemoveMutation) {
                continue;
              }

              await tx.delete({
                collectionName: INTERNAL_SCHEMA[MUTATION].name,
                key: mutation.id,
              });
            }

            await this.#applyRemoteChange(
              result!.change,
              result!.cursor ?? undefined,
              tx
            );

            await tx.commit();
          } catch (error) {
            tx.rollback();
            throw error;
          }
        },
        2 * ONE_MINUTE_IN_MS
      );
    } catch (error) {
      reject(error);
    } finally {
      if (result) {
        resolve(result);

        this.#pullPromise = null;

        return result;
      }

      this.#pullPromise = null;
    }
  }
}

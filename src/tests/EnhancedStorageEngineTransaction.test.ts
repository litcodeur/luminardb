import { faker } from "@faker-js/faker";
import "fake-indexeddb/auto";
import { openDB } from "idb";
import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import {
  AnyDatabaseSchema,
  Collection,
  CollectionMetadata,
  createIDBStorageEngine,
} from "..";
import { Condition } from "../Condition";
import { IDBStorageEngine } from "../IDBStorageEngine/IDBStorageEngine";
import { DatabaseMutation, INTERNAL_SCHEMA, MUTATION } from "../InternalSchema";
import { getIncrementingTimestamp } from "../utils";
import { CDCEvent } from "../types/CDCEvent";
import { EnhancedStorageEngineTransaction } from "../EnhancedStorageEngineTransaction";

const IDB_NAME = "test";
const IDB_VERSION = 1;

type Todo = {
  id: string;
  title: string;
  status: "finished" | "overdue" | "incomplete";
  createdAt: string;
  updatedAt: string;
};

function generateRandomTodo(status?: Todo["status"]) {
  return {
    id: faker.string.uuid(),
    title: faker.lorem.sentence(),
    status:
      status ||
      faker.helpers.arrayElement(["finished", "overdue", "incomplete"]),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  } satisfies Todo;
}

async function getIDB() {
  return await openDB(IDB_NAME, IDB_VERSION);
}

const todoCollectionMetaData = {
  indexes: {
    status: true,
  },
} satisfies CollectionMetadata<Todo>;

const TODO_COLLECTION_NAME = "todos";

const todoCollection = new Collection<Todo, typeof todoCollectionMetaData>(
  TODO_COLLECTION_NAME,
  todoCollectionMetaData
);

const testSchema = {
  todo: todoCollection,
} as const satisfies AnyDatabaseSchema;

describe("EnhancedStorageEngineTransaction", () => {
  let storageEngine: IDBStorageEngine<AnyDatabaseSchema>;

  beforeEach(async () => {
    storageEngine = createIDBStorageEngine({
      name: IDB_NAME,
      version: IDB_VERSION,
      schema: testSchema,
    });

    await storageEngine.initialize();
  });

  afterEach(async () => {
    const idb = await getIDB();

    const objectStores = idb.objectStoreNames;

    for (let objectStore of objectStores) {
      await idb.clear(objectStore);
    }
  });

  async function getTransaction() {
    return storageEngine.startTransaction();
  }

  async function insertTodo(todo: Todo) {
    const idb = await getIDB();
    await idb.add(TODO_COLLECTION_NAME, { key: todo.id, value: todo }, todo.id);
  }

  describe("Basic CRUD operations without optimistic data", () => {
    it("Inserts a document in correct format", async () => {
      const todo = generateRandomTodo();

      const tx = await getTransaction();

      await tx.insert({
        collectionName: TODO_COLLECTION_NAME,
        key: todo.id,
        value: todo,
      });

      const idb = await getIDB();

      const result = await idb.get(TODO_COLLECTION_NAME, todo.id);

      expect(result.key).toBeDefined();
      expect(result.value).toBeDefined();

      expect(result.value).toEqual(todo);
      expect(result.key).toEqual(todo.id);
    });

    it("Throws an error if insert is called on existing document", async () => {
      const todo = generateRandomTodo();

      const tx = await getTransaction();

      await tx.insert({
        collectionName: TODO_COLLECTION_NAME,
        key: todo.id,
        value: todo,
      });

      await expect(
        tx.insert({
          collectionName: TODO_COLLECTION_NAME,
          key: todo.id,
          value: todo,
        })
      ).rejects.toThrowError();
    });

    it("Updates a document in place if exists", async () => {
      const idb = await getIDB();

      const todo = generateRandomTodo();

      await insertTodo(todo);

      const delta: Partial<Todo> = {
        title: faker.lorem.sentence(),
      };

      const tx = await getTransaction();

      await tx.update({
        collectionName: TODO_COLLECTION_NAME,
        key: todo.id,
        value: delta,
      });

      const result = await idb.get(TODO_COLLECTION_NAME, todo.id);

      expect(result.value).toEqual({ ...todo, ...delta });
    });

    it("Returns null if update is called on non-existent document", async () => {
      const delta: Partial<Todo> = {
        title: faker.lorem.sentence(),
      };

      const tx = await getTransaction();

      const result = await tx.update({
        collectionName: TODO_COLLECTION_NAME,
        key: faker.string.uuid(),
        value: delta,
      });

      expect(result).toBeNull();
    });

    it("Deletes a document in place if exists", async () => {
      const idb = await getIDB();

      const todo = generateRandomTodo();

      await insertTodo(todo);

      const tx = await getTransaction();

      await tx.delete({
        collectionName: TODO_COLLECTION_NAME,
        key: todo.id,
      });

      const result = await idb.get(TODO_COLLECTION_NAME, todo.id);

      expect(result).toBeUndefined();
    });

    it("Returns null if delete is called on non-existent document", async () => {
      const tx = await getTransaction();

      const result = await tx.delete({
        collectionName: TODO_COLLECTION_NAME,
        key: faker.string.uuid(),
      });

      expect(result).toBeNull();
    });

    it("Returns a single document if it exists", async () => {
      const todo = generateRandomTodo();

      await insertTodo(todo);

      const tx = await getTransaction();

      const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

      expect(result.has(todo.id)).toEqual(true);

      expect(result.get(todo.id)).toEqual(todo);
    });

    it("Returns no document if it is queried by key on non-existent document", async () => {
      const tx = await getTransaction();

      const result = await tx.queryByKey(
        TODO_COLLECTION_NAME,
        faker.string.uuid()
      );

      expect(result.size).toEqual(0);
    });

    it("Returns documents matching a condition", async () => {
      const finishedTodos = [...new Array(5)].map(() =>
        generateRandomTodo("finished")
      );

      const incompleteTodos = [...new Array(5)].map(() =>
        generateRandomTodo("incomplete")
      );

      const overdueTodos = [...new Array(5)].map(() =>
        generateRandomTodo("overdue")
      );

      const allTodos = [...finishedTodos, ...incompleteTodos, ...overdueTodos];

      for (let todo of allTodos) {
        await insertTodo(todo);
      }

      const tx = await getTransaction();

      const condition = new Condition<typeof todoCollection>({
        where: { status: { eq: "finished" } },
      });

      const queryResult = await tx.queryByCondition(
        TODO_COLLECTION_NAME,
        condition
      );

      expect(queryResult.size).toEqual(5);

      for (let todo of finishedTodos) {
        expect(queryResult.has(todo.id)).toEqual(true);
        expect(queryResult.get(todo.id)).toEqual(todo);
      }
    });

    it("Returns no document if no documents match condition", async () => {
      const finishedTodo = generateRandomTodo("finished");

      await insertTodo(finishedTodo);

      const tx = await getTransaction();

      const condition = new Condition<typeof todoCollection>({
        where: { status: { eq: "overdue" } },
      });

      const queryResult = await tx.queryByCondition(
        TODO_COLLECTION_NAME,
        condition
      );

      expect(queryResult.size).toEqual(0);
    });

    it("Returns all documents if querAll is called", async () => {
      const todosCount = faker.number.int({ min: 2, max: 20 });

      const allTodos = [...new Array(todosCount)].map(() =>
        generateRandomTodo()
      );

      for (let todo of allTodos) {
        await insertTodo(todo);
      }

      const tx = await getTransaction();

      const queryResult = await tx.queryAll(TODO_COLLECTION_NAME);

      expect(queryResult.size).toEqual(todosCount);

      for (let todo of allTodos) {
        expect(queryResult.has(todo.id)).toEqual(true);
        expect(queryResult.get(todo.id)).toEqual(todo);
      }
    });

    it("Returns no document if queryAll is called on an empty collection", async () => {
      const tx = await getTransaction();

      const queryResult = await tx.queryAll(TODO_COLLECTION_NAME);

      expect(queryResult.size).toEqual(0);
    });
  });

  describe("Read operations with optimistic data", () => {
    let id = 0;
    async function createOptimisticTodo(todo?: Todo) {
      todo = todo || generateRandomTodo();

      const idb = await getIDB();

      const mutationId = ++id;
      let mutation: DatabaseMutation = {
        id: mutationId,
        changes: [
          {
            id: `${mutationId}-${getIncrementingTimestamp()}`,
            action: "INSERT",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            timestamp: getIncrementingTimestamp(),
            value: todo,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: true,
        mutationName: "insertTodo",
        mutationArgs: {},
        isPushed: false,
      };

      await idb.add(
        INTERNAL_SCHEMA[MUTATION].name,
        { value: mutation, key: mutation.id },
        mutation.id
      );

      return todo;
    }

    async function updateOptimisticTodo(
      currentValue: Todo,
      delta: Partial<Todo>
    ): Promise<Todo> {
      const idb = await getIDB();

      const mutationId = ++id;

      const updatedTodo = { ...currentValue, ...delta };
      let mutation: DatabaseMutation = {
        id: mutationId,
        changes: [
          {
            id: `${mutationId}-${getIncrementingTimestamp()}`,
            action: "UPDATE",
            collectionName: TODO_COLLECTION_NAME,
            key: currentValue.id,
            timestamp: getIncrementingTimestamp(),
            delta,
            preUpdateValue: currentValue,
            postUpdateValue: updatedTodo,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: true,
        mutationName: "updateTodo",
        mutationArgs: {},
        isPushed: false,
      };

      await idb.add(
        INTERNAL_SCHEMA[MUTATION].name,
        { value: mutation, key: mutation.id },
        mutation.id
      );

      return updatedTodo;
    }

    async function deleteOptimisticTodo(todo: Todo) {
      const idb = await getIDB();

      const mutationId = ++id;

      let mutation: DatabaseMutation = {
        id: mutationId,
        changes: [
          {
            id: `${mutationId}-${getIncrementingTimestamp()}`,
            action: "DELETE",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            timestamp: getIncrementingTimestamp(),
            value: todo,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: true,
        mutationName: "deleteTodo",
        mutationArgs: {},
        isPushed: false,
      };

      await idb.add(
        INTERNAL_SCHEMA[MUTATION].name,
        {
          value: mutation,
          key: mutation.id,
        },
        mutation.id
      );
    }

    afterEach(() => {
      id = 0;
    });

    describe("Query document by key", () => {
      it("reads a document if it is inserted optimistically", async () => {
        const todo = await createOptimisticTodo();

        const tx = await getTransaction();

        const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

        expect(result.has(todo.id)).toEqual(true);

        expect(result.get(todo.id)).toEqual(todo);
      });

      it("returns an updated version of document if the document was inserted optimistically", async () => {
        const todo = await createOptimisticTodo();

        const delta: Partial<Todo> = {
          title: faker.lorem.sentence(),
        };

        const updatedTodo = await updateOptimisticTodo(todo, delta);

        const tx = await getTransaction();

        const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

        expect(result.has(todo.id)).toEqual(true);

        expect(result.get(todo.id)).toEqual(updatedTodo);
      });

      it("returns an updated version of document if document was inserted authoritatively and then updated optimistically", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        const delta: Partial<Todo> = {
          title: faker.lorem.sentence(),
        };

        const updatedTodo = await updateOptimisticTodo(todo, delta);

        const tx = await getTransaction();

        const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

        expect(result.has(todo.id)).toEqual(true);
        expect(result.get(todo.id)).toEqual(updatedTodo);
      });

      it("returns no documents if only an update was inserted optimistically but no document was inserted previously either optimistically or authoritatively", async () => {
        const todo = generateRandomTodo();
        const delta: Partial<Todo> = {
          title: faker.lorem.sentence(),
        };

        await updateOptimisticTodo(todo, delta);

        const tx = await getTransaction();

        const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

        expect(result.size).toEqual(0);
      });

      it("returns no document if a document was deleted optimistically", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        await deleteOptimisticTodo(todo);

        const tx = await getTransaction();

        const result = await tx.queryByKey(TODO_COLLECTION_NAME, todo.id);

        expect(result.size).toEqual(0);
      });
    });

    describe("Query documents by condition", () => {
      it("returns matching documents if all documents are inserted authoritatively and one is updated optimistically", async () => {
        const finishedTodosCount = 3;

        const finishedTodos = [...new Array(finishedTodosCount)].map(() =>
          generateRandomTodo("finished")
        );

        for (let todo of finishedTodos) {
          await insertTodo(todo);
        }

        const randomFinishedTodo = faker.helpers.arrayElement(finishedTodos);

        const delta: Partial<Todo> = {
          status: "incomplete",
        };

        const updatedTodo = await updateOptimisticTodo(
          randomFinishedTodo,
          delta
        );

        const condition = new Condition<typeof todoCollection>({
          where: { status: { eq: "incomplete" } },
        });

        const tx = await getTransaction();

        const queryResult = await tx.queryByCondition(
          TODO_COLLECTION_NAME,
          condition
        );

        expect(queryResult.size).toEqual(1);
        expect(queryResult.has(updatedTodo.id)).toEqual(true);
        expect(queryResult.get(updatedTodo.id)).toEqual(updatedTodo);
      });

      it("returns matching documents when there are some optimistic inserts, updates and deletes with authoritatively inserted documents", async () => {
        const authoritativelyFinishedTodosCount = faker.number.int({
          min: 4,
          max: 6,
        });

        const authoritativelyFinishedTodos = [
          ...new Array(authoritativelyFinishedTodosCount),
        ].map(() => generateRandomTodo("finished"));

        const authoritativelyIncompleteTodosCount = faker.number.int({
          min: 2,
          max: 5,
        });

        const authoritativelyIncompleteTodos = [
          ...new Array(authoritativelyIncompleteTodosCount),
        ].map(() => generateRandomTodo("incomplete"));

        const authoritativelyOverdueTodosCount = faker.number.int({
          min: 2,
          max: 5,
        });

        const authoritativelyOverdueTodos = [
          ...new Array(authoritativelyOverdueTodosCount),
        ].map(() => generateRandomTodo("overdue"));

        const allTodos = [
          ...authoritativelyFinishedTodos,
          ...authoritativelyIncompleteTodos,
          ...authoritativelyOverdueTodos,
        ];

        for (let todo of allTodos) {
          await insertTodo(todo);
        }

        const randomFinishedTodo = faker.helpers.arrayElement(
          authoritativelyFinishedTodos
        );

        const delta: Partial<Todo> = {
          status: "incomplete",
        };

        const updatedFinishedTodo = await updateOptimisticTodo(
          randomFinishedTodo,
          delta
        );

        const randomOverdueTodo = faker.helpers.arrayElement(
          authoritativelyOverdueTodos
        );

        const updatedOverdueTodo = await updateOptimisticTodo(
          randomOverdueTodo,
          delta
        );

        const randomIncompleteTodoToDelete = faker.helpers.arrayElement(
          authoritativelyIncompleteTodos
        );

        await deleteOptimisticTodo(
          randomIncompleteTodoToDelete as unknown as Todo
        );

        const optimisticallyInsertedIncompleteTodo =
          generateRandomTodo("incomplete");
        await createOptimisticTodo(optimisticallyInsertedIncompleteTodo);

        const optimisticallyInsertedFinishedTodo =
          generateRandomTodo("finished");
        await createOptimisticTodo(optimisticallyInsertedFinishedTodo);

        const optimisticallyUpdatedOptimisticallyInsertedFinishedTodo =
          await updateOptimisticTodo(optimisticallyInsertedFinishedTodo, delta);

        const OptimisticallyInsertedIncompleteTodoCount = 1;
        const OptimisticallyMarkedIncompleteFinsihedTodoCount = 1;
        const OptimisticallyMarkedIncompleteOverdueTodoCount = 1;
        const OptimisticallyDeletedIncompleteTodoCount = 1;
        const OptimisticallyMakredIncompleteOptimisticFinsihedTodoCount = 1;

        const finalIncompleteTodosCount =
          authoritativelyIncompleteTodosCount +
          OptimisticallyInsertedIncompleteTodoCount +
          OptimisticallyMarkedIncompleteFinsihedTodoCount +
          OptimisticallyMakredIncompleteOptimisticFinsihedTodoCount +
          OptimisticallyMarkedIncompleteOverdueTodoCount -
          OptimisticallyDeletedIncompleteTodoCount;

        const condition = new Condition<typeof todoCollection>({
          where: { status: { eq: "incomplete" } },
        });

        const tx = await getTransaction();

        const queryResult = await tx.queryByCondition(
          TODO_COLLECTION_NAME,
          condition
        );

        expect(queryResult.size).toEqual(finalIncompleteTodosCount);

        expect(queryResult.has(updatedFinishedTodo.id)).toEqual(true);
        expect(queryResult.get(updatedFinishedTodo.id)).toEqual(
          updatedFinishedTodo
        );

        expect(queryResult.has(updatedOverdueTodo.id)).toEqual(true);
        expect(queryResult.get(updatedOverdueTodo.id)).toEqual(
          updatedOverdueTodo
        );

        expect(queryResult.has(randomIncompleteTodoToDelete.id)).toEqual(false);

        expect(
          queryResult.has(optimisticallyInsertedIncompleteTodo.id)
        ).toEqual(true);
        expect(
          queryResult.get(optimisticallyInsertedIncompleteTodo.id)
        ).toEqual(optimisticallyInsertedIncompleteTodo);

        expect(
          queryResult.has(
            optimisticallyUpdatedOptimisticallyInsertedFinishedTodo.id
          )
        ).toEqual(true);
        expect(
          queryResult.get(
            optimisticallyUpdatedOptimisticallyInsertedFinishedTodo.id
          )
        ).toEqual(optimisticallyUpdatedOptimisticallyInsertedFinishedTodo);
      });
    });

    describe("Query all documents", () => {
      it("Merges optimistic and authoritative data correctly", async () => {
        const authoritativelyFinishedTodosCount = faker.number.int({
          min: 5,
          max: 10,
        });

        const authoritativelyFinishedTodos = [
          ...new Array(authoritativelyFinishedTodosCount),
        ].map(() => generateRandomTodo("finished"));

        for (let todo of authoritativelyFinishedTodos) {
          await insertTodo(todo);
        }

        const randomFinishedTodo = faker.helpers.arrayElement(
          authoritativelyFinishedTodos
        );

        const delta: Partial<Todo> = {
          status: "incomplete",
        };

        const updatedTodo = await updateOptimisticTodo(
          randomFinishedTodo,
          delta
        );

        const optimisticTodosCount = faker.number.int({ min: 2, max: 20 });

        const optimisticTodos = [...new Array(optimisticTodosCount)].map(() =>
          generateRandomTodo()
        );

        for (let todo of optimisticTodos) {
          await createOptimisticTodo(todo);
        }

        const tx = await getTransaction();

        const queryResult = await tx.queryAll(TODO_COLLECTION_NAME);

        expect(queryResult.size).toEqual(
          optimisticTodosCount + authoritativelyFinishedTodosCount
        );

        for (let todo of optimisticTodos) {
          expect(queryResult.has(todo.id)).toEqual(true);
          expect(queryResult.get(todo.id)).toEqual(todo);
        }

        for (let todo of authoritativelyFinishedTodos) {
          if (todo.id === randomFinishedTodo.id) {
            expect(queryResult.has(todo.id)).toEqual(true);
            expect(queryResult.get(todo.id)).toEqual(updatedTodo);
            continue;
          }
          expect(queryResult.has(todo.id)).toEqual(true);
          expect(queryResult.get(todo.id)).toEqual(todo);
        }
      });
    });
  });

  describe("CDC events", () => {
    describe("Basic CUD operations without optimistic data", () => {
      it("emits a document inserted event when a document is inserted", async () => {
        const todo = generateRandomTodo();

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await tx.insert({
          collectionName: TODO_COLLECTION_NAME,
          key: todo.id,
          value: todo,
        });

        await tx.commit();

        expect(fn).toHaveBeenCalledWith([
          {
            action: "INSERT",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: todo,
            timestamp: expect.any(Number),
          },
        ] as Array<CDCEvent>);
      });

      it("emits a document updated event when a document is updated", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        const delta: Partial<Todo> = {
          title: faker.lorem.sentence(),
        };

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await tx.update({
          collectionName: TODO_COLLECTION_NAME,
          key: todo.id,
          value: delta,
        });

        await tx.commit();

        expect(fn).toHaveBeenCalledWith([
          {
            action: "UPDATE",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            delta,
            timestamp: expect.any(Number),
            preUpdateValue: todo,
            postUpdateValue: { ...todo, ...delta },
          },
        ] as Array<CDCEvent>);
      });

      it("emits a document deleted event when a document is deleted", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await tx.delete({
          collectionName: TODO_COLLECTION_NAME,
          key: todo.id,
        });

        await tx.commit();

        expect(fn).toHaveBeenCalledWith([
          {
            action: "DELETE",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: todo,
            timestamp: expect.any(Number),
          },
        ] as Array<CDCEvent>);
      });
    });

    let id = 0;

    afterEach(() => {
      id = 0;
    });

    async function createOptimisticTodo(
      tx: EnhancedStorageEngineTransaction,
      todo?: Todo
    ) {
      todo = todo || generateRandomTodo();

      const mutationId = ++id;

      const mutation = {
        id: mutationId,
        changes: [
          {
            action: "INSERT",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            timestamp: getIncrementingTimestamp(),
            value: todo,
            id: `${mutationId}-${getIncrementingTimestamp()}`,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: false,
        mutationName: "insertTodo",
        mutationArgs: {},
        isPushed: false,
      } satisfies DatabaseMutation;

      const { key } = await tx.insert({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        value: mutation,
        key: mutation.id,
      });

      await tx.update({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key,
        value: {
          isCompleted: true,
        },
      });

      return todo;
    }

    async function updateOptimisticTodo(
      tx: EnhancedStorageEngineTransaction,
      currentValue: Todo,
      delta: Partial<Todo>
    ): Promise<Todo> {
      const updatedTodo = { ...currentValue, ...delta };

      const mutationId = ++id;
      const mutation = {
        id: mutationId,
        changes: [
          {
            action: "UPDATE",
            collectionName: TODO_COLLECTION_NAME,
            key: currentValue.id,
            timestamp: getIncrementingTimestamp(),
            delta,
            preUpdateValue: currentValue,
            postUpdateValue: updatedTodo,
            id: `${++id}-${getIncrementingTimestamp()}`,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: false,
        mutationName: "updateTodo",
        mutationArgs: {},
        isPushed: false,
      } satisfies DatabaseMutation;

      const { key } = await tx.insert({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        value: mutation,
        key: mutation.id,
      });

      await tx.update({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key,
        value: {
          isCompleted: true,
        },
      });

      return updatedTodo;
    }

    async function deleteOptimisticTodo(
      tx: EnhancedStorageEngineTransaction,
      todo: Todo
    ) {
      const mutationId = ++id;
      const mutation = {
        id: mutationId,
        changes: [
          {
            action: "DELETE",
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            timestamp: getIncrementingTimestamp(),
            value: todo,
            id: `${++id}-${getIncrementingTimestamp()}`,
          },
        ],
        collectionsAffected: [TODO_COLLECTION_NAME],
        isCompleted: false,
        mutationName: "deleteTodo",
        mutationArgs: {},
        isPushed: false,
      } satisfies DatabaseMutation;

      const { key } = await tx.insert({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        value: mutation,
        key: mutation.id,
      });

      await tx.update({
        collectionName: INTERNAL_SCHEMA[MUTATION].name,
        key,
        value: {
          isCompleted: true,
        },
      });

      return todo;
    }

    describe("Basic CUD operations with optimistic data", () => {
      it("emits a document inserted event when a document is inserted optimistically", async () => {
        const todo = generateRandomTodo();

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await createOptimisticTodo(tx, todo);

        await tx.commit();

        expect(fn).toHaveBeenCalledWith(
          expect.arrayContaining([
            expect.objectContaining({
              action: "INSERT",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
              timestamp: expect.any(Number),
            } as CDCEvent),
          ])
        );
      });

      it("emits a document updated event when a document is updated optimistically", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        const delta: Partial<Todo> = {
          title: faker.lorem.sentence(),
        };

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await updateOptimisticTodo(tx, todo, delta);

        await tx.commit();

        expect(fn).toHaveBeenCalledWith(
          expect.arrayContaining([
            expect.objectContaining({
              action: "UPDATE",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              delta,
              timestamp: expect.any(Number),
              preUpdateValue: todo,
              postUpdateValue: { ...todo, ...delta },
            } as CDCEvent),
          ])
        );
      });

      it("emits a document deleted event when a document is deleted optimistically", async () => {
        const todo = generateRandomTodo();

        await insertTodo(todo);

        const tx = await getTransaction();

        const fn = vitest.fn();

        tx.onComplete(fn);

        await deleteOptimisticTodo(tx, todo as unknown as Todo);

        await tx.commit();

        expect(fn).toHaveBeenCalledWith(
          expect.arrayContaining([
            expect.objectContaining({
              action: "DELETE",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
              timestamp: expect.any(Number),
            } as CDCEvent),
          ])
        );
      });
    });

    describe("Insert operations", () => {
      describe("Insert a document auhtoritatively", () => {
        it("emits a document insert event when no pending mutations for the key exist", async () => {
          const todo = generateRandomTodo();

          const tx = await getTransaction();

          const fn = vitest.fn();

          tx.onComplete(fn);

          await tx.insert({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: todo,
          });

          await tx.commit();

          expect(fn).toHaveBeenCalledWith([
            {
              action: "INSERT",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
              timestamp: expect.any(Number),
            },
          ] as Array<CDCEvent>);
        });
        it("emits a document update event if there is a pending insert change for the key", async () => {
          const todo = generateRandomTodo();

          const tx1 = await getTransaction();

          await createOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await tx2.insert({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: updatedTodo,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta: todo,
                timestamp: expect.any(Number),
                preUpdateValue: updatedTodo,
                postUpdateValue: todo,
              }),
            ])
          );
        });
        it("emits no events if there is a pending delete change for the key", async () => {
          const todo = generateRandomTodo();

          const tx1 = await getTransaction();

          await deleteOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          await tx2.insert({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: todo,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).not.toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "INSERT",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              }),
            ])
          );
        });
        it("emits a document insert event merged with updates if there is a pending update change for the key", async () => {
          const todo = generateRandomTodo();

          const tx1 = await getTransaction();

          const delta = {
            title: faker.lorem.sentence(),
          };

          await updateOptimisticTodo(tx1, todo, delta);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          const updatedTodo = {
            ...todo,
            title: faker.lorem.sentence(),
          } satisfies Todo;

          tx2.onComplete(fn);

          await tx2.insert({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: updatedTodo,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "INSERT",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                timestamp: expect.any(Number),
                value: { ...updatedTodo, ...delta },
              } satisfies CDCEvent),
            ])
          );
        });
      });
      describe("Insert a document optimistically", () => {
        it("throws an error if there a key that exists for the document", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx = await getTransaction();

          await expect(
            tx.insert({
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
            })
          ).rejects.toThrowError();
        });
        it("throws an error if there is a pending document insert for the key", async () => {
          const todo = generateRandomTodo();

          const tx1 = await getTransaction();

          await createOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          await expect(
            tx2.insert({
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
            })
          ).rejects.toThrowError();
        });
        it("emits a document insert event if there are no pending changes", async () => {
          const todo = generateRandomTodo();

          const tx = await getTransaction();

          const fn = vitest.fn();

          tx.onComplete(fn);

          await createOptimisticTodo(tx, todo);

          await tx.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "INSERT",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              } as CDCEvent),
            ])
          );
        });
      });
    });

    describe("Delete operations", () => {
      describe("Authoritative delete", () => {
        it("emits a document delete event if there are no pending changes", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx = await getTransaction();

          const fn = vitest.fn();

          tx.onComplete(fn);

          await tx.delete({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
          });

          await tx.commit();

          expect(fn).toHaveBeenCalledWith([
            {
              action: "DELETE",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              value: todo,
              timestamp: expect.any(Number),
            },
          ] as Array<CDCEvent>);
        });
        it("emits no events if there is a pending insert change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          await deleteOptimisticTodo(tx1, todo);

          await createOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          await tx2.delete({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).not.toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "DELETE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              }),
            ])
          );
        });
        it("emits a document delete event if there is a pending update change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          const delta = {
            title: faker.lorem.sentence(),
          };

          await updateOptimisticTodo(tx1, todo, delta);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          await tx2.delete({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "DELETE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              }),
            ])
          );
        });
        it("emits no event if there is a pending delete change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          await deleteOptimisticTodo(tx1, todo as unknown as Todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          await tx2.delete({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            skipOptimisticDataCheck: true,
          });

          await tx2.commit();

          expect(fn).not.toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "DELETE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              }),
            ])
          );
        });
      });

      describe("Optimistic delete", () => {
        it("emits a document delete event in any scenario", async () => {
          const todo = generateRandomTodo();

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const tx1 = await getTransaction();

          await createOptimisticTodo(tx1, todo);

          await updateOptimisticTodo(tx1, todo, delta);

          await deleteOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          await deleteOptimisticTodo(tx2, todo);

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "DELETE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                value: todo,
                timestamp: expect.any(Number),
              }),
            ])
          );
        });
      });
    });

    describe("Update operations", () => {
      describe("Authoritative update", () => {
        it("emits a document update event if there are no pending changes", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx = await getTransaction();

          const fn = vitest.fn();

          tx.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await tx.update({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: delta,
          });

          await tx.commit();

          expect(fn).toHaveBeenCalledWith([
            {
              action: "UPDATE",
              collectionName: TODO_COLLECTION_NAME,
              key: todo.id,
              delta,
              timestamp: expect.any(Number),
              preUpdateValue: todo,
              postUpdateValue: updatedTodo,
            },
          ] as Array<CDCEvent>);
        });
        it("emits no events if there is a pending insert change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          await deleteOptimisticTodo(tx1, todo);

          await createOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await tx2.update({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: delta,
          });

          await tx2.commit();

          expect(fn).not.toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta,
                timestamp: expect.any(Number),
                preUpdateValue: todo,
                postUpdateValue: updatedTodo,
              }),
            ])
          );
        });
        it("emits a document update event if there is a pending update change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          const delta = {
            title: faker.lorem.sentence(),
          };

          const updatedTodo = await updateOptimisticTodo(tx1, todo, delta);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const newDelta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          await tx2.update({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: newDelta,
          });

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta: { ...newDelta, ...delta },
                timestamp: expect.any(Number),
                preUpdateValue: { ...todo, ...newDelta },
                postUpdateValue: updatedTodo,
              }),
            ])
          );
        });
        it("emits no events if there is a pending delete change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          await deleteOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await tx2.update({
            collectionName: TODO_COLLECTION_NAME,
            key: todo.id,
            value: delta,
          });

          await tx2.commit();

          expect(fn).not.toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta,
                timestamp: expect.any(Number),
                preUpdateValue: todo,
                postUpdateValue: updatedTodo,
              }),
            ])
          );
        });
      });

      describe("Optimistic updates", async () => {
        it("emits a document update event if there are no pending changes", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx = await getTransaction();

          const fn = vitest.fn();

          tx.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await updateOptimisticTodo(tx, todo, delta);

          await tx.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta,
                timestamp: expect.any(Number),
                preUpdateValue: todo,
                postUpdateValue: updatedTodo,
              }),
            ])
          );
        });

        it("emits a document update event if there is a pending insert change for the key", async () => {
          const todo = generateRandomTodo();

          const tx1 = await getTransaction();

          await createOptimisticTodo(tx1, todo);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const delta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const updatedTodo = { ...todo, ...delta };

          await updateOptimisticTodo(tx2, todo, delta);

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta,
                timestamp: expect.any(Number),
                preUpdateValue: todo,
                postUpdateValue: updatedTodo,
              }),
            ])
          );
        });

        it("emits a document update event if there is a pending update change for the key", async () => {
          const todo = generateRandomTodo();

          await insertTodo(todo);

          const tx1 = await getTransaction();

          const delta = {
            title: faker.lorem.sentence(),
          };

          const updatedTodo = await updateOptimisticTodo(tx1, todo, delta);

          await tx1.commit();

          const tx2 = await getTransaction();

          const fn = vitest.fn();

          tx2.onComplete(fn);

          const newDelta = {
            title: faker.lorem.sentence(),
          } satisfies Partial<Todo>;

          const newUpdatedTodo = { ...updatedTodo, ...newDelta };

          await updateOptimisticTodo(tx2, todo, newDelta);

          await tx2.commit();

          expect(fn).toHaveBeenCalledWith(
            expect.arrayContaining([
              expect.objectContaining({
                action: "UPDATE",
                collectionName: TODO_COLLECTION_NAME,
                key: todo.id,
                delta: { ...delta, ...newDelta },
                timestamp: expect.any(Number),
                preUpdateValue: updatedTodo,
                postUpdateValue: newUpdatedTodo,
              }),
            ])
          );
        });
      });
    });
  });
});

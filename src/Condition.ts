import { Collection } from "./Collection";
import { FilterOption } from "./types/Query";
import { PrimitiveType, StorableJSONObject } from "./types/types";

const ComparatorList = ["eq", "gt", "lt", "gte", "lte"] as const;

export type ConditionComparators = (typeof ComparatorList)[number];

export class Condition<T extends Collection<any, any> = Collection<any, any>> {
  #key!: string;
  #value!: PrimitiveType;
  #comparator!: ConditionComparators;

  constructor(option: FilterOption<T>) {
    const where = option.where;

    if (!where) {
      throw new Error("Invalid condition, where object must be present.");
    }

    const keys = Object.keys(where);

    if (keys.length === 0) {
      throw new Error("Invalid condition, where object must have one key.");
    }

    if (keys.length > 1) {
      throw new Error(
        "Invalid condition, where object must have only one key."
      );
    }

    const key = keys[0]!;

    const comparatorKeys = Object.keys(where[key]!);

    if (comparatorKeys.length === 0) {
      throw new Error(
        `Invalid condition, ${key} filter must have one comparator of key: ${comparatorKeys.join(
          ", "
        )}`
      );
    }

    if (comparatorKeys.length > 1) {
      throw new Error(
        `Invalid condition, ${key} filter must have only one comparator of key: ${comparatorKeys.join(
          ", "
        )}`
      );
    }

    const comparator = comparatorKeys[0] as ConditionComparators;

    if (!ComparatorList.includes(comparator)) {
      throw new Error(
        `Invalid condition, ${key} filter must have a valid comparator of key: ${comparatorKeys.join(
          ", "
        )}`
      );
    }

    const value = where[key]![comparator]!;

    this.#key = key;
    this.#comparator = comparator;
    this.#value = value;
  }

  get key() {
    return this.#key;
  }

  get value() {
    return this.#value;
  }

  get comparator() {
    return this.#comparator;
  }

  generateIDBKeyRange() {
    let keyRange: IDBKeyRange | undefined = undefined;

    if (this.#comparator === "lt") {
      keyRange = IDBKeyRange.upperBound(this.#value, true);
    }
    if (this.#comparator === "gt") {
      keyRange = IDBKeyRange.lowerBound(this.#value, true);
    }
    if (this.#comparator === "lte") {
      keyRange = IDBKeyRange.upperBound(this.#value);
    }
    if (this.#comparator === "gte") {
      keyRange = IDBKeyRange.lowerBound(this.#value);
    }
    if (this.#comparator === "eq") {
      keyRange = IDBKeyRange.only(this.#value);
    }

    if (!keyRange) {
      throw new Error("Invalid where condition");
    }

    return keyRange as IDBKeyRange;
  }

  doesDataSatisfyCondition(data: StorableJSONObject) {
    if (!this.#key) return false;

    const valueAtKey = data[this.#key];

    const keyRange = this.generateIDBKeyRange();

    return keyRange.includes(valueAtKey);
  }
}

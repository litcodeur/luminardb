import { Subscribable } from "./Subscribeable";

export class Store<T> extends Subscribable<(s: T) => void> {
  #data: T;

  constructor(initialData: T) {
    super();
    this.#data = initialData;
  }

  get() {
    return this.#data;
  }

  set(data: Partial<T> | ((data: T) => Partial<T>)) {
    if (typeof data === "function") {
      this.#data = { ...this.#data, ...(data as any)(this.#data) };
    } else {
      this.#data = { ...this.#data, ...data };
    }
    this.notifyListeners(this.#data);
  }
}

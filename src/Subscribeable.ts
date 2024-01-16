type Listener = () => void;

export class Subscribable<TListener extends (...args: any) => void = Listener> {
  #listeners: Set<TListener>;

  constructor() {
    this.#listeners = new Set();
    this.subscribe = this.subscribe.bind(this);
  }

  protected notifyListeners(...d: Parameters<TListener>): void {
    this.#listeners.forEach((l) => l(...(d as any)));
  }

  subscribe(listener: TListener): () => void {
    this.#listeners.add(listener);

    this.#onSubscribe(listener);

    return () => {
      this.#listeners.delete(listener);
      this.#onUnsubscribe(listener);
    };
  }

  hasListeners(): boolean {
    return this.#listeners.size > 0;
  }

  #onSubscribe(_listener: TListener): void {}

  #onUnsubscribe(_listener: TListener): void {}
}

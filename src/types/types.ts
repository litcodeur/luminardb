export type StorableJSONValue =
  | undefined
  | string
  | number
  | boolean
  | StorableJSONObject
  | Array<StorableJSONValue>;

export type StorableJSONObject = {
  [key: string | number | symbol]: StorableJSONValue;
};

export type PrimitiveType = string | number | boolean;

export type MaybePromise<T> = T | Promise<T>;

export type Prettify<T> = {
  [K in keyof T]: T[K];
} & {};

export type AtLeastOne<T, U = { [K in keyof T]: Pick<T, K> }> = Partial<T> &
  U[keyof U];

type Explode<T> = keyof T extends infer K
  ? K extends unknown
    ? { [I in keyof T]: I extends K ? T[I] : never }
    : never
  : never;

export type AtMostOne<T> = Explode<Partial<T>>;

export type ExactlyOne<T> = AtMostOne<T> & AtLeastOne<T>;

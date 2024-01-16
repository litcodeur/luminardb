import { WriteTransaction } from "../WriteTransaction";
import { RetryConfig } from "../utils";
import { AnyDatabaseSchema } from "./Database";
import { MaybePromise } from "./types";

export type OnSuccessOptions<T = any> = {
  result: T;
};

export type Mutators<
  TDatabaseSchema extends AnyDatabaseSchema = AnyDatabaseSchema
> = {
  [key: string]: (...args: any) => {
    localResolver: (tx: WriteTransaction<TDatabaseSchema>) => MaybePromise<any>;
    remoteResolver?: {
      mutationFn: (data: any) => Promise<{ serverMutationId: number }>;
      /**
       * If set to a function, the remote resolver will be retried if it fails and the function returns true.
       * If set to true the remote resolver will be retried if it fails.
       * If set to a number, the remote resolver will be retried that many times if it fails and the retry number persists across page refreshes.
       * @default true
       */
      shouldRetry?: RetryConfig;
      onSuccess?: (options: OnSuccessOptions) => MaybePromise<void>;
    };
  };
};

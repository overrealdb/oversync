import { useSettingsStore } from "@/stores/settings";
import type { CreateClientConfig } from "./generated/client.gen";

export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: useSettingsStore.getState().apiBaseUrl,
  throwOnError: false,
  headers: {
    ...(config?.headers ?? {}),
    "Content-Type": "application/json",
  },
});

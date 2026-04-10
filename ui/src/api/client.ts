import { useSettingsStore } from "@/stores/settings";

class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const baseUrl = useSettingsStore.getState().apiBaseUrl;
  const res = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiError(res.status, body.error ?? res.statusText);
  }

  if (res.status === 204) return undefined as T;
  return res.json();
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "PUT", body: body ? JSON.stringify(body) : undefined }),
  del: <T>(path: string) => request<T>(path, { method: "DELETE" }),
};

export function generatedRequestOptions() {
  return {
    baseUrl: useSettingsStore.getState().apiBaseUrl,
  };
}

type GeneratedFieldsResult = {
  data?: unknown;
  error?: unknown;
  response: Response;
};

export async function unwrapGeneratedResult<T>(
  promise: Promise<GeneratedFieldsResult>,
): Promise<T> {
  const result = await promise;
  if (result.error !== undefined) {
    const error =
      typeof result.error === "object" &&
      result.error !== null &&
      "error" in result.error &&
      typeof result.error.error === "string"
        ? result.error.error
        : result.response.statusText;
    throw new ApiError(result.response.status, error);
  }
  return result.data as T;
}

export { ApiError };

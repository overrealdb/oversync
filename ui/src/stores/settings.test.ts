import { describe, it, expect, beforeEach } from "vitest";
import { useSettingsStore } from "./settings";

beforeEach(() => {
  useSettingsStore.setState({
    apiBaseUrl: "/api",
    refreshInterval: 5000,
    theme: "dark",
  });
});

describe("settings store", () => {
  it("has correct defaults", () => {
    const state = useSettingsStore.getState();
    expect(state.apiBaseUrl).toBe("/api");
    expect(state.refreshInterval).toBe(5000);
    expect(state.theme).toBe("dark");
  });

  it("setApiBaseUrl updates the URL", () => {
    useSettingsStore.getState().setApiBaseUrl("http://localhost:3000/api");
    expect(useSettingsStore.getState().apiBaseUrl).toBe(
      "http://localhost:3000/api",
    );
  });

  it("setRefreshInterval updates the interval", () => {
    useSettingsStore.getState().setRefreshInterval(10000);
    expect(useSettingsStore.getState().refreshInterval).toBe(10000);
  });

  it("setTheme updates theme", () => {
    useSettingsStore.getState().setTheme("light");
    expect(useSettingsStore.getState().theme).toBe("light");
  });

  it("toggleTheme switches between dark and light", () => {
    expect(useSettingsStore.getState().theme).toBe("dark");
    useSettingsStore.getState().toggleTheme();
    expect(useSettingsStore.getState().theme).toBe("light");
    useSettingsStore.getState().toggleTheme();
    expect(useSettingsStore.getState().theme).toBe("dark");
  });
});

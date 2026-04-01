import { create } from "zustand";
import { persist } from "zustand/middleware";

interface SettingsState {
  apiBaseUrl: string;
  refreshInterval: number;
  theme: "dark" | "light";
  setApiBaseUrl: (url: string) => void;
  setRefreshInterval: (ms: number) => void;
  setTheme: (theme: "dark" | "light") => void;
  toggleTheme: () => void;
}

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set) => ({
      apiBaseUrl: "/api",
      refreshInterval: 5000,
      theme: "dark",
      setApiBaseUrl: (apiBaseUrl) => set({ apiBaseUrl }),
      setRefreshInterval: (refreshInterval) => set({ refreshInterval }),
      setTheme: (theme) => {
        document.documentElement.classList.toggle("dark", theme === "dark");
        document.documentElement.classList.toggle("light", theme === "light");
        set({ theme });
      },
      toggleTheme: () =>
        set((s) => {
          const theme = s.theme === "dark" ? "light" : "dark";
          document.documentElement.classList.toggle("dark", theme === "dark");
          document.documentElement.classList.toggle("light", theme === "light");
          return { theme };
        }),
    }),
    { name: "oversync-settings" },
  ),
);

export const useSettings = useSettingsStore;

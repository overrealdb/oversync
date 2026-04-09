import { createContext, useContext } from "react";

export interface ToastContextValue {
  toast: (type: "success" | "error", message: string) => void;
}

export const ToastContext = createContext<ToastContextValue>({
  toast: () => {},
});

export function useToast() {
  return useContext(ToastContext);
}

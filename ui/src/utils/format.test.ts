import { describe, it, expect } from "vitest";
import {
  formatDate,
  formatDuration,
  formatNumber,
  formatRelative,
  cycleDurationMs,
} from "./format";

describe("formatDate", () => {
  it("returns dash for null/undefined", () => {
    expect(formatDate(null)).toBe("\u2014");
    expect(formatDate(undefined)).toBe("\u2014");
    expect(formatDate("")).toBe("\u2014");
  });

  it("formats a valid ISO string", () => {
    const result = formatDate("2025-01-15T10:30:00Z");
    expect(result).toBeTruthy();
    expect(result).not.toBe("\u2014");
  });
});

describe("formatDuration", () => {
  it("returns dash for null/undefined", () => {
    expect(formatDuration(null)).toBe("\u2014");
    expect(formatDuration(undefined)).toBe("\u2014");
  });

  it("formats milliseconds", () => {
    expect(formatDuration(500)).toBe("500ms");
    expect(formatDuration(0)).toBe("0ms");
    expect(formatDuration(999)).toBe("999ms");
  });

  it("formats seconds", () => {
    expect(formatDuration(1000)).toBe("1.0s");
    expect(formatDuration(5500)).toBe("5.5s");
    expect(formatDuration(59999)).toBe("60.0s");
  });

  it("formats minutes", () => {
    expect(formatDuration(60000)).toBe("1.0m");
    expect(formatDuration(150000)).toBe("2.5m");
  });
});

describe("formatNumber", () => {
  it("returns dash for null/undefined", () => {
    expect(formatNumber(null)).toBe("\u2014");
    expect(formatNumber(undefined)).toBe("\u2014");
  });

  it("formats numbers with locale", () => {
    expect(formatNumber(0)).toBe("0");
    expect(formatNumber(42)).toBe("42");
    // Large numbers get locale formatting
    const result = formatNumber(1000000);
    expect(result).toBeTruthy();
    expect(result).not.toBe("\u2014");
  });
});

describe("formatRelative", () => {
  it("returns dash for null/undefined/empty", () => {
    expect(formatRelative(null)).toBe("\u2014");
    expect(formatRelative(undefined)).toBe("\u2014");
    expect(formatRelative("")).toBe("\u2014");
  });

  it("formats seconds ago", () => {
    const recent = new Date(Date.now() - 30_000).toISOString();
    expect(formatRelative(recent)).toBe("30s ago");
  });

  it("formats minutes ago", () => {
    const fiveMinAgo = new Date(Date.now() - 300_000).toISOString();
    expect(formatRelative(fiveMinAgo)).toBe("5m ago");
  });

  it("formats hours ago", () => {
    const twoHoursAgo = new Date(Date.now() - 7_200_000).toISOString();
    expect(formatRelative(twoHoursAgo)).toBe("2h ago");
  });

  it("formats days ago", () => {
    const twoDaysAgo = new Date(Date.now() - 172_800_000).toISOString();
    expect(formatRelative(twoDaysAgo)).toBe("2d ago");
  });
});

describe("cycleDurationMs", () => {
  it("returns null when finishedAt is null", () => {
    expect(cycleDurationMs("2025-01-15T10:00:00Z", null)).toBeNull();
  });

  it("calculates duration between two dates", () => {
    expect(
      cycleDurationMs("2025-01-15T10:00:00Z", "2025-01-15T10:00:05Z"),
    ).toBe(5000);
  });

  it("handles same timestamp", () => {
    expect(
      cycleDurationMs("2025-01-15T10:00:00Z", "2025-01-15T10:00:00Z"),
    ).toBe(0);
  });
});

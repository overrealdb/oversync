import { expect, test } from "./fixtures";

test.describe("Navigation", () => {
  test("navigate to each primary page", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");

    await page.click('nav a:has-text("Pipes")');
    await expect(page.locator("h1")).toContainText("Pipes");

    await page.click('nav a:has-text("Saved Recipes")');
    await expect(page.locator("h1")).toContainText("Recipe Library");

    await page.click('nav a:has-text("Sinks")');
    await expect(page.locator("h1")).toContainText("Sinks");

    await page.click('nav a:has-text("History")');
    await expect(page.locator("h1")).toContainText("History");

    await page.click('nav a:has-text("Settings")');
    await expect(page.locator("h1")).toContainText("Settings");
  });
});

test.describe("Dashboard", () => {
  test("shows pipe-first overview", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");
    await expect(page.getByText("query lanes configured")).toBeVisible();
    await expect(page.getByText("Pipes configured")).toBeVisible();
    await expect(page.getByText("Sinks configured")).toBeVisible();
    await expect(page.getByText("Cycles").first()).toBeVisible();
  });

  test("pause and resume sync", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("button:has-text('Pause syncing')")).toBeVisible();
    await page.locator("button:has-text('Pause syncing')").click();
    await expect(page.locator("button:has-text('Resume syncing')")).toBeVisible({
      timeout: 5000,
    });
    await page.locator("button:has-text('Resume syncing')").click();
    await expect(page.locator("button:has-text('Pause syncing')")).toBeVisible({
      timeout: 5000,
    });
  });
});

test.describe("Pipes", () => {
  test("shows pipes table", async ({ page, mockApi }) => {
    await page.goto("/pipes");
    await expect(page.locator("h1")).toContainText("Pipes");
    await expect(page.getByText("catalog-sync")).toBeVisible();
    await expect(page.getByText("postgres_snapshot")).toBeVisible();
  });

  test("create a new pipe", async ({ page, mockApi }) => {
    await page.goto("/pipes");
    await page.click("button:has-text('Add Pipe')");
    await expect(page.getByText("Create PostgreSQL Pipe")).toBeVisible();

    await page.fill('input[placeholder="catalog-postgres"]', "test-new-pipe");
    await page.fill('input[placeholder="postgres://readonly@host:5432/db"]', "postgres://readonly@pg-new:5432/app");
    await page.fill('input[placeholder="kafka-prod"]', "kafka-prod");
    await page.fill('input[placeholder="catalog"]', "app");

    await page.click("button:has-text('Create')");
    await expect(page.getByText("Create PostgreSQL Pipe")).not.toBeVisible({
      timeout: 5000,
    });
  });

  test("open pipe detail page", async ({ page, mockApi }) => {
    await page.goto("/pipes");
    await page.locator("a:has-text('catalog-sync')").first().click();
    await expect(page.locator("h1:has-text('catalog-sync')")).toBeVisible();
    await expect(page.getByText("Effective Queries")).toBeVisible();
    await expect(page.getByText("Runtime Shape")).toBeVisible();
  });
});

test.describe("Saved Recipes", () => {
  test("shows recipe library", async ({ page, mockApi }) => {
    await page.goto("/recipes");
    await expect(page.locator("h1")).toContainText("Recipe Library");
    await expect(page.getByText("generic-postgres-snapshot")).toBeVisible();
  });

  test("open create recipe dialog", async ({ page, mockApi }) => {
    await page.goto("/recipes");
    await page.click("button:has-text('Add Recipe')");
    await expect(page.getByText("Create Saved Recipe")).toBeVisible();
  });
});

test.describe("Sinks", () => {
  test("shows sinks table", async ({ page, mockApi }) => {
    await page.goto("/sinks");
    await expect(page.locator("h1")).toContainText("Sinks");
    await expect(page.getByText("kafka-prod")).toBeVisible();
    await expect(page.getByText("stdout-debug")).toBeVisible();
  });
});

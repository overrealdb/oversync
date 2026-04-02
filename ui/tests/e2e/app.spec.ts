import { test, expect } from "./fixtures";

test.describe("Navigation", () => {
  test("navigate to each page", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");

    await page.click('nav a:has-text("Sources")');
    await expect(page.locator("h1")).toContainText("Sources");

    await page.click('nav a:has-text("Sinks")');
    await expect(page.locator("h1")).toContainText("Sinks");

    await page.click('nav a:has-text("History")');
    await expect(page.locator("h1")).toContainText("History");

    await page.click('nav a:has-text("Settings")');
    await expect(page.locator("h1")).toContainText("Settings");
  });
});

test.describe("Dashboard", () => {
  test("shows overview cards and charts", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");

    // Overview cards should show data
    await expect(page.getByText("Total Sources")).toBeVisible();
    await expect(page.getByText("Total Sinks")).toBeVisible();

    // Status badge should be visible
    await expect(
      page.locator("[class*='emerald'],[class*='blue']").first(),
    ).toBeVisible();

    // Charts sections should render
    await expect(page.getByText("Cycles").first()).toBeVisible();
  });

  test("pause and resume sync", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");

    // Click pause
    const pauseBtn = page.locator("button:has-text('Pause')");
    await expect(pauseBtn).toBeVisible();
    await pauseBtn.click();

    // Should show resume button after pause
    await expect(page.locator("button:has-text('Resume')")).toBeVisible({
      timeout: 5000,
    });

    // Click resume
    await page.locator("button:has-text('Resume')").click();
    await expect(page.locator("button:has-text('Pause')")).toBeVisible({
      timeout: 5000,
    });
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Dashboard");
    await page.waitForTimeout(1000);
    await page.screenshot({
      path: "tests/screenshots/dashboard.png",
      fullPage: true,
    });
  });
});

test.describe("Sources", () => {
  test("shows sources table", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.locator("h1")).toContainText("Sources");

    // Should see source names
    await expect(page.getByText("pg-main")).toBeVisible();
    await expect(page.getByText("http-api")).toBeVisible();
  });

  test("create a new source", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.locator("h1")).toContainText("Sources");

    // Click Add Source
    await page.click("button:has-text('Add Source')");

    // Fill form
    await expect(page.getByText("Create Source")).toBeVisible();
    await page.fill(
      'input[placeholder="my-postgres-source"]',
      "test-new-source",
    );
    await page.selectOption("select", { value: "postgres" });

    // Submit
    await page.click("button:has-text('Create')");

    // Should close dialog and show toast
    await expect(page.getByText("Create Source")).not.toBeVisible({
      timeout: 5000,
    });
  });

  test("edit a source", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.getByText("pg-main")).toBeVisible();

    // Click edit button on first source row
    await page.locator("tr:has-text('pg-main') button[title='Edit']").click();

    // Should see edit dialog
    await expect(page.getByText("Edit Source: pg-main")).toBeVisible();

    // Submit update
    await page.click("button:has-text('Update')");
    await expect(page.getByText("Edit Source: pg-main")).not.toBeVisible({
      timeout: 5000,
    });
  });

  test("delete a source with confirmation", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.getByText("pg-main")).toBeVisible();

    // Click delete button
    await page.locator("tr:has-text('pg-main') button[title='Delete']").click();

    // Confirm dialog should appear
    await expect(page.getByText("Delete Source")).toBeVisible();
    await expect(
      page.getByText('Are you sure you want to delete "pg-main"?'),
    ).toBeVisible();

    // Confirm delete
    await page
      .locator('[role="alertdialog"] button:has-text("Delete")')
      .click();

    // Source should be removed
    await expect(page.getByText("Delete Source")).not.toBeVisible({
      timeout: 5000,
    });
  });

  test("trigger sync from sources table", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.getByText("pg-main")).toBeVisible();

    // Click trigger button
    await page
      .locator("tr:has-text('pg-main') button[title='Trigger sync']")
      .click();

    // Should show success toast
    await expect(page.getByText(/triggered|Sync/i).first()).toBeVisible({
      timeout: 5000,
    });
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.getByText("pg-main")).toBeVisible();
    await page.waitForTimeout(500);
    await page.screenshot({
      path: "tests/screenshots/sources.png",
      fullPage: true,
    });
  });

  test("screenshot create dialog", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await page.click("button:has-text('Add Source')");
    await expect(page.getByText("Create Source")).toBeVisible();
    await page.waitForTimeout(500);
    await page.screenshot({
      path: "tests/screenshots/source-create-dialog.png",
      fullPage: true,
    });
  });
});

test.describe("Source Detail", () => {
  test("shows source detail page", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await expect(page.getByText("pg-main")).toBeVisible();

    // Click source name link
    await page.locator("a:has-text('pg-main')").first().click();

    // Should see source detail
    await expect(page.locator("h1:has-text('pg-main')")).toBeVisible();
    await expect(page.getByText("postgres").first()).toBeVisible();
    await expect(page.getByText("users_sync").first()).toBeVisible();
    await expect(page.getByText("Trigger Sync")).toBeVisible();
  });

  test("trigger sync from detail page", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await page.locator("a:has-text('pg-main')").first().click();
    await expect(page.locator("h1:has-text('pg-main')")).toBeVisible();

    await page.click("button:has-text('Trigger Sync')");
    await expect(page.getByText(/triggered/i)).toBeVisible({ timeout: 5000 });
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sources")');
    await page.locator("a:has-text('pg-main')").first().click();
    await expect(page.locator("h1:has-text('pg-main')")).toBeVisible();
    await page.waitForTimeout(1000);
    await page.screenshot({
      path: "tests/screenshots/source-detail.png",
      fullPage: true,
    });
  });
});

test.describe("Sinks", () => {
  test("shows sinks table", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sinks")');
    await expect(page.locator("h1")).toContainText("Sinks");

    await expect(page.getByText("kafka-prod")).toBeVisible();
    await expect(page.getByText("stdout-debug")).toBeVisible();
  });

  test("create a new sink", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sinks")');

    await page.click("button:has-text('Add Sink')");
    await expect(page.getByText("Create Sink")).toBeVisible();

    await page.fill('input[placeholder="my-kafka-sink"]', "test-new-sink");
    await page.locator("form select").first().selectOption({ value: "kafka" });

    await page.click("button:has-text('Create')");
    await expect(page.getByText("Create Sink")).not.toBeVisible({
      timeout: 5000,
    });
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Sinks")');
    await expect(page.getByText("kafka-prod")).toBeVisible();
    await page.waitForTimeout(500);
    await page.screenshot({
      path: "tests/screenshots/sinks.png",
      fullPage: true,
    });
  });
});

test.describe("History", () => {
  test("shows history table and charts", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("History")');
    await expect(page.locator("h1")).toContainText("History");

    // Should see cycle data
    await expect(page.getByText("users_sync").first()).toBeVisible();
    await expect(page.getByText("pg-main").first()).toBeVisible();

    // Should see showing count
    await expect(page.getByText(/Showing \d+ of \d+ cycles/)).toBeVisible();
  });

  test("filters work", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("History")');
    await expect(page.locator("h1")).toContainText("History");

    // Filter by status
    await page.locator("select").first().selectOption("failed");
    await expect(page.getByText("Connection timeout")).toBeVisible();
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("History")');
    await expect(page.getByText("users_sync").first()).toBeVisible();
    await page.waitForTimeout(1000);
    await page.screenshot({
      path: "tests/screenshots/history.png",
      fullPage: true,
    });
  });
});

test.describe("Settings", () => {
  test("shows settings page", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Settings")');
    await expect(page.locator("h1")).toContainText("Settings");

    // Should see API endpoint field
    await expect(page.locator('input[type="text"]')).toBeVisible();

    // Should see theme options
    await expect(page.getByText("Dark")).toBeVisible();
    await expect(page.getByText("Light")).toBeVisible();
  });

  test("toggle theme", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Settings")');

    // Click Light theme
    await page.locator("label:has-text('Light')").click();

    // HTML should have light class
    const htmlClass = await page.locator("html").getAttribute("class");
    expect(htmlClass).toContain("light");

    // Switch back to dark
    await page.locator("label:has-text('Dark')").click();
    const htmlClass2 = await page.locator("html").getAttribute("class");
    expect(htmlClass2).toContain("dark");
  });

  test("screenshot", async ({ page, mockApi }) => {
    await page.goto("/");
    await page.click('nav a:has-text("Settings")');
    await expect(page.locator("h1")).toContainText("Settings");
    await page.waitForTimeout(500);
    await page.screenshot({
      path: "tests/screenshots/settings.png",
      fullPage: true,
    });
  });
});

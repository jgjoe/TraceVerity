import { expect, test } from "@playwright/test";

test("production workbench exposes deterministic BPIC12 facts and case states", async ({ page }) => {
  await page.goto("/");

  await expect(page).toHaveTitle("TraceVerity — Process Intelligence Workbench");
  await expect(page.getByRole("heading", { name: "TraceVerity" })).toBeVisible();
  await expect(page.getByText("Process Intelligence Workbench", { exact: true })).toBeVisible();
  await expect(page.getByTestId("dataset-select")).toHaveValue("bpic2012");
  await expect(page.getByTestId("selected-dataset-name")).toHaveText("BPI Challenge 2012");
  await expect(page.getByTestId("selected-dataset-status")).toContainText("ready");
  await expect(page.getByTestId("case-count")).toContainText("13,087");
  await expect(page.getByTestId("raw-events")).toContainText("262,200");
  await expect(page.getByTestId("variant-count")).toContainText("4,336");
  await expect(page.getByText(/configured test threshold: 604800000 ms/i)).toBeVisible();
  await expect(page.getByTestId("sla-card")).toContainText(
    "raw share 41.09% · configured test threshold: 604800000 ms",
  );
  await expect(page.getByText(/not true queue waiting/i).first()).toBeVisible();

  await expect(page.getByTestId("variants-table").locator("tbody tr").first()).toBeVisible();
  await expect(
    page.getByTestId("variants-table").locator("tbody tr").first().locator("td").last(),
  ).toHaveText("26.20%");
  await expect(page.getByTestId("transitions-list").locator(".transition-row").first()).toBeVisible();
  await expect(page.getByTestId("activities-table").locator("tbody tr").first()).toBeVisible();

  const input = page.getByLabel("Case ID", { exact: true });
  await input.fill("173688");
  await page.getByRole("button", { name: "Look up case" }).click();
  await expect(page.getByTestId("case-trace")).toContainText("CASE 173688");
  await expect(page.getByTestId("case-trace").locator("li").first()).toBeVisible();

  await input.fill("not-a-real-bpic12-case");
  await page.getByRole("button", { name: "Look up case" }).click();
  await expect(page.getByTestId("trace-error")).toContainText(
    "No BPI Challenge 2012 case found for “not-a-real-bpic12-case”.",
  );
});

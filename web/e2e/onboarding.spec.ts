import { expect, test } from "@playwright/test";

/**
 * The real Help Desk CSV is selected from the gitignored local raw area; it is
 * never committed. The path is resolved against the `web/` working directory
 * that `npm run test:e2e` uses.
 */
const HELP_DESK_SOURCE = "../data/raw/helpdesk/finale.csv";

test("a real CSV event log is onboarded in the browser and analyzed immediately", async ({
  page,
}) => {
  await page.goto("/");

  // 1-2: the canonical BPIC12 baseline is the default selection.
  await expect(page.getByTestId("selected-dataset-name")).toHaveText("BPI Challenge 2012");
  await expect(page.getByTestId("case-count")).toContainText("13,087");
  await expect(page.getByTestId("variant-count")).toContainText("4,336");

  // 3-5: open the import panel and read the actual source file.
  await page.getByTestId("import-toggle").click();
  await expect(page.getByTestId("import-panel")).toBeVisible();
  await page.getByTestId("import-file").setInputFiles(HELP_DESK_SOURCE);
  await expect(page.getByTestId("import-format")).toHaveValue("csv");
  await page.getByTestId("import-preview-button").click();
  await expect(page.getByTestId("import-preview")).toContainText("finale.csv");
  await expect(page.getByTestId("import-preview")).toContainText("21,348 data rows");
  await expect(page.getByTestId("import-preview")).toContainText("16 columns");
  await expect(page.getByTestId("import-preview")).toContainText(
    "Duplicated header names (unmapped only): Variant",
  );
  await expect(page.getByTestId("import-preview")).toContainText(
    "31024fa6da0a35578643d50f2bea6e90d5ce97628b64851e271b521f005eef1c",
  );

  // 6-8: explicit mapping; lifecycle stays unmapped.
  await page.getByTestId("mapping-case_id").selectOption("Case ID");
  await page.getByTestId("mapping-activity").selectOption("Activity");
  await page.getByTestId("mapping-timestamp").selectOption("Complete Timestamp");
  await page.getByTestId("mapping-resource").selectOption("Resource");
  await expect(page.getByTestId("mapping-lifecycle")).toHaveValue("");

  // 9-10: explicit timestamp interpretation, with the convention caveat visible.
  await expect(page.getByTestId("timezone-caveat")).toContainText("normalization convention");
  await expect(page.getByTestId("timezone-caveat")).toContainText(
    "not automatically a claim about the original event timezone",
  );
  await page.getByTestId("timestamp-format").fill("%Y/%m/%d %H:%M:%S.%f");
  await page.getByTestId("assume-timezone").fill("UTC");

  // 11-13: identity, then validate and import.
  await page.getByTestId("import-log-id").fill("helpdesk");
  await page.getByTestId("import-display-name").fill("Italian Help Desk");
  await page.getByTestId("import-build-button").click();
  await expect(page.getByTestId("import-status")).toContainText(
    "Validated and imported Italian Help Desk",
    { timeout: 180_000 },
  );
  await expect(page.getByTestId("import-error")).toHaveCount(0);

  // 11-15: the new dataset is selected and analyzed by the same dashboard.
  await expect(page.getByTestId("dataset-select")).toHaveValue("helpdesk");
  await expect(page.getByTestId("selected-dataset-name")).toHaveText("Italian Help Desk");
  await expect(page.getByTestId("case-count")).toContainText("4,580");
  await expect(page.getByTestId("raw-events")).toContainText("21,348");
  await expect(page.getByTestId("analysis-events")).toContainText("21,348");
  await expect(page.getByTestId("variant-count")).toContainText("226");
  await expect(page.getByTestId("sla-card")).toHaveCount(0);
  await expect(page.getByText(/configured test threshold/i)).toHaveCount(0);
  await expect(page.getByText(/not true queue waiting/i).first()).toBeVisible();
  await expect(page.getByTestId("activities-table").locator("tbody tr")).toHaveCount(10);

  // 16: a real Help Desk case resolves in the selected dataset.
  await page.getByLabel("Case ID", { exact: true }).fill("Case 1");
  await page.getByRole("button", { name: "Look up case" }).click();
  await expect(page.getByTestId("case-trace")).toContainText("CASE Case 1");
  await expect(page.getByTestId("case-trace")).toContainText("Assign seriousness");
  await expect(page.getByTestId("case-trace").locator("li")).toHaveCount(5);

  // 17-18: switching back proves the BPIC12 dashboard is unchanged.
  await page.getByTestId("dataset-select").selectOption("bpic2012");
  await expect(page.getByTestId("selected-dataset-name")).toHaveText("BPI Challenge 2012");
  await expect(page.getByTestId("case-count")).toContainText("13,087");
  await expect(page.getByTestId("variant-count")).toContainText("4,336");
  await expect(page.getByTestId("sla-card")).toContainText("configured test threshold: 604800000 ms");
  await expect(page.getByTestId("case-trace")).toHaveCount(0);
  await expect(page.getByTestId("trace-error")).toHaveCount(0);
});

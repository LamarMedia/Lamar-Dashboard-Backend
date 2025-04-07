const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");
const router = express.Router();

const bigquery = new BigQuery();

router.post("/saveThresholdAlert", async (req, res) => {
    try {
        const tableName = "`dp01-dev-app-01.d_transformation_dp01_bq_01.Threshold_alert`";
        const alertArray = req.body;

        if (!Array.isArray(alertArray) || alertArray.length === 0) {
            return res.status(400).json({ error: "Request body should be a non-empty array of alerts." });
        }

        const currentTimestamp = new Date().toISOString().replace("T", " ").split(".")[0];
        const insertedAlerts = [];
        const skippedDuplicates = [];

        for (const alert of alertArray) {
            const {
                Avg_Run_Time,
                Condition_Met,
                Frequency,
                Status,
                Email,
                Owner,
                Field_Title,
                Dashboard_Element_Id,
                Id,
                FieldName,
                Followed,
                Followable,
                Is_Disabled,
                Disabled_Reason,
                Is_Public,
                Threshold,
                Created_By
            } = alert;

            if (Dashboard_Element_Id === undefined || Id === undefined) {
                // Skip record with missing required fields
                skippedDuplicates.push({ reason: "Missing Id or Dashboard_Element_Id", alert });
                continue;
            }

            // Check for duplicates
            const checkQuery = `
                SELECT COUNT(*) as total FROM ${tableName}
                WHERE Id = @Id AND Dashboard_Element_Id = @Dashboard_Element_Id
            `;

            const [checkResult] = await bigquery.query({
                query: checkQuery,
                params: { Id, Dashboard_Element_Id },
                types: {
                    Id: "INTEGER",
                    Dashboard_Element_Id: "INTEGER"
                }
            });

            if (checkResult[0].total > 0) {
                // Skip duplicate
                skippedDuplicates.push({ Id, Dashboard_Element_Id, reason: "Duplicate" });
                continue;
            }

            const insertQuery = `
                INSERT INTO ${tableName} (
                    Avg_Run_Time, Condition_Met, Frequency, Status, Email, Owner, Field_Title,
                    Dashboard_Element_Id, Id, FieldName, Followed, Followable, Is_Disabled,
                    Disabled_Reason, Is_Public, Threshold, Created_By, Created_Date, Updated_Date
                )
                VALUES (
                    @Avg_Run_Time, @Condition_Met, @Frequency, @Status, @Email, @Owner, @Field_Title,
                    @Dashboard_Element_Id, @Id, @FieldName, @Followed, @Followable, @Is_Disabled,
                    @Disabled_Reason, @Is_Public, @Threshold, @Created_By, @Created_Date, @Updated_Date
                )
            `;

            const queryParams = {
                Avg_Run_Time: Avg_Run_Time || null,
                Condition_Met: Condition_Met || null,
                Frequency: Frequency || null,
                Status: Status || null,
                Email: Email || null,
                Owner: Owner || null,
                Field_Title: Field_Title || null,
                Dashboard_Element_Id,
                Id,
                FieldName: FieldName || null,
                Followed: Followed ?? null,
                Followable: Followable ?? null,
                Is_Disabled: Is_Disabled ?? null,
                Disabled_Reason: Disabled_Reason || null,
                Is_Public: Is_Public ?? null,
                Threshold: Threshold ?? null,
                Created_By: Created_By || null,
                Created_Date: currentTimestamp,
                Updated_Date: currentTimestamp
            };

            const queryTypes = {
                Avg_Run_Time: "string",
                Condition_Met: "string",
                Frequency: "string",
                Status: "string",
                Email: "string",
                Owner: "string",
                Field_Title: "string",
                Dashboard_Element_Id: "int64",
                Id: "int64",
                FieldName: "string",
                Followed: "bool",
                Followable: "bool",
                Is_Disabled: "bool",
                Disabled_Reason: "string",
                Is_Public: "bool",
                Threshold: "int64",
                Created_By: "string",
                Created_Date: "datetime",
                Updated_Date: "datetime"
            };

            await bigquery.query({
                query: insertQuery,
                params: queryParams,
                types: queryTypes
            });

            insertedAlerts.push({ Id, Dashboard_Element_Id });
        }

        res.status(201).json({
            message: `${insertedAlerts.length} alert(s) inserted, ${skippedDuplicates.length} skipped.`,
            inserted: insertedAlerts,
            skipped: skippedDuplicates
        });

    } catch (error) {
        console.error("Error in saveThresholdAlert:", error);
        res.status(500).json({ error: "Failed to save threshold alerts." });
    }
});


// Function to insert and fetch unapproved invoices in new format
async function fetchAndInsertUnapprovedInvoices(email) {
    const datasetId = "d_transformation_dp01_bq_01";
    const tableId = "Unaprooved_Invoice_Summary"; // Keep name as in your table

    console.log(`Merging unapproved invoice data for email: ${email}`);

    const mergeQuery = `
        MERGE \`${datasetId}.${tableId}\` AS target
        USING (
            SELECT DISTINCT 
                Payment_Date, 
                Invoice_Id, 
                email_user AS Email_User, 
                FALSE AS IsSeen,
                CURRENT_DATETIME() AS GeneratedTimeStamp
            FROM \`${datasetId}.supplier_flattened_v\`
            WHERE Payment_Status = 'UNPAID' AND email_user = @email
        ) AS source
        ON target.Invoice_Id = source.Invoice_Id
            AND target.Email_User = source.Email_User
            AND DATE(target.Payment_Date) = DATE(source.Payment_Date)
        WHEN NOT MATCHED THEN
        INSERT (Payment_Date, Invoice_Id, Email_User, IsSeen, GeneratedTimeStamp)
        VALUES (source.Payment_Date, source.Invoice_Id, source.Email_User, source.IsSeen, source.GeneratedTimeStamp)
    `;

    const mergeOptions = {
        query: mergeQuery,
        params: { email }
    };

    try {
        // Run MERGE
        const [mergeJob] = await bigquery.createQueryJob(mergeOptions);
        await mergeJob.getQueryResults();
        console.log("Merge completed successfully.");

        // Fetch grouped alert summary
        const fetchQuery = `
            SELECT 
                CONCAT(
                    COUNT(*), 
                    ' unapproved invoices need approval by ', 
                    FORMAT_DATE('%b %d', DATE(Payment_Date)), 
                    ' at ', 
                    FORMAT_TIME('%I:%M %p', TIME(Payment_Date))
                ) AS Message,
                CASE 
                    WHEN DATE_DIFF(DATE(Payment_Date), CURRENT_DATE(), DAY) <= 5 THEN 'Red'
                    WHEN DATE_DIFF(DATE(Payment_Date), CURRENT_DATE(), DAY) BETWEEN 6 AND 10 THEN 'Amber'
                    ELSE 'Yellow'
                END AS Status_RAG,
                STRING_AGG(CAST(Invoice_Id AS STRING)) AS Ids,
                CAST(MAX(GeneratedTimeStamp) AS STRING) AS Generated_Timestamp,
                FALSE AS IsSeen,
                'Invoice' as Type
            FROM \`${datasetId}.${tableId}\`
            WHERE Email_User = @email AND IsSeen = false
            GROUP BY Payment_Date
            ORDER BY Generated_Timestamp DESC
        `;

        const fetchOptions = {
            query: fetchQuery,
            params: { email }
        };

        const [fetchJob] = await bigquery.createQueryJob(fetchOptions);
        const [rows] = await fetchJob.getQueryResults();

        // No need for flattening since timestamps are already cast to string
        return {
            success: true,
            message: "Unapproved invoices merged and summarized.",
            data: rows
        };

    } catch (error) {
        console.error("Error running query:", error);
        return {
            success: false,
            message: "Query execution failed.",
            error: error.message
        };
    }
}

// API Route remains the same
router.get("/unapprovedInvoiceAlertbyMail", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email) {
            return res.status(400).json({ error: "Email parameter is required." });
        }

        const result = await fetchAndInsertUnapprovedInvoices(email);
        res.json(result);
    } catch (error) {
        console.error("API error:", error);
        res.status(500).json({ error: "Internal server error." });
    }
});


async function fetchAndInsertUnapprovedInsurance(email) {
    const datasetId = "d_transformation_dp01_bq_01";
    const tableId = "Insurance_Summary_Alerts"; // 

    console.log(`Merging insurance summary alerts for email: ${email}`);

    const mergeQuery = `
        MERGE \`${datasetId}.${tableId}\` AS target
        USING (
            SELECT DISTINCT 
                Insurance_Expiration_Date,
                Supplier_Id,
                email_user AS Email_user,
                FALSE AS IsSeen,
                CURRENT_DATETIME() AS GeneratedTimeStamp
            FROM \`${datasetId}.supplier_flattened_v\`
            WHERE email_user = @email
                AND Insurance_Expiration_Date IS NOT NULL
                AND DATE_DIFF(Insurance_Expiration_Date, CURRENT_DATE(), DAY) BETWEEN 0 AND 365
        ) AS source
        ON target.Supplier_Id = source.Supplier_Id
            AND DATE(target.Insurance_Expiration_Date) = DATE(source.Insurance_Expiration_Date)
            AND target.Email_user = source.Email_user
        WHEN NOT MATCHED THEN
        INSERT (
            Insurance_Expiration_Date, Supplier_Id, Email_user, IsSeen, GeneratedTimeStamp
        )
        VALUES (
            source.Insurance_Expiration_Date, source.Supplier_Id, source.Email_user, source.IsSeen, source.GeneratedTimeStamp
        )
    `;

    const mergeOptions = {
        query: mergeQuery,
        params: { email }
    };

    try {
        // Run MERGE
        const [mergeJob] = await bigquery.createQueryJob(mergeOptions);
        await mergeJob.getQueryResults();
        console.log("Merge completed successfully.");

        // Fetch grouped summary
        const fetchQuery = `
            SELECT 
                CONCAT(
                    COUNT(*), 
                    ' insurance policies will expire on ', 
                    FORMAT_DATE('%b %d, %Y', DATE(Insurance_Expiration_Date))
                ) AS Message,
                CASE 
                    WHEN DATE_DIFF(DATE(Insurance_Expiration_Date), CURRENT_DATE(), DAY) > 120 THEN 'Red'
                    WHEN DATE_DIFF(DATE(Insurance_Expiration_Date), CURRENT_DATE(), DAY) BETWEEN 90 AND 120 THEN 'Amber'
                    ELSE 'Yellow'
                END AS Status_RAG,
                STRING_AGG(CAST(Supplier_Id AS STRING)) AS Ids,
                CAST(MAX(GeneratedTimeStamp) AS STRING) AS Generated_Timestamp,
                FALSE AS IsSeen,
                'Insurance' as Type
            FROM \`${datasetId}.${tableId}\`
            WHERE Email_user = @email AND IsSeen = false
            GROUP BY Insurance_Expiration_Date
            ORDER BY Generated_Timestamp DESC
        `;

        const fetchOptions = {
            query: fetchQuery,
            params: { email }
        };

        const [fetchJob] = await bigquery.createQueryJob(fetchOptions);
        const [rows] = await fetchJob.getQueryResults();

        return {
            success: true,
            message: "Insurance alerts merged and summarized.",
            data: rows
        };
    } catch (error) {
        console.error("Error running insurance merge query:", error);
        return {
            success: false,
            message: "Error executing insurance alert query.",
            error: error.message
        };
    }
}

router.get("/UnapprovedInsuranceSummaryByMail", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email) {
            return res.status(400).json({ error: "Email parameter is required." });
        }

        const result = await fetchAndInsertUnapprovedInsurance(email);
        res.json(result);
    } catch (error) {
        console.error("Error fetching unapproved insurance alerts:", error);
        res.status(500).json({ error: "Failed to fetch unapproved insurance alerts." });
    }
});



async function fetchAndInsertBudgetSummary(email) {
    const datasetId = "d_transformation_dp01_bq_01";
    const tableId = "Budget_Summary";

    console.log(`Merging budget alerts for email: ${email}`);

    const mergeQuery = `
        MERGE \`${datasetId}.${tableId}\` AS target
        USING (
            SELECT DISTINCT
                Actual,
                Budget,
                Project_Id,
                email_user AS Email_user,
                FALSE AS IsSeen,
                CURRENT_DATETIME() AS GeneratedTimeStamp
            FROM \`${datasetId}.supplier_flattened_v\`
            WHERE Actual > Budget AND email_user = @email
        ) AS source
        ON target.Project_Id = source.Project_Id
            AND target.Email_user = source.Email_user
            AND target.Actual = source.Actual
            AND target.Budget = source.Budget
        WHEN NOT MATCHED THEN
        INSERT (
            Actual, Budget, Project_Id, Email_user, IsSeen, GeneratedTimeStamp
        )
        VALUES (
            source.Actual, source.Budget, source.Project_Id, source.Email_user, source.IsSeen, source.GeneratedTimeStamp
        )
    `;

    const mergeOptions = {
        query: mergeQuery,
        params: { email }
    };

    try {
        // Run the MERGE query
        const [mergeJob] = await bigquery.createQueryJob(mergeOptions);
        await mergeJob.getQueryResults();
        console.log("Merge completed successfully.");

        // Fetch distinct suppliers who breached budget
        const fetchQuery = `
            SELECT 
                ARRAY_AGG(Project_Id) AS Supplier_Ids,
                COUNT(DISTINCT Project_Id) AS SupplierCount,
                MAX(GeneratedTimeStamp) AS LatestGeneratedTimeStamp
            FROM \`${datasetId}.${tableId}\`
            WHERE Email_user = @email AND IsSeen = false
        `;

        const fetchOptions = {
            query: fetchQuery,
            params: { email }
        };

        const [fetchJob] = await bigquery.createQueryJob(fetchOptions);
        const [rows] = await fetchJob.getQueryResults();

        const count = rows[0].SupplierCount || 0;
        const ids = (rows[0].Supplier_Ids || []).map(id => String(id)).join(", ");
        const latestTimestamp = rows[0].LatestGeneratedTimeStamp
            ? rows[0].LatestGeneratedTimeStamp.value
            : null;

        const summaryMessage = `Budget threshold exceeded for ${count} supplier${count !== 1 ? "s" : ""}`;

        return {
            success: true,
            message: summaryMessage,
            count,
            data: count > 0
                ? [{
                    Message: summaryMessage,
                    Status_RAG: "Red",
                    Ids: ids,
                    Generated_Timestamp: latestTimestamp,
                    IsSeen: false,
                    Type: "Budget"
                }]
                : []
        };

    } catch (error) {
        console.error("Error running budget query:", error);
        return {
            success: false,
            message: "Error executing budget alert query.",
            error: error.message
        };
    }
}


router.get("/budgetSummaryAlertByMail", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email) {
            return res.status(400).json({ error: "Email parameter is required." });
        }

        const result = await fetchAndInsertBudgetSummary(email);
        res.json(result);
    } catch (error) {
        console.error("Error fetching budget summary alerts:", error);
        res.status(500).json({ error: "Failed to fetch budget summary alerts." });
    }
});


router.post("/alertDetailsByTypeAndIds", async (req, res) => {
    try {
        let { type, ids } = req.body;

        // Normalize ids to array
        if (typeof ids === "string") {
            ids = ids.split(",").map((id) => id.trim()).filter(Boolean);
        } else if (Array.isArray(ids)) {
            if (ids.length === 1 && typeof ids[0] === "string" && ids[0].includes(",")) {
                ids = ids[0].split(",").map((id) => id.trim()).filter(Boolean);
            }
        }

        // Validate input
        if (!type || !Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({
                success: false,
                error: "Request must include 'type' and a non-empty 'ids' array or comma-separated string.",
            });
        }

        // Determine filter column
        let filterColumn;
        switch (type.toLowerCase()) {
            case "invoice":
                filterColumn = "Invoice_Id";
                break;
            case "insurance":
                filterColumn = "Supplier_Id";
                break;
            case "budget":
                filterColumn = "Project_Id";
                break;
            default:
                return res.status(400).json({
                    success: false,
                    error: "Invalid type. Must be 'Invoice', 'Insurance', or 'Budget'.",
                });
        }

        // SQL Query
        const query = `
            SELECT DISTINCT
                ROUND(Actual, 2) AS Actual, 
                ROUND(Payment_Amount, 2) AS Payment_Amount,
                IFNULL(Budget, 0) AS Budget,
                Invoice_Date,
                Invoice_Due_Date,
                Supplier_Name,
                Supplier_Id,
                Insurance_Expiration_Date,
                ${filterColumn} AS Matched_Id
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\`
            WHERE CAST(${filterColumn} AS STRING) IN UNNEST(@idList)
        `;

        const queryOptions = {
            query,
            params: {
                idList: ids.map(String),
            },
        };

        console.log(`[API] Fetching ${type} details for IDs:`, ids);

        const [job] = await bigquery.createQueryJob(queryOptions);
        const [rows] = await job.getQueryResults();

        // Deduplicate rows by Matched_Id (keep first occurrence)
        const deduplicatedRows = Object.values(
            rows.reduce((acc, row) => {
                const id = row.Matched_Id;
                if (!acc[id]) {
                    acc[id] = row;
                }
                return acc;
            }, {})
        );

        // Optional: To deduplicate by most recent Invoice_Date, use this instead:
        /*
        const deduplicatedRows = Object.values(
            rows.reduce((acc, row) => {
                const id = row.Matched_Id;
                const existing = acc[id];

                const newDate = new Date(row.Invoice_Date?.value || row.Invoice_Date);
                const existingDate = new Date(existing?.Invoice_Date?.value || existing?.Invoice_Date || 0);

                if (!existing || newDate > existingDate) {
                    acc[id] = row;
                }

                return acc;
            }, {})
        );
        */

        return res.json({
            success: true,
            type,
            ids,
            data: deduplicatedRows,
        });

    } catch (error) {
        console.error("❌ Error fetching alert details:", error);
        return res.status(500).json({
            success: false,
            message: "Failed to fetch alert details.",
            error: error.message,
        });
    }
});



router.get("/getinvoicelinkbyid/:invoice_Id", async (req, res) => {
    const dataset = "d_transformation_dp01_bq_01";
    const projectId = "dp01-dev-app-01";
    const table = "supplier_flattened_v";

    const invoice_Id = parseInt(req.params.invoice_Id, 10);

    if (isNaN(invoice_Id)) {
        return res.status(400).json({ error: "invoice_Id must be a number" });
    }

    const query = `
        SELECT DISTINCT URL
        FROM \`${projectId}.${dataset}.${table}\`
        WHERE Invoice_Id = @invoice_Id
        AND URL IS NOT NULL
    `;

    try {
        const [rows] = await bigquery.query({
            query,
            params: { invoice_Id }
        });

        res.status(200).json({ invoiceLinks: rows });
    } catch (error) {
        console.error("Error fetching invoice links:", error);
        res.status(500).json({ error: "Failed to fetch invoice links" });
    }
});




module.exports = router;

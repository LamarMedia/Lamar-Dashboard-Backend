const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");
const router = express.Router();
const admin = require("firebase-admin");
const bigquery = new BigQuery();
const db = admin.firestore();

// Add push notification setting
router.post("/pushNotificationAdd", async (req, res) => {
    const dataset = "d_transformation_dp01_bq_01";
    const projectId = "dp01-dev-app-01";
    const table = "PushNotification_Settings";

    const {
        Colab,
        Insurance,
        Invoice,
        Budget,
        User_Mail
    } = req.body;

    if (!User_Mail) {
        return res.status(400).json({ error: "User_Mail is required." });
    }

    const CreatedTime = new Date().toISOString().slice(0, 19).replace("T", " ");

    try {
        // Check if user already has a record
        const checkQuery = `
            SELECT User_Mail FROM \`${projectId}.${dataset}.${table}\`
            WHERE User_Mail = @User_Mail
            LIMIT 1
        `;

        const [existingRows] = await bigquery.query({
            query: checkQuery,
            params: { User_Mail }
        });

        if (existingRows.length > 0) {
            // Update existing record
            const updateQuery = `
                UPDATE \`${projectId}.${dataset}.${table}\`
                SET Colab = @Colab,
                    Insurance = @Insurance,
                    Invoice = @Invoice,
                    Budget = @Budget,
                    CreatedTime = @CreatedTime
                WHERE User_Mail = @User_Mail
            `;

            await bigquery.query({
                query: updateQuery,
                params: {
                    Colab,
                    Insurance,
                    Invoice,
                    Budget,
                    CreatedTime,
                    User_Mail
                }
            });

            res.status(200).json({ message: "Push notification setting updated successfully" });

        } else {
            // Insert new record
            const insertQuery = `
                INSERT INTO \`${projectId}.${dataset}.${table}\`
                (Colab, Insurance, Invoice, Budget, User_Mail, CreatedTime)
                VALUES (@Colab, @Insurance, @Invoice, @Budget, @User_Mail, @CreatedTime)
            `;

            await bigquery.query({
                query: insertQuery,
                params: {
                    Colab,
                    Insurance,
                    Invoice,
                    Budget,
                    User_Mail,
                    CreatedTime
                }
            });

            res.status(200).json({ message: "Push notification setting added successfully" });
        }

    } catch (error) {
        console.error("Error during upsert of push notification setting:", error);
        res.status(500).json({ error: "Failed to insert/update push notification setting" });
    }
});



// Get push notification settings
router.get("/pushNotificationGet/:email", async (req, res) => {
    const dataset = "d_transformation_dp01_bq_01";
    const projectId = "dp01-dev-app-01";
    const table = "PushNotification_Settings";
    const email = req.params.email;

    if (!email || email.trim() === "") {
        return res.status(400).json({ error: "Email is required" });
    }

    const query = `
        SELECT * 
        FROM \`${projectId}.${dataset}.${table}\` 
        WHERE User_Mail = @email
        LIMIT 1
    `;

    try {
        const [rows] = await bigquery.query({
            query,
            params: { email }
        });

        const result = rows[0] || null;
        res.status(200).json({ pushNotification: result });

    } catch (error) {
        console.error("Error fetching push notification settings:", error);
        res.status(500).json({ error: "Failed to fetch push notification settings" });
    }
});


// Delete push notification settings
router.delete("/pushNotificationDelete/:email", async (req, res) => {
    const dataset = "d_transformation_dp01_bq_01";
    const projectId = "dp01-dev-app-01";
    const table = "PushNotification_Settings";
    const email = req.params.email;

    if (!email || email.trim() === "") {
        return res.status(400).json({ error: "Email is required" });
    }

    const queryDelete = `
        DELETE FROM \`${projectId}.${dataset}.${table}\`
        WHERE User_Mail = @email
    `;

    try {
        await bigquery.query({
            query: queryDelete,
            params: { email }
        });

        res.status(200).json({ message: "Push notification settings deleted successfully" });
    } catch (error) {
        console.error("Error deleting push notification settings:", error);
        res.status(500).json({ error: "Failed to delete push notification settings" });
    }
});

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

router.get("/getAlertsFromPushNotification", async (req, res) => {
    const email = req.query.email;

    if (!email) {
        return res.status(400).json({ success: false, message: "Missing email parameter." });
    }

    try {
        // Step 1: Fetch user's push notification settings
        const [settings] = await bigquery.query({
            query: `
                SELECT colab, Insurance, Invoice, Budget
                FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.PushNotification_Settings\`
                WHERE User_Mail = @email
                LIMIT 1
            `,
            params: { email }
        });

        if (!settings.length) {
            return res.status(404).json({ success: false, message: "User notification settings not found." });
        }

        const { colab, Insurance, Invoice, Budget } = settings[0];

        // Step 2: Prepare alert promises
        const alertPromises = [
            Budget ? fetchAndInsertBudgetSummary(email) : Promise.resolve({ success: true, data: [] }),
            Insurance ? fetchAndInsertUnapprovedInsurance(email) : Promise.resolve({ success: true, data: [] }),
            Invoice ? fetchAndInsertUnapprovedInvoices(email) : Promise.resolve({ success: true, data: [] })
        ];

        // Step 3: Colab Alert Logic (Inline)
        const [firstName, lastName] = email.split("@")[0].split(".");
        const fullName = lastName ? `${firstName} ${lastName}` : firstName;
        let colabAlerts = [];

        if (colab) {
            try {
                const snapshot = await db.collection("drive_comments")
                    .orderBy("created_at", "desc")
                    .get();

                if (!snapshot.empty) {
                    colabAlerts = snapshot.docs
                        .map(doc => ({ id: doc.id, ...doc.data() }))
                        .filter(comment =>
                            comment.isSeen === false &&
                            (
                                (comment.mentioned_users && comment.mentioned_users.includes(email)) ||
                                (comment.author && comment.author.toLowerCase() === fullName.toLowerCase())
                            )
                        )
                        .map(comment => ({
                            Message: comment.content,
                            Status_RAG: "",
                            Ids: comment.id,
                            Generated_Timestamp: comment.created_at?._seconds
                                ? new Date(comment.created_at._seconds * 1000).toISOString()
                                : null,
                            IsSeen: comment.isSeen ?? false,
                            Type: "Colab",
                            File_Id: comment.file_id || null,
                            File_Name: comment.file_name || null,
                            Replies: (comment.replies || []).map(reply => ({
                                id: reply.id,
                                content: reply.content,
                                author: reply.author,
                                authorEmail: reply.authorEmail,
                                mentioned_users: reply.mentioned_users || [],
                                created_at: reply.created_at?._seconds
                                    ? new Date(reply.created_at._seconds * 1000).toISOString()
                                    : null
                            }))
                        }));
                }
            } catch (firestoreErr) {
                console.error("Colab error:", firestoreErr.message);
            }
        }

        // Step 4: Await the remaining alerts
        const [budgetRes, insuranceRes, invoiceRes] = await Promise.all(alertPromises);
        const getData = (res) => res?.success && Array.isArray(res.data) ? res.data : [];

        const allAlerts = [
            ...getData(budgetRes),
            ...getData(insuranceRes),
            ...getData(invoiceRes),
            ...colabAlerts
        ];

        return res.json({
            success: true,
            message: "Fetched alerts from PushNotification settings.",
            email,
            total: allAlerts.length,
            alerts: allAlerts
        });

    } catch (error) {
        console.error("Error in getAlertsFromPushNotification:", error);
        return res.status(500).json({
            success: false,
            message: "Internal server error fetching alerts.",
            error: error.message
        });
    }
});

function isNumeric(str) {
    return /^\d+$/.test(str);
}

function formatId(id) {
    const trimmed = id.trim();
    if (trimmed.startsWith("'") && trimmed.endsWith("'")) return trimmed;
    if (isNumeric(trimmed)) return trimmed;
    return `'${trimmed}'`;
}

async function updateAlertsIsSeen(ids, userEmail, type) {
    if (type === "Colab") {
        const id = ids.trim(); // expect a single doc ID

        try {
            const docRef = db.collection("drive_comments").doc(id);

            const result = await db.runTransaction(async (transaction) => {
                const docSnapshot = await transaction.get(docRef);

                if (!docSnapshot.exists) {
                    return { success: false, message: `Comment ${id} not found.` };
                }

                const commentData = docSnapshot.data();

                if (commentData.isSeen) {
                    return { success: true, message: `Comment ${id} already marked as seen.` };
                }

                transaction.update(docRef, { isSeen: true });
                return { success: true, message: `Comment ${id} marked as seen.` };
            });

            return [result]; // always return an array for consistency

        } catch (error) {
            console.error(`Firestore error for comment ${id}:`, error);
            throw new Error(`Failed to update comment ${id}`);
        }
    }

    // BigQuery path for Budget, Insurance, Invoice
    const idArray = [...new Set(ids.split(',').map(id => id.trim()).filter(Boolean))];
    let table, idColumn;

    switch (type) {
        case "Budget":
            table = "`dp01-dev-app-01.d_transformation_dp01_bq_01.Budget_Summary`";
            idColumn = "Project_Id";
            break;
        case "Insurance":
            table = "`dp01-dev-app-01.d_transformation_dp01_bq_01.Insurance_Summary_Alerts`";
            idColumn = "Supplier_Id";
            break;
        case "Invoice":
            table = "`dp01-dev-app-01.d_transformation_dp01_bq_01.Unaprooved_Invoice_Summary`";
            idColumn = "Invoice_Id";
            break;
        default:
            throw new Error(`Unsupported alert type: ${type}`);
    }

    const formattedIds = idArray.map(formatId);
    const idListStr = formattedIds.join(', ');

    const query = `
      UPDATE ${table}
      SET IsSeen = TRUE
      WHERE ${idColumn} IN (${idListStr})
      AND Email_User = '${userEmail}'
    `;

    try {
        const [job] = await bigquery.createQueryJob({ query });
        await job.getQueryResults();
        return [{ success: true, message: `IsSeen updated in BigQuery for type ${type}` }];
    } catch (err) {
        console.error(`BigQuery update error:`, err);
        throw new Error(`BigQuery error: ${err.message}`);
    }
}

router.post('/update-alerts', async (req, res) => {
    const { ids, email, type } = req.body;

    if (!ids || !email || !type) {
        return res.status(400).json({
            success: false,
            message: "Missing one or more required fields: ids, email, type"
        });
    }

    try {
        await updateAlertsIsSeen(ids, email, type);
        res.status(200).json({ success: true, message: "Alerts updated successfully." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

module.exports = router;

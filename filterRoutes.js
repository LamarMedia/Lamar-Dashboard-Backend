const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");
const router = express.Router();

const bigquery = new BigQuery();




router.post("/addSupplierFilter", async (req, res) => {
    try {
        const queryMaxId = `
            SELECT MAX(Supplier_Filter_ID) AS max_id 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
        `;
        const [maxIdRows] = await bigquery.query(queryMaxId);
        const lastId = maxIdRows[0]?.max_id || 0;
        const newId = lastId + 1;

        const {
            Supplier_Name,
            Filter_Name,
            Supplier_Plant,
            Supplier_Type,
            Supplier_Project_Name,
            Supplier_City,
            Supplier_State,
            Created_By,
            Employee_Number,
            Expiring_Soon,
            Supplier_SetupDate,
            Tax_Id
        } = req.body;

        if (!Filter_Name) {
            return res.status(400).json({ error: "Missing required field: Filter_Name" });
        }

        const isActive = true;
        const Created_Date = new Date().toISOString().replace("T", " ").split(".")[0];

        const insertQuery = `
            INSERT INTO \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
            (
                Supplier_Filter_ID,
                Supplier_Name,
                Filter_Name,
                Supplier_Plant,
                Supplier_Type,
                Supplier_Project_Name,
                Supplier_City,
                Supplier_State,
                isActive,
                Created_Date,
                Created_By,
                Employee_Number,
                Expiring_Soon,
                Supplier_SetupDate,
                Tax_Id
            )
            VALUES (
                @Supplier_Filter_ID,
                @Supplier_Name,
                @Filter_Name,
                @Supplier_Plant,
                @Supplier_Type,
                @Supplier_Project_Name,
                @Supplier_City,
                @Supplier_State,
                @isActive,
                DATETIME(TIMESTAMP(@Created_Date)),
                @Created_By,
                @Employee_Number,
                @Expiring_Soon,
                @Supplier_SetupDate,
                @Tax_Id
            )
        `;

        const params = {
            Supplier_Filter_ID: newId,
            Supplier_Name: Supplier_Name || null,
            Filter_Name,
            Supplier_Plant: Supplier_Plant || null,
            Supplier_Type: Supplier_Type || null,
            Supplier_Project_Name: Supplier_Project_Name || null,
            Supplier_City: Supplier_City || null,
            Supplier_State: Supplier_State || null,
            isActive,
            Created_Date,
            Created_By: Created_By || null,
            Employee_Number: Employee_Number || null,
            Expiring_Soon: Expiring_Soon !== undefined ? String(Expiring_Soon) : null, // supports false
            Supplier_SetupDate: Supplier_SetupDate || null,
            Tax_Id:Tax_Id|| null
        };

        const types = {
            Supplier_Filter_ID: "INT64",
            Supplier_Name: "STRING",
            Filter_Name: "STRING",
            Supplier_Plant: "STRING",
            Supplier_Type: "STRING",
            Supplier_Project_Name: "STRING",
            Supplier_City: "STRING",
            Supplier_State: "STRING",
            isActive: "BOOL",
            Created_Date: "STRING",
            Created_By: "STRING",
            Employee_Number: "STRING",
            Expiring_Soon: "STRING",
            Supplier_SetupDate: "STRING",
            Tax_Id:"STRING"
        };

        await bigquery.query({
            query: insertQuery,
            params,
            types
        });

        res.status(201).json({
            message: "Record inserted successfully",
            Supplier_Filter_ID: newId,
            ...params
        });
    } catch (error) {
        console.error("Error inserting record into BigQuery:", error);
        res.status(500).json({ error: "Failed to insert record" });
    }
});



router.get("/supplierFilters", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email) {
            return res.status(400).json({ error: "Email parameter is required" });
        }

        const query = `
            SELECT * FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
            WHERE Created_By = @email
        `;

        const options = {
            query,
            params: { email },
        };

        const [rows] = await bigquery.query(options);

        console.log(" Suppliers fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error(" Error fetching supplier filters:", error);
        res.status(500).json({ error: "Failed to fetch supplier filters" });
    }
});


router.put("/updateSupplierFilter/:id", async (req, res) => {
    try {
        const supplierId = parseInt(req.params.id, 10);

        const {
            Supplier_Name,
            Filter_Name,
            Supplier_Plant,
            Supplier_Type,
            Supplier_Project_Name,
            Supplier_City,
            Supplier_State,
            isActive,
            Created_By,
            Employee_Number,
            Expiring_Soon,
            Supplier_SetupDate
        } = req.body;

        if (
            !Supplier_Name && !Filter_Name && !Supplier_Plant && !Supplier_Type &&
            !Supplier_Project_Name && !Supplier_City && !Supplier_State &&
            isActive === undefined && !Created_By &&
            !Employee_Number && !Expiring_Soon && !Supplier_SetupDate
        ) {
            return res.status(400).json({ error: "At least one field must be provided for update" });
        }

        let updateFields = [];
        let params = [];

        if (Supplier_Name) {
            updateFields.push("Supplier_Name = ?");
            params.push(Supplier_Name);
        }
        if (Filter_Name) {
            updateFields.push("Filter_Name = ?");
            params.push(Filter_Name);
        }
        if (Supplier_Plant) {
            updateFields.push("Supplier_Plant = ?");
            params.push(Supplier_Plant);
        }
        if (Supplier_Type) {
            updateFields.push("Supplier_Type = ?");
            params.push(Supplier_Type);
        }
        if (Supplier_Project_Name) {
            updateFields.push("Supplier_Project_Name = ?");
            params.push(Supplier_Project_Name);
        }
        if (Supplier_City) {
            updateFields.push("Supplier_City = ?");
            params.push(Supplier_City);
        }
        if (Supplier_State) {
            updateFields.push("Supplier_State = ?");
            params.push(Supplier_State);
        }
        if (isActive !== undefined) {
            updateFields.push("isActive = ?");
            params.push(isActive);
        }
        if (Created_By) {
            updateFields.push("Created_By = ?");
            params.push(Created_By);
        }
        if (Employee_Number) {
            updateFields.push("Employee_Number = ?");
            params.push(Employee_Number);
        }
        if (Expiring_Soon !== undefined) {
            updateFields.push("Expiring_Soon = ?");
            params.push(Expiring_Soon);
        }
        if (Supplier_SetupDate) {
            updateFields.push("Supplier_SetupDate = ?");
            params.push(Supplier_SetupDate);
        }

        params.push(supplierId);

        const updateQuery = `
            UPDATE \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
            SET ${updateFields.join(", ")}
            WHERE Supplier_Filter_ID = ?
        `;

        await bigquery.query({ query: updateQuery, params });

        res.status(200).json({
            message: "Record updated successfully",
            Supplier_Filter_ID: supplierId
        });

    } catch (error) {
        console.error("Error updating record in BigQuery:", error);
        res.status(500).json({ error: "Failed to update record" });
    }
});



router.delete("/deleteSupplierFilter/:id", async (req, res) => {
    try {
        const supplierId = parseInt(req.params.id, 10);

        if (isNaN(supplierId)) {
            return res.status(400).json({ error: "Invalid Supplier_Filter_ID" });
        }

        // Check if the record exists before attempting deletion
        const checkQuery = `
            SELECT COUNT(*) AS count FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
            WHERE Supplier_Filter_ID = ?
        `;

        const [checkResult] = await bigquery.query({ query: checkQuery, params: [supplierId] });

        if (checkResult[0].count === 0) {
            return res.status(404).json({ error: "Supplier not found" });
        }

        // Delete record
        const deleteQuery = `
            DELETE FROM  \`dp01-dev-app-01.d_transformation_dp01_bq_01.Suppliers_filter\`
            WHERE Supplier_Filter_ID = ?
        `;

        await bigquery.query({ query: deleteQuery, params: [supplierId] });

        console.log(` Supplier_Filter_ID ${supplierId} deleted successfully`);
        res.status(200).json({ message: "Record deleted successfully", Supplier_Filter_ID: supplierId });

    } catch (error) {
        console.error(" Error deleting record in BigQuery:", error);
        res.status(500).json({ error: "Failed to delete record" });
    }
});

router.get("/supplierPopupList", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email) {
            return res.status(400).json({ error: "Email parameter is required" });
        }

        const query = `
        SELECT 
            DISTINCT Tax_Id, 
                Employee_Id as Employee_Number,
                CAST(Supplier_Setup_Date AS STRING) AS Supplier_Setup_Date,
                Plant AS Supplier_Plant,
                CASE WHEN (DATE_DIFF(supplier.Insurance_Expiration_Date,CURRENT_DATE(),day)) <0  or ((DATE_DIFF(supplier.Insurance_Expiration_Date,CURRENT_DATE(),day)) >=0 and (DATE_DIFF(supplier.Insurance_Expiration_Date,CURRENT_DATE(),day))  <=120)
                THEN 'Yes' ELSE 'No' END AS Contract_Insurance_Expiring_Soon,
                Project_Name AS Supplier_Project_Name,
                Supplier_City,
                Supplier_State,
                Supplier_Type_Cd AS Supplier_Type,
                Supplier_Name
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\` AS supplier
            WHERE email_user = @email
            ORDER BY Supplier_City ASC,
                Supplier_Name ASC,
                Supplier_Plant ASC,
                Supplier_Project_Name ASC,
                Supplier_State ASC,
                Supplier_Type ASC
        `;

        const options = {
            query,
            params: { email },
        };

        const [rows] = await bigquery.query(options);

        console.log("Supplier list fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching supplier list:", error);
        res.status(500).json({ error: "Failed to fetch supplier list" });
    }
});


//invoice filter list
router.get("/invoiceFilterList/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        const query = `
            SELECT DISTINCT
                Supplier_Type_Cd as Supplier_Type,
                Supplier_Name,
                Payment_Amount AS Amount,
                Invoice_Id AS Invoice_No,
                CAST(invoice_date AS STRING) AS Invoice_Date,  -- Convert to string
                CAST(payment_date AS STRING) AS Paid_Date,  -- Convert to string
                project_name AS Project,
                CAST(Invoice_Due_Date AS STRING) AS Invoice_Due_Date,
                PO_Number as Purchase_Order_Number,
                Job_Ticket_No,
                Space_Contract_NO as Space_Contract_No,
                Production_Contract_No,
                Fixed_Asset_ID,
                Panel as Panel_No,
                Invoice_Status
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\`
            WHERE Payment_Date >= '2024-01-01'
            AND email_user = @createdByEmail
            ORDER BY paid_date DESC
        `;

        const [rows] = await bigquery.query({
            query: query,
            params: { createdByEmail: createdByEmail }
        });

        console.log("Invoice filter list fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching invoice filter list:", error);
        res.status(500).json({ error: "Failed to fetch invoice filter list" });
    }
});


//add invoice
router.post("/addInvoice", async (req, res) => {
    try {
        const queryMaxId = `
            SELECT MAX(Invoice_Filter_Id) AS max_id 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Invoice_Filter\`
        `;

        const [maxIdRows] = await bigquery.query(queryMaxId);
        const lastId = maxIdRows[0]?.max_id || 0;
        const newId = lastId + 1;

        // Extract fields from request body
        let {
            Supplier_Name,
            Invoice_No,
            Invoice_Date,
            Paid_Date,
            Project,
            Invoice_Status,
            Created_By,
            InvoiceFilter_Name,
            Amount,
            Created_Date,
            Supplier_Type,
            Purchase_Order_No,
            Space_Contract_No,
            Job_Ticket_No,
            Production_Contract_No,
            Fixed_Asset_ID,
            Panel_No,
            Due_Date
        } = req.body;

        if (!InvoiceFilter_Name) {
            return res.status(400).json({ error: "Missing required field: InvoiceFilter_Name" });
        }

        Created_Date = Created_Date || new Date().toISOString().replace("T", " ").split(".")[0];

        const queryInsert = `
            INSERT INTO \`dp01-dev-app-01.d_transformation_dp01_bq_01.Invoice_Filter\`
            (
                Invoice_Filter_Id,
                Supplier_Name,
                Invoice_No,
                Invoice_Date,
                Paid_Date,
                Project,
                Invoice_Status,
                Created_By,
                InvoiceFilter_Name,
                Amount,
                Created_Date,
                Supplier_Type,
                Purchase_Order_No,
                Space_Contract_No,
                Job_Ticket_No,
                Production_Contract_No,
                Fixed_Asset_ID,
                Panel_No,
                Due_Date
            )
            VALUES (
                @Invoice_Filter_Id,
                @Supplier_Name,
                @Invoice_No,
                @Invoice_Date,
                @Paid_Date,
                @Project,
                @Invoice_Status,
                @Created_By,
                @InvoiceFilter_Name,
                @Amount,
                @Created_Date,
                @Supplier_Type,
                @Purchase_Order_No,
                @Space_Contract_No,
                @Job_Ticket_No,
                @Production_Contract_No,
                @Fixed_Asset_ID,
                @Panel_No,
                @Due_Date
            )
        `;

        const queryParams = {
            Invoice_Filter_Id: newId,
            Supplier_Name: Supplier_Name || null,
            Invoice_No: Invoice_No || null,
            Invoice_Date: Invoice_Date || null,
            Paid_Date: Paid_Date || null,
            Project: Project || null,
            Invoice_Status: Invoice_Status || null,
            Created_By: Created_By || null,
            InvoiceFilter_Name,
            Amount: Amount || null,
            Created_Date,
            Supplier_Type: Supplier_Type || null,
            Purchase_Order_No: Purchase_Order_No || null,
            Space_Contract_No: Space_Contract_No || null,
            Job_Ticket_No: Job_Ticket_No || null,
            Production_Contract_No: Production_Contract_No || null,
            Fixed_Asset_ID: Fixed_Asset_ID || null,
            Panel_No: Panel_No || null,
            Due_Date: Due_Date || null
        };

        const queryOptions = {
            query: queryInsert,
            params: queryParams,
            types: {
                Invoice_Filter_Id: "INTEGER",
                Supplier_Name: "STRING",
                Invoice_No: "STRING",
                Invoice_Date: "STRING",
                Paid_Date: "STRING",
                Project: "STRING",
                Invoice_Status: "STRING",
                Created_By: "STRING",
                InvoiceFilter_Name: "STRING",
                Amount: "STRING",
                Created_Date: "STRING",
                Supplier_Type: "STRING",
                Purchase_Order_No: "STRING",
                Space_Contract_No: "STRING",
                Job_Ticket_No: "STRING",
                Production_Contract_No: "STRING",
                Fixed_Asset_ID: "STRING",
                Panel_No: "STRING",
                Due_Date: "STRING"
            }
        };

        await bigquery.query(queryOptions);

        res.status(201).json({
            message: "Record inserted successfully",
            Invoice_Filter_Id: newId,
            Supplier_Name,
            Invoice_No,
            Invoice_Date,
            Paid_Date,
            Project,
            Invoice_Status,
            Created_By,
            InvoiceFilter_Name,
            Amount,
            Created_Date,
            Supplier_Type,
            Purchase_Order_No,
            Space_Contract_No,
            Job_Ticket_No,
            Production_Contract_No,
            Fixed_Asset_ID,
            Panel_No,
            Due_Date
        });
    } catch (error) {
        console.error("Error inserting record into BigQuery:", error);
        res.status(500).json({ error: "Failed to insert record" });
    }
});



router.get("/fetchInvoices/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        // Fetch records by Created_By email using parameterized query
        const queryFetch = `
            SELECT * 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Invoice_Filter\` 
            WHERE Created_By = @createdByEmail
        `;

        const queryParams = { createdByEmail };

        const [rows] = await bigquery.query({ query: queryFetch, params: queryParams });

        res.status(200).json({ invoices: rows });
    } catch (error) {
        console.error("Error fetching records from BigQuery:", error);
        res.status(500).json({ error: "Failed to fetch records" });
    }
});

router.delete("/deleteInvoice/:id", async (req, res) => {
    try {
        const invoiceId = req.params.id;

        // Delete record from BigQuery
        const queryDelete = `
            DELETE FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Invoice_Filter\`
            WHERE Invoice_Filter_Id = CAST(@Invoice_Filter_Id AS INT64)
        `;

        const queryParams = { Invoice_Filter_Id: invoiceId };

        await bigquery.query({ query: queryDelete, params: queryParams });

        console.log(`Record deleted with Invoice ID: ${invoiceId}`);

        res.status(200).json({
            message: "Record deleted successfully",
            Invoice_Filter_Id: invoiceId
        });
    } catch (error) {
        console.error("Error deleting record from BigQuery:", error);
        res.status(500).json({ error: "Failed to delete record" });
    }
});

router.get("/dashboardFilterList/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        // Ensure email is provided and not empty
        if (!createdByEmail || createdByEmail.trim() === "") {
            return res.status(400).json({ error: "Email parameter is required" });
        }

        const query = `
            SELECT DISTINCT
                FORMAT_DATE('%Y-%m-%d', Payment_Date) AS Transaction_Date,  -- Ensures YYYY-MM-DD format
                region_name AS Region,
                teritorry_name AS Territory,
                Plant,
                Supplier_Name AS Supplier
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\`
            WHERE email_user = @email
            ORDER BY Supplier ASC,
                    Transaction_Date ASC,
                    Region ASC,
                    Territory ASC
        `;

        const queryOptions = {
            query,
            params: { email: createdByEmail }, // Correct parameter binding    
        };

        const [rows] = await bigquery.query(queryOptions);

        console.log("Dashboard filter list fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching dashboard filter list:", error);
        res.status(500).json({ error: "Failed to fetch dashboard filter list" });
    }
});



router.get("/expenseFilterList/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        // Ensure email is provided and not empty
        if (!createdByEmail || createdByEmail.trim() === "") {
            return res.status(400).json({ error: "Email parameter is required" });
        }

        // SQL query with parameterized format
        const query = `
            SELECT DISTINCT
                Invoice_Id, 
                Plant,
                CAST(Payment_Date AS STRING) AS Payment_Date, -- Convert to plain string
                Supplier_Name,
                Supplier_Type_Cd AS Supplier_Type,
                project_name AS Project,
                Expense_Account_Name as Account_Name,
                Expense_Sub_Account_Code as Sub_Account_Code
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\`
            WHERE email_user = @email
            ORDER BY Plant ASC,
                    Supplier_Name ASC,
                    Supplier_Type ASC,
                    Project ASC
        `;

        // Define query parameters
        const queryOptions = {
            query,
            params: { email: createdByEmail }
        };

        // Execute the query
        const [rows] = await bigquery.query(queryOptions);

        console.log("Expense filter list fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching expense filter list:", error);
        res.status(500).json({ error: "Failed to fetch expense filter list" });
    }
});




// Add Expense Filter
router.post("/addExpenseFilter", async (req, res) => {
    try {
        // Get the latest ExpenseFilter_Id
        const queryMaxId = `
            SELECT MAX(ExpenseFilter_Id) AS max_id 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Expense_Filter\`
        `;
        const [maxIdRows] = await bigquery.query(queryMaxId);
        const lastId = maxIdRows[0]?.max_id || 0;
        const newId = lastId + 1;

        // Extract fields from request body
        const {
            Invoice_No,
            Plant,
            Payment_Date,
            Supplier_Name,
            Supplier_Type,
            Project,
            Created_By,
            ExpenseFilter_Name,
            Account_Name,
            Sub_Account_Name,
        } = req.body;

        if (!ExpenseFilter_Name) {
            return res.status(400).json({ error: "Missing required field: ExpenseFilter_Name" });
        }

        
        // Format Created_Date as DATETIME string
        const Created_Date = new Date().toISOString().replace("T", " ").split(".")[0];

        // Insert query
        const queryInsert = `
            INSERT INTO \`dp01-dev-app-01.d_transformation_dp01_bq_01.Expense_Filter\` 
            (
                ExpenseFilter_Id,
                Invoice_No,
                Plant,
                Payment_Date,
                Supplier_Name,
                Supplier_Type,
                Project,
                Created_By,
                ExpenseFilter_Name,
                Created_Date,
                Account_Name,
                Sub_Account_Name
            )
            VALUES (
                @ExpenseFilter_Id,
                @Invoice_No,
                @Plant,
                @Payment_Date,
                @Supplier_Name,
                @Supplier_Type,
                @Project,
                @Created_By,
                @ExpenseFilter_Name,
                @Created_Date,
                @Account_Name,
                @Sub_Account_Name
            )
        `;

        const queryParams = {
            ExpenseFilter_Id: newId,
            Invoice_No: Invoice_No || null,
            Plant: Plant || null,
            Payment_Date: Payment_Date || null,
            Supplier_Name: Supplier_Name || null,
            Supplier_Type: Supplier_Type || null,
            Project: Project || null,
            Created_By: Created_By || null,
            ExpenseFilter_Name,
            Created_Date,
            Account_Name: Account_Name || null,
            Sub_Account_Name: Sub_Account_Name || null
        };

        const queryOptions = {
            query: queryInsert,
            params: queryParams,
            types: {
                ExpenseFilter_Id: "INT64",
                Invoice_No: "STRING",
                Plant: "STRING",
                Payment_Date: "STRING",
                Supplier_Name: "STRING",
                Supplier_Type: "STRING",
                Project: "STRING",
                Created_By: "STRING",
                ExpenseFilter_Name: "STRING",
                Created_Date: "STRING",
                Account_Name: "STRING",
                Sub_Account_Name: "STRING",
            }
        };

        await bigquery.query(queryOptions);

        res.status(201).json({
            message: "Expense Filter inserted successfully",
            ExpenseFilter_Id: newId,
            Invoice_No,
            Plant,
            Payment_Date,
            Supplier_Name,
            Supplier_Type,
            Project,
            Created_By,
            ExpenseFilter_Name,
            Created_Date,
            Account_Name,
            Sub_Account_Name,
        });
    } catch (error) {
        console.error("Error inserting expense filter into BigQuery:", error);
        res.status(500).json({ error: "Failed to insert expense filter" });
    }
});


// Delete Expense Filter by ID
router.delete("/deleteExpenseFilter/:ExpenseFilter_Id", async (req, res) => {
    try {
        const { ExpenseFilter_Id } = req.params;

        if (!ExpenseFilter_Id) {
            return res.status(400).json({ error: "Missing required parameter: ExpenseFilter_Id" });
        }

        // Check if the record exists before deleting
        const queryCheck = `
            SELECT COUNT(*) AS count 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Expense_Filter\` 
            WHERE ExpenseFilter_Id = @ExpenseFilter_Id
        `;

        const checkOptions = {
            query: queryCheck,
            params: { ExpenseFilter_Id: parseInt(ExpenseFilter_Id) },
        };

        const [checkRows] = await bigquery.query(checkOptions);
        const recordExists = checkRows[0]?.count > 0;

        if (!recordExists) {
            return res.status(200).json([]);
        }

        // Delete query
        const queryDelete = `
            DELETE FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Expense_Filter\` 
            WHERE ExpenseFilter_Id = @ExpenseFilter_Id
        `;

        const deleteOptions = {
            query: queryDelete,
            params: { ExpenseFilter_Id: parseInt(ExpenseFilter_Id) },
        };

        await bigquery.query(deleteOptions);

        res.status(200).json({ message: "Expense Filter deleted successfully" });
    } catch (error) {
        console.error("Error deleting expense filter:", error);
        res.status(500).json({ error: "Failed to delete expense filter" });
    }
});

// Fetch Expense Filters by Mail ID (Created_By)
router.get("/fetchExpenseFilter", async (req, res) => {
    try {
        const { mailId } = req.query; // Get email ID from query params

        if (!mailId) {
            return res.status(400).json({ error: "Missing required parameter: mailId" });
        }

        const queryFetch = `
            SELECT * 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Expense_Filter\`
            WHERE Created_By = @mailId
        `;

        const options = {
            query: queryFetch,
            params: { mailId },
        };

        const [rows] = await bigquery.query(options);

        if (rows.length === 0) {
            return res.status(200).json([]);
        }

        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching expense filters by mailId:", error);
        res.status(500).json({ error: "Failed to fetch expense filters" });
    }
});

router.get("/projectPageFilterList/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        const query = `
            SELECT DISTINCT
                Template,
                Structure,
                Panel as  Panel_No,
                Assigned_To,
                cast(Days_Assigned as integer) as Days_Assigned,
                Project_Name,
                Plant,
                Project_Status,
                region_name AS Region,
                AFM
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.supplier_flattened_v\`
            WHERE Payment_Date >= DATE('2024-01-01')
            AND email_user = @createdByEmail
        `;

        const [rows] = await bigquery.query({
            query: query,
            params: { createdByEmail: createdByEmail }
        });

        console.log("Project page filter list fetched:", rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching project page filter list:", error);
        res.status(500).json({ error: "Failed to fetch project page filter list" });
    }
});

router.post("/addProjectFilter", async (req, res) => {
    try {
        // Get the max ProjectFilter_Id
        const queryMaxId = `
            SELECT MAX(ProjectFilter_Id) AS max_id 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Project_Filter\`
        `;

        const [maxIdRows] = await bigquery.query(queryMaxId);
        const lastId = maxIdRows[0]?.max_id || 0;
        const newId = lastId + 1;

        // Extract fields from request body
        let {
            ProjectFilter_Name,
            Project_Name,
            Plant,
            Project_Status,
            Region,
            AFM,
            Created_By,
            Created_Date,
            Template,
            Structure,
            Panel_No,
            Assigned_To,
            Days_Assigned
        } = req.body;

        if (!ProjectFilter_Name) {
            return res.status(400).json({ error: "Missing required field: ProjectFilter_Name" });
        }

        Created_Date = Created_Date
            ? new Date(Created_Date).toISOString().replace("T", " ").split(".")[0]
            : new Date().toISOString().replace("T", " ").split(".")[0];

        const queryInsert = `
            INSERT INTO \`dp01-dev-app-01.d_transformation_dp01_bq_01.Project_Filter\`
            (
                ProjectFilter_Id,
                ProjectFilter_Name,
                Project_Name,
                Plant,
                Project_Status,
                Region,
                AFM,
                Created_By,
                Created_Date,
                Template,
                Structure,
                Panel_No,
                Assigned_To,
                Days_Assigned
            )
            VALUES (
                @ProjectFilter_Id,
                @ProjectFilter_Name,
                @Project_Name,
                @Plant,
                @Project_Status,
                @Region,
                @AFM,
                @Created_By,
                @Created_Date,
                @Template,
                @Structure,
                @Panel_No,
                @Assigned_To,
                @Days_Assigned
            )
        `;

        const queryParams = {
            ProjectFilter_Id: newId,
            ProjectFilter_Name: ProjectFilter_Name || null,
            Project_Name: Project_Name || null,
            Plant: Plant || null,
            Project_Status: Project_Status || null,
            Region: Region || null,
            AFM: AFM || null,
            Created_By: Created_By || null,
            Created_Date,
            Template: Template || null,
            Structure: Structure || null,
            Panel_No: Panel_No || null,
            Assigned_To: Assigned_To || null,
            Days_Assigned: Days_Assigned !== undefined ? parseInt(Days_Assigned, 10) : null
        };

        const queryOptions = {
            query: queryInsert,
            params: queryParams,
            types: {
                ProjectFilter_Id: "INT64",
                ProjectFilter_Name: "STRING",
                Project_Name: "STRING",
                Plant: "STRING",
                Project_Status: "STRING",
                Region: "STRING",
                AFM: "STRING",
                Created_By: "STRING",
                Created_Date: "STRING",
                Template: "STRING",
                Structure: "STRING",
                Panel_No: "STRING",
                Assigned_To: "STRING",
                Days_Assigned: "INT64"
            }
        };

        await bigquery.query(queryOptions);

        res.status(201).json({
            message: "Record inserted successfully",
            ProjectFilter_Id: newId,
            ProjectFilter_Name,
            Project_Name,
            Plant,
            Project_Status,
            Region,
            AFM,
            Created_By,
            Created_Date,
            Template,
            Structure,
            Panel_No,
            Assigned_To,
            Days_Assigned
        });
    } catch (error) {
        console.error("Error inserting record into BigQuery:", error);
        res.status(500).json({ error: "Failed to insert record" });
    }
});



router.get("/projectFilter/:email", async (req, res) => {
    try {
        const createdBy = req.params.email;

        const query = `
            SELECT * FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Project_Filter\`
            WHERE Created_By = @createdBy
        `;

        const queryParams = { createdBy };

        const [rows] = await bigquery.query({ query, params: queryParams });

        console.log("Project filter list fetched for Created_By:", createdBy, rows);
        res.status(200).json(rows);
    } catch (error) {
        console.error("Error fetching project filter list:", error);
        res.status(500).json({ error: "Failed to fetch project filter list" });
    }
});

router.delete("/deleteProjectFilter/:id", async (req, res) => {
    try {
        const filterId = req.params.id;

        // Delete record from BigQuery
        const queryDelete = `
            DELETE FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Project_Filter\`
            WHERE ProjectFilter_Id = @ProjectFilter_Id
        `;

        const queryParams = { ProjectFilter_Id: parseInt(filterId, 10) };

        await bigquery.query({ query: queryDelete, params: queryParams });

        console.log(`Record deleted with ProjectFilter_Id: ${filterId}`);

        res.status(200).json({
            message: "Record deleted successfully",
            ProjectFilter_Id: filterId
        });
    } catch (error) {
        console.error("Error deleting record from BigQuery:", error);
        res.status(500).json({ error: "Failed to delete record" });
    }
});

/**
 * Safely parse a date input into a valid JavaScript Date object
 */
const formatToBigQueryDatetimeString = (input) => {
    const date = new Date(input);
    if (!(date instanceof Date) || isNaN(date.getTime())) {
        throw new Error(`Invalid date input: ${input}`);
    }
    return date.toISOString().replace("T", " ").replace("Z", "").split(".")[0];
};

router.post("/addDashboardFilter", async (req, res) => {
    try {
        // Get next ID
        const queryMaxId = `
            SELECT MAX(DashboardFilter_Id) AS max_id 
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Dashboard_Filter\`
        `;
        const [maxIdRows] = await bigquery.query(queryMaxId);
        const lastId = maxIdRows[0]?.max_id || 0;
        const newId = lastId + 1;

        // Extract from request
        const {
            Transaction_Date = null,
            Region = null,
            Territory = null,
            Plant = null,
            Supplier = null,
            Created_By = "system",
            DashboardFilter_Name
        } = req.body;

        if (!DashboardFilter_Name) {
            return res.status(400).json({ error: "Missing DashboardFilter_Name" });
        }

        // Format dates properly
        const formatted_Transaction_Date =
            Transaction_Date && Transaction_Date.trim() !== ""
                ? formatToBigQueryDatetimeString(Transaction_Date)
                : null;

        const formatted_Created_Date = formatToBigQueryDatetimeString(new Date());

        console.log("📅 Raw Transaction_Date:", Transaction_Date);
        console.log("✅ Formatted Transaction_Date:", formatted_Transaction_Date);
        console.log("✅ Formatted Created_Date:", formatted_Created_Date);

        // Insert query with explicit DATETIME casting
        const queryInsert = `
            INSERT INTO \`dp01-dev-app-01.d_transformation_dp01_bq_01.Dashboard_Filter\`
            (
                DashboardFilter_Id,
                Transaction_Date,
                Region,
                Territory,
                Plant,
                Supplier,
                Created_By,
                DashboardFilter_Name,
                Created_Date
            )
            VALUES (
                @DashboardFilter_Id,
                DATETIME(@Transaction_Date),
                @Region,
                @Territory,
                @Plant,
                @Supplier,
                @Created_By,
                @DashboardFilter_Name,
                DATETIME(@Created_Date)
            )
        `;

        const queryParams = {
            DashboardFilter_Id: newId,
            Transaction_Date: formatted_Transaction_Date,
            Region,
            Territory,
            Plant,
            Supplier,
            Created_By,
            DashboardFilter_Name,
            Created_Date: formatted_Created_Date
        };

        console.log(" Query Params Sent:", queryParams);

        const queryOptions = {
            query: queryInsert,
            params: queryParams,
            types: {
                DashboardFilter_Id: "INT64",
                Transaction_Date: "STRING",   // must be STRING here since we're formatting manually
                Created_Date: "STRING",       // same here
                Region: "STRING",
                Territory: "STRING",
                Plant: "STRING",
                Supplier: "STRING",
                Created_By: "STRING",
                DashboardFilter_Name: "STRING"
            }
        };

        await bigquery.query(queryOptions);

        console.log(`✅ Inserted Dashboard Filter: ${DashboardFilter_Name}`);

        res.status(201).json({
            message: "Dashboard Filter inserted successfully",
            DashboardFilter_Id: newId,
            Transaction_Date: formatted_Transaction_Date,
            Region,
            Territory,
            Plant,
            Supplier,
            Created_By,
            DashboardFilter_Name,
            Created_Date: formatted_Created_Date
        });
    } catch (error) {
        console.error("❌ Error inserting dashboard filter:", error);
        res.status(500).json({ error: "Failed to insert dashboard filter" });
    }
});


router.get("/fetchDashboardFilters/:email", async (req, res) => {
    try {
        const createdByEmail = req.params.email;

        // Ensure email is provided and not empty
        if (!createdByEmail || createdByEmail.trim() === "") {
            return res.status(400).json({ error: "Email parameter is required" });
        }

        // Fetch records by Created_By email, cast dates as string
        const queryFetch = `
            SELECT 
                DashboardFilter_Id,
                CAST(Transaction_Date AS STRING) AS Transaction_Date,
                Region,
                Territory,
                Plant,
                Supplier,
                Created_By,
                DashboardFilter_Name,
                CAST(Created_Date AS STRING) AS Created_Date
            FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Dashboard_Filter\`
            WHERE Created_By = @email
        `;

        const queryOptions = {
            query: queryFetch,
            params: { email: createdByEmail },
            types: { email: "STRING" }
        };

        const [rows] = await bigquery.query(queryOptions);

        res.status(200).json({ dashboardFilters: rows });
    } catch (error) {
        console.error("Error fetching dashboard filters from BigQuery:", error);
        res.status(500).json({ error: "Failed to fetch dashboard filters" });
    }
});


router.delete("/deleteDashboardFilter/:id", async (req, res) => {
    try {
        const filterId = parseInt(req.params.id, 10); // Convert to integer

        // Ensure ID is valid
        if (isNaN(filterId)) {
            return res.status(400).json({ error: "Invalid or missing DashboardFilter_Id" });
        }

        // BigQuery DELETE Query
        const queryDelete = `
            DELETE FROM \`dp01-dev-app-01.d_transformation_dp01_bq_01.Dashboard_Filter\`
            WHERE DashboardFilter_Id = @id
        `;

        const queryOptions = {
            query: queryDelete,
            params: { id: filterId },  // Named parameter binding
            types: { id: "INT64" }     // Explicitly define type
        };

        const [job] = await bigquery.query(queryOptions);

        console.log(`Deleted DashboardFilter with ID: ${filterId}`);

        res.status(200).json({
            message: `Dashboard Filter with ID ${filterId} deleted successfully`,
        });
    } catch (error) {
        console.error("Error deleting dashboard filter from BigQuery:", error);
        res.status(500).json({ error: "Failed to delete dashboard filter" });
    }
});
module.exports = router;

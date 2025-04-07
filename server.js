require("dotenv").config();
const cron = require("node-cron");
const alertRoutes = require("./alertRoutes");
const filterRoutes = require("./filterRoutes");
const commentRoutes = require("./commentRoutes");
const pushNotificationRoutes = require("./pushNotificationRoutes");
const express = require("express");
const { google } = require("googleapis");
const admin = require("firebase-admin");
const crypto = require("crypto");
const cors = require("cors");
const { BigQuery } = require("@google-cloud/bigquery");
const jwt = require("jsonwebtoken");
const axios = require("axios");
const bigquery = new BigQuery();
const app = express();
app.use(express.json());
app.use(cors());

// const { Looker40SDK } = require("@looker/sdk");
// Initialize Firebase Admin SDK

// Load Looker API credentials from .env or config

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert("service-account.json"),
        //credential: admin.credential.cert("service-account_new.json"),
    });
}
const db = admin.firestore();
console.log("Firestore initialized.");
const datasetId = "d_transformation_dp01_bq_01";
const tableId = "Suppliers_filter";
const tableFullPath = `dp01-dev-app-01.${datasetId}.${tableId}`;
const SupplierTableId = "supplier_flattened_rls";
const SupplierTableFullPath = `dp01-dev-app-01.${datasetId}.${SupplierTableId}`;
const LOOKER_API_URL = process.env.REACT_APP_LOOKER_API_URL;

// Google Drive API Authentication details
const auth = new google.auth.GoogleAuth({
    //keyFile: "service-account_new.json",
    keyFile: "service-account.json",
    scopes: ["https://www.googleapis.com/auth/drive"],
});

async function authenticateDrive() {
    return google.drive({ version: "v3", auth });
}

const FOLDER_ID = "19UhGb-c6vbAE3aYAm5TYWNGA6yps311a"; // Shared folder ID

// Fetch all files from the shared folder
async function fetchSharedFolderFiles() {
    try {
        console.log("Fetching files from shared folder...");
        const drive = await authenticateDrive();

        const response = await drive.files.list({
            q: `'${FOLDER_ID}' in parents and trashed=false`,
            fields: "*", // Log everything for debugging
            supportsAllDrives: true,
            includeItemsFromAllDrives: true,
        });

        console.log(" API Response:", JSON.stringify(response.data, null, 2));
        console.log(" Files in shared folder:", response.data.files);
        return response.data.files || [];
    } catch (error) {
        console.error(" Error fetching files from shared folder:", error);
        return [];
    }
}

// Fetch comments and nested replies for a given file ID
async function fetchComments(fileId) {
    try {
        console.log(`Fetching comments for file: ${fileId}`);

        // Authenticate and get the Drive API instance
        const drive = await authenticateDrive();

        // Fetch comments from Google Drive API
        const response = await drive.comments.list({
            fileId: fileId,
            fields: "comments(id,content,author/displayName,author/emailAddress,createdTime,replies(id,content,author/emailAddress,author/displayName,createdTime))",
            supportsAllDrives: true, // Required for shared files
        });

        // Return comments or an empty array if none exist
        return response.data?.comments || [];
    } catch (error) {
        console.error("❌ Error fetching comments:", error.message || error);

        // Handle specific API errors
        if (error.response) {
            console.error(`🚨 API Response Error: ${error.response.status} - ${error.response.statusText}`);
        }

        return [];
    }
}

// Extract @mentions from comment content
function extractMentions(commentText) {
    const mentionPattern = /@([a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]+)/g;
    const matches = commentText.match(mentionPattern);
    return matches ? matches.map(name => name.replace("@", "")) : [];
}

// Store comment and nested replies in Firestore
async function storeCommentInFirestore(comment, fileId, fileName) {
    const docRef = db.collection("drive_comments").doc(comment.id);
    try {
        const docSnapshot = await docRef.get();
        let isCommentNew = !docSnapshot.exists;
        let shouldUpdate = false;
        let updatedReplies = [];

        if (isCommentNew) {
            // If it's a new comment, mark it as unseen
            await docRef.set({
                content: comment.content,
                author: comment.author.displayName,
                authorEmail: comment.author.emailAddress || null,  // Handle undefined email
                created_at: new Date(comment.createdTime),
                mentioned_users: extractMentions(comment.content),
                isSeen: false,  // New comment should be marked as unseen
                file_id: fileId,
                file_name: fileName,
                replies: comment.replies
                    ? comment.replies.map(reply => ({
                        id: reply.id,
                        mentioned_users: extractMentions(reply.content),
                        content: reply.content,
                        author: reply.author.displayName,
                        authorEmail: reply.author.emailAddress || null,  // Handle undefined email
                        created_at: new Date(reply.createdTime)
                    }))
                    : []
            });
            console.log(" New comment stored in Firestore:", comment.content);
        } else {
            let existingData = docSnapshot.data();
            let existingReplies = existingData.replies || [];

            // Check if comment content has changed
            if (existingData.content !== comment.content) {
                shouldUpdate = true;
            }

            // Check replies for updates
            let newReplies = comment.replies
                ? comment.replies.map(reply => ({
                    id: reply.id,
                    content: reply.content,
                    mentioned_users: extractMentions(reply.content),
                    author: reply.author.displayName,
                    authorEmail: reply.author.emailAddress || null,  // Handle undefined email
                    created_at: new Date(reply.createdTime)
                }))
                : [];

            existingReplies.forEach(existingReply => {
                let matchingReply = newReplies.find(newReply => newReply.id === existingReply.id);
                if (!matchingReply) {
                    // A reply was removed, update needed
                    shouldUpdate = true;
                } else if (matchingReply.content !== existingReply.content) {
                    // A reply content was modified
                    shouldUpdate = true;
                }
            });

            newReplies.forEach(newReply => {
                let matchingReply = existingReplies.find(existingReply => existingReply.id === newReply.id);
                if (!matchingReply) {
                    // A new reply was added, update needed
                    shouldUpdate = true;
                }
            });

            if (shouldUpdate) {
                await docRef.update({
                    content: comment.content,
                    replies: newReplies,
                    isSeen: false  // Only update isSeen if there's a change
                });
                console.log("Comment updated in Firestore:", comment.content);
            } else {
                console.log("No changes detected, Firestore update skipped.");
            }
        }
    } catch (error) {
        console.error(" Error storing/updating comment in Firestore:", error);
    }
}

async function isFileAlreadyRegistered(fileId) {
    const doc = await db.collection("registered_files").doc(fileId).get();
    return doc.exists;
}

async function markFileAsRegistered(fileId, fileName) {
    await db.collection("registered_files").doc(fileId).set({
        name: fileName,
        registeredAt: new Date(),
    });
}

// Webhook listener
app.post("/webhook", async (req, res) => {
    console.log("Drive Change Notification Received:", req.body);
    try {
        const files = await fetchSharedFolderFiles();
        for (const file of files) {
            const comments = await fetchComments(file.id);
            for (const comment of comments) {
                await storeCommentInFirestore(comment, file.id, file.name);
            }
        }
    } catch (error) {
        console.error("Error processing webhook:", error);
    }
    res.sendStatus(200);
});

// Watch Drive Changes - Subscribe to Webhooks
async function watchDriveChanges() {
    try {
        console.log("🔎 Checking for new files in shared folder...");
        const drive = await authenticateDrive();
        const files = await fetchSharedFolderFiles();

        for (const file of files) {
            const alreadyRegistered = await isFileAlreadyRegistered(file.id);

            if (!alreadyRegistered) {
                try {
                    const response = await drive.files.watch({
                        fileId: file.id,
                        requestBody: {
                            id: crypto.randomUUID(),
                            type: "web_hook",
                            address: process.env.WEBHOOK_URL,
                        },
                        supportsAllDrives: true,
                    });

                    console.log(`Webhook registered for new file: ${file.name} (${file.id})`);

                    // Store the file ID to prevent re-watching
                    await markFileAsRegistered(file.id, file.name);

                    // Fetch and store its comments
                    const comments = await fetchComments(file.id);
                    for (const comment of comments) {
                        await storeCommentInFirestore(comment, file.id, file.name);
                    }
                } catch (error) {
                    console.error(`❌ Error watching new file ${file.name} (${file.id}):`, error);
                }
            } else {
                console.log(`📁 File already registered: ${file.name}`);
            }
        }

        return { success: true, message: "New file watch registration complete." };
    } catch (error) {
        console.error("❌ Failed during watchDriveChanges:", error);
        return { success: false, error: error.message };
    }
}

// API endpoint to call watchDriveChanges
app.get("/CallWatchDriveChange", async (req, res) => {
    try {
        const result = await watchDriveChanges();
        res.json(result); // Send response

    } catch (error) {
        console.error("Error in CallWatchDriveChange API:", error);
        res.status(500).json({ error: "Failed to start drive watching" });
    }
});

// Start watching changes (Webhooks)
watchDriveChanges();

let isPolling = false;

cron.schedule("*/5 * * * *", async () => {
    if (isPolling) {
        console.log("Polling already in progress, skipping this run.");
        return;
    }

    isPolling = true;
    console.log("Running scheduled poll for new comments...");

    try {
        const files = await fetchSharedFolderFiles();
        for (const file of files) {
            const comments = await fetchComments(file.id);
            for (const comment of comments) {
                await storeCommentInFirestore(comment, file.id, file.name);
            }
        }
        console.log("Polling completed.");
    } catch (err) {
        console.error("Error during polling:", err);
    }

    isPolling = false;
});


app.use("/", alertRoutes);
app.use("/", filterRoutes);
app.use("/", commentRoutes);
app.use("/", pushNotificationRoutes);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

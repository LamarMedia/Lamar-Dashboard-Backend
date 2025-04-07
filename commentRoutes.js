const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");
const router = express.Router();
const admin = require("firebase-admin");
const bigquery = new BigQuery();


if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert("service-account.json"),
        //credential: admin.credential.cert("service-account_new.json"),
    });
}

const db = admin.firestore();

// Watch Drive Changes - Subscribe to Webhooks
async function watchDriveChanges() {
    try {
        console.log("🔎 Subscribing to Google Drive Webhooks for shared folder files...");
        const drive = await authenticateDrive();
        const files = await fetchSharedFolderFiles();

        for (const file of files) {
            try {
                const response = await drive.files.watch({
                    fileId: file.id,
                    requestBody: {
                        id: crypto.randomUUID(),
                        type: "web_hook",
                        address: process.env.WEBHOOK_URL,
                    },
                    supportsAllDrives: true, //  Required for Shared Drives
                });
                console.log(` Webhook registered for file: ${file.name} (ID: ${file.id})`);

            } catch (error) {
                if (error.response && error.response.status === 404) {
                    console.warn(`⚠️ Skipping file (not found): ${file.id}`);
                } else {
                    console.error(` Error setting up webhook for file ${file.id}:`, error);
                }
            }
        }
        return { success: true, message: "Drive watch setup completed" };
    } catch (error) {
        console.error(" Failed to Set Up Drive Watch for Shared Folder Files:", error);
    }
}



// API to Retrieve Stored Comments
router.get("/comments", async (req, res) => {
    try {
        const { email } = req.query;

        // Check if email is provided and is not empty
        if (!email || email.trim() === "") {
            return res.json([]); // Return an empty array if email is missing
        }

        // Extract first and last name from email
        const [firstName, lastName] = email.split("@")[0].split(".");
        const fullName = lastName ? `${firstName} ${lastName}` : firstName; // Handle single-word names

        // Fetch all comments from Firestore
        const snapshot = await db.collection("drive_comments").orderBy("created_at", "desc").get();
        if (snapshot.empty) {
            console.log("No comments found in Firestore.");
            return res.json([]);
        }

        // Filter comments where the user is mentioned OR is the author
        const filteredComments = snapshot.docs
            .map(doc => ({ id: doc.id, ...doc.data() }))
            .filter(comment =>
                (comment.mentioned_users && comment.mentioned_users.includes(email)) ||
                (comment.author && comment.author.toLowerCase() === fullName.toLowerCase())
            );

        res.json(filteredComments);
    } catch (error) {
        console.error("Error fetching filtered comments:", error);
        res.status(500).json({ error: "Failed to retrieve filtered comments" });
    }
});

// API to get unique names with mail IDs excluding the input email
router.get("/TeamsByComments", async (req, res) => {
    try {
        const { email } = req.query;

        if (!email || email.trim() === "") {
            return res.json([]);
        }

        const snapshot = await db.collection("drive_comments").orderBy("created_at", "desc").get();
        if (snapshot.empty) {
            console.log("No comments found in Firestore.");
            return res.json([]);
        }

        const filteredUsers = new Map();

        snapshot.docs.forEach(doc => {
            const comment = doc.data();

            if (comment.mentioned_users && comment.mentioned_users.includes(email)) {
                if (comment.author) {
                    const nameParts = comment.author.split(" ");
                    const fullName = comment.author;
                    let authorEmail = "";

                    if (nameParts.length > 1) {
                        authorEmail = `${nameParts[0].toLowerCase()}.${nameParts.slice(1).join(".").toLowerCase()}@lamar.com`;
                    } else {
                        authorEmail = `${nameParts[0].toLowerCase()}@lamar.com`;
                    }

                    filteredUsers.set(comment.author, { fullName, mail: authorEmail });
                }
            }
        });

        const distinctUsers = Array.from(filteredUsers.values()).filter(user => user.fullName && user.mail);

        console.log(distinctUsers);
        res.json(distinctUsers);

    } catch (error) {
        console.error("Error fetching filtered comments:", error);
        res.status(500).json({ error: "Failed to retrieve filtered comments" });
    }
});


// API to Update isSeen = true for a specific comment
router.patch("/comments/:id/seen", async (req, res) => {
    const { id } = req.params; // Get the comment ID from the request URL

    try {
        const docRef = db.collection("drive_comments").doc(id);

        // Run Firestore transaction to ensure atomic update
        await db.runTransaction(async (transaction) => {
            const docSnapshot = await transaction.get(docRef);

            if (!docSnapshot.exists) {
                return res.status(404).json({ error: "Comment not found" });
            }

            const commentData = docSnapshot.data();

            // Check if already seen to avoid unnecessary writes
            if (commentData.isSeen) {
                return res.json({ success: true, message: "Comment is already marked as seen", comment: commentData });
            }

            // Update isSeen to true
            transaction.update(docRef, { isSeen: true });

            console.log(` Comment ${id} marked as seen`);
            return res.json({ success: true, message: `Comment ${id} marked as seen`, comment: { ...commentData, isSeen: true } });
        });

    } catch (error) {
        console.error(" Error updating isSeen:", error);
        res.status(500).json({ error: "Failed to update isSeen" });
    }
});


module.exports = router;

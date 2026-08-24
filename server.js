const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Client } = require('pg');
const axios = require('axios');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db'); // 👈 KEEP YOUR EXISTING DB POOL INTACT!

const app = express();
const PORT = process.env.PORT || 5000;

const multer = require("multer");
const path = require("path");
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;

// 🟢 DECLARE IN-MEMORY DRIVER STATE MAP AT TOP LEVEL
const activeDrivers = new Map();

// ⚡ Global Parsers & Middlewares
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🔌 Wrap Express app in HTTP server for WebSockets
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });
//
app.use((req, res, next) => {
  req.io = io;
  next();
});

// 🔔 DEDICATED CLIENT FOR POSTGRES LISTEN / NOTIFY 
const pgListener = new Client({
  connectionString: process.env.DATABASE_URL 
});

pgListener.connect()
  .then(async () => {
    console.log("🔌 Dedicated Postgres client connected for LISTEN/NOTIFY");
    await pgListener.query("LISTEN route_pool_changed");
    await pgListener.query("LISTEN queue_changes");
  })
  .catch(err => console.error("❌ Postgres listener connection failed:", err));

pgListener.on("notification", async (msg) => {
  try {
    const payload = JSON.parse(msg.payload);
    console.log(`🔔 Postgres Notification Caught on [${msg.channel}]:`, payload);
    
    io.emit("route_pool_updated", payload);
    
    if (payload.hub_id) {
      const hubResult = await pool.query(`SELECT name FROM hubs WHERE id = $1`, [payload.hub_id]);
      if (hubResult.rows.length > 0) {
        const hubName = hubResult.rows[0].name;
        // await broadcastHubQueueUpdate(io, pool, hubName);
      }
    }
  } catch (err) {
    console.error("❌ Error processing notification payload:", err);
  }
});

// ------------------------------------------------------------------
// UNIFIED SOCKET.IO ENGINE
// ------------------------------------------------------------------

io.on("connection", (socket) => {
  console.log("⚡ Client connected via Socket.io:", socket.id);

  // 1. Hub Room Joining
  socket.on("join_hub", ({ hub_name }) => {
    if (!hub_name) return;
    const roomName = `hub_${hub_name.toLowerCase().trim()}`;
    socket.join(roomName);
    console.log(`🔌 Socket ${socket.id} joined room: ${roomName}`);
  });

  // 2. Driver Channel Registration
  socket.on("register_driver_channel", ({ driverId, driver_id }) => {
    const id = String(driverId || driver_id);
    if (!id) return;

    const roomName = `driver_${id}`;
    socket.join(roomName);
    socket.driverId = id;

    // Guaranteed fallback initialization in activeDrivers Map
    const existing = activeDrivers.get(id) || {};
    activeDrivers.set(id, {
      ...existing,
      driverId: id,
      socketId: socket.id,
      status: existing.status || "online",
      operatingMode: existing.operatingMode || "STANDARD",
    });

    console.log(`🚖 Driver ${id} registered to room: ${roomName}`);
  });

  // Alias support for legacy join_driver_room events
  socket.on("join_driver_room", ({ driverId, driver_id }) => {
    const id = String(driverId || driver_id);
    if (!id) return;
    const roomName = `driver_${id}`;
    socket.join(roomName);
    socket.driverId = id;

    const existing = activeDrivers.get(id) || {};
    activeDrivers.set(id, {
      ...existing,
      driverId: id,
      socketId: socket.id,
      status: existing.status || "online",
      operatingMode: existing.operatingMode || "STANDARD",
    });
    console.log(`🎯 Socket ${socket.id} registered as Driver Room: ${roomName}`);
  });

  // 3. Passenger Channel Registration
  socket.on("register_user_channel", ({ userId }) => {
    if (!userId) return;
    const roomName = `user_${userId}`;
    socket.join(roomName);
    console.log(`👤 Passenger ${userId} registered to room: ${roomName}`);
  });

  // 4. Ride Monitoring Room
  socket.on("join_ride_room", ({ rideId, userId }) => {
    if (!rideId) return;
    socket.join(`ride_${rideId}`);
    socket.userId = userId;
    socket.rideId = rideId;
    console.log(`👤 User ${userId} joined room for pending ride #${rideId}`);
  });

  /* 🚗 DRIVER ACCEPTANCE VIA SOCKET */
  socket.on("solo_ride_accept", (data) => {
    const { rideId, driverId, passengerId } = data;
    console.log(`✅ Driver ${driverId} accepted solo ride ${rideId}`);

    const payload = {
      rideId: rideId,
      ride_id: rideId,
      driverId: driverId,
      driver_id: driverId,
      status: "accepted",
      timestamp: new Date().toISOString(),
    };

    // Broadcast to ride room and direct passenger personal channel
    io.to(`ride_${rideId}`).emit("solo_ride_accepted", payload);
    io.to(`ride_${rideId}`).emit("solo_ride_status_updated", payload);

    if (passengerId) {
      io.to(`user_${passengerId}`).emit("solo_ride_accepted", payload);
      io.to(`user_${passengerId}`).emit("solo_ride_status_updated", payload);
    }
  });

  /* 📍 DRIVER ARRIVAL VIA SOCKET */
  socket.on("solo_driver_arrived", (data) => {
    const { rideId, driverId, passengerId } = data;
    console.log(`📍 Driver ${driverId} arrived for solo ride ${rideId}`);

    const payload = {
      rideId: rideId,
      ride_id: rideId,
      driverId: driverId,
      driver_id: driverId,
      status: "arrived",
      timestamp: new Date().toISOString(),
    };

    io.to(`ride_${rideId}`).emit("driver_arrived", payload);
    io.to(`ride_${rideId}`).emit("solo_ride_status_updated", payload);

    if (passengerId) {
      io.to(`user_${passengerId}`).emit("driver_arrived", payload);
      io.to(`user_${passengerId}`).emit("solo_ride_status_updated", payload);
    }
  });


// Handle incoming live GPS ping from the driver app
  socket.on("update_driver_location", async (data) => {
    const { driver_id, ride_id, user_id, latitude, longitude, heading } = data;

    try {
      // 1. Upsert coordinates directly into active_user_sessions table
      await pool.query(
        `INSERT INTO active_user_sessions (user_id, active_role, lat, lng, heading, status, last_ping_at)
         VALUES ($1, 'driver', $2, $3, $4, 'online', NOW())
         ON CONFLICT (user_id) 
         DO UPDATE SET lat = EXCLUDED.lat, 
                       lng = EXCLUDED.lng, 
                       heading = EXCLUDED.heading, 
                       status = 'online',
                       last_ping_at = NOW();`,
        [driver_id, latitude, longitude, heading || 0]
      );

      // 2. Broadcast live coordinates to the passenger room
      const payload = { latitude, longitude, heading };
      
      if (ride_id) {
        io.to(`ride_${ride_id}`).emit("driver_location_update", payload);
      }
      if (user_id) {
        io.to(`user_${user_id}`).emit("driver_location_update", payload);
      }

    } catch (err) {
      console.error("❌ Error updating active_user_sessions:", err.message);
    }
  });

  /* 🚗 TRIP STARTED / IN_TRIP VIA SOCKET */
  socket.on("solo_trip_started", async (data) => {
    const { rideId, driverId, passengerId } = data;
    console.log(`🚗 Driver ${driverId} started trip for solo ride ${rideId}`);

    try {
      // 1. Update PostgreSQL status to 'in_trip'
      await pool.query(
        `UPDATE ride_request_solo SET status = 'in_trip', updated_at = NOW() WHERE id = $1`,
        [rideId]
      );
    } catch (err) {
      console.error("❌ Error updating ride status in DB:", err.message);
    }

    // 2. Broadcast the update payload to the ride room and passenger channel
    const payload = {
      rideId: rideId,
      ride_id: rideId,
      driverId: driverId,
      driver_id: driverId,
      status: "in_trip",
      timestamp: new Date().toISOString(),
    };

    io.to(`ride_${rideId}`).emit("trip_started", payload);
    io.to(`ride_${rideId}`).emit("solo_ride_status_updated", payload);

    if (passengerId) {
      io.to(`user_${passengerId}`).emit("trip_started", payload);
      io.to(`user_${passengerId}`).emit("solo_ride_status_updated", payload);
    }
  });


  // 5. Automatic Cancellation on App Close or Disconnect
  socket.on("disconnect", async () => {
    console.log(`🔌 Client disconnected: ${socket.id}`);

    if (socket.driverId && activeDrivers.has(socket.driverId)) {
      const driverData = activeDrivers.get(socket.driverId);
      driverData.socketId = null;
      activeDrivers.set(socket.driverId, driverData);
    }

    if (socket.rideId && socket.userId) {
      try {
        const cancelQuery = `
          UPDATE ride_request_solo 
          SET status = 'cancelled', updated_at = NOW() 
          WHERE id = $1 AND user_id = $2 AND status = 'pending'
          RETURNING id;
        `;
        const result = await pool.query(cancelQuery, [socket.rideId, socket.userId]);
        if (result.rows.length > 0) {
          console.log(`🧹 Auto-cancelled abandoned pending ride #${socket.rideId} for user ${socket.userId}`);
        }
      } catch (err) {
        console.error("❌ Error auto-cancelling ride on socket disconnect:", err.message);
      }
    }
  });
});



// ------------------------------------------------------------------
// DRIVER LOCATION PING API HANDLER
// ------------------------------------------------------------------
function handleDriverPing(req, res) {
  const { driver_id, lat, lng, heading, status, operating_mode } = req.body;

  if (!driver_id || lat === undefined || lng === undefined) {
    return res.status(400).json({ success: false, message: "Missing driver location payload" });
  }

  const driverKey = String(driver_id);
  const existingState = activeDrivers.get(driverKey) || {};

  const updatedState = {
    ...existingState,
    driverId: driverKey,
    location: { latitude: parseFloat(lat), longitude: parseFloat(lng) },
    heading: heading || 0,
    status: status || "online", // 'online' | 'offline'
    operatingMode: operating_mode || "STANDARD", // 'STANDARD' | 'CORRIDOR'
    lastPing: Date.now(),
  };

  if (status === "offline") {
    activeDrivers.delete(driverKey);
  } else {
    activeDrivers.set(driverKey, updatedState);
  }

  return res.status(200).json({ success: true, timestamp: updatedState.lastPing });
}



const TelegramModule = require('node-telegram-bot-api');
// Safely resolve the constructor across different library versions (v0.x vs v1.x)
const TelegramBot = TelegramModule.default || TelegramModule.TelegramBot || TelegramModule.Bot || TelegramModule;

const TOKEN = process.env.TELEGRAM_BOT_TOKEN;

// Create the bot instance safely
const bot = new TelegramBot(TOKEN, { polling: true });

// In-memory store for tracking session payloads while waiting for contact share
const activeLoginSessions = {}; 

// Listen for incoming messages or start commands from users
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const text = msg.text || '';

  console.log(`📥 [Telegram Bot] Received message from chatId ${chatId}: "${text}"`);

  if (text.startsWith('/start')) {
    const payload = text.split(' ')[1]; 
    console.log(`🔗 [Telegram Bot] Deep link payload extracted: "${payload}"`);

    if (!payload) {
      bot.sendMessage(chatId, "Welcome to Corridor! Please open the app and tap 'Verify with Telegram' or 'Sign In with Telegram'.");
      return;
    }

    activeLoginSessions[chatId] = payload;
    console.log(`💾 [Telegram Bot] Saved session mapping for chatId ${chatId} -> ${payload}`);

    bot.sendMessage(chatId, "Welcome to Corridor! Please tap the button below to securely share your phone number and complete authentication.", {
      reply_markup: {
        keyboard: [
          [{ text: "📱 Share Phone Number", request_contact: true }]
        ],
        one_time_keyboard: true,
        resize_keyboard: true
      }
    });
  }

 if (msg.contact) {
    const phoneNumber = msg.contact.phone_number;
    const sessionPayload = activeLoginSessions[chatId];

    if (!sessionPayload) {
      bot.sendMessage(chatId, "Session expired. Please restart the sign-in or registration process from the app.");
      return;
    }

    try {
      if (sessionPayload.startsWith('verify_')) {
        // --- REGISTRATION FLOW ---
        const userId = sessionPayload.replace('verify_', '');

        // Fetch the role for this specific temporary user row
        const userRoleQuery = await pool.query(`SELECT role FROM users WHERE id = $1`, [userId]);
        const userRole = userRoleQuery.rows[0]?.role;

        // 🛡️ Check if this phone number is already registered FOR THIS SAME ROLE
        const existingUserCheck = await pool.query(
          `SELECT id FROM users WHERE phone_number = $1 AND role = $2 AND id != $3`,
          [phoneNumber, userRole, userId]
        );

        if (existingUserCheck.rows.length > 0) {
          bot.sendMessage(chatId, `❌ This phone number is already registered as a ${userRole}! Please use 'Sign In' instead.`);
          // Clean up the temporary empty user row
          await pool.query(`DELETE FROM users WHERE id = $1`, [userId]);
          delete activeLoginSessions[chatId];
          return;
        }

        // Finalize registration
        await pool.query(
          `UPDATE users SET phone_number = $1, phone_verified = TRUE WHERE id = $2`,
          [phoneNumber, userId]
        );
        bot.sendMessage(chatId, `✅ Registration & Phone verification successful as a ${userRole}! Return to the Corridor app.`);
        delete activeLoginSessions[chatId];

      } else if (sessionPayload.startsWith('login_')) {
        const sessionId = sessionPayload.replace('login_', '');
        
        const userRes = await pool.query(
          `SELECT id, full_name, role, phone_verified, profile_photo_url FROM users WHERE phone_number = $1`,
          [phoneNumber]
        );

        if (userRes.rows.length === 0) {
          bot.sendMessage(chatId, "❌ No account found with this phone number. Please register on the app first.");
          return;
        }

        // Pass all matching accounts back to the app session watcher
        activeLoginSessions[sessionId] = {
          success: true,
          requiresRoleSelection: userRes.rows.length > 1,
          accounts: userRes.rows,
          // Fallback primary user if single account
          userId: userRes.rows[0].id,
          role: userRes.rows[0].role,
          phone_verified: true
        };

        bot.sendMessage(chatId, `✅ Phone verified successfully! Return to the Corridor app to continue.`);
        delete activeLoginSessions[chatId];
      }
    } catch (err) {
      console.error("Telegram contact error:", err);
      bot.sendMessage(chatId, "⚠️ An error occurred. Please try again.");
    }
  }
});

// ==========================================
// AUTHENTICATION & VERIFICATION ENDPOINTS
// ==========================================

// 1. Initialize Registration Record
app.post('/api/auth/register-init', async (req, res) => {
  try {
    const { full_name, role } = req.body;
    
    const query = `
      INSERT INTO users (full_name, role, phone_verified) 
      VALUES ($1, $2, FALSE) 
      RETURNING id, full_name, role;
    `;
    const result = await pool.query(query, [full_name, role]);
    res.json({ userId: result.rows[0].id });
  } catch (err) {
    console.error("Init registration error:", err);
    res.status(500).json({ error: "Database exception" });
  }
});

// 2. Check registration phone verification status

// 1. SIGN-IN STATUS CHECK ENDPOINT
app.get('/api/auth/check-signin/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  const sessionData = activeLoginSessions[sessionId];

  if (sessionData && sessionData.success) {
    // Session found and verified! Return user details
    res.json(sessionData);
    // Optional: clear session after successful fetch so it can't be reused
    delete activeLoginSessions[sessionId];
  } else {
    res.status(404).json({ success: false, message: "Sign-in pending or session expired" });
  }
});

// Add this to your server.js
app.post('/api/auth/signin-init', (req, res) => {
  const sessionId = "signin_" + Date.now();
  // Optional: pre-initialize in memory if needed
  activeLoginSessions[sessionId] = { success: false };
  res.json({ sessionId });
});

// 2. REGISTRATION STATUS CHECK ENDPOINT (Checks Database directly)
// In your server.js
app.get('/api/auth/check-status/:userId', async (req, res) => {
  const { userId } = req.params;
  try {
    const result = await pool.query(
      `SELECT id, role, phone_verified FROM users WHERE id = $1`, // 👈 Must include 'role'
      [userId]
    );

    if (result.rows.length > 0 && result.rows[0].phone_verified) {
      res.json({ 
        success: true, 
        userId: result.rows[0].id,
        role: result.rows[0].role,        // 👈 Sent to frontend
        phone_verified: true 
      });
    } else {
      res.json({ success: false, phone_verified: false });
    }
  } catch (err) {
    console.error("Status check error:", err);
    res.status(500).json({ error: "Server error" });
  }
});




// ==========================================
// 1. Helper to fetch fresh metrics and broadcast
// ==========================================
const broadcastHubQueueUpdate = async (io, pool, hubName) => {
  if (!hubName) return;
  try {
    const query = `
      SELECT 
        r.id AS route_id,
        r.hub_id,
        r.pickup_hub_id,
        r.drop_off_hub_id,
        r.pass_through_hub_id,
        h_pickup.name AS pickup_hub_name,
        h_drop.name AS destination_name,
        h_pass.name AS pass_through_name,
        COALESCE(r.recommended_base_price, 100) AS recommended_base_price,
        COALESCE(ro_latest.total_seats, 4) AS capacity,
        COALESCE(ro_latest.available_seats, 4) AS available_seats,
        ro_latest.id AS ride_offer_id,
        ro_latest.status AS offer_status,
        COALESCE(driver_counts.waiting_drivers, 0)::INTEGER AS driver_queue_count,
        COALESCE(avg_wait_stats.avg_wait_mins, 0)::INTEGER AS average_wait_mins
      FROM routes r
      JOIN hubs h_pickup ON r.pickup_hub_id = h_pickup.id
      JOIN hubs h_drop ON r.drop_off_hub_id = h_drop.id
      LEFT JOIN hubs h_pass ON r.pass_through_hub_id = h_pass.id
      LEFT JOIN (
        SELECT DISTINCT ON (route_id) 
          id, total_seats, available_seats, status, route_id
        FROM ride_offers
        WHERE LOWER(status) IN ('pending', 'matched', 'active', 'inprogress')
        ORDER BY route_id, created_at DESC
      ) ro_latest ON r.id = ro_latest.route_id
      LEFT JOIN (
        SELECT route_id, COUNT(driver_id) AS waiting_drivers
        FROM driver_queue
        WHERE LOWER(status) = 'queued'
        GROUP BY route_id
      ) driver_counts ON r.id = driver_counts.route_id
      LEFT JOIN (
        SELECT 
          route_id, 
          ROUND(AVG(EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60)) AS avg_wait_mins
        FROM driver_queue
        WHERE LOWER(status) = 'dispatched' 
          AND exited_at IS NOT NULL
          AND entered_at IS NOT NULL
          AND exited_at >= NOW() - INTERVAL '12 hours'
        GROUP BY route_id
      ) avg_wait_stats ON r.id = avg_wait_stats.route_id
      WHERE LOWER(h_pickup.name) = LOWER($1);
    `;

    const result = await pool.query(query, [hubName]);
    const roomName = `hub_${hubName.toLowerCase().trim()}`;
    
    io.to(roomName).emit("queue_update", result.rows);
  } catch (err) {
    console.error("❌ Error broadcasting hub queue update:", err);
  }
};

// ==========================================
// 2. Postgres Notification Listener (Calls #1)
// ==========================================
const setupPostgresListener = (pgClient, io, pool) => {
  pgClient.on('notification', async (msg) => {
    try {
      const payload = JSON.parse(msg.payload);
      console.log("🔔 Postgres Notification Caught on Server:", payload);

      if (payload.hub_id) {
        // Resolve hub ID to hub name
        const hubResult = await pool.query(`SELECT name FROM hubs WHERE id = $1`, [payload.hub_id]);
        if (hubResult.rows.length > 0) {
          const hubName = hubResult.rows[0].name;
          await broadcastHubQueueUpdate(io, pool, hubName);
        }
      }
    } catch (err) {
      console.error("❌ Error processing Postgres notification:", err);
    }
  });
};


// ==========================================
// YOUR EXISTING ROUTES (e.g., /api/routes/from/:hubName, etc.)
// remain completely unaffected below here...
// ==========================================

// ⚠️ IMPORTANT: Use server.listen instead of app.listen so WebSockets work!
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT} with WebSockets and Postgres Listener`);
});



app.use("/uploads", express.static(path.join(__dirname, "uploads")));

const { getDistanceFromLatLonInKm } = require('./utils/distance');
const { appendFile } = require('fs');
const { asyncWrapProviders } = require('async_hooks');

// Configure disk storage architecture for profile images
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/"); // Make sure to create a folder named 'uploads' in your project root!
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(null, file.fieldname + "-" + uniqueSuffix + path.extname(file.originalname));
  }
});
const upload = multer({ storage: storage });




// 🎯 TRENDING OTP VERIFY / ONBOARDING INTEGRATION
app.post("/api/auth/verify-otp", upload.single("photo"), async (req, res) => {
  // 🔬 IMMEDIATE DIAGNOSTIC TRACE
  console.log("-----------------------------------------");
  console.log("Incoming request body payload parsed:", req.body);
  console.log("Incoming file upload stream metadata:", req.file);
  console.log("-----------------------------------------");

  const client = await pool.connect();
  try {
    const { phone_number, full_name, role, email, license_number, vehicle_model, license_plate } = req.body;
    
    if (!phone_number) {
      console.log("🛑 REJECTED: phone_number string parameter missing from parsed body state.");
      return res.status(400).json({ error: "Missing required parameter: phone_number." });
    }

    const cleanPhone = phone_number.trim();
    await client.query("BEGIN");

    // 1. Check if user already exists (Login Routine Handshake)
    const userLookup = await client.query("SELECT * FROM users WHERE TRIM(phone_number) = $1", [cleanPhone]);
    
    if (userLookup.rows.length > 0) {
      const existingUser = userLookup.rows[0];
      await client.query("COMMIT");
      
      console.log(`🔑 Login Handshake Verified: Existing user [${existingUser.full_name}] logged in.`);
      return res.status(200).json({
        success: true,
        message: "Handshake verified. Existing user logged in successfully.",
        isNewUser: false,
        user: existingUser
      });
    }

    // 2. Registration Phase Validation Check
    if (!role || !full_name) {
      await client.query("ROLLBACK");
      console.log("🛑 REJECTED: New profile layout flagged, but missing role or full_name.");
      return res.status(400).json({ 
        error: "New user profile detected. full_name and role fields are strictly required to initialize registration." 
      });
    }

    const targetRole = role.trim().toLowerCase();
    const cleanName = full_name.trim();
    const cleanEmail = email && email.trim() !== "" ? email.trim() : null;
    const profilePhotoUrl = req.file ? `/uploads/${req.file.filename}` : "https://placehold.co/150";

    console.log(`✨ Executing database insertion block for user: [${cleanName}] as [${targetRole}]`);

    // 3. Write into your established users table layout
    const userInsert = await client.query(
      `INSERT INTO users (full_name, phone_number, email, role, profile_photo_url, rating) 
       VALUES ($1, $2, $3, $4, $5, 5.00) RETURNING *`,
      [cleanName, cleanPhone, cleanEmail, targetRole, profilePhotoUrl]
    );
    const newUser = userInsert.rows[0];
    console.log(`✅ Core User row inserted cleanly. Generated User ID: ${newUser.id}`);

    // 4. Populate Driver Profile Metadata if string condition balances
    if (targetRole === "driver") {
      if (!license_number || !vehicle_model || !license_plate) {
        await client.query("ROLLBACK");
        console.log("🛑 REJECTED: Driver profile requested but compliance sub-fields missing.");
        return res.status(400).json({ 
          error: "Missing driver compliance fields: license_number, vehicle_model, or license_plate are required for role: driver." 
        });
      }

      console.log(`🗂️ Writing fields into driver_profiles table for User ID: ${newUser.id}`);

      await client.query(
        `INSERT INTO driver_profiles (driver_id, license_number, vehicle_model, license_plate) 
         VALUES ($1, $2, $3, $4)`,
        [newUser.id, license_number.trim(), vehicle_model.trim(), license_plate.trim()]
      );
      console.log(`✅ Driver profile row initialized.`);
    }

    await client.query("COMMIT");
    console.log(`🏁 Onboarding Sequence committed to disk for User ID ${newUser.id}`);

    return res.status(201).json({
      success: true,
      message: "New user registered and synchronized successfully.",
      isNewUser: true,
      user: newUser
    });

  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ CRITICAL AUTH MATRIX EXCEPTION:", error.stack);
    return res.status(500).json({ error: `Authentication engine pipeline failure: ${error.message}` });
  } finally {
    client.release();
  }
});






/* ===========================================================================
   🚀 1. BASE ROOT IDENTITY HANDLERS
=========================================================================== */
app.get('/', (req, res) => {
    res.json({ message: "Welcome to the Corridor Pool API Engine" });
});

/* ===========================================================================
   🚀 2. HIGH PRIORITY ISOLATED STATUS ENDPOINT
   Positioned at the very top of the routing stack to guarantee clean JSON 
   passthrough and eliminate the persistent 404 HTML override error.
=========================================================================== */
app.get("/api/routes/status/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ error: "Missing user identification query path." });
  }

  try {
    // 🎯 FIX 1: Search for rows that are either 'matched' OR 'arrived' 
    const currentPoolQuery = await pool.query(
      `SELECT id, matched_ride_offer_id, status, joined_at 
       FROM route_pool 
       WHERE user_id = $1 AND status IN ('matched', 'arrived') 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(userId, 10)]
    );

    // If no row is matched or arrived yet, return the default searching state
    if (currentPoolQuery.rows.length === 0) {
      // Find how many other people are waiting at the same hub for queue counts
      const countQuery = await pool.query(
        `SELECT COUNT(*) FROM route_pool WHERE status = 'searching'`
      );
      
      return res.json({
        status: "searching",
        currentPoolCount: parseInt(countQuery.rows[0].count, 10) || 1
      });
    }

    const activePoolRow = currentPoolQuery.rows[0];
    const rideOfferId = activePoolRow.matched_ride_offer_id;

    // 🎯 FIX 2: Gather co-riders matching this trip lifecycle block
    const passengersQuery = await pool.query(
      `SELECT user_id, status 
       FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status IN ('matched', 'arrived')`,
      [rideOfferId]
    );

    const parsedPassengers = passengersQuery.rows.map(row => ({
      userId: row.user_id,
      status: row.status // Will return 'matched' or 'arrived' cleanly
    }));

    // Fetch the ride tracking state from offer parameters
    const rideOfferQuery = await pool.query(
      `SELECT status FROM ride_offers WHERE id = $1`,
      [rideOfferId]
    );

    const rideStatus = rideOfferQuery.rows[0]?.status || "pending";

    // Send complete structural mapping back to the frontend loop
    return res.json({
      status: activePoolRow.status, // Returns 'matched' or 'arrived'
      matched_ride_offer_id: rideOfferId,
      rideStatus: rideStatus,
      finalPrice: 80, // Dynamic cost matrix placeholder
      passengers: parsedPassengers
    });

  } catch (err) {
    console.error("❌ BACKEND MATCHING QUEUE RECOVERY ERROR:", err.stack);
    return res.status(500).json({ error: "Internal exception executing pool status checks." });
  }
});

// A DEDICATED ENDPOINT TO FETCH MAP COORDINATES FOR DRIVER OR RIDE OFFER
app.get('/api/rides/map_coordinates', async (req, res) => {
  try {
    const { driver_id, ride_offer_id } = req.query;

    let query = `
      SELECT 
        ro.id AS ride_offer_id,
        ro.driver_id,
        ro.status,
        ro.price_per_seat,
        ro.total_seats,
        ro.available_seats,
        sh.id AS start_hub_id,
        sh.name AS start_hub_name,
        sh.latitude AS start_latitude,
        sh.longitude AS start_longitude,
        eh.id AS end_hub_id,
        eh.name AS end_hub_name,
        eh.latitude AS end_latitude,
        eh.longitude AS end_longitude
      FROM ride_offers ro
      JOIN hubs sh ON ro.start_location::integer = sh.id
      JOIN hubs eh ON ro.end_location::integer = eh.id
    `;

    let queryParams = [];
    if (ride_offer_id) {
      query += ` WHERE ro.id = $1`;
      queryParams.push(parseInt(ride_offer_id, 10));
    } else if (driver_id) {
      query += ` WHERE ro.driver_id = $1 AND ro.status IN ('pending', 'accepted', 'arrived', 'in_trip') ORDER BY ro.created_at DESC LIMIT 1`;
      queryParams.push(parseInt(driver_id, 10));
    } else {
      return res.status(400).json({ success: false, error: "Missing driver_id or ride_offer_id" });
    }

    const result = await pool.query(query, queryParams);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: "No active map coordinates found for this driver" });
    }

    res.json({ success: true, coordinates: result.rows[0] });
  } catch (err) {
    console.error("Error fetching map coordinates:", err);
    res.status(500).json({ success: false, error: "Server error fetching map coordinates" });
  }
});



// passenger screen status gatewaykeeper 

// 🎯 DEDICATED PASSENGER STATUS LIFECYCLE ENDPOINT
app.get("/api/passenger/status/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ error: "Missing user identification query path." });
  }

  try {
    // Read ONLY from route_pool, sorted by latest activity
    const currentPoolQuery = await pool.query(
      `SELECT id, matched_ride_offer_id, status, route_id
       FROM route_pool 
       WHERE user_id = $1 AND status IN ('searching', 'matched', 'accepted', 'arrived', 'in_trip', 'completed') 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(userId, 10)]
    );

    // If no row exists at all, treat them as idle
    if (currentPoolQuery.rows.length === 0) {
      return res.json({ status: "idle" });
    }

    const activeRow = currentPoolQuery.rows[0];

    // Return the clean structural snapshot directly from route_pool
    return res.json({
      status: activeRow.status, // 'searching', 'matched', 'accepted', 'arrived', 'in_trip', 'completed'
      matched_ride_offer_id: activeRow.matched_ride_offer_id,
      routeId: activeRow.route_id
    });

  } catch (err) {
    console.error("❌ PURE PASSENGER ROUTE POOL RECOVERY ERROR:", err.stack);
    return res.status(500).json({ error: "Internal exception executing pure passenger status checks." });
  }
});


//this for fetching ride_offer_id for passenger to check which offer he is assigned to 
app.get("/api/rides/status/:userId", async (req, res) => {
  const { userId } = req.params;
  const cleanUserId = parseInt(userId, 10);

  if (!userId || isNaN(cleanUserId)) {
    return res.status(400).json({ error: "A valid numeric userId parameter is required." });
  }

  try {
    // 🚀 SINGLE-QUERY EXECUTION: Gets active pool, offer status, and co-riders in ONE database hop
    const singleQuery = `
      SELECT 
        rp.matched_ride_offer_id,
        rp.status AS my_status,
        ro.status AS offer_status,
        (
          SELECT json_agg(json_build_object(
            'userId', p.user_id,
            'user_id', p.user_id,
            'status', p.status
          ))
          FROM route_pool p
          WHERE p.matched_ride_offer_id = rp.matched_ride_offer_id
        ) AS passengers
      FROM route_pool rp
      LEFT JOIN ride_offers ro ON ro.id = rp.matched_ride_offer_id
      WHERE rp.user_id = $1
        AND rp.status NOT IN ('cancelled', 'completed')
      ORDER BY rp.joined_at DESC
      LIMIT 1;
    `;

    const { rows } = await pool.query(singleQuery, [cleanUserId]);

    // Case 1: User has no active pool or ride
    if (rows.length === 0) {
      return res.json({
        id: null,
        ride_offer_id: null,
        matched_ride_offer_id: null,
        rideStatus: "pending",
        driver_status: "pending",
        passengers: []
      });
    }

    const data = rows[0];
    const offerId = data.matched_ride_offer_id ? parseInt(data.matched_ride_offer_id, 10) : null;

    // Case 2: Standard response normalized for ALL frontend formats (Driver + Passenger)
    return res.json({
      id: offerId,
      ride_offer_id: offerId,
      matched_ride_offer_id: offerId,
      rideStatus: data.offer_status || "pending",
      driver_status: data.offer_status || "pending", // Fixed: Added driver_status key
      status: data.my_status,                         // Individual passenger's status
      passengers: data.passengers || [
        { userId: cleanUserId, user_id: cleanUserId, status: data.my_status }
      ]
    });

  } catch (err) {
    console.error("❌ CRITICAL POLL ROUTE ERROR:", err.message);
    return res.status(500).json({ 
      error: "Internal server error occurred resolving status check.",
      details: err.message 
    });
  }
});



// Dedicated endpoint to fetch precise passenger and ride status for a user
app.get("/api/rides/pool-status/:userId", async (req, res) => {
  try {
    const { userId } = req.params;
    const parsedUserId = parseInt(userId, 10);

    // 1. Query the route_pool table for this user to get exact status & matched offer
    const poolQuery = `
      rp.id, rp.route_id, rp.user_id, rp.status as passenger_status, rp.matched_ride_offer_id
      FROM route_pool rp
      WHERE rp.user_id = $1
      ORDER BY rp.joined_at DESC
      LIMIT 1
    `;
    const poolResult = await pool.query(`SELECT ${poolQuery}`, [parsedUserId]);

    if (poolResult.rows.length === 0) {
      return res.status(404).json({ error: "User not found in route pool" });
    }

    const poolRow = poolResult.rows[0];
    const rideOfferId = poolRow.matched_ride_offer_id;

    // 2. Fetch ride offer details & accepted_passengers array first
    let rideOffer = null;
    let acceptedList = [];
    if (rideOfferId) {
      const rideQuery = `SELECT id, driver_id, status as driver_status, available_seats, accepted_passengers FROM ride_offers WHERE id = $1`;
      const rideResult = await pool.query(rideQuery, [rideOfferId]);
      rideOffer = rideResult.rows[0] || null;

      if (rideOffer && rideOffer.accepted_passengers) {
        if (Array.isArray(rideOffer.accepted_passengers)) {
          acceptedList = rideOffer.accepted_passengers.map(id => parseInt(id, 10));
        } else if (typeof rideOffer.accepted_passengers === 'string') {
          // Fallback parser if PG returns string representation like "{12,14}"
          acceptedList = rideOffer.accepted_passengers
            .replace(/[{}]/g, "")
            .split(",")
            .map(id => parseInt(id.trim(), 10))
            .filter(id => !isNaN(id));
        }
      }
    }

    // 3. Fetch all co-passengers in the same matched ride offer or route pool
    const coPassengersQuery = `
      rp.user_id, rp.status, rp.pickup_hub_id
      FROM route_pool rp
      WHERE rp.matched_ride_offer_id = $1 OR rp.route_id = $2
    `;
    const coPassengersResult = await pool.query(`SELECT ${coPassengersQuery}`, [
      rideOfferId || 0,
      poolRow.route_id,
    ]);

    // 4. Determine effective status (If user ID is in accepted_passengers, force 'accepted')
    const isUserAccepted = acceptedList.includes(parsedUserId);
    const effectivePassengerStatus = isUserAccepted ? "accepted" : poolRow.passenger_status;

    // 5. Return precise structured status payload
    return res.json({
      user_id: parsedUserId,
      status: effectivePassengerStatus, // Returns 'accepted' if present in ride_offers.accepted_passengers
      ride_offer_id: rideOfferId,
      driver_status: rideOffer ? rideOffer.driver_status : "pending",
      accepted_passengers: acceptedList,
      ride_offer: rideOffer,
      passengers: coPassengersResult.rows.map(row => {
        const pId = parseInt(row.user_id, 10);
        const isPassengerAccepted = acceptedList.includes(pId);
        return {
          user_id: pId,
          status: isPassengerAccepted ? "accepted" : (row.status || "matched"),
        };
      }),
    });
  } catch (err) {
    console.error("❌ Error in /api/rides/pool-status:", err.message);
    return res.status(500).json({ error: "Internal server error" });
  }
});



app.post("/api/routes/join", async (req, res) => {
  const { route_id, user_id, pickup_hub_id, dropoff_hub_id } = req.body;

  if (!route_id || !user_id) {
    return res.status(400).json({ 
      error: "MISSING_FIELDS", 
      message: "Required mapping references (route_id, user_id) are missing." 
    });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1. Duplicate Verification Loop
    const duplicateCheck = await client.query(
      `SELECT id, status FROM route_pool 
       WHERE user_id = $1 AND TRIM(LOWER(status)) != 'completed' 
       AND status != 'cancelled' LIMIT 1 FOR UPDATE;`, 
      [parseInt(user_id, 10)]
    );

    if (duplicateCheck.rows.length > 0) {
      const existingPool = duplicateCheck.rows[0];
      await client.query("COMMIT");
      return res.status(200).json({
        success: true,
        alreadyActive: true,
        navigateTo: "MatchingScreen",
        pool: { id: existingPool.id, status: existingPool.status }
      });
    }

    // 2. DYNAMIC LOOKUP: Resolve pickup & dropoff hubs directly from the routes table if missing
    let targetPickupHubId = pickup_hub_id;
    let finalDropoffHubId = dropoff_hub_id;

    if (!targetPickupHubId || !finalDropoffHubId) {
      const routeLookup = await client.query(
        `SELECT pickup_hub_id, drop_off_hub_id FROM routes WHERE id = $1;`,
        [parseInt(route_id, 10)]
      );

      if (routeLookup.rows.length === 0) {
        await client.query("ROLLBACK");
        return res.status(404).json({
          error: "ROUTE_NOT_FOUND",
          message: `Route with ID ${route_id} does not exist.`
        });
      }

      const routeData = routeLookup.rows[0];
      if (!targetPickupHubId) targetPickupHubId = routeData.pickup_hub_id;
      if (!finalDropoffHubId) finalDropoffHubId = routeData.drop_off_hub_id;
    }

    if (!targetPickupHubId) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "MISSING_PICKUP_HUB",
        message: "pickup_hub_id is required and could not be resolved from route."
      });
    }

    // 3. Insert Entry strictly into route_pool with 'searching' status
    const insertRes = await client.query(
      `INSERT INTO route_pool (user_id, route_id, pickup_hub_id, dropoff_hub_id, status, joined_at)
       VALUES ($1, $2, $3, $4, 'searching', NOW()) RETURNING id, status;`,
      [parseInt(user_id, 10), parseInt(route_id, 10), parseInt(targetPickupHubId, 10), parseInt(finalDropoffHubId, 10)]
    );

    const newPool = insertRes.rows[0];
    console.log(`✅ User ${user_id} registered into route_pool queue (searching). Triggering matching engine...`);

    // ⚡ 4. INVOKE UNIFIED MATCHING ENGINE INSTANTLY
    const matchPipelineResult = await executeCorridorAutoMatch(
      client, 
      parseInt(route_id, 10)
    );

    console.log(`⚡ Match Pipeline Result for Route #${route_id}:`, matchPipelineResult);

    await client.query("COMMIT");
    
    return res.status(200).json({
      success: true,
      message: "Successfully registered into match queue and evaluated by engine.",
      alreadyActive: false,
      pool: {
        id: newPool.id,
        status: newPool.status
      },
      match_pipeline: matchPipelineResult
    });
    
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Database operation failed:", err.message);
    return res.status(500).json({ error: "DATABASE_ERROR", message: err.message });
  } finally {
    client.release();
  }
});


// Checking passenger Distance from requested hub 
app.post("/api/passenger/validate-location", async (req, res) => {
  const { user_lat, user_lng, user_id } = req.body;

  // Fallback to Ayat (9.0220, 38.8450) if coordinates are missing from request body
  const lat = user_lat !== undefined ? parseFloat(user_lat) : 9.0220;
  const lng = user_lng !== undefined ? parseFloat(user_lng) : 38.8450;

  try {
    // 1. Fetch all hubs from database
    const hubs = await pool.query("SELECT id, name, latitude, longitude FROM hubs");

    if (hubs.rows.length === 0) {
      return res.status(404).json({ error: "No hubs registered in database." });
    }

    // 2. Calculate distance to all hubs and find the closest one
    let nearestHub = null;
    let minDistance = Infinity;

    for (const hub of hubs.rows) {
      const distance = getDistanceFromLatLonInKm(
        lat, lng, 
        parseFloat(hub.latitude), parseFloat(hub.longitude)
      );

      if (distance < minDistance) {
        minDistance = distance;
        nearestHub = { 
          ...hub, 
          distanceInMeters: Math.round(distance * 1000) 
        };
      }
    }

    // 3. Optional 1km Range Gatekeeper (Increase threshold slightly during development if needed, e.g., 2.0km)
    if (minDistance > 2.0) {
      return res.status(403).json({ 
        error: "OUT_OF_RANGE", 
        message: `You are too far from any corridor hub. Nearest is ${nearestHub.name} (${Math.round(minDistance * 1000)}m away).` 
      });
    }

    // 4. Return the nearest hub context successfully
    return res.status(200).json({ 
      status: "VALID", 
      nearestHub: nearestHub,
      distanceInMeters: nearestHub.distanceInMeters 
    });

  } catch (err) {
    console.error("Error validating passenger location:", err);
    return res.status(500).json({ error: "Internal server error validating location." });
  }
});


// 🚗 Dedicated Driver Location Validation Endpoint
app.post('/api/driver/validate-location', async (req, res) => {
  const { user_lat, user_lng, user_id } = req.body;

  if (!user_lat || !user_lng) {
    return res.status(400).json({ error: "Latitude and longitude are required." });
  }

  try {
    // Haversine formula to find the nearest hub and its distance in kilometers
    const hubQuery = `
      SELECT id, name, latitude, longitude,
      (6371 * acos(
        cos(radians($1)) * cos(radians(latitude)) * 
        cos(radians(longitude) - radians($2)) + 
        sin(radians($1)) * sin(radians(latitude))
      )) AS distance_km
      FROM hubs
      ORDER BY distance_km ASC
      LIMIT 1;
    `;

    const result = await pool.query(hubQuery, [user_lat, user_lng]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "No hubs configured in the system." });
    }

    const nearestHub = result.rows[0];
    const distanceKm = parseFloat(nearestHub.distance_km);
    
    // Driver allowance radius: 2.0 kilometers (2000 meters)
    const DRIVER_MAX_RADIUS_KM = 2.0;

    if (distanceKm <= DRIVER_MAX_RADIUS_KM) {
      return res.json({
        success: true,
        inRange: true,
        nearestHub: {
          id: nearestHub.id,
          name: nearestHub.name,
          latitude: nearestHub.latitude,
          longitude: nearestHub.longitude,
          distanceKm: distanceKm.toFixed(1)
        }
      });
    } else {
      // Out of range, but provide helpful data so frontend can guide the driver
      return res.json({
        success: false,
        inRange: false,
        closestHubName: nearestHub.name,
        distanceKm: distanceKm.toFixed(1),
        error: `You are out of range. Closest hub is ${nearestHub.name} (${distanceKm.toFixed(1)} km away).`
      });
    }

  } catch (err) {
    console.error("❌ Error validating driver location:", err);
    res.status(500).json({ error: "Internal server error during location validation." });
  }
});

// GET /api/hubs - Fetch all system hubs for the waypoint map display for driver
app.get('/api/hubs', async (req, res) => {
  try {
    const query = `
      SELECT id, name, latitude, longitude 
      FROM hubs 
      ORDER BY id ASC;
    `;
    
    const result = await pool.query(query);
    
    // Return standard array expected by React Native frontend
    res.status(200).json(result.rows);
  } catch (err) {
    console.log("❌ Error fetching hubs:", err.message);
    res.status(500).json({ error: "Failed to retrieve system hubs" });
  }
});























app.post("/api/test/distance", (req, res) => {
  const { user_lat, user_lng } = req.body;
  
  if (!user_lat || !user_lng) {
    return res.status(400).json({ error: "Missing coordinates" });
  }

  const MEGENAGNA_LAT = 9.0106;
  const MEGENAGNA_LNG = 38.7894;

  const dist = getDistanceFromLatLonInKm(user_lat, user_lng, MEGENAGNA_LAT, MEGENAGNA_LNG);

  console.log(`📍 Test Request from: ${user_lat}, ${user_lng}`);
  console.log(`📏 Calculated Distance: ${dist.toFixed(4)} km`);

  res.json({ 
    distance_km: parseFloat(dist.toFixed(4)), 
    withinRange: dist <= 1.0 
  });
});


async function executeCorridorAutoMatch(client, route_id) {
  console.log(`\n🎬 [ENGINE START] Corridor Matching Pipeline | Route [${route_id}]`);

  // ==========================================
  // 1. DRIVER DECLINE & OVERFLOW RECOVERY SWEEP
  // ==========================================
  // If an offer is declined/cancelled, reset all passengers back to 'searching'
  await client.query(
    `UPDATE route_pool rp
     SET status = 'searching',
         matched_ride_offer_id = NULL
     FROM ride_offers ro
     WHERE rp.matched_ride_offer_id = ro.id
       AND TRIM(LOWER(ro.status)) IN ('declined', 'cancelled', 'expired')
       AND rp.status NOT IN ('completed', 'cancelled');`
  );

  // ==========================================
  // 2. MANUAL BOOKING AUTOMATED LIFECYCLE MIMICKING (is_manual = true ONLY)
  // ==========================================
  
  // Rule A: Manual bookings automatically move from 'matched' to 'accepted' when the driver accepts the offer
  await client.query(`
    UPDATE route_pool rp
    SET status = 'accepted'
    FROM ride_offers ro
    WHERE rp.matched_ride_offer_id = ro.id
      AND rp.is_manual = true
      AND rp.status = 'matched'
      AND ro.status IN ('accepted', 'arrived', 'in_trip');
  `);

  // Rule B: Synchronize route_pool statuses to mimic driver lifecycle status (arrived, in_trip, completed) FOR MANUAL BOOKINGS ONLY
  await client.query(`
    UPDATE route_pool rp
    SET status = ro.status
    FROM ride_offers ro
    WHERE rp.matched_ride_offer_id = ro.id
      AND rp.is_manual = true
      AND ro.status IN ('arrived', 'in_trip', 'completed')
      AND rp.status IN ('matched', 'accepted', 'arrived', 'in_trip')
      AND rp.status != ro.status;
  `);

  // ==========================================
  // 3. PEER MIMICKING FOR MANUAL BOOKINGS SHARING AN OFFER
  // ==========================================
  // If a manual booking is matched alongside others, mimic active peer status FOR MANUAL BOOKINGS ONLY
  await client.query(`
    UPDATE route_pool rp_manual
    SET status = peer_status.status
    FROM (
      SELECT DISTINCT ON (matched_ride_offer_id) matched_ride_offer_id, status
      FROM route_pool
      WHERE status IN ('accepted', 'arrived', 'in_trip', 'completed')
      ORDER BY matched_ride_offer_id, 
        CASE status 
          WHEN 'completed' THEN 4 
          WHEN 'in_trip' THEN 3 
          WHEN 'arrived' THEN 2 
          WHEN 'accepted' THEN 1 
          ELSE 0 
        END DESC
    ) peer_status
    WHERE rp_manual.matched_ride_offer_id = peer_status.matched_ride_offer_id
      AND rp_manual.is_manual = true
      AND rp_manual.status = 'matched'
      AND rp_manual.status != peer_status.status;
  `);

  // ==========================================
  // 4. FETCH ELIGIBLE SEARCHING PASSENGERS
  // ==========================================
  const waitingPassengersQuery = await client.query(
    `SELECT rp.id, rp.user_id, rp.status, rp.is_manual,
            TRIM(rp.pickup_hub_id::text) AS pickup_hub_id,
            TRIM(rp.dropoff_hub_id::text) AS dropoff_hub_id
     FROM route_pool rp
     WHERE rp.route_id = $1 
       AND TRIM(LOWER(rp.status)) = 'searching'
     ORDER BY rp.joined_at ASC 
     FOR UPDATE OF rp;`,
    [route_id]
  );

  let availableRiders = waitingPassengersQuery.rows;
  console.log(`👥 Searching Passengers Found: ${availableRiders.length}`);

  if (availableRiders.length === 0) return { status: "idle", matchedCount: 0 };

  // ==========================================
  // 5. FETCH PENDING RIDE OFFERS
  // ==========================================
  const openOffersQuery = await client.query(
    `SELECT ro.id, ro.driver_id, ro.total_seats, ro.available_seats,
            TRIM(ro.start_location::text) AS start_location,
            TRIM(ro.end_location::text) AS end_location
     FROM ride_offers ro
     WHERE ro.route_id = $1 
       AND TRIM(LOWER(ro.status)) = 'pending' 
       AND ro.available_seats > 0
     ORDER BY ro.created_at ASC 
     FOR UPDATE OF ro;`,
    [route_id]
  );

  const openOffers = openOffersQuery.rows;
  console.log(`🚗 Pending Ride Offers Available: ${openOffers.length}`);

  let totalMatchedCount = 0;

  // ==========================================
  // 6. MATCHING PIPELINE
  // ==========================================
  for (const offer of openOffers) {
    let unassigned = [];
    let seatsAvailable = Number(offer.available_seats);
    let matchesMadeThisOffer = 0;

    for (const pax of availableRiders) {
      const isSamePickup = pax.pickup_hub_id === offer.start_location;
      const isSameDropoff = pax.dropoff_hub_id === offer.end_location;
      const isValidSegment = isSamePickup && isSameDropoff && seatsAvailable > 0;

      if (isValidSegment) {
        await client.query(
          `UPDATE route_pool 
           SET status = 'matched', matched_ride_offer_id = $1 
           WHERE id = $2;`,
          [offer.id, pax.id]
        );
        seatsAvailable--;
        matchesMadeThisOffer++;
        totalMatchedCount++;
        console.log(`   ⚡ MATCHED: Rider ${pax.user_id} (Manual: ${pax.is_manual}) assigned to Driver #${offer.driver_id}`);
      } else {
        unassigned.push(pax);
      }
    }

    if (matchesMadeThisOffer > 0) {
      await client.query(
        `UPDATE ride_offers SET available_seats = $1 WHERE id = $2;`,
        [seatsAvailable, offer.id]
      );
    }

    availableRiders = unassigned; 
    if (availableRiders.length === 0) break;
  }

  return { status: "success", matchedCount: totalMatchedCount };
}



/* ---------------- HUB SEARCH ---------------- */
app.get("/api/hubs/search/:term", async (req, res) => {
  try {
    const { term } = req.params;
    const result = await pool.query(
      `SELECT id, name, latitude, longitude FROM hubs
       WHERE LOWER(name) LIKE LOWER($1) ORDER BY name LIMIT 10`,
      [`%${term}%`]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error searching hubs:", err.message);
    res.status(500).json({ error: "Server error" });
  }
});

/* ---------------- GET CORRIDORS FROM PICKUP HUB (MATCHING EXISTING RIDE_OFFERS) ---------------- */
app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;
    console.log(`🔍 [API] Fetching corridors for pickupHub parameter: "${pickupHub}"`);
    
    const result = await pool.query(
      `SELECT 
        r.id AS route_id,
        r.id AS id,
        r.hub_id AS parent_hub_id,
        r.pickup_hub_id,
        COALESCE(h_pickup.name, 'Unknown Hub') AS pickup_hub_name,
        r.drop_off_hub_id,
        COALESCE(h_drop.name, 'Destination') AS name,
        COALESCE(h_drop.name, 'Destination') AS destination_name,
        r.pass_through_hub_id,
        h_pass.name AS pass_through_name,
        r.recommended_base_price,
        
        -- Hybrid Pricing: Driver custom price or fallback recommended base price
        COALESCE(latest_ro.price_per_seat, r.recommended_base_price) AS price,
        
        latest_ro.available_seats,
        latest_ro.total_seats,
        latest_ro.id AS ride_offer_id,
        latest_ro.status AS offer_status
      FROM routes r
      LEFT JOIN hubs h_pickup ON h_pickup.id = r.pickup_hub_id
      LEFT JOIN hubs h_drop ON h_drop.id = r.drop_off_hub_id
      LEFT JOIN hubs h_pass ON h_pass.id = r.pass_through_hub_id
      
      -- Attach latest active driver ride offer matching this route
      LEFT JOIN LATERAL (
        SELECT id, price_per_seat, available_seats, total_seats, status
        FROM ride_offers 
        WHERE ride_offers.route_id = r.id 
          AND status IN ('pending', 'matched', 'active')
        ORDER BY id DESC 
        LIMIT 1
      ) latest_ro ON true
      
      WHERE LOWER(h_pickup.name) = LOWER($1) OR r.pickup_hub_id::text = $1
      ORDER BY r.id;`,
      [pickupHub]
    );

    console.log(`📦 [API] Successfully compiled ${result.rows.length} corridors for hub: "${pickupHub}"`);
    if (result.rows.length > 0) {
      console.log("📝 Sample Row Data:", JSON.stringify(result.rows[0], null, 2));
    }

    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching corridors from hub:", err.message);
    res.status(500).json({ error: "Server error", details: err.message });
  }
});

app.post("/api/rides/start-trip", async (req, res) => {
  // 🛡️ GUARD: Prevent crash if req.body isn't parsed
  if (!req || !req.body) {
    return res.status(400).json({ 
      error: "Malformed request payload. Ensure express.json() middleware is active." 
    });
  }

  const { driver_id, driverId } = req.body;
  const targetDriverId = parseInt(driver_id || driverId, 10);

  console.log(`🎬 [START TRIP] Request received for Driver ID: ${targetDriverId}`);

  if (!targetDriverId || isNaN(targetDriverId)) {
    return res.status(400).json({ error: "Missing or invalid driver_id parameter format." });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Locate and update the driver's active 'arrived' ride offer to 'in_trip'
    const updateOffer = await client.query(
      `UPDATE ride_offers 
       SET status = 'in_trip',
           created_at = NOW()
       WHERE driver_id = $1 
         AND TRIM(LOWER(status)) IN ('arrived', 'in_trip')
       RETURNING id, status, route_id, driver_id;`,
      [targetDriverId]
    );

    if (updateOffer.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`⚠️ [START TRIP REJECTION] No active 'arrived' ride offer found for Driver ID #${targetDriverId}.`);
      return res.status(404).json({ 
        error: `No active ride offer in 'arrived' status found for driver ID ${targetDriverId}.` 
      });
    }

    const activeOffer = updateOffer.rows[0];
    const activeRideOfferId = activeOffer.id;
    console.log(`✅ [START TRIP] ride_offers row #${activeRideOfferId} successfully updated to [${activeOffer.status}]`);

    // 2. 🟢 COMMIT COMMITTED RIDERS: Transition 'accepted' or 'arrived' riders to 'in_trip'
    const poolUpdateInTrip = await client.query(
      `UPDATE route_pool 
       SET status = 'in_trip'
       WHERE matched_ride_offer_id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived', 'in_trip')
       RETURNING id, user_id, status;`,
      [activeRideOfferId]
    );

    console.log(`👥 [START TRIP] Upgraded ${poolUpdateInTrip.rows.length} committed passenger records to 'in_trip'.`);

    // 3. 🔄 RECYCLE UNCOMMITTED RIDERS: Boot 'matched' passengers back to 'searching' and unlink this offer
    const poolResetMatched = await client.query(
      `UPDATE route_pool 
       SET status = 'searching',
           matched_ride_offer_id = NULL
       WHERE matched_ride_offer_id = $1 
         AND TRIM(LOWER(status)) = 'matched'
       RETURNING id, user_id, status;`,
      [activeRideOfferId]
    );

    const recycledCount = poolResetMatched.rows.length;
    console.log(`♻️ [START TRIP] Recycled ${recycledCount} uncommitted ('matched') passengers back to 'searching'.`);

    // Commit the database transaction *before* calling the matching engine
    await client.query("COMMIT");

    // 4. 🚀 TRIGGER MATCHING ENGINE
    if (recycledCount > 0) {
      console.log(`⚡ [START TRIP] Triggering Corridor Matching Engine to re-assign ${recycledCount} passenger(s)...`);
      
      if (typeof runMatchingEngine === "function") {
        runMatchingEngine().catch(err => {
          console.error("❌ [Matching Engine Process Error]:", err.message);
        });
      } else {
        fetch(`api/rides/match`, { 
          method: "POST",
          headers: { "Content-Type": "application/json" }
        }).catch(err => {
          console.error("❌ [Matching Engine Network Trigger Failed]:", err.message);
        });
      }
    }

    return res.status(200).json({
      success: true,
      message: "Corridor trip started. Committed riders in transit; uncommitted matches recycled and sent back to matching queue.",
      ride_offer: {
        id: activeOffer.id,
        status: activeOffer.status.trim(),
        routeId: activeOffer.route_id,
        driverId: activeOffer.driver_id
      },
      synchronizedRidersCount: poolUpdateInTrip.rows.length,
      recycledRidersCount: recycledCount
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: START TRIP FAULT:", err.message);
    
    return res.status(500).json({ 
      error: "Internal database state validation exception starting trip.",
      details: err.message 
    });
  } finally {
    client.release();
  }
});






/* ===========================================================================
   🚖 5. DRIVER OPERATIONAL CONTROLLERS
=========================================================================== */

/* ---------------- DRIVER SIDE: THE LIVE DEMAND RADAR FEED ---------------- */
app.get("/api/drivers/radar", async (req, res) => {
  try {
    const liveDemandQuery = `
      SELECT 
        rp.route_id, rp.pickup_hub_id, rp.dropoff_hub_id,
        h_start.name AS pickup_name, h_end.name AS dropoff_name,
        COUNT(rp.id) AS waiting_passengers,
        r.base_price * COUNT(rp.id) AS raw_value,
        CASE WHEN COUNT(rp.id) >= 2 THEN 'BUNDLE' ELSE 'SINGLE' END as demand_type
      FROM route_pool rp
      JOIN routes r ON rp.route_id = r.id
      JOIN hubs h_start ON rp.pickup_hub_id = h_start.id
      JOIN hubs h_end ON rp.dropoff_hub_id = h_end.id
      WHERE rp.status = 'searching'
      GROUP BY rp.route_id, rp.pickup_hub_id, rp.dropoff_hub_id, h_start.name, h_end.name
      ORDER BY waiting_passengers DESC, rp.route_id ASC;`;

    const result = await pool.query(liveDemandQuery);

    const formattedDemands = result.rows.map(row => {
      const filled = parseInt(row.waiting_passengers, 10);
      const base_price = 180; 
      
      let price_per_person = base_price;
      if (filled === 2) price_per_person = Math.round(base_price * 0.85);
      if (filled >= 3) price_per_person = Math.round(base_price * 0.75);

      const driver_payout = filled === 1 ? base_price : Math.round(price_per_person * filled * 1.1);

      return { ...row, payout_text: `${driver_payout} ETB`, price_per_seat: price_per_person };
    });

    res.json({ active_demands: formattedDemands });
  } catch (err) {
    console.error("❌ RADAR ENGINE DATA FAILURE:", err.message);
    res.status(500).json({ error: "Failed to load live driver radar data stream" });
  }
});

/* ---------------- DRIVER SIDE: GO ONLINE / DEPLOY INDEPENDENT OFFER ---------------- */
app.post("/api/rides", async (req, res) => {
  const { driver_id, vehicle_id, route_id, start_location, end_location, total_seats, price_per_seat } = req.body;
  try {
    const result = await pool.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, status, created_at)
       VALUES ($1, $2, $3, $4, $5, NOW(), $6, $6, $7, 'pending', NOW()) RETURNING *;`,
      [driver_id, vehicle_id, route_id, start_location, end_location, total_seats, price_per_seat]
    );
    res.status(201).json({ message: "Driver is online operating corridor path.", ride: result.rows[0] });
  } catch (err) {
    console.error("❌ Error deploying active ride offer:", err.message);
    res.status(500).json({ error: "Server error while launching corridor deployment." });
  }
});



/* ---------------- GET ACTIVE MANIFEST FOR THE DRIVER'S RIDE POOL ---------------- */
app.get("/api/rides/:rideId/pool-manifest", async (req, res) => {
  try {
    const { rideId } = req.params;
    const result = await pool.query(
      `SELECT rb.id AS booking_id, rb.seats_booked, rb.status, h_start.name AS pickup_hub, h_end.name AS dropoff_hub
       FROM ride_bookings rb
       JOIN hubs h_start ON h_start.id = rb.pickup_hub_id
       JOIN hubs h_end ON h_end.id = rb.dropoff_hub_id
       WHERE rb.ride_id = $1 AND rb.status IN ('booked', 'onboard')
       ORDER BY rb.booked_at ASC;`,
      [rideId]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching pool manifest:", err.message);
    res.status(500).json({ error: "Server error reading manifest." });
  }
});


// ---------------- DRIVER SIDE: CREATE A NEW RIDE OFFER ----------------

app.post("/api/rides/create", async (req, res) => {
  const client = await pool.connect();
  try {
    // 🎯 1. Extract fields from frontend payload
    const { 
      driver_id, 
      route_id, 
      total_seats,
      start_location, // Expects Hub ID number (e.g., 1 or "1")
      end_location,   // Expects Hub ID number (e.g., 4 or "4")
      pickup_lat,     
      pickup_lng      
    } = req.body;

    if (!driver_id || !route_id) {
      return res.status(400).json({ 
        error: "Missing Parameters", 
        message: "Missing driver_id or route_id parameter vectors." 
      });
    }

    // 1. DYNAMIC PROFILE & VEHICLE EXTRACTION
    const driverProfileQuery = await client.query(
      `SELECT full_name FROM users WHERE id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );
    
    if (driverProfileQuery.rows.length === 0) {
      return res.status(404).json({ 
        error: "PROFILE_NOT_FOUND", 
        message: "No user profile matches this driver_id." 
      });
    }

    const vehicleQuery = await client.query(
      `SELECT id, make, model, capacity FROM vehicles WHERE driver_id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );

    // 🚨 VEHICLE CONSTRAINT GUARD
    if (vehicleQuery.rows.length === 0) {
      return res.status(400).json({ 
        error: "VEHICLE_REQUIRED", 
        message: "No vehicle profile detected. You must link a vehicle to this driver account before going online." 
      });
    }

    const driverName = driverProfileQuery.rows[0].full_name;
    const resolvedVehicleId = vehicleQuery.rows[0].id;
    const carMake = vehicleQuery.rows[0].make || "Corridor";
    const carModel = vehicleQuery.rows[0].model || "Vehicle";
    const vehicleCapacity = vehicleQuery.rows[0].capacity || total_seats || 4;

    // 🚨 BEGIN DATABASE TRANSACTION
    await client.query("BEGIN");

    // 2. CONSTRAINT CHECK: Strictly target active operational statuses only
    const activeOfferCheck = await client.query(
      `SELECT id, status FROM ride_offers 
       WHERE driver_id = $1 
         AND TRIM(LOWER(status)) IN ('pending', 'proposed', 'accepted', 'arrived', 'inprogress')
       LIMIT 1;`,
      [parseInt(driver_id, 10)]
    );

    if (activeOfferCheck.rows.length > 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: "ACTIVE_SESSION_EXISTS",
        message: `Overlap Alert: ${driverName} already has an active corridor trip running (Offer ID #${activeOfferCheck.rows[0].id}).` 
      });
    }

    // 3. Collect Route Geometry & Hub Details from New Flat Routes Schema
    const routeGeomQuery = await client.query(
      `SELECT 
        COALESCE(r.recommended_base_price, 100) AS base_price,
        r.pickup_hub_id AS start_hub_id,
        h_pickup.name AS start_hub_name,
        h_pickup.latitude AS start_lat,
        h_pickup.longitude AS start_lng,
        r.drop_off_hub_id,
        h_drop.name AS drop_off_hub_name,
        r.pass_through_hub_id,
        h_pass.name AS pass_through_hub_name
       FROM routes r
       JOIN hubs h_pickup ON h_pickup.id = r.pickup_hub_id
       JOIN hubs h_drop ON h_drop.id = r.drop_off_hub_id
       LEFT JOIN hubs h_pass ON h_pass.id = r.pass_through_hub_id
       WHERE r.id = $1 
       LIMIT 1;`,
      [parseInt(route_id, 10)]
    );
    
    const routeData = routeGeomQuery.rows[0];
    if (!routeData) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: "INCOMPLETE_ROUTE", 
        message: "The chosen corridor route blueprint does not exist." 
      });
    }

    // Construct flat hub arrays dynamically
    const hub_ids = [routeData.start_hub_id];
    const hub_names = [routeData.start_hub_name];
    if (routeData.pass_through_hub_id) {
      hub_ids.push(routeData.pass_through_hub_id);
      hub_names.push(routeData.pass_through_hub_name);
    }
    hub_ids.push(routeData.drop_off_hub_id);
    hub_names.push(routeData.drop_off_hub_name);

    // 🎯 4. Enforce saving Hub ID numbers
    const finalStartLoc = start_location !== undefined && start_location !== null ? String(start_location) : String(routeData.start_hub_id);
    const finalEndLoc = end_location !== undefined && end_location !== null ? String(end_location) : String(routeData.drop_off_hub_id);
    const finalPickupLat = pickup_lat || routeData.start_lat;
    const finalPickupLng = pickup_lng || routeData.start_lng;
    const pricePerSeat = routeData.base_price;

    // 5. SYNCHRONIZE DRIVER QUEUE STATE
    const queueCheck = await client.query(
      `SELECT id FROM driver_queue WHERE driver_id = $1 LIMIT 1;`,
      [parseInt(driver_id, 10)]
    );

    if (queueCheck.rows.length > 0) {
      await client.query(
        `UPDATE driver_queue 
         SET vehicle_id = $1, 
             route_id = $2, 
             current_hub_id = $3, 
             current_hub_name = $4, 
             status = 'queued',
             capacity = $5, 
             entered_at = CURRENT_TIMESTAMP
         WHERE driver_id = $6;`,
        [resolvedVehicleId, parseInt(route_id, 10), routeData.start_hub_id, routeData.start_hub_name, vehicleCapacity, parseInt(driver_id, 10)]
      );
    } else {
      await client.query(
        `INSERT INTO driver_queue (driver_id, vehicle_id, route_id, current_hub_id, current_hub_name, status, capacity, entered_at)
         VALUES ($1, $2, $3, $4, $5, 'queued', $6, CURRENT_TIMESTAMP);`,
        [parseInt(driver_id, 10), resolvedVehicleId, parseInt(route_id, 10), routeData.start_hub_id, routeData.start_hub_name, vehicleCapacity]
      );
    }

    // 🎯 6. GENERATE THE NEW RIDE OFFER
    const rideOfferQuery = await client.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng, status, accepted_passengers, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, 'pending', '{}', CURRENT_TIMESTAMP)
       RETURNING id AS offer_id, status, available_seats, price_per_seat;`,
      [
        parseInt(driver_id, 10), 
        resolvedVehicleId, 
        parseInt(route_id, 10), 
        finalStartLoc,     
        finalEndLoc,       
        vehicleCapacity, 
        vehicleCapacity, 
        pricePerSeat, 
        finalPickupLat,    
        finalPickupLng     
      ]
    );
    
    let activeOfferData = rideOfferQuery.rows[0];
    let matchPipelineResult = { status: "no_passengers_waiting", matchedCount: 0 };

    // 🔍 DEMAND GUARD
    const activeDemandQuery = await client.query(
      `SELECT COUNT(*) FROM route_pool 
       WHERE route_id = $1 
         AND pickup_hub_id = $2 
         AND (
           TRIM(LOWER(status)) = 'searching' 
           OR (TRIM(LOWER(status)) = 'matched' AND matched_ride_offer_id IS NULL)
         );`,
      [parseInt(route_id, 10), routeData.start_hub_id]
    );

    const waitingCount = parseInt(activeDemandQuery.rows[0].count, 10);

    // ⚡ RUN THE ENGINE MATCH LOOP ONLY IF DEMAND EXISTS
    if (waitingCount > 0) {
      console.log(`⚡ ${waitingCount} passenger(s) waiting. Executing direct matching loop for Offer ID #${activeOfferData.offer_id}...`);
      
      matchPipelineResult = await executeCorridorAutoMatch(
        client, 
        parseInt(route_id, 10), 
        activeOfferData.offer_id
      );

      if (matchPipelineResult.status === "success") {
        console.log(`🎉 Instant Match Executed! ${matchPipelineResult.matchedCount} riders synchronized.`);
        
        const refreshedOfferQuery = await client.query(
          `SELECT available_seats, price_per_seat FROM ride_offers WHERE id = $1;`,
          [activeOfferData.offer_id]
        );
        if (refreshedOfferQuery.rows.length > 0) {
          activeOfferData.available_seats = refreshedOfferQuery.rows[0].available_seats;
          activeOfferData.price_per_seat = refreshedOfferQuery.rows[0].price_per_seat;
        }
      }
    } else {
      console.log(`ℹ️ Bypassing matching engine: 0 passengers waiting at Hub #${routeData.start_hub_id} for Route #${route_id}.`);
    }

    await client.query("COMMIT");

    const filledCount = vehicleCapacity - activeOfferData.available_seats;

    return res.status(201).json({
      message: "Corridor pool initialized successfully.",
      ride_id: activeOfferData.offer_id,
      status: activeOfferData.status,
      start_location: finalStartLoc,
      end_location: finalEndLoc,    
      total_seats: vehicleCapacity,
      filled: filledCount, 
      price_per_seat: activeOfferData.price_per_seat,
      driver_name: driverName,
      make: carMake,
      model: carModel,
      instant_match_pipeline: matchPipelineResult
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error executing dynamic ride creator:", err.message);
    return res.status(500).json({ 
      error: "SERVER_ERROR", 
      message: "Internal server error deploying live ride offer matrix." 
    });
  } finally {
    client.release();
  }
});
/* ===========================================================================
   📈 6. ANALYTICS & LOGGING HUD MATRIX ENGINES
=========================================================================== */

/* ---------------- POOL CALCULATION LIVE HUD LOOKUP ---------------- */
app.get("/api/routes/engine/:route_id", async (req, res) => {
  const { route_id } = req.params;
  try {
    const route = await pool.query(`SELECT capacity, recommended_base_price FROM routes WHERE id = $1`, [route_id]);
    if (route.rows.length === 0) return res.status(404).json({ error: "Route configuration map missing" });

    const count = await pool.query(`SELECT COUNT(*) FROM route_pool WHERE route_id = $1 AND status = 'searching'`, [route_id]);

    const filled = parseInt(count.rows[0].count, 10);
    const capacity = route.rows[0].capacity || 3;
    const base_price = route.rows[0].base_price || 180;

    let status = "waiting";
    let price_per_person = base_price;

    if (filled === 2) {
      status = "active";
      price_per_person = Math.round(base_price * 0.85);
    } else if (filled >= 3) {
      status = "optimized";
      price_per_person = Math.round(base_price * 0.75);
    }

    const driver_payout = filled === 1 ? base_price : Math.round(price_per_person * filled * 1.1);

    res.json({
      route_id, filled, capacity, status, price_per_person, driver_payout,
      remaining: Math.max(0, capacity - filled), is_ready: filled >= 2, profit_model: "dynamic_pooling_v1",
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Engine execution calculation crash" });
  }
});

// GET ALL ACTIVE RIDES BY CORRIDOR ID ONLY
app.get("/api/rides", async (req, res) => {
  const { route_id } = req.query;
  try {
    let query = `SELECT * FROM ride_offers`;
    const params = [];
    if (route_id) {
      params.push(parseInt(route_id, 10));
      query += ` WHERE route_id = $1`;
    }
    query += ` ORDER BY created_at DESC;`;
    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching rides by route_id:", err.message);
    res.status(500).json({ error: "Server error" });
  }
});

// Driver Waiting For riders active pool status
// GET active waiting pool status for a driver by driver_id
app.get('/api/rides/waiting-pool-status/:driverId', async (req, res) => {
  try {
    const { driverId } = req.params;

    const rideQuery = `
      SELECT ro.id AS ride_offer_id, 
             ro.route_id, 
             ro.start_location AS hub_id, 
             ro.end_location AS destination_id, 
             ro.price_per_seat,
             ro.total_seats,
             ro.available_seats,
             ro.status,
             h_start.name AS start_hub_name,
             h_end.name AS end_hub_name
      FROM ride_offers ro
      LEFT JOIN hubs h_start ON ro.start_location::integer = h_start.id
      LEFT JOIN hubs h_end ON ro.end_location::integer = h_end.id
      WHERE ro.driver_id = $1::integer AND ro.status = 'pending'
      ORDER BY ro.created_at DESC
      LIMIT 1
    `;
    
    const rideResult = await pool.query(rideQuery, [driverId]);

    if (rideResult.rows.length === 0) {
      return res.status(404).json({ 
        error: "No active pending ride offer found for this driver." 
      });
    }

    const activeRide = rideResult.rows[0];
    const seatsFilled = (activeRide.total_seats || 3) - (activeRide.available_seats || 0);

    return res.status(200).json({
      success: true,
      rideOfferId: activeRide.ride_offer_id,
      routeId: activeRide.route_id,
      hubId: activeRide.hub_id,
      hubName: activeRide.start_hub_name || "Unknown Hub",
      destinationName: activeRide.end_hub_name || "Unknown Destination",
      pricePerSeat: activeRide.price_per_seat,
      totalSearching: 0,
      totalMatched: seatsFilled,
      matchedRiders: []
    });

  } catch (err) {
    console.error("❌ Error fetching driver waiting pool status:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});



















/* ------------- FETCH ACTIVE PASSENGER REQUESTS (DEMAND MATRIX) ------------- */
app.get("/api/corridors/passenger-requests", async (req, res) => {
  try {
    const query = `
      SELECT rp.id AS request_id, rp.route_id, rp.user_id AS passenger_id, u.full_name AS passenger_name,
        h1.name AS start_location, h2.name AS end_location, rp.joined_at
      FROM route_pool rp
      JOIN users u ON rp.user_id = u.id
      JOIN hubs h1 ON rp.pickup_hub_id = h1.id
      JOIN hubs h2 ON rp.dropoff_hub_id = h2.id
      WHERE rp.status = 'searching' ORDER BY rp.joined_at DESC;`;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching passenger requests from route_pool:", err.message);
    res.status(500).json({ error: "Server error querying live passenger demand matrix" });
  }
});

// GET ALL BASELINE SYSTEM CORRIDORS
app.get("/api/routes/all", async (req, res) => {
  try {
    const result = await pool.query("SELECT route_id, name, stops, base_price FROM routes ORDER BY name ASC;");
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching predefined corridors:", err.message);
    res.status(500).json({ error: "Server error fetching routes list" });
  }
});

/* ---------------- FETCH LIVE PICKUP ACCEPTANCE MATRIX ---------------- */
app.get("/api/rides/proposal-status/:rideOfferId", async (req, res) => {
  const { rideOfferId } = req.params;
  try {
    // 1. Fetch the ride offer details along with who has accepted so far
    const rideQuery = await pool.query(
      `SELECT accepted_passengers, route_id FROM ride_offers WHERE id = $1`,
      [rideOfferId]
    );

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "Active ride deployment run not found." });
    }

    const { accepted_passengers, route_id } = rideQuery.rows[0];

    // 2. Fetch all passengers grouped into this specific active deployment manifest
    const ridersQuery = await pool.query(
      `SELECT user_id FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status = 'matched'`,
      [rideOfferId]
    );

    const manifestRiders = ridersQuery.rows.map(r => r.user_id);

    res.json({
      rideOfferId: parseInt(rideOfferId, 10),
      routeId: route_id,
      manifestPassengers: manifestRiders, // e.g. [1, 68, 69]
      acceptedPassengers: accepted_passengers || [] // e.g. [1, 68]
    });
  } catch (err) {
    console.error("❌ ERROR FETCHING PROPOSAL MATRIX STATUS:", err.stack);
    res.status(500).json({ error: "Internal transaction failure querying acceptance fields." });
  }
});



/* ---------------- PASSENGER ACCEPTS PROPOSED PICKUP POINT ---------------- */
// ==================================================================
// PASSENGER ACCEPT PROPOSAL ENDPOINT
// POST /api/rides/accept-proposal
// ==================================================================
app.post("/api/rides/accept-proposal", async (req, res) => {
  const { user_id, userId } = req.body;
  const targetUserId = parseInt(user_id || userId, 10);

  if (!targetUserId || isNaN(targetUserId)) {
    return res.status(400).json({ error: "Missing or invalid user_id parameter." });
  }

  // Get a dedicated client connection for transaction safety
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Find active route_pool record (allowing both 'matched' and 'accepted' for idempotency)
    const poolCheck = await client.query(
      `SELECT id, matched_ride_offer_id, status 
       FROM route_pool 
       WHERE user_id = $1 
         AND (status ILIKE '%matched%' OR status = 'accepted')
         AND matched_ride_offer_id IS NOT NULL
       ORDER BY joined_at DESC
       LIMIT 1
       FOR UPDATE`, // Row-level lock to prevent concurrent race conditions
      [targetUserId]
    );

    if (poolCheck.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: "No active matched proposal with a matched ride offer found for this user." 
      });
    }

    const activeRideOfferId = poolCheck.rows[0].matched_ride_offer_id;
    const poolRecordId = poolCheck.rows[0].id;

    // 2. Safely append user_id to accepted_passengers array without duplicates
    await client.query(
      `UPDATE ride_offers 
       SET accepted_passengers = array_append(
         array_remove(COALESCE(accepted_passengers, '{}'), $2), $2
       ) 
       WHERE id = $1`,
      [activeRideOfferId, targetUserId]
    );

    // 3. Update the user's route_pool status to 'accepted'
    await client.query(
      `UPDATE route_pool 
       SET status = 'accepted' 
       WHERE id = $1`,
      [poolRecordId]
    );

    await client.query("COMMIT");q

    return res.status(200).json({ 
      success: true, 
      ride_offer_id: activeRideOfferId,
      user_id: targetUserId,
      message: "Proposal accepted successfully and added to accepted_passengers." 
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ ERROR RECORDING PROPOSAL ACCEPTANCE:", err.stack);
    return res.status(500).json({ error: "Failed to record proposal acceptance state." });
  } finally {
    client.release(); // Always release client back to the pool
  }
});


app.post("/api/rides/accept-active-proposal", async (req, res) => {
  const { user_id } = req.body;

  if (!user_id) {
    return res.status(400).json({ error: "Missing user_id parameter." });
  }

  const client = await pool.connect();

  try {
    // Start a SQL transaction to keep things atomic and secure
    await client.query("BEGIN");

    // 🕵️‍♂️ Step 1: Scan route_pool to find the active ride_offer_id for this passenger
    const poolQuery = await client.query(
      `SELECT matched_ride_offer_id, status 
       FROM route_pool 
       WHERE user_id = $1 AND status IN ('matched', 'arrived') 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(user_id, 10)]
    );

    if (poolQuery.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: "No active matched proposal found for this user in the route pool." 
      });
    }

    const activePoolRow = poolQuery.rows[0];
    const resolvedRideOfferId = activePoolRow.matched_ride_offer_id;

    if (!resolvedRideOfferId) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: "User is matched, but no ride offer ID is registered to this cluster." 
      });
    }

    // 📝 Step 2: Append user_id safely to accepted_passengers in ride_offers
    await client.query(
      `UPDATE ride_offers 
       SET accepted_passengers = array_append(
         array_remove(accepted_passengers, $2), $2
       ) 
       WHERE id = $1`,
      [resolvedRideOfferId, parseInt(user_id, 10)]
    );

    // 📝 Step 3: Update passenger status to 'accepted' in route_pool
    await client.query(
      `UPDATE route_pool 
       SET status = 'accepted' 
       WHERE user_id = $1 AND matched_ride_offer_id = $2`,
      [parseInt(user_id, 10), resolvedRideOfferId]
    );

    // Commit transaction if all steps succeeded
    await client.query("COMMIT");

    console.log(`✅ [PROPOSAL COMMITTED] User ${user_id} added to Offer ${resolvedRideOfferId}`);

    res.json({ 
      success: true, 
      matched_ride_offer_id: resolvedRideOfferId,
      message: "Successfully matched, accepted, and recorded against the ledger!" 
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ ERROR RECORDING PROPOSAL ACCEPTANCE:", err.stack);
    res.status(500).json({ error: "Failed to post acceptance state changes." });
  } finally {
    client.release();
  }
});



app.post('/api/rides/accept', async (req, res) => {
  const { userId, rideOfferId } = req.body;

  if (!userId || !rideOfferId) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    // 1. Update the user's specific status in the pool to 'accepted'
    const updateQuery = `
      UPDATE route_pool
      SET status = 'accepted', updated_at = NOW()
      WHERE user_id = $1 AND ride_offer_id = $2
      RETURNING *;
    `;
    const { rowCount } = await pool.query(updateQuery, [userId, rideOfferId]);

    if (rowCount === 0) {
      return res.status(404).json({ error: "Passenger record not found" });
    }

    // 2. (Optional but recommended) Check if everyone in the pool has accepted
    // If yes, you can automatically update ride_offers.pool_status = 'ready' here.
    const poolCheckQuery = `
      SELECT COUNT(*) as total, 
             SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) as accepted_count
      FROM route_pool 
      WHERE ride_offer_id = $1;
    `;
    const poolCheck = await pool.query(poolCheckQuery, [rideOfferId]);
    
    if (poolCheck.rows[0].total === poolCheck.rows[0].accepted_count) {
      await pool.query(`UPDATE ride_offers SET pool_status = 'ready' WHERE id = $1`, [rideOfferId]);
    }

    return res.status(200).json({ 
      success: true, 
      message: "Pickup proposal accepted" 
    });

  } catch (error) {
    console.error("Error accepting pickup:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});


/* =========================================================================
   2. 📍 ARRIVAL CHECKPOINT SIGNAL: Commit Passenger Presence Node Validation
   ========================================================================= */

app.post("/api/rides/signal-arrival", async (req, res) => {
  const { user_id, userId } = req.body;
  const targetUserId = parseInt(user_id || userId, 10);

  if (!targetUserId || isNaN(targetUserId)) {
    return res.status(400).json({ error: "Missing or invalid user_id." });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1. Find active route_pool record for the user ensuring:
    //    - route_pool status is 'accepted'
    //    - ride_offers status is either 'accepted' or 'arrived'
    const poolCheckQuery = `
      SELECT rp.id, rp.matched_ride_offer_id, rp.status, ro.status AS ride_status, ro.accepted_passengers
      FROM route_pool rp
      JOIN ride_offers ro ON rp.matched_ride_offer_id = ro.id
      WHERE rp.user_id = $1 
        AND LOWER(TRIM(rp.status)) = 'accepted'
        AND LOWER(TRIM(ro.status)) IN ('accepted', 'arrived')
        AND $1 = ANY(ro.accepted_passengers)
      ORDER BY rp.joined_at DESC
      LIMIT 1;
    `;

    const poolResult = await client.query(poolCheckQuery, [targetUserId]);

    if (poolResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: `No eligible active ride pool match found for user ${targetUserId}. Check that rider status is 'accepted' and driver ride offer is 'accepted' or 'arrived'.` 
      });
    }

    const activePoolRow = poolResult.rows[0];
    const targetRideId = activePoolRow.matched_ride_offer_id;
    const poolRowId = activePoolRow.id;

    // 2. Update the active route_pool record status to 'arrived'
    await client.query(
      `UPDATE route_pool 
       SET status = 'arrived' 
       WHERE id = $1;`,
      [poolRowId]
    );

    // 3. Fetch updated co-riders manifest for this ride offer
    const passengersQuery = await client.query(
      `SELECT user_id, TRIM(LOWER(status)) as status FROM route_pool WHERE matched_ride_offer_id = $1`,
      [targetRideId]
    );

    await client.query("COMMIT");
    console.log(`📍 Presence Verification Success: User ${targetUserId} set to 'arrived' for Ride Offer #${targetRideId}.`);

    return res.status(200).json({ 
      success: true, 
      message: "Arrival presence validated cleanly.",
      rideOfferId: targetRideId,
      driverStatus: activePoolRow.ride_status.toLowerCase().trim(), 
      passengers: passengersQuery.rows.map(row => ({
        userId: parseInt(row.user_id, 10),
        status: row.status || "arrived"
      }))
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ SIGNAL ARRIVAL TRANSACTION FAILURE:", err.stack);
    return res.status(500).json({ error: `Internal Server Error: ${err.message}` });
  } finally {
    client.release();
  }
});




app.get("/api/rides/corridor-pool-status", async (req, res) => {
  try {
    // 1. Grab the single freshest live ride offer container created on the system
    const activeOfferQuery = await pool.query(
      `SELECT id, route_id 
       FROM ride_offers 
       WHERE status IN ('pending', 'active')
       ORDER BY created_at DESC LIMIT 1;`
    );

    if (activeOfferQuery.rows.length === 0) {
      return res.json({
        totalSearching: 0,
        totalMatched: 0,
        matchedRiders: [],
        routeId: null
      });
    }

    const currentOfferId = activeOfferQuery.rows[0].id;
    const currentRouteId = activeOfferQuery.rows[0].route_id;

    // 2. Query counts directly linked to this specific ride offer container ID
    const poolMetricsQuery = await pool.query(
      `SELECT 
        COUNT(CASE WHEN status = 'searching' AND route_id = $1 THEN 1 END)::int AS "searchingCount",
        COUNT(CASE WHEN matched_ride_offer_id = $2 AND status = 'matched' THEN 1 END)::int AS "matchedCount"
       FROM route_pool`,
      [currentRouteId, currentOfferId]
    );

    // 3. Get user rows assigned to this exact ride offer id
    const matchedRidersQuery = await pool.query(
      `SELECT user_id AS "userId", status 
       FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status = 'matched'
       ORDER BY joined_at ASC`,
      [currentOfferId]
    );

    return res.json({
      routeId: currentRouteId,
      totalSearching: poolMetricsQuery.rows[0]?.searchingCount || 0,
      totalMatched: poolMetricsQuery.rows[0]?.matchedCount || 0,
      matchedRiders: matchedRidersQuery.rows || []
    });

  } catch (err) {
    console.error("❌ SERVER-SIDE OFFER MATCH TRACKING BREAKDOWN:", err.stack);
    return res.status(500).json({ error: "Internal server error gathering pool states." });
  }
});



/* ---------------- PASSENGER SIDE: DECLINE PROPOSAL & SWAP ---------------- */
app.post("/api/routes/decline-proposal", async (req, res) => {
  const { user_id } = req.body;

  if (!user_id) {
    return res.status(400).json({ error: "Missing required user_id." });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Fetch details of the record being declined where status is 'matched'
    const currentMatchQuery = await client.query(
      `SELECT id, route_id, pickup_hub_id, matched_ride_offer_id 
       FROM route_pool 
       WHERE user_id = $1 AND status = 'matched'
       FOR UPDATE;`,
      [parseInt(user_id, 10)]
    );

    const matchRecord = currentMatchQuery.rows[0];

    if (!matchRecord) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Active matching proposal record not found." });
    }

    const { route_id, pickup_hub_id, matched_ride_offer_id: ride_offer_id } = matchRecord;

    if (!ride_offer_id) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "No associated ride offer found for this match." });
    }

    // 2. Mark the declining passenger as 'cancelled' and clear their matched ride offer id
    await client.query(
      `UPDATE route_pool SET status = 'cancelled', matched_ride_offer_id = NULL WHERE id = $1;`,
      [matchRecord.id]
    );

    console.log(`❌ Passenger ${user_id} declined proposal for Ride Offer ${ride_offer_id}.`);

    // 3. SEAT SWAP GATEWAY: Try to fill this specific seat with an inline waiting passenger
    const replacementPassengerQuery = await client.query(
      `SELECT id FROM route_pool 
       WHERE route_id = $1 AND pickup_hub_id = $2 AND status = 'searching'
       ORDER BY joined_at ASC 
       LIMIT 1 FOR UPDATE SKIP LOCKED;`,
      [route_id, pickup_hub_id]
    );

    const replacementPassenger = replacementPassengerQuery.rows[0];
    let swappedIn = false;

    if (replacementPassenger) {
      console.log(`⚡ Swap Engine: Moving waiting row ID ${replacementPassenger.id} into Ride Offer ${ride_offer_id}`);
      await client.query(
        `UPDATE route_pool SET status = 'matched', matched_ride_offer_id = $1 WHERE id = $2;`,
        [ride_offer_id, replacementPassenger.id]
      );
      swappedIn = true;
    } else {
      // No instant replacement: release the seat slot back to the ride offer configuration
      await client.query(
        `UPDATE ride_offers SET available_seats = available_seats + 1 WHERE id = $1;`,
        [ride_offer_id]
      );
    }

    // 4. POOL VIABILITY CHECK: Evaluate if remaining active members drop below 2
    const checkRemainingQuery = await client.query(
      `SELECT id, user_id FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status IN ('matched', 'arrived');`,
      [ride_offer_id]
    );

    const activeCount = checkRemainingQuery.rows.length;
    let poolDissolved = false;

    if (activeCount < 2) {
      console.log(`♻️ Pool unviable (${activeCount} left). Dissolving group layout for Ride Offer ID [${ride_offer_id}]`);
      poolDissolved = true;

      // Revert the lonely remaining passenger back to 'searching' inside this transaction block
      await client.query(
        `UPDATE route_pool 
         SET status = 'searching', matched_ride_offer_id = NULL 
         WHERE matched_ride_offer_id = $1 AND status IN ('matched', 'arrived');`,
        [ride_offer_id]
      );

      // Cancel the unviable ride offer record
      await client.query(
        `UPDATE ride_offers SET status = 'cancelled' WHERE id = $1;`,
        [ride_offer_id]
      );
    }

    // 5. 🌟 DECOUPLED CALL: Invoke upgraded Matching Engine if defined to resolve searching/stranded entries
    if (typeof executeCorridorAutoMatch === 'function') {
      await executeCorridorAutoMatch(client, route_id, pickup_hub_id);
    }

    await client.query("COMMIT");

    res.status(200).json({
      message: "Processed proposal decline with unified engine recovery successfully.",
      swappedInReplacement: swappedIn,
      poolDissolved: poolDissolved
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ PASSENGER DECLINE SWAP ENGINE ERROR:", err.stack);
    res.status(500).json({ error: "Internal processing failure during swap sequence." });
  } finally {
    client.release();
  }
});

// 🔄 1. TRACK LIVE OFFER TELEMETRY ENDPOINT
app.get("/api/rides/track-live/offer/:id", async (req, res) => {
  const rideOfferId = parseInt(req.params.id, 10);

  if (isNaN(rideOfferId)) {
    return res.status(400).json({ error: "Invalid structural tracking ID parameter vector." });
  }

  try {
    // 1. Fetch offer details including total_seats and resolved hub coordinates
          const offerQuery = await pool.query(
        `SELECT ro.id, ro.status, ro.price_per_seat, ro.pickup_lat, ro.pickup_lng, ro.route_id,
                ro.start_location, ro.end_location, ro.total_seats,
                h_start.name AS start_hub_name, h_start.latitude AS start_hub_lat, h_start.longitude AS start_hub_lng,
                h_end.name AS end_hub_name, h_end.latitude AS end_hub_lat, h_end.longitude AS end_hub_lng
        FROM ride_offers ro
        LEFT JOIN hubs h_start ON ro.start_location::integer = h_start.id
        LEFT JOIN hubs h_end ON ro.end_location::integer = h_end.id
        WHERE ro.id = $1`,
        [rideOfferId]
      );
    if (offerQuery.rows.length === 0) {
      return res.status(404).json({ error: "Target Corridor Offer profile index not found." });
    }

    const offer = offerQuery.rows[0];

    // 2. Fetch passengers with complete profile information from users table
    const passengersQuery = await pool.query(
      `SELECT rp.user_id, rp.status, 
              u.full_name, u.phone_number, u.email, u.role, u.rating, u.profile_photo_url
       FROM route_pool rp
       JOIN users u ON rp.user_id = u.id
       WHERE rp.matched_ride_offer_id = $1
         AND TRIM(LOWER(rp.status)) IN ('matched', 'accepted', 'arrived', 'in_trip', 'intransit', 'picked_up')`,
      [rideOfferId]
    );

    // 3. Construct rich payload
    const trackingPayload = {
      rideOfferId: offer.id,
      rideStatus: offer.status, 
      status: offer.status,
      route_id: offer.route_id,
      price_per_seat: offer.price_per_seat,
      total_seats: parseInt(offer.total_seats, 10) || 4,
      
      start_location: offer.start_location,
      end_location: offer.end_location,
      hubName: offer.start_hub_name || offer.start_location || "Departure Hub",
      destinationName: offer.end_hub_name || offer.end_location || "Destination Hub",
      
      driverLocation: {
        latitude: parseFloat(offer.pickup_lat) || 9.0106,
        longitude: parseFloat(offer.pickup_lng) || 38.7894
      },
      destinationLocation: {
        latitude: parseFloat(offer.end_hub_lat) || 9.0125,
        longitude: parseFloat(offer.end_hub_lng) || 38.7535
      },
      
      passengers: passengersQuery.rows.map(row => ({
        userId: parseInt(row.user_id, 10), 
        user_id: parseInt(row.user_id, 10), 
        full_name: row.full_name || `Rider ${row.user_id}`,
        phone_number: row.phone_number || "",
        email: row.email || "",
        role: row.role || "rider",
        rating: row.rating ? parseFloat(row.rating).toFixed(2) : "5.00",
        profile_photo_url: row.profile_photo_url || "",
        status: row.status ? row.status.toString().trim().toLowerCase() : "in_trip" 
      }))
    };

    return res.status(200).json(trackingPayload);

  } catch (err) {
    console.error("Telemetry engine failure:", err);
    return res.status(500).json({ error: "Internal server telemetry data retrieval error." });
  }
});
/**
 * 🔄 GET ACTIVE ASSIGNMENT PROPOSAL FOR DRIVER
 * Route: GET /api/rides/driver-proposal/:driverId
 */
app.get("/api/rides/driver-proposal/:driverId", async (req, res) => {
  const driverId = parseInt(req.params.driverId, 10);

  if (isNaN(driverId)) {
    return res.status(400).json({ error: "Invalid driver identification parameter." });
  }

  try {
    // 1. Fetch active ride offer for this driver with hub name resolution
    const proposalQuery = await pool.query(
      `SELECT ro.id, ro.route_id, ro.status, ro.start_location, ro.end_location, ro.departure_time, 
              ro.total_seats, ro.available_seats, ro.price_per_seat, ro.pickup_lat, ro.pickup_lng,
              ro.accepted_passengers,
              h_start.name AS start_hub_name,
              h_end.name AS end_hub_name
       FROM ride_offers ro
       LEFT JOIN hubs h_start ON ro.start_location::integer = h_start.id
       LEFT JOIN hubs h_end ON ro.end_location::integer = h_end.id
       WHERE ro.driver_id = $1 
         AND ro.status IN ('pending', 'accepted', 'arrived', 'matched', 'in_trip', 'inprogress')
       ORDER BY ro.created_at DESC 
       LIMIT 1`,
      [driverId]
    );

    if (proposalQuery.rows.length === 0) {
      return res.status(404).json({ hasActiveProposal: false, message: "No active proposals found." });
    }

    const offer = proposalQuery.rows[0];

    // 2. Parse accepted_passengers array safely
    let rawAcceptedIds = offer.accepted_passengers || [];
    let acceptedIdsArray = [];

    if (Array.isArray(rawAcceptedIds)) {
      acceptedIdsArray = rawAcceptedIds.map(id => parseInt(id, 10)).filter(id => !isNaN(id));
    } else if (typeof rawAcceptedIds === "string") {
      acceptedIdsArray = rawAcceptedIds
        .replace(/{|}/g, "")
        .split(",")
        .map(id => parseInt(id.trim(), 10))
        .filter(id => !isNaN(id));
    }

    // 3. Fetch all assigned passengers strictly from route_pool (Source of Truth for Status)
    let passengersQuery = await pool.query(
      `SELECT rp.user_id, rp.status, u.full_name AS passenger_name, u.phone_number 
       FROM route_pool rp
       JOIN users u ON rp.user_id = u.id
       WHERE rp.matched_ride_offer_id = $1`,
      [offer.id]
    );

    // 4. Fallback: If route_pool entries are unlinked, fetch from users but map status accurately (default to 'matched')
    if (passengersQuery.rows.length === 0 && acceptedIdsArray.length > 0) {
      const fallbackQuery = await pool.query(
        `SELECT id AS user_id, 'matched' AS status, full_name AS passenger_name, phone_number 
         FROM users 
         WHERE id = ANY($1::int[])`,
        [acceptedIdsArray]
      );
      passengersQuery = fallbackQuery;
    }

    return res.status(200).json({
      hasActiveProposal: true,
      id: offer.id,
      ride_offer_id: offer.id,
      route_id: offer.route_id,
      status: offer.status,
      start_location: offer.start_location,
      end_location: offer.end_location,
      hubName: offer.start_hub_name || offer.start_location,
      destinationName: offer.end_hub_name || offer.end_location,
      departure_time: offer.departure_time,
      total_seats: offer.total_seats,
      available_seats: offer.available_seats,
      price_per_seat: offer.price_per_seat,
      pickup_lat: offer.pickup_lat,
      pickup_lng: offer.pickup_lng,
      accepted_passengers: acceptedIdsArray,
      passengers: passengersQuery.rows
    });

  } catch (err) {
    console.error("Driver proposal fetch error:", err);
    return res.status(500).json({ error: "Internal server error retrieving driver proposal." });
  }
});


// 📌 DEDICATED ENDPOINT FOR CLUSTER OFFER PROPOSAL & SYNC
app.get("/api/rides/driver-cluster-offer/:driverId", async (req, res) => {
  let driverId = parseInt(req.params.driverId, 10);
  if (isNaN(driverId)) driverId = 10;

  try {
    // 1. Fetch active ride offer for the given driver with hub name resolution
    const offerQuery = await pool.query(
      `SELECT ro.id, ro.route_id, ro.status, ro.start_location, ro.end_location, ro.departure_time, 
              ro.total_seats, ro.available_seats, ro.price_per_seat, ro.pickup_lat, ro.pickup_lng,
              ro.accepted_passengers, ro.driver_id,
              h_start.name AS start_hub_name,
              h_end.name AS end_hub_name
       FROM ride_offers ro
       LEFT JOIN hubs h_start ON ro.start_location::integer = h_start.id
       LEFT JOIN hubs h_end ON ro.end_location::integer = h_end.id
       WHERE ro.driver_id = $1 
         AND ro.status IN ('pending', 'proposed', 'accepted', 'arrived', 'inprogress')
       ORDER BY ro.created_at DESC 
       LIMIT 1`,
      [driverId]
    );

    // 2. Return 200 OK with hasActiveProposal: false if no active offer exists
    if (offerQuery.rows.length === 0) {
      return res.status(200).json({ 
        hasActiveProposal: false, 
        message: "No active cluster offers found for this driver." 
      });
    }

    const offer = offerQuery.rows[0];

    // 3. Safely parse accepted_passengers array from PostgreSQL
    let rawAcceptedIds = offer.accepted_passengers || [];
    let acceptedIdsArray = [];

    if (Array.isArray(rawAcceptedIds)) {
      acceptedIdsArray = rawAcceptedIds.map(id => parseInt(id, 10)).filter(id => !isNaN(id));
    } else if (typeof rawAcceptedIds === "string") {
      acceptedIdsArray = rawAcceptedIds
        .replace(/[{}]/g, "")
        .split(",")
        .map(id => parseInt(id.trim(), 10))
        .filter(id => !isNaN(id));
    }

    // 4. Fetch ONLY passengers linked strictly to THIS active ride offer ID
    // (Prevents duplicate historical records from leaking in from previous completed rides)
    const passengersQuery = await pool.query(
      `SELECT rp.user_id, rp.status, 
              COALESCE(u.full_name, 'Passenger ' || rp.user_id) AS passenger_name, 
              u.phone_number 
       FROM route_pool rp
       LEFT JOIN users u ON rp.user_id = u.id
       WHERE rp.matched_ride_offer_id = $1
       ORDER BY rp.joined_at ASC`,
      [offer.id]
    );

    // 5. Map passengers and prioritize 'accepted' state if present in accepted_passengers array
    const formattedPassengers = passengersQuery.rows.map(passenger => {
      const pId = parseInt(passenger.user_id, 10);
      const dbStatus = passenger.status ? passenger.status.trim().toLowerCase() : "";
      const isAccepted = acceptedIdsArray.includes(pId);

      // Preserve advanced statuses (like 'arrived', 'picked_up', 'completed') from route_pool
      let finalStatus = dbStatus;
      if (!dbStatus || dbStatus === "pending" || dbStatus === "matched") {
        if (isAccepted) {
          finalStatus = "accepted";
        } else {
          finalStatus = "matched";
        }
      }

      return {
        user_id: pId,
        status: finalStatus,
        name: passenger.passenger_name,
        phone_number: passenger.phone_number
      };
    });

    const totalSeats = parseInt(offer.total_seats, 10) || 4;
    const computedAvailableSeats = Math.max(0, totalSeats - acceptedIdsArray.length);

    // 6. Return response payload with resolved hub names
    return res.status(200).json({
      hasActiveProposal: true,
      id: offer.id,
      ride_offer_id: offer.id,
      route_id: offer.route_id,
      status: offer.status,
      start_location: offer.start_location,
      end_location: offer.end_location,
      hubName: offer.start_hub_name || offer.start_location || "Megenagna Departure Hub",
      destinationName: offer.end_hub_name || offer.end_location || "Piassa",
      departure_time: offer.departure_time,
      total_seats: totalSeats,
      available_seats: computedAvailableSeats,
      price_per_seat: offer.price_per_seat || 80.00,
      pickup_lat: offer.pickup_lat,
      pickup_lng: offer.pickup_lng,
      accepted_passengers: acceptedIdsArray,
      passengers: formattedPassengers
    });

  } catch (err) {
    console.error("Cluster offer fetch error:", err);
    return res.status(500).json({ error: "Internal server error retrieving driver cluster offer." });
  }
});



















// Express Endpoint: GET /api/rides/active-accepted-offer
app.get("/api/rides/active-accepted-offer", async (req, res) => {
  try {
    // 1. Get current active offer for driver (returns 'accepted' OR 'arrived')
    const offerQuery = `
      SELECT id, route_id, status 
      FROM ride_offers 
      WHERE status IN ('accepted', 'arrived') 
      ORDER BY created_at DESC 
      LIMIT 1;
    `;
    const offerRes = await pool.query(offerQuery);

    if (offerRes.rows.length === 0) {
      return res.status(404).json({ message: "No active offer found." });
    }

    const offer = offerRes.rows[0];

    // 2. Fetch all joined passengers for this offer from route_pool
    const passengersQuery = `
      SELECT rp.id, rp.user_id, u.full_name, u.phone_number, u.profile_photo_url, rp.status
      FROM route_pool rp
      JOIN users u ON rp.user_id = u.id
      WHERE rp.matched_ride_offer_id = $1 AND rp.status IN ('accepted', 'arrived', 'matched');
    `;
    const passRes = await pool.query(passengersQuery, [offer.id]);

    return res.json({
      id: offer.id,
      route_id: offer.route_id,
      status: offer.status, // 👈 CRITICAL: returning 'arrived' if already pressed
      passengers: passRes.rows // 👈 CRITICAL: returning the actual rider count
    });
  } catch (err) {
    console.error("Error fetching active offer state:", err.message);
    return res.status(500).json({ error: "Internal server error" });
  }
});


// ==================================================================
// DRIVER ACCEPT DISPATCH MANIFEST ENDPOINT
// POST /api/rides/driver-accept
// ==================================================================
app.post("/api/rides/driver-accept", async (req, res) => {
  const { driver_id } = req.body;

  if (!driver_id) {
    return res.status(400).json({ error: "Missing required parameter: driver_id." });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Identify and lock the active pending ride offer for this driver
    const activeOfferQuery = `
      SELECT id, route_id, accepted_passengers, total_seats 
      FROM ride_offers 
      WHERE driver_id = $1 
        AND LOWER(TRIM(status)) = 'pending'
      ORDER BY created_at DESC 
      LIMIT 1 
      FOR UPDATE
    `;
    const offerRes = await client.query(activeOfferQuery, [driver_id]);

    if (offerRes.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: "No active pending dispatch offer found for this driver." 
      });
    }

    const offer = offerRes.rows[0];
    const ride_offer_id = offer.id; // Resolved dynamically from DB
    const { route_id, total_seats } = offer;

    // 2. Safely parse accepted_passengers (handles JS Array or Postgres string "{12,17}")
    let confirmedPassengerIds = [];
    if (offer.accepted_passengers) {
      if (Array.isArray(offer.accepted_passengers)) {
        confirmedPassengerIds = offer.accepted_passengers.map((id) => parseInt(id, 10));
      } else if (typeof offer.accepted_passengers === "string") {
        confirmedPassengerIds = offer.accepted_passengers
          .replace(/[{}]/g, "")
          .split(",")
          .map((id) => parseInt(id.trim(), 10))
          .filter((id) => !isNaN(id));
      }
    }

    // 3. Minimum requirement validation
    if (confirmedPassengerIds.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: "Cannot accept dispatch. Minimum core passenger threshold (2) not met." 
      });
    }

    const finalAvailableSeats = total_seats - confirmedPassengerIds.length;
    
    // 4. Update the master ride offer status to 'accepted'
    const updateOfferQuery = `
      UPDATE ride_offers 
      SET status = 'accepted', 
          available_seats = $1 
      WHERE id = $2
    `;
    await client.query(updateOfferQuery, [finalAvailableSeats, ride_offer_id]);

    // 5. Sync active passenger roster in route_pool to 'accepted'
    if (confirmedPassengerIds.length > 0) {
      const updatePassengerPoolQuery = `
        UPDATE route_pool 
        SET status = 'accepted' 
        WHERE matched_ride_offer_id = $1 
          AND user_id IN (${confirmedPassengerIds.map((_, i) => `$${i + 2}`).join(",")})
      `;
      await client.query(updatePassengerPoolQuery, [ride_offer_id, ...confirmedPassengerIds]);
    }

    // 🟢 6. UPDATE DRIVER QUEUE LOGS (Record exit time for hybrid wait analytics)
    const updateQueueQuery = `
      UPDATE driver_queue 
      SET status = 'dispatched', 
          exited_at = NOW() 
      WHERE driver_id = $1 
        AND LOWER(status) = 'queued'
    `;
    await client.query(updateQueueQuery, [driver_id]);

    // 7. Resolve pickup hub ID for auto-matching background engine
    let targetHubId = 1; 
    const hubLookupQuery = `
      SELECT pickup_hub_id 
      FROM route_pool 
      WHERE matched_ride_offer_id = $1 
      LIMIT 1
    `;
    const hubRes = await client.query(hubLookupQuery, [ride_offer_id]);
    if (hubRes.rows.length > 0 && hubRes.rows[0].pickup_hub_id) {
      targetHubId = parseInt(hubRes.rows[0].pickup_hub_id, 10);
    }

    // 8. Recipient recycle: reset unaccepted riders back to 'searching' pool
    if (confirmedPassengerIds.length > 0) {
      const resetLeftoverPoolQuery = `
        UPDATE route_pool 
        SET status = 'searching', matched_ride_offer_id = NULL 
        WHERE matched_ride_offer_id = $1 
          AND user_id NOT IN (${confirmedPassengerIds.map((_, i) => `$${i + 2}`).join(",")})
        RETURNING user_id
      `;
      
      const resetRes = await client.query(resetLeftoverPoolQuery, [ride_offer_id, ...confirmedPassengerIds]);
      console.log(`[Corridor Dispatch] Locked Offer #${ride_offer_id} for Driver ${driver_id}. Confirmed: ${confirmedPassengerIds.length}, Recycled: ${resetRes.rows.length}`);
    }

    await client.query("COMMIT");

    // 9. Asynchronously trigger next matching cycle
    executeCorridorAutoMatch(pool, route_id, targetHubId)
      .then(() => console.log(`[AutoMatch] Execution loop context processed cleanly for Route ${route_id}`))
      .catch((err) => console.error("[AutoMatch Error] Async dispatch node loop failure:", err));

    return res.status(200).json({
      success: true,
      message: "Manifest and passenger pool locked in as 'accepted'.",
      rideOfferId: ride_offer_id,
      driverId: parseInt(driver_id, 10),
      acceptedCount: confirmedPassengerIds.length,
      availableSeats: finalAvailableSeats
    });

  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Critical failure during driver-accept execution sequence:", error);
    return res.status(500).json({ error: "Internal transaction failure inside processing node." });
  } finally {
    client.release();
  }
});






app.post('/api/rides/driver-decline', async (req, res) => {
  const { ride_offer_id, driver_id } = req.body;

  if (!ride_offer_id || !driver_id) {
    return res.status(400).json({ error: "Missing required ride_offer_id or driver_id parameters." });
  }

  const client = await pool.connect();
  try {
    // Start transaction to guarantee atomic rollbacks if anything fails
    await client.query('BEGIN');

    // 1. Mark the ride offer as rejected for this driver
    const offerQuery = `
      UPDATE ride_offers
      SET status = 'rejected'
      WHERE id = $1 AND driver_id = $2
      RETURNING id;
    `;
    const offerResult = await client.query(offerQuery, [ride_offer_id, driver_id]);

    if (offerResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: "Active ride offer not found or unauthorized driver." });
    }

    // 2. Release pooled passengers: clear their matched offer ID and return them to the search queue
    const releasePoolQuery = `
      UPDATE route_pool
      SET matched_ride_offer_id = NULL,
          status = 'searching'
      WHERE matched_ride_offer_id = $1;
    `;
    await client.query(releasePoolQuery, [ride_offer_id]);

    // Commit changes
    await client.query('COMMIT');
    
    return res.status(200).json({ 
      success: true, 
      message: "Offer cluster declined cleanly and passengers returned to the matching queue." 
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error("Error handling driver decline & queue release:", err);
    return res.status(500).json({ error: "Internal Write Process Failure during driver decline." });
  } finally {
    client.release();
  }
});
//session timeout handler for drivers who have not accepted the offer within the time limit
app.post('/api/rides/driver-expired', async (req, res) => {
  const { ride_offer_id, driver_id } = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const currentOfferRes = await client.query(
      `SELECT route_id, start_location, end_location 
       FROM ride_offers 
       WHERE id = $1`,
      [ride_offer_id]
    );

    if (currentOfferRes.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: "Ride offer not found" });
    }

    const { route_id: rId, start_location, end_location } = currentOfferRes.rows[0];

    await client.query(
      `UPDATE route_pool 
       SET matched_ride_offer_id = NULL, status = 'searching' 
       WHERE matched_ride_offer_id = $1`,
      [ride_offer_id]
    );

    await client.query(
      `UPDATE ride_offers SET status = 'processing' WHERE id = $1`,
      [ride_offer_id]
    );

    await executeCorridorAutoMatch(client, rId);

    const matchCheckRes = await client.query(
      `SELECT COUNT(*) FROM route_pool 
       WHERE matched_ride_offer_id IS NOT NULL 
         AND matched_ride_offer_id != $1`,
      [ride_offer_id]
    );
    const successfullyMatchedCount = parseInt(matchCheckRes.rows[0].count, 10);

    let actionType = "requeued";

    if (successfullyMatchedCount > 0) {
      await client.query(
        `UPDATE ride_offers SET status = 'expired' WHERE id = $1`,
        [ride_offer_id]
      );
      await client.query(
        `UPDATE driver_queue 
         SET status = 'queued' 
         WHERE driver_id = $1 AND route_id = $2`,
        [driver_id, rId]
      );
      actionType = "requeued";
    } else {
      await client.query(
        `DELETE FROM ride_offers WHERE id = $1`,
        [ride_offer_id]
      );
      await client.query(
        `DELETE FROM driver_queue 
         WHERE driver_id = $1 AND route_id = $2`,
        [driver_id, rId]
      );
      actionType = "deleted";
    }

    await client.query('COMMIT');

    return res.json({ 
      success: true, 
      action: actionType,
      message: actionType === "requeued" 
        ? "Offer expired and accepted passengers successfully migrated." 
        : "Offer expired, ride offer and driver queue entry deleted." 
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error("❌ Error in driver-expired handler:", err);
    return res.status(500).json({ error: "Internal server error" });
  } finally {
    client.release();
  }
});


// GET /api/rides/driver-active-session/:driverId
app.get("/api/rides/driver-active-session/:driverId", async (req, res) => {
  try {
    const { driverId } = req.params;
    const parsedDriverId = parseInt(driverId, 10);

    if (isNaN(parsedDriverId)) {
      return res.status(400).json({ error: "Invalid driver ID format." });
    }

    // 🎯 REFACTOR: Fetch the absolute latest trip record created by this driver
    const result = await pool.query(
      `SELECT id, route_id, status, driver_id, created_at
       FROM ride_offers 
       WHERE driver_id = $1 
       ORDER BY created_at DESC 
       LIMIT 1;`,
      [parsedDriverId]
    );

    if (result.rows.length === 0) {
      return res.status(200).json({ hasActiveSession: false });
    }

    const latestOffer = result.rows[0];
    const normalizedStatus = latestOffer.status.trim().toLowerCase();

    // 1. If it's an ongoing active trip, pass it up instantly
    if (normalizedStatus !== 'cancelled' && normalizedStatus !== 'completed') {
      return res.status(200).json({
        hasActiveSession: true,
        rideOfferId: latestOffer.id,
        routeId: latestOffer.route_id,
        status: latestOffer.status.trim(),
        driverId: latestOffer.driver_id
      });
    }

    // 2. If it's 'completed' but happened within the last 15 minutes, route to Earnings
    if (normalizedStatus === 'completed') {
      const completionTime = new Date(latestOffer.joined_at).getTime();
      const currentTime = new Date().getTime();
      const minutesElapsed = (currentTime - completionTime) / (1000 * 60);

      if (minutesElapsed < 15) { // ⏳ 15-minute persistent window
        return res.status(200).json({
          hasActiveSession: true, // Mark true to force interception lock
          rideOfferId: latestOffer.id,
          routeId: latestOffer.route_id,
          status: 'completed',
          driverId: latestOffer.driver_id
        });
      }
    }

    // If it's a cold/stale completed trip, let them access manual route config
    return res.status(200).json({ hasActiveSession: false });

  } catch (err) {
    console.error("❌ GATEWAY FAULT:", err.message);
    return res.status(500).json({ error: err.message });
  }
});



app.get("/api/rides/status/:rideOfferId", async (req, res) => {
  try {
    const { rideOfferId } = req.params;

    const rideQuery = await pool.query(
      `SELECT id, start_location, end_location, total_seats, price_per_seat, status, accepted_passengers
       FROM ride_offers 
       WHERE id = $1 LIMIT 1;`,
      [parseInt(rideOfferId, 10)]
    );

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "Active ride offer targets not found." });
    }

    const offer = rideQuery.rows[0];
    
    // Dynamically calculate passenger counts based on the integer array layout length
    const passengerCount = Array.isArray(offer.accepted_passengers) 
      ? offer.accepted_passengers.length 
      : 0;

    return res.json({
      id: offer.id,
      start_location: offer.start_location,
      end_location: offer.end_location,
      total_seats: offer.total_seats,
      price_per_seat: offer.price_per_seat,
      status: offer.status,
      passenger_count: passengerCount
    });

  } catch (err) {
    console.error("❌ ERROR FETCHING LIVE RIDE METRICS:", err.message);
    return res.status(500).json({ error: "Internal server error reading ride states." });
  }
});


// Soft-cancel passenger pool request by updating status to 'cancelled'
// PATCH Passenger cancels while in searching or matched state

app.patch('/api/rides/solo/status', async (req, res) => {
  try {
    const { ride_id, status } = req.body;

    if (!ride_id || !status) {
      return res.status(400).json({ success: false, message: "Missing ride_id or status." });
    }

    const query = `
      UPDATE ride_request_solo 
      SET status = $1, updated_at = NOW() 
      WHERE id = $2 
      RETURNING *
    `;
    const result = await pool.query(query, [status, ride_id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "Ride request not found." });
    }

    const updatedRide = result.rows[0];

    // 🚀 USE req.io (matching your middleware setup)
    const io = req.io; 
    if (io) {
      const room = `ride_${ride_id}`;
      
      if (status === 'arrived') {
        io.to(room).emit('solo_driver_arrived', { ride_id, status });
      } else if (status === 'in_trip') {
        io.to(room).emit('solo_trip_started', { ride_id, status });
        // Also emit standard names just in case client listens to them
        io.to(room).emit('trip_started', { ride_id, status });
      } else if (status === 'completed') {
        io.to(room).emit('trip_completed', { ride_id, status });
      }
      
      io.to(room).emit('ride_status_changed', { ride_id, status });
      io.to(room).emit('solo_ride_status_updated', { ride_id, status });
    }

    res.json({ success: true, ride: updatedRide });
  } catch (err) {
    console.error("❌ Error updating ride_request_solo status:", err);
    res.status(500).json({ success: false, error: "Internal server error." });
  }
});



















/* ========================================================================= 
   🚀 ENDPOINT 2: UPDATE STATUS TO ARRIVED (THE ARRIVAL BUTTON MATRIX) DRIVER
   ========================================================================= */
app.post("/api/rides/arrive", async (req, res) => {
  // 🛡️ GUARD: Prevent crash if req.body isn't parsed/populated
  if (!req || !req.body) {
    return res.status(400).json({ 
      error: "Malformed request payload. Ensure express.json() middleware is active." 
    });
  }

  const { driver_id, driverId } = req.body;
  const targetDriverId = parseInt(driver_id || driverId, 10);

  if (!targetDriverId || isNaN(targetDriverId)) {
    return res.status(400).json({ error: "Missing or invalid required parameter: driver_id" });
  }

  try {
    // Update ONLY the ride_offers table status to 'arrived'
    const updateRideOfferQuery = await pool.query(
      `UPDATE ride_offers 
       SET status = 'arrived' 
       WHERE driver_id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived', 'matched')
       RETURNING id, driver_id, status;`,
      [targetDriverId]
    );

    if (updateRideOfferQuery.rows.length === 0) {
      return res.status(404).json({ 
        error: `No active ride offer found for driver ID ${targetDriverId} to mark as arrived.` 
      });
    }

    const activeRide = updateRideOfferQuery.rows[0];

    return res.status(200).json({
      success: true,
      message: "Driver arrival state updated to arrived in ride_offers table.",
      ride_offer_id: activeRide.id,
      driver_id: targetDriverId,
      status: "arrived"
    });

  } catch (err) {
    console.error("❌ ARRIVAL SYSTEM FAULT:", err.stack || err.message);
    return res.status(500).json({ error: `Internal execution error: ${err.message}` });
  }
});
















// GET /api/rides/driver-active-session/:driverId
app.get("/api/rides/driver-active-session/:driverId", async (req, res) => {
  try {
    const { driverId } = req.params;

    if (!driverId) {
      return res.status(400).json({ error: "Missing driverId tracking parameter." });
    }

    // 🎯 FIX: Force LOWER() handling to ensure camelCase 'inTransit' isn't dropped by strict matches
    const result = await pool.query(
      `SELECT id, route_id, status 
       FROM ride_offers 
       WHERE driver_id = $1 
         AND LOWER(status) NOT IN ('cancelled', 'completed')
       ORDER BY created_at DESC 
       LIMIT 1;`,
      [parseInt(driverId, 10)]
    );

    // If no row is open, tell the gatekeeper it's safe to stay on CreateCorridorScreen
    if (result.rows.length === 0) {
      return res.status(200).json({ 
        hasActiveSession: false,
        message: "No active or ongoing corridor sessions found." 
      });
    }

    const activeOffer = result.rows[0];
    console.log(`📡 Gateway API serving active offer #${activeOffer.id} with status: [${activeOffer.status}]`);

    // Return the response keys structured for your frontend switch matrix
    return res.status(200).json({
      hasActiveSession: true,
      rideOfferId: activeOffer.id,
      routeId: activeOffer.route_id,
      status: activeOffer.status // Passes 'inTransit' straight up
    });

  } catch (err) {
    console.error("❌ GATEKEEPER SERVICE REJECT:", err.message);
    return res.status(500).json({ error: `Internal gatekeeper database fault: ${err.message}` });
  }
});

// POST: /api/rides/complete

app.post("/api/rides/complete", async (req, res) => {
  const { driver_id, ride_offer_id } = req.body;

  if (!driver_id && !ride_offer_id) {
    return res.status(400).json({ error: "Missing driver_id or ride_offer_id parameters." });
  }

  try {
    let targetRideOfferId = ride_offer_id;
    let pricePerSeat = 0;

    // 1. If driver_id is provided, find the active ride offer (prioritizing 'in_trip')
    if (driver_id && !targetRideOfferId) {
      const offerCheck = await pool.query(
        `SELECT id, price_per_seat, status 
         FROM ride_offers 
         WHERE driver_id = $1 AND LOWER(TRIM(status)) IN ('in_trip', 'accepted', 'arrived', 'heading_to_pickup')
         ORDER BY CASE WHEN LOWER(TRIM(status)) = 'in_trip' THEN 1 ELSE 2 END, id DESC 
         LIMIT 1`,
        [driver_id]
      );

      if (offerCheck.rows.length === 0) {
        return res.status(404).json({ error: "No active ride offer found for this driver." });
      }

      targetRideOfferId = offerCheck.rows[0].id;
      pricePerSeat = parseFloat(offerCheck.rows[0].price_per_seat) || 0;
    } else {
      // Fallback lookup using direct ride_offer_id
      const offerCheck = await pool.query(
        "SELECT price_per_seat FROM ride_offers WHERE id = $1",
        [targetRideOfferId]
      );

      if (offerCheck.rows.length === 0) {
        return res.status(404).json({ error: "Ride offer target profile instance not found." });
      }

      pricePerSeat = parseFloat(offerCheck.rows[0].price_per_seat) || 0;
    }

    // 2. Synchronize associated users in route_pool to 'completed'
    const passengerUpdate = await pool.query(
      `UPDATE route_pool 
       SET status = 'completed'
       WHERE matched_ride_offer_id = $1
       RETURNING id, user_id, status`,
      [targetRideOfferId]
    );

    const completedRidersCount = passengerUpdate.rowCount || 0;
    const calculatedFinalPrice = completedRidersCount * pricePerSeat;

    // 3. Finalize parent driver offer row state to 'completed'
    const offerUpdate = await pool.query(
      `UPDATE ride_offers 
       SET status = 'completed', 
           final_price = $1
       WHERE id = $2 
       RETURNING id, status, final_price`,
      [calculatedFinalPrice, targetRideOfferId]
    );

    console.log(`🏁 Transactional Lifecycle Sealed for Corridor Run ID #${targetRideOfferId} (Driver ID #${driver_id || 'N/A'}):`);
    console.log(`   🔹 ride_offers status -> completed (final_price: ETB ${calculatedFinalPrice})`);
    console.log(`   🔹 route_pool passengers -> ${completedRidersCount} records synchronized`);

    return res.status(200).json({
      success: true,
      message: "Corridor parameters successfully updated to completed state profiles.",
      rideOffer: offerUpdate.rows[0],
      updatedRidersCount: completedRidersCount,
      finalPrice: calculatedFinalPrice
    });

  } catch (err) {
    console.error("Database operational fault inside completion transaction execution:", err);
    return res.status(500).json({ error: "Internal database tracking matrix generation runtime fault." });
  }
});



// ==================================================================
// 1. PASSENGER STATUS POLLING ENDPOINT
// GET /api/passenger-livetracking/status/:userId
// ==================================================================
app.get("/api/passenger-livetracking/status/:userId", async (req, res) => {
  const targetUserId = parseInt(req.params.userId, 10);

  if (isNaN(targetUserId)) {
    return res.status(400).json({ error: "Invalid userId parameter." });
  }

  try {
    const statusQuery = `
      SELECT 
        rp.status, 
        rp.matched_ride_offer_id, 
        COALESCE(ro.final_price, ro.price_per_seat, 80) AS final_price
      FROM route_pool rp
      LEFT JOIN ride_offers ro ON rp.matched_ride_offer_id = ro.id
      WHERE rp.user_id = $1 
        AND LOWER(TRIM(rp.status)) NOT IN ('cancelled', 'expired', 'idle')
      ORDER BY rp.joined_at DESC
      LIMIT 1;
    `;

    const result = await pool.query(statusQuery, [targetUserId]);

    if (result.rows.length === 0) {
      return res.status(200).json({ 
        status: "idle", 
        matched_ride_offer_id: null,
        finalPrice: 80 
      });
    }

    const row = result.rows[0];
    return res.status(200).json({
      status: row.status.toLowerCase().trim(),
      matched_ride_offer_id: row.matched_ride_offer_id,
      finalPrice: parseFloat(row.final_price),
    });
  } catch (err) {
    console.error("❌ Error fetching passenger status:", err.stack || err);
    return res.status(500).json({ error: "Failed to fetch passenger status." });
  }
});



// ==================================================================
// HELPER: Haversine Great-Circle Distance (in kilometers)
// ==================================================================
function calculateHaversineDistance(lat1, lon1, lat2, lon2) {
  if (!lat1 || !lon1 || !lat2 || !lon2) return 0;
  const R = 6371; // Earth's radius in km
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}


// ==================================================================
// 2. PASSENGER LIVE TELEMETRY & TRACKING ENDPOINT
// GET /api/rides/passenger-track/:rideOfferId/user/:userId
// ==================================================================

app.get("/api/rides/passenger-track/:rideOfferId/user/:userId", async (req, res) => {
  const { rideOfferId, userId } = req.params;
  const targetRideOfferId = parseInt(rideOfferId, 10);
  const targetUserId = parseInt(userId, 10);

  if (isNaN(targetRideOfferId) || isNaN(targetUserId)) {
    return res.status(400).json({ error: "Invalid parameter IDs." });
  }

  try {
    // 1. Query Ride Offer, Driver, Vehicle, and Hub details
    const rideQuery = `
      SELECT 
        r.id,
        r.status AS driver_status,
        r.price_per_seat,
        CASE 
          WHEN r.final_price IS NOT NULL AND r.final_price > 0 THEN r.final_price
          ELSE COALESCE(r.price_per_seat, 80)
        END AS final_price,
        r.pickup_lat,
        r.pickup_lng,
        r.accepted_passengers,
        r.start_location,
        r.end_location,
        u.full_name AS driver_name,
        COALESCE(u.rating, 4.9) AS driver_rating,
        v.make AS vehicle_make,
        v.model AS vehicle_model,
        v.plate_number,
        ph.name AS pickup_hub_name,
        ph.latitude AS pickup_hub_lat,
        ph.longitude AS pickup_hub_lng,
        dh.name AS dropoff_hub_name,
        dh.latitude AS dropoff_hub_lat,
        dh.longitude AS dropoff_hub_lng
      FROM ride_offers r
      LEFT JOIN users u ON r.driver_id = u.id
      LEFT JOIN vehicles v ON r.vehicle_id = v.id
      LEFT JOIN hubs ph ON CASE WHEN r.start_location ~ '^[0-9]+$' THEN CAST(r.start_location AS INTEGER) ELSE NULL END = ph.id
      LEFT JOIN hubs dh ON CASE WHEN r.end_location ~ '^[0-9]+$' THEN CAST(r.end_location AS INTEGER) ELSE NULL END = dh.id
      WHERE r.id = $1
      LIMIT 1;
    `;

    const rideRes = await pool.query(rideQuery, [targetRideOfferId]);

    if (!rideRes.rows.length) {
      return res.status(404).json({ error: "Active ride offer not found." });
    }

    const trip = rideRes.rows[0];

    // 2. Resolve Coordinates from available columns
    const pickupLat = parseFloat(trip.pickup_lat || trip.pickup_hub_lat || 9.0115);
    const pickupLng = parseFloat(trip.pickup_lng || trip.pickup_hub_lng || 38.7945);
    const dropoffLat = parseFloat(trip.dropoff_hub_lat || 8.9985);
    const dropoffLng = parseFloat(trip.dropoff_hub_lng || 38.7855);

    // Dynamic vehicle location
    const carLat = pickupLat;
    const carLng = pickupLng;

    // 3. Distance & ETA Calculation
    const currentStatus = (trip.driver_status || "in_trip").toLowerCase().trim();
    const isHeadingToPickup = ["accepted", "arrived", "heading_to_pickup", "matched"].includes(currentStatus);

    const destinationLat = isHeadingToPickup ? pickupLat : dropoffLat;
    const destinationLng = isHeadingToPickup ? pickupLng : dropoffLng;

    const distanceKm = calculateHaversineDistance(carLat, carLng, destinationLat, destinationLng);
    const AVERAGE_URBAN_SPEED_KMH = 25;
    const rawMinutes = (distanceKm / AVERAGE_URBAN_SPEED_KMH) * 60;
    const estimatedMins = Math.max(1, Math.round(rawMinutes));

    // 4. Fetch Active Passengers
    const passengersQuery = `
      SELECT 
        rp.user_id, 
        rp.status,
        u.full_name
      FROM route_pool rp
      LEFT JOIN users u ON rp.user_id = u.id
      WHERE rp.matched_ride_offer_id = $1 
        AND LOWER(TRIM(rp.status)) IN ('accepted', 'arrived', 'in_trip', 'completed')
    `;

    const passengersRes = await pool.query(passengersQuery, [targetRideOfferId]);
    let passengers = passengersRes.rows;

    if (passengers.length === 0 && Array.isArray(trip.accepted_passengers) && trip.accepted_passengers.length > 0) {
      const usersRes = await pool.query(
        `SELECT id AS user_id, 'accepted' AS status, full_name FROM users WHERE id = ANY($1::int[])`,
        [trip.accepted_passengers]
      );
      passengers = usersRes.rows;
    }

    // 5. Label Formatting
    const plate = trip.plate_number || trip.license_plate || "";
    const vehicleModel = `${trip.vehicle_make || ""} ${trip.vehicle_model || ""}`.trim() || "Corridor Shuttle";
    const vehicleDisplay = plate ? `${vehicleModel} • ${plate}` : vehicleModel;

    const pickupHubName = trip.pickup_hub_name || `Hub #${trip.start_location || "Origin"}`;
    const dropoffHubName = trip.dropoff_hub_name || `Hub #${trip.end_location || "Destination"}`;

    // 6. Output JSON Response
    return res.status(200).json({
      success: true,
      rideOfferId: trip.id,
      driver_status: trip.driver_status || "accepted",
      driverName: trip.driver_name || "Assigned Driver",
      vehicleInfo: vehicleDisplay,
      driverRating: parseFloat(trip.driver_rating).toFixed(1),
      finalPrice: parseFloat(trip.final_price),

      pickup_hub: pickupHubName,
      dropoff_hub: dropoffHubName,
      corridor: `${pickupHubName} ➔ ${dropoffHubName}`,

      etaMinutes: estimatedMins,
      distanceRemainingKm: parseFloat(distanceKm.toFixed(1)),
      passengersCount: passengers.length || trip.accepted_passengers?.length || 1,

      pickup_lat: pickupLat,
      pickup_lng: pickupLng,
      dropoff_lat: dropoffLat,
      dropoff_lng: dropoffLng,
      car_lat: carLat,
      car_lng: carLng,

      passengers: passengers.map((p) => ({
        user_id: p.user_id,
        full_name: p.full_name || `Rider ${p.user_id}`,
        status: p.status || "accepted",
      })),
    });
  } catch (err) {
    console.error("Error fetching passenger tracking details:", err.stack || err);
    return res.status(500).json({ error: "Failed to stream trip tracking details." });
  }
});

// ==================================================================
// PASSENGER HUB PRESENCE & STATUS ENDPOINT FOR LIVE TRACKING SCREEN
// GET /api/rides/presence-status/:rideOfferId/:userId
// ==================================================================
app.get("/api/rides/presence-status/:rideOfferId/:userId", async (req, res) => {
  const { rideOfferId, userId } = req.params;
  const targetRideOfferId = parseInt(rideOfferId, 10);
  const targetUserId = parseInt(userId, 10);

  if (isNaN(targetRideOfferId) || isNaN(targetUserId)) {
    return res.status(400).json({ 
      success: false, 
      error: "Invalid parameter IDs provided." 
    });
  }

  try {
    const rideQuery = `
      SELECT 
        r.id,
        r.status AS driver_status,
        r.pickup_lat,
        r.pickup_lng,
        r.accepted_passengers,
        r.start_location,
        r.end_location,
        u.full_name AS driver_name,
        v.make AS vehicle_make,
        v.model AS vehicle_model,
        v.plate_number,
        ph.name AS pickup_hub_name,
        ph.latitude AS pickup_hub_lat,
        ph.longitude AS pickup_hub_lng,
        dh.name AS dropoff_hub_name,
        dh.latitude AS dropoff_hub_lat,
        dh.longitude AS dropoff_hub_lng
      FROM ride_offers r
      LEFT JOIN users u ON r.driver_id = u.id
      LEFT JOIN vehicles v ON r.vehicle_id = v.id
      LEFT JOIN hubs ph ON CASE WHEN r.start_location ~ '^[0-9]+$' THEN CAST(r.start_location AS INTEGER) ELSE NULL END = ph.id
      LEFT JOIN hubs dh ON CASE WHEN r.end_location ~ '^[0-9]+$' THEN CAST(r.end_location AS INTEGER) ELSE NULL END = dh.id
      WHERE r.id = $1
      LIMIT 1;
    `;

    const rideRes = await pool.query(rideQuery, [targetRideOfferId]);

    if (!rideRes.rows.length) {
      return res.status(404).json({ 
        success: false, 
        error: "Active ride offer not found." 
      });
    }

    const trip = rideRes.rows[0];

    // 1. Resolve Coordinates for Hub Meetup Marker
    const pickupLat = parseFloat(trip.pickup_lat || trip.pickup_hub_lat || 9.0115);
    const pickupLng = parseFloat(trip.pickup_lng || trip.pickup_hub_lng || 38.7945);

    // 2. Fetch Active Co-Riders Manifest & Presence Statuses
    const passengersQuery = `
      SELECT 
        rp.user_id, 
        rp.status,
        u.full_name
      FROM route_pool rp
      LEFT JOIN users u ON rp.user_id = u.id
      WHERE rp.matched_ride_offer_id = $1 
        AND LOWER(TRIM(rp.status)) IN ('accepted', 'arrived', 'matched', 'in_trip', 'completed')
    `;

    const passengersRes = await pool.query(passengersQuery, [targetRideOfferId]);
    let passengers = passengersRes.rows;

    if (passengers.length === 0 && Array.isArray(trip.accepted_passengers) && trip.accepted_passengers.length > 0) {
      const usersRes = await pool.query(
        `SELECT id AS user_id, 'accepted' AS status, full_name FROM users WHERE id = ANY($1::int[])`,
        [trip.accepted_passengers]
      );
      passengers = usersRes.rows;
    }

    // 3. Label Formatting
    const plate = trip.plate_number || trip.license_plate || "";
    const vehicleModel = `${trip.vehicle_make || ""} ${trip.vehicle_model || ""}`.trim() || "Corridor Shuttle";
    const vehicleDisplay = plate ? `${vehicleModel} • ${plate}` : vehicleModel;

    const pickupHubName = trip.pickup_hub_name || `Hub #${trip.start_location || "Origin"}`;
    const dropoffHubName = trip.dropoff_hub_name || `Hub #${trip.end_location || "Destination"}`;

    // 4. Return Lightweight JSON Response
    return res.status(200).json({
      success: true,
      rideOfferId: trip.id,
      driver_status: trip.driver_status || "accepted",
      driverName: trip.driver_name || "Assigned Driver",
      vehicleInfo: vehicleDisplay,

      pickup_hub: pickupHubName,
      dropoff_hub: dropoffHubName,
      corridor: `${pickupHubName} ➔ ${dropoffHubName}`,

      pickup_lat: pickupLat,
      pickup_lng: pickupLng,

      passengers: passengers.map((p) => ({
        user_id: p.user_id,
        userId: p.user_id,
        full_name: p.full_name || `Rider ${p.user_id}`,
        status: p.status || "accepted",
      })),
    });
  } catch (err) {
    console.error("❌ Error fetching hub presence status:", err.stack || err);
    return res.status(500).json({ 
      success: false, 
      error: "Failed to fetch hub presence tracking status." 
    });
  }
});


app.get("/api/rides/proposal-manifest/:userId", async (req, res) => {
  const { userId } = req.params;
  const targetUserId = parseInt(userId, 10);

  if (isNaN(targetUserId)) {
    return res.status(400).json({ error: "Invalid userId" });
  }

  try {
    const myPoolRes = await pool.query(`
      SELECT rp.matched_ride_offer_id, rp.status, h.name AS hub_name, h.latitude AS hub_lat, h.longitude AS hub_lng
      FROM route_pool rp
      LEFT JOIN hubs h ON rp.pickup_hub_id = h.id
      WHERE rp.user_id = $1 
        AND (rp.status ILIKE '%matched%' OR rp.status ILIKE '%accepted%')
      ORDER BY rp.joined_at DESC 
      LIMIT 1;
    `, [targetUserId]);

    if (myPoolRes.rows.length === 0) {
      console.log(`⚠️ [Manifest] No active matched/accepted row found for User ${targetUserId}.`);
      return res.status(404).json({ error: "No active proposal found for user." });
    }

    const activeRideOfferId = myPoolRes.rows[0].matched_ride_offer_id;
    console.log(`✅ [Manifest] Found Active Pool for User ${targetUserId} -> Ride Offer #${activeRideOfferId}, Status: "${myPoolRes.rows[0].status}"`);

    const hub = {
      name: myPoolRes.rows[0].hub_name || "Designated Stop",
      latitude: parseFloat(myPoolRes.rows[0].hub_lat || 0),
      longitude: parseFloat(myPoolRes.rows[0].hub_lng || 0)
    };

    const offerRes = await pool.query(`
      SELECT status AS driver_status, accepted_passengers 
      FROM ride_offers 
      WHERE id = $1;
    `, [activeRideOfferId]);
    
    const rideOffer = offerRes.rows[0] || { driver_status: "pending", accepted_passengers: [] };

    // 🔍 JOIN users table to pull full_name alongside route_pool status
    const passengersRes = await pool.query(`
      SELECT rp.user_id, rp.status, u.full_name 
      FROM route_pool rp
      JOIN users u ON rp.user_id = u.id
      WHERE rp.matched_ride_offer_id = $1 
        AND (rp.status ILIKE '%matched%' OR rp.status ILIKE '%accepted%')
    `, [activeRideOfferId]);

    return res.json({
      rideOfferId: activeRideOfferId,
      driver_status: rideOffer.driver_status,
      accepted_passengers: rideOffer.accepted_passengers || [],
      hub: hub,
      passengers: passengersRes.rows
    });

  } catch (error) {
    console.error("📍 Proposal Manifest Error:", error.stack || error);
    return res.status(500).json({ error: "Failed to fetch proposal manifest" });
  }
});



// ==================================================================
// FETCH PAYMENT & DRIVER PHONE BY LOGGED-IN PASSENGER USER ID
// GET /api/rides/payment-details-by-user/:userId
// ==================================================================

app.get("/api/rides/payment-details-by-user/:userId", async (req, res) => {
  const { userId } = req.params;
  const targetUserId = parseInt(userId, 10);

  if (isNaN(targetUserId)) {
    return res.status(400).json({ success: false, error: "Invalid user ID." });
  }

  try {
    const query = `
      SELECT 
        r.id AS ride_offer_id,
        r.final_price,
        r.price_per_seat,
        COALESCE(u.full_name, 'Assigned Driver') AS driver_name,
        COALESCE(NULLIF(TRIM(u.phone_number), ''), 'Phone Not Available') AS driver_phone,
        ph.name AS pickup_hub_name,
        dh.name AS dropoff_hub_name
      FROM route_pool rp
      JOIN ride_offers r ON rp.matched_ride_offer_id = r.id
      LEFT JOIN users u ON r.driver_id = u.id
      LEFT JOIN hubs ph ON rp.pickup_hub_id = ph.id
      LEFT JOIN hubs dh ON rp.dropoff_hub_id = dh.id
      WHERE rp.user_id = $1
      ORDER BY rp.id DESC
      LIMIT 1;
    `;

    const result = await pool.query(query, [targetUserId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: "No active ride found for this user." });
    }

    const row = result.rows[0];
    const fixedPrice = parseFloat(row.final_price || row.price_per_seat || 80);
    const corridorName = `${row.pickup_hub_name || 'Origin'} ➔ ${row.dropoff_hub_name || 'Destination'}`;
    const driverPhone = row.driver_phone;

    return res.status(200).json({
      success: true,
      rideOfferId: row.ride_offer_id,
      fixedPrice: fixedPrice,
      corridor: corridorName,
      driverName: row.driver_name,
      driverPhone: driverPhone,
      phone_number: driverPhone
    });
  } catch (err) {
    console.error("❌ Error fetching payment details by user:", err.stack || err);
    return res.status(500).json({ success: false, error: "Failed to fetch corridor and driver details." });
  }
});


// ==================================================================
// FETCH AVAILABLE CORRIDORS FOR A HUB (WITH REAL-TIME SEAT COUNT) WITH TIME WAIT TIME & DRIVER QUEUE METRICS
// GET /api/routes/from/:hubName
// ==================================================================
app.get('/api/routes/live-corridors/:hubName', async (req, res) => {
  try {
    const { hubName } = req.params;

    const query = `
      SELECT 
        r.id AS route_id,
        r.hub_id,
        r.pickup_hub_id,
        r.drop_off_hub_id,
        r.pass_through_hub_id,
        h_pickup.name AS pickup_hub_name,
        h_drop.name AS destination_name,
        h_pass.name AS pass_through_name,
        COALESCE(r.recommended_base_price, 100) AS recommended_base_price,
        COALESCE(ro_latest.total_seats, 4) AS capacity,
        COALESCE(ro_latest.available_seats, 4) AS available_seats,
        ro_latest.id AS ride_offer_id,
        ro_latest.status AS offer_status,
        -- 🚗 Real-time driver queue count join
        COALESCE(driver_counts.waiting_drivers, 0)::INTEGER AS driver_queue_count,
        -- ⏱️ Average wait time calculated using entered_at & exited_at
        COALESCE(avg_wait_stats.avg_wait_mins, 0)::INTEGER AS average_wait_mins
      FROM routes r
      JOIN hubs h_pickup ON r.pickup_hub_id = h_pickup.id
      JOIN hubs h_drop ON r.drop_off_hub_id = h_drop.id
      LEFT JOIN hubs h_pass ON r.pass_through_hub_id = h_pass.id
      
      -- 🛡️ Prevent row duplication by grabbing the latest/active ride offer per route
      LEFT JOIN (
        SELECT DISTINCT ON (route_id) 
          id, total_seats, available_seats, status, route_id
        FROM ride_offers
        WHERE LOWER(status) IN ('pending', 'matched', 'active', 'inprogress')
        ORDER BY route_id, created_at DESC
      ) ro_latest ON r.id = ro_latest.route_id
      
      -- 🚗 Real-time driver count aggregated cleanly and independently
      LEFT JOIN (
        SELECT route_id, COUNT(driver_id) AS waiting_drivers
        FROM driver_queue
        WHERE LOWER(status) = 'queued'
        GROUP BY route_id
      ) driver_counts ON r.id = driver_counts.route_id

      -- ⏱️ Average wait time stats join using entered_at instead of created_at
      LEFT JOIN (
        SELECT 
          route_id, 
          ROUND(AVG(EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60)) AS avg_wait_mins
        FROM driver_queue
        WHERE LOWER(status) = 'dispatched' 
          AND exited_at IS NOT NULL
          AND entered_at IS NOT NULL
          AND exited_at >= NOW() - INTERVAL '12 hours'
        GROUP BY route_id
      ) avg_wait_stats ON r.id = avg_wait_stats.route_id

      WHERE LOWER(h_pickup.name) = LOWER($1);
    `;

    const result = await pool.query(query, [hubName]);
    console.log("🔥 CORRECTED SCHEMA ROWS WITH QUEUE & WAIT METRICS:", result.rows);
    return res.json(result.rows);
    
  } catch (err) {
    console.error("❌ Error fetching live corridors:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});


// 📴 Driver Go Offline Endpoint (Full Cleanup, Route Pool Reset & Matching Engine Trigger)
app.post('/api/rides/go-offline', async (req, res) => {
  const { driver_id } = req.body;

  if (!driver_id) {
    return res.status(400).json({ error: "Driver ID is required from AuthContext." });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Find all active/pending/matched ride offers for this driver
    const rideQuery = await client.query(
      "SELECT id FROM ride_offers WHERE driver_id = $1 AND status IN ('pending', 'matched', 'accepted')",
      [driver_id]
    );

    const rideOfferIds = rideQuery.rows.map(row => row.id);

    if (rideOfferIds.length > 0) {
      // 2. Nullify matched_ride_offer_id in route_pool and reset status for affected riders
      await client.query(
        "UPDATE route_pool SET matched_ride_offer_id = NULL, status = 'searching' WHERE matched_ride_offer_id = ANY($1::int[]) AND status IN ('matching', 'matched', 'accepted')",
        [rideOfferIds]
      );

      // 3. Delete the ride offer(s) from ride_offer table
      await client.query(
        "DELETE FROM ride_offers WHERE id = ANY($1::int[])",
        [rideOfferIds]
      );
    }

    // 4. Remove the driver from driver_queue where status is 'queued'
    const queueDeleteResult = await client.query(
      "DELETE FROM driver_queue WHERE driver_id = $1 AND status = 'queued'",
      [driver_id]
    );

    await client.query('COMMIT');

    console.log(`📴 Driver [${driver_id}] went offline. Unlinked route_pool records, deleted ride offers, and cleared driver_queue.`);

    // 5. Invoke Matching Engine so unassigned riders are immediately re-processed
    try {
      if (typeof runMatchingEngine === 'function') {
        await runMatchingEngine();
        console.log("🔄 Matching engine successfully re-triggered after driver went offline.");
      }
    } catch (matchingErr) {
      console.error("⚠️ Warning: Matching engine invocation failed post-offline:", matchingErr);
    }

    res.json({
      success: true,
      message: "Successfully went offline, queue, ride offers, and pool links cleared."
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error("❌ Error processing driver go offline transaction:", err);
    res.status(500).json({ error: "Internal server error while processing go offline request." });
  } finally {
    client.release();
  }
});


app.get("/api/rides/TripSummary", async (req, res) => {
  const { driver_id, ride_offer_id } = req.query;

  if (!driver_id && !ride_offer_id) {
    return res.status(400).json({ error: "Driver ID or Ride Offer ID is required." });
  }

  try {
    let queryText = `
      SELECT ro.*, 
             COALESCE(h_start.name, 'Hub #' || ro.start_location) AS start_hub_name, 
             COALESCE(h_end.name, 'Hub #' || ro.end_location) AS end_hub_name,
             COALESCE(v.model, 'Standard Fleet') AS vehicle_model,
             COALESCE(v.make, '') AS vehicle_make,
             COALESCE(v.plate_number, 'N/A') AS vehicle_plate
      FROM ride_offers ro
      LEFT JOIN hubs h_start ON h_start.id = NULLIF(ro.start_location::text, '')::integer
      LEFT JOIN hubs h_end ON h_end.id = NULLIF(ro.end_location::text, '')::integer
      LEFT JOIN vehicles v ON ro.vehicle_id = v.id
    `;

    let queryParams = [];

    if (ride_offer_id) {
      queryText += ` WHERE ro.id = $1`;
      queryParams.push(ride_offer_id);
    } else {
      queryText += ` WHERE ro.driver_id = $1 ORDER BY ro.id DESC, ro.created_at DESC LIMIT 1`;
      queryParams.push(driver_id);
    }

    const rideQuery = await pool.query(queryText, queryParams);

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "No ride offer found for this driver session." });
    }

    const rideOffer = rideQuery.rows[0];
    const pricePerSeat = parseFloat(rideOffer.price_per_seat) || 0.00;

    let passengerIds = [];
    const rawPassengers = rideOffer.accepted_passengers;

    if (Array.isArray(rawPassengers)) {
      passengerIds = rawPassengers.map(id => parseInt(id, 10)).filter(id => !isNaN(id));
    } else if (typeof rawPassengers === 'string') {
      passengerIds = rawPassengers
        .replace(/[{}]/g, '')
        .split(',')
        .map(id => parseInt(id.trim(), 10))
        .filter(id => !isNaN(id));
    }

    let activePassengers = [];
    if (passengerIds.length > 0) {
      // Fetch rating and phone_number from users table
      const passengersQuery = await pool.query(
        `SELECT id AS user_id, 
                COALESCE(full_name, 'Rider #' || id) AS full_name,
                COALESCE(phone_number, 'N/A') AS phone_number,
                COALESCE(rating, 5.00) AS rating,
                COALESCE(profile_photo_url, '') AS profile_photo_url
         FROM users
         WHERE id = ANY($1::int[])`,
        [passengerIds]
      );
      activePassengers = passengersQuery.rows;
    }

    const totalPassengers = activePassengers.length;
    const grossEarnings = totalPassengers * pricePerSeat;
    const platformFee = grossEarnings * 0.10;
    const netPayout = rideOffer.final_price !== null && rideOffer.final_price !== undefined
      ? parseFloat(rideOffer.final_price) 
      : (grossEarnings - platformFee);

    return res.status(200).json({
      success: true,
      tripDetails: {
        rideOfferId: rideOffer.id,
        routeId: rideOffer.route_id,
        startHub: rideOffer.start_hub_name,
        endHub: rideOffer.end_hub_name,
        departureTime: rideOffer.departure_time,
        completedAt: rideOffer.created_at || new Date().toISOString(),
        vehicleInfo: {
          model: `${rideOffer.vehicle_make} ${rideOffer.vehicle_model}`.trim(),
          plate: rideOffer.vehicle_plate
        }
      },
      financials: {
        totalPassengers,
        pricePerSeat,
        grossEarnings: parseFloat(grossEarnings.toFixed(2)),
        platformFee: parseFloat(platformFee.toFixed(2)),
        netPayout: parseFloat(netPayout.toFixed(2))
      },
      passengers: activePassengers.map((p) => ({
        userId: p.user_id,
        fullName: p.full_name,
        phoneNumber: p.phone_number,
        rating: p.rating ? parseFloat(p.rating).toFixed(2) : "5.00",
        profilePhotoUrl: p.profile_photo_url
      }))
    });

  } catch (err) {
    console.error("Trip summary fetch error:", err);
    return res.status(500).json({ error: "Internal server error while fetching trip summary." });
  }
});


// DRAWER MENU ENDPOINTS FOR USER PROFILE SUMMARY AND TRIP HISTORY
// 1. Get user profile summary for drawer header
app.get('/api/users/profile/:userId', async (req, res) => {
  const { userId } = req.params;
  try {
    const result = await pool.query(
      `SELECT id, full_name, role, phone_number, rating, created_at, profile_picture FROM users WHERE id = $1`,
      [userId]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "User not found" });
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch user profile" });
  }
});

// 2. Get last 5 completed trips for trip history
app.get('/api/trips/history/:userId/:role', async (req, res) => {
  const { userId, role } = req.params;
  try {
    const query = role === 'driver'
      ? `SELECT id, corridor, final_price AS price, created_at FROM rides WHERE driver_id = $1 AND status = 'completed' ORDER BY created_at DESC LIMIT 5`
      : `SELECT rp.id, rp.route_id, rp.status, rp.created_at, r.corridor, r.final_price AS price 
         FROM route_pool rp 
         LEFT JOIN rides r ON rp.matched_ride_offer_id = r.id 
         WHERE rp.user_id = $1 AND rp.status = 'completed' ORDER BY rp.created_at DESC LIMIT 5`;

    const result = await pool.query(query, [userId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch trip history" });
  }
});

// SECTION FOR STANDARD SOLO TRIPS


app.get('/api/rides/autocomplete', async (req, res) => {
  try {
    const { input, location } = req.query; // location optional format: "lat,lng" for biasing results

    if (!input) {
      return res.status(400).json({ error: 'Input query is required' });
    }

    let url = `https://maps.googleapis.com/maps/api/place/autocomplete/json?input=${encodeURIComponent(
      input
    )}&key=${GOOGLE_MAPS_API_KEY}&components=country:et`; // Biased to Ethiopia (change country code if needed)

    if (location) {
      url += `&location=${location}&radius=50000`; // 50km bias around user current location
    }

    const response = await axios.get(url);

    const predictions = response.data.predictions.map((item) => ({
      placeId: item.place_id,
      mainText: item.structured_formatting.main_text,
      secondaryText: item.structured_formatting.secondary_text,
      description: item.description,
    }));

    return res.status(200).json({ success: true, predictions });
  } catch (error) {
    console.error('Google Places Autocomplete Error:', error.message);
    return res.status(500).json({ error: 'Failed to fetch places suggestions' });
  }
});

app.get('/api/rides/details', async (req, res) => {
  try {
    const { placeId } = req.query;

    if (!placeId) {
      return res.status(400).json({ error: 'Place ID is required' });
    }

    const url = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=geometry,formatted_address,name&key=${GOOGLE_MAPS_API_KEY}`;
    
    const response = await axios.get(url);
    const result = response.data.result;

    if (!result) {
      return res.status(404).json({ error: 'Place details not found' });
    }

    return res.status(200).json({
      success: true,
      place: {
        name: result.name,
        address: result.formatted_address,
        lat: result.geometry.location.lat,
        lng: result.geometry.location.lng,
      },
    });
  } catch (error) {
    console.error('Google Place Details Error:', error.message);
    return res.status(500).json({ error: 'Failed to fetch place details' });
  }
});

app.post('/api/rides/estimate-fare', async (req, res) => {
  try {
    const { origin_lat, origin_lng, destination_lat, destination_lng } = req.body;

    if (!origin_lat || !origin_lng || !destination_lat || !destination_lng) {
      return res.status(400).json({ success: false, message: 'Missing coordinate parameters.' });
    }

    // 1. Calculate distance natively in PostgreSQL using PostGIS (Longitude, Latitude order)
    const distQuery = `
      SELECT (ST_Distance(
        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
        ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography
      ) / 1000.0) * 1.25 AS calculated_distance_km;
    `;
    
    const distResult = await pool.query(distQuery, [
      parseFloat(origin_lng), 
      parseFloat(origin_lat), 
      parseFloat(destination_lng), 
      parseFloat(destination_lat)
    ]);

    const rawDist = parseFloat(distResult.rows[0]?.calculated_distance_km || 1.0);
    const distanceKm = Math.max(1, Math.round(rawDist * 10) / 10);

    // 2. Fetch active pricing rules from pricing_configs (ID 1 for Addis Ababa)
    let pricingConfig = {
      base_flag_down: 95.00,
      rate_per_km: 16.00,
      rate_per_minute: 1.50,
      minimum_fare: 120.00,
      fuel_multiplier: 1.00,
      surge_multiplier: 1.00
    };

    try {
      const dbResult = await pool.query(
        "SELECT * FROM pricing_configs WHERE id = 1 LIMIT 1"
      );
      if (dbResult.rows.length > 0) {
        pricingConfig = dbResult.rows[0];
      }
    } catch (dbErr) {
      console.warn("⚠️ Failed to load pricing config from DB, using fallback defaults:", dbErr.message);
    }

    // 3. Estimate duration based on city traffic speed (~3.5 mins per km)
    const estimatedMinutes = Math.round(distanceKm * 3.5);

    // 4. Compute formula matching the exact driver-side calculation logic
    const baseFee = parseFloat(pricingConfig.base_flag_down);
    const kmRate = parseFloat(pricingConfig.rate_per_km);
    const minRate = parseFloat(pricingConfig.rate_per_minute);
    const fuelMult = parseFloat(pricingConfig.fuel_multiplier);
    const surgeMult = parseFloat(pricingConfig.surge_multiplier);
    const minFare = parseFloat(pricingConfig.minimum_fare);

    let calculatedPrice = (baseFee + (distanceKm * kmRate) + (estimatedMinutes * minRate)) * fuelMult * surgeMult;

    // 5. Enforce floor minimum fare constraint (120 ETB)
    if (calculatedPrice < minFare) {
      calculatedPrice = minFare;
    }

    return res.json({
      success: true,
      distanceKm: distanceKm,
      durationText: `${estimatedMinutes} mins`,
      estimatedPrice: Math.round(calculatedPrice),
      breakdown: {
        baseFee,
        distanceKm,
        fuelMultiplier: fuelMult,
        surgeMultiplier: surgeMult
      }
    });

  } catch (err) {
    console.error("❌ Fare estimation calculation crash:", err);
    return res.status(500).json({ success: false, message: 'Server error calculating fare.' });
  }
});


function dispatchSoloRequestToDrivers(newRide) {
  if (!io) return;

  const offerPayload = {
    id: newRide.id,                // Primary key
    ride_id: newRide.id,           // Explicit ride identifier
    user_id: newRide.user_id,      // Passenger user ID
    passenger_id: newRide.user_id, // Alias for clarity
    pickup_lat: newRide.pickup_lat,
    pickup_lng: newRide.pickup_lng,
    destination_name: newRide.destination_name,
    destination_address: newRide.destination_address,
    price: newRide.price,
    distance_km: newRide.distance_km,
    status: newRide.status,
    created_at: newRide.created_at
  };

  console.log(`📡 Emitting 'new_ride_offer' for Passenger #${newRide.user_id} (Ride #${newRide.id})`);
  
  // Broadcast matching your driver screen's socket listener
  io.emit("new_ride_offer", offerPayload);
}


// Cancel active solo/standard ride request with strict user verification
app.post('/api/rides/solo/cancel', async (req, res) => {
  const { requestId, user_id } = req.body;

  if (!requestId || !user_id) {
    return res.status(400).json({ 
      success: false, 
      message: 'Missing required request ID or user ID.' 
    });
  }

  try {
    const updateQuery = `
      UPDATE ride_request_solo 
      SET status = 'cancelled', updated_at = NOW() 
      WHERE id = $1 AND user_id = $2 AND status = 'pending'
      RETURNING id, user_id, status;
    `;

    const result = await pool.query(updateQuery, [requestId, user_id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ 
        success: false, 
        message: 'Active ride request not found, already cancelled, or unauthorized.' 
      });
    }

    const cancelledRide = result.rows[0];

    return res.status(200).json({
      success: true,
      message: 'Ride request cancelled successfully.',
      ride: cancelledRide
    });

  } catch (err) {
    console.error("❌ Error cancelling solo ride request:", err.message);
    return res.status(500).json({ 
      success: false, 
      message: 'Server error while cancelling ride request.' 
    });
  }
});



// GET /api/drivers/nearby?lat=9.0420778&lng=38.8741641&radius_km=15
app.get('/api/drivers/nearby', async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lng = parseFloat(req.query.lng);
    const radiusKm = parseFloat(req.query.radius_km) || 15;

    if (isNaN(lat) || isNaN(lng)) {
      return res.status(400).json({ success: false, message: "Invalid latitude or longitude" });
    }

    // Pure SQL Haversine distance formula (works directly on lat/lng columns)
    const query = `
      SELECT 
        user_id,
        active_role,
        lat,
        lng,
        heading,
        status,
        last_ping_at,
        device_id,
        (
          6371 * acos(
            LEAST(1.0, GREATEST(-1.0,
              sin(radians($1)) * sin(radians(lat)) +
              cos(radians($1)) * cos(radians(lat)) *
              cos(radians(lng) - radians($2))
            ))
          )
        ) AS distance_km
      FROM active_user_sessions
      WHERE active_role = 'driver'
        AND status = 'online'
        AND last_ping_at >= NOW() - INTERVAL '2 hours'
      ORDER BY distance_km ASC;
    `;

    const { rows } = await pool.query(query, [lat, lng]);

    // Filter drivers within requested radius
    const nearby = rows.filter(driver => driver.distance_km <= radiusKm);

    console.log(`📍 Nearby drivers query for (${lat}, ${lng}): Found ${nearby.length} drivers`);

    return res.json({
      success: true,
      count: nearby.length,
      drivers: nearby
    });

  } catch (error) {
    console.error("❌ Error fetching nearby drivers:", error);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
});

// ------------------------------------------------------------------
// DISPATCH ENDPOINT & HELPERS
// ------------------------------------------------------------------

app.post('/api/rides/solo/create', async (req, res) => {
  const {
    user_id,
    hub_id,
    pickup_lat,
    pickup_lng,
    destination_lat,
    destination_lng,
    destination_name,
    destination_address
  } = req.body || {};

  if (!user_id || !pickup_lat || !pickup_lng || !destination_lat || !destination_lng) {
    return res.status(400).json({ 
      success: false, 
      message: 'Missing required location or user parameters.' 
    });
  }

  try {
    // 1. Check if user already has an active ride request
    const existingQuery = `
      SELECT * FROM ride_request_solo 
      WHERE user_id = $1 AND status IN ('pending', 'accepted', 'in_progress')
      ORDER BY created_at DESC 
      LIMIT 1;
    `;
    const existingResult = await pool.query(existingQuery, [user_id]);

    if (existingResult.rows.length > 0) {
      const activeRide = existingResult.rows[0];

      return res.status(200).json({
        success: true,
        message: `Active ride request (${activeRide.status}) already exists.`,
        ride_id: activeRide.id,
        ride: activeRide,
        resolvedHubId: activeRide.hub_id,
        isExisting: true
      });
    }

    // 2. Calculate distance server-side using PostGIS (with 1.25 Addis road detour factor)
    const distQuery = `
      SELECT (ST_Distance(
        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
        ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography
      ) / 1000.0) * 1.25 AS calculated_distance_km;
    `;
    
    const distResult = await pool.query(distQuery, [
      parseFloat(pickup_lng), 
      parseFloat(pickup_lat), 
      parseFloat(destination_lng), 
      parseFloat(destination_lat)
    ]);

    const rawDist = parseFloat(distResult.rows[0]?.calculated_distance_km || 1.0);
    const distance_km = Math.max(1, Math.round(rawDist * 10) / 10);

    // 3. Fetch active pricing configuration dynamically from pricing_configs table
    let pricingConfig = {
      base_flag_down: 95.00,
      rate_per_km: 16.00,
      rate_per_minute: 1.50,
      minimum_fare: 120.00,
      fuel_multiplier: 1.00,
      surge_multiplier: 1.00
    };

    try {
      const dbResult = await pool.query(
        "SELECT base_flag_down, rate_per_km, rate_per_minute, minimum_fare, fuel_multiplier, surge_multiplier FROM pricing_configs WHERE city_code = 'ADDIS_ABABA' AND is_active = TRUE LIMIT 1"
      );
      if (dbResult.rows.length > 0) {
        pricingConfig = dbResult.rows[0];
      }
    } catch (dbErr) {
      console.warn("⚠️ Failed to load pricing config from DB, using fallback defaults:", dbErr.message);
    }

    // 4. Compute exact price server-side
    const estimatedMinutes = Math.round(distance_km * 3.5);
    const baseFee = parseFloat(pricingConfig.base_flag_down);
    const kmRate = parseFloat(pricingConfig.rate_per_km);
    const minRate = parseFloat(pricingConfig.rate_per_minute);
    const fuelMult = parseFloat(pricingConfig.fuel_multiplier);
    const surgeMult = parseFloat(pricingConfig.surge_multiplier);
    const minFare = parseFloat(pricingConfig.minimum_fare);

    let calculatedPrice = (baseFee + (distance_km * kmRate) + (estimatedMinutes * minRate)) * fuelMult * surgeMult;

    if (calculatedPrice < minFare) {
      calculatedPrice = minFare;
    }
    const price = Math.round(calculatedPrice);

    // 5. Resolve Hub ID (Use passed hub_id or find nearest if missing)
    let resolvedHubId = hub_id;
    if (!resolvedHubId) {
      const hubQuery = `
        SELECT id,
        ( 6371 * acos( cos( radians($1) ) * cos( radians(latitude) ) * cos( radians(longitude) - radians($2) ) + sin( radians($1) ) * sin( radians(latitude) ) ) ) AS distance
        FROM hubs
        ORDER BY distance ASC
        LIMIT 1;
      `;
      const hubResult = await pool.query(hubQuery, [pickup_lat, pickup_lng]);
      resolvedHubId = hubResult.rows.length > 0 ? hubResult.rows[0].id : null;
    }

    // 6. Insert into ride_request_solo using server-computed price and distance
    const insertQuery = `
      INSERT INTO ride_request_solo (
        user_id,
        pickup_lat,
        pickup_lng,
        destination_lat,
        destination_lng,
        destination_name,
        destination_address,
        price,
        distance_km,
        hub_id,
        status,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', NOW(), NOW())
      RETURNING *;
    `;

    const values = [
      user_id,
      pickup_lat,
      pickup_lng,
      destination_lat,
      destination_lng,
      destination_name || 'Destination',
      destination_address || '',
      price,
      distance_km,
      resolvedHubId
    ];

    const result = await pool.query(insertQuery, values);
    const newRide = result.rows[0];

    if (typeof dispatchSoloRequestToDrivers === 'function') {
      dispatchSoloRequestToDrivers(newRide);
    }

    return res.status(201).json({
      success: true,
      message: 'Solo ride request created successfully.',
      ride_id: newRide.id,
      ride: newRide,
      resolvedHubId: resolvedHubId,
      isExisting: false
    });

  } catch (err) {
    console.error("❌ Error creating solo ride request:", err.message);
    return res.status(500).json({ 
      success: false, 
      message: 'Server error while dispatching solo ride request.',
      error: err.message
    });
  }
});


// Broadcast Solo Request to nearby drivers in STANDARD mode
function dispatchSoloRequestToDrivers(rideSoloRow) {
  if (!io) return;

  const offerPayload = {
    rideId: rideSoloRow.id,
    userId: rideSoloRow.user_id,
    hubId: rideSoloRow.hub_id,
    fare: `ETB ${rideSoloRow.price}`,
    destinationName: rideSoloRow.destination_name,
    destinationAddress: rideSoloRow.destination_address,
    pickupCoords: {
      latitude: parseFloat(rideSoloRow.pickup_lat),
      longitude: parseFloat(rideSoloRow.pickup_lng),
    },
    destinationCoords: {
      latitude: parseFloat(rideSoloRow.destination_lat),
      longitude: parseFloat(rideSoloRow.destination_lng),
    },
    distanceKm: parseFloat(rideSoloRow.distance_km),
    expiresInSeconds: 20,
  };

  activeDrivers.forEach((driverState, driverId) => {
    if (driverState.status === "online" && driverState.operatingMode === "STANDARD") {
      console.log(`📡 Emitting 'new_ride_offer' to driver room: driver_${driverId}`);
      io.to(`driver_${driverId}`).emit("new_ride_offer", offerPayload);
    }
  });
}

// Retract offer modal across drivers if claimed or canceled
function retractOfferFromDrivers(rideId) {
  if (io) {
    io.emit("offer_retracted", { rideId });
  }
}

// Notify passenger when driver accepts
function notifyPassengerRideAccepted(userId, rideData) {
  if (io) {
    io.to(`user_${userId}`).emit("ride_accepted", {
      rideId: rideData.id,
      driverId: rideData.driver_id,
      status: rideData.status,
      timestamp: rideData.updated_at,
    });
  }
}


// ----------------------------------------------------
// 1. ACCEPT SOLO RIDE ROUTE 
// ----------------------------------------------------
app.post('/api/rides/solo/accept', async (req, res) => {
  const { user_id, driver_id } = req.body || {};

  // Validation: Only require driver_id and user_id
  if (!driver_id || !user_id) {
    return res.status(400).json({
      success: false,
      message: 'Missing required parameters: driver_id and user_id are required.'
    });
  }

  try {
    // Optional: Automatically fetch the vehicle assigned to this driver
    let vehicleId = null;
    try {
      const driverRes = await pool.query(
        `SELECT vehicle_id FROM drivers WHERE id = $1;`, 
        [driver_id]
      );
      if (driverRes.rows.length > 0) {
        vehicleId = driverRes.rows[0].vehicle_id;
      }
    } catch (driverErr) {
      console.warn("⚠️ Could not auto-fetch driver vehicle:", driverErr.message);
    }

    // Atomically find and update the latest pending ride for this user
    const updateQuery = `
      WITH target_ride AS (
        SELECT id FROM ride_request_solo 
        WHERE user_id = $1 
          AND status = 'pending'
        ORDER BY created_at DESC 
        LIMIT 1
      )
      UPDATE ride_request_solo 
      SET driver_id = $2, 
          vehicle_id = $3, 
          status = 'accepted', 
          updated_at = NOW() 
      FROM target_ride 
      WHERE ride_request_solo.id = target_ride.id 
      RETURNING ride_request_solo.*;
    `;

    const values = [user_id, driver_id, vehicleId];
    const result = await pool.query(updateQuery, values);

    // If no row was updated, check why
    if (!result.rows || result.rows.length === 0) {
      const checkQuery = `
        SELECT status FROM ride_request_solo 
        WHERE user_id = $1 
        ORDER BY created_at DESC 
        LIMIT 1;
      `;
      const checkResult = await pool.query(checkQuery, [user_id]);
      
      if (!checkResult.rows || checkResult.rows.length === 0) {
        return res.status(404).json({ success: false, message: 'No ride requests found for this user.' });
      }

      return res.status(409).json({
        success: false,
        message: `Ride is no longer available (current status: ${checkResult.rows[0].status}).`
      });
    }

    const acceptedRide = result.rows[0];

    // Fetch Passenger details
    let passenger = null;
    try {
      const userResult = await pool.query(
        `SELECT id, full_name, phone_number FROM users WHERE id = $1;`, 
        [acceptedRide.user_id]
      );
      passenger = userResult.rows?.[0] || { id: acceptedRide.user_id, full_name: 'Passenger', phone_number: '' };
    } catch (userErr) {
      console.warn("⚠️ Could not query user details:", userErr.message);
      passenger = { id: acceptedRide.user_id, full_name: 'Passenger', phone_number: '' };
    }

    // Payload broadcast structure
    const payload = {
      rideId: acceptedRide.id,
      ride_id: acceptedRide.id,
      driverId: driver_id,
      driver_id: driver_id,
      passengerId: acceptedRide.user_id,
      user_id: acceptedRide.user_id,
      status: "accepted",
      timestamp: new Date().toISOString(),
      ride: acceptedRide,
      passenger: passenger
    };

    // Broadcast via Socket.io
    const ioServer = req.io || global.io;
    if (ioServer) {
      ioServer.to(`ride_${acceptedRide.id}`).emit("solo_ride_accepted", payload);
      ioServer.to(`ride_${acceptedRide.id}`).emit("solo_ride_status_updated", payload);
      ioServer.to(`user_${acceptedRide.user_id}`).emit("solo_ride_accepted", payload);
      ioServer.to(`user_${acceptedRide.user_id}`).emit("solo_ride_status_updated", payload);
      console.log(`✅ Driver ${driver_id} accepted user ${user_id}'s pending ride via HTTP`);
    } else {
      console.warn("⚠️ Socket server instance not attached!");
    }

    return res.status(200).json({
      success: true,
      message: 'Ride request accepted successfully.',
      ride_id: acceptedRide.id,
      ride: acceptedRide,
      passenger
    });

  } catch (err) {
    console.error("❌ Error accepting solo ride by user_id:", err.message);
    return res.status(500).json({
      success: false,
      message: 'Server error while accepting ride request.',
      error: err.message
    });
  }
});

// ----------------------------------------------------
// 2. DRIVER ARRIVED ROUTE (HTTP POST)
// ----------------------------------------------------
app.post('/api/rides/solo/arrived', async (req, res) => {
  const { ride_id, driver_id } = req.body || {};

  if (!driver_id) {
    return res.status(400).json({
      success: false,
      message: 'Missing required parameter: driver_id.'
    });
  }

  try {
    const updateQuery = ride_id
      ? `UPDATE ride_request_solo SET status = 'arrived', updated_at = NOW() WHERE id = $1 AND driver_id = $2 AND status = 'accepted' RETURNING *;`
      : `UPDATE ride_request_solo SET status = 'arrived', updated_at = NOW() WHERE driver_id = $1 AND status = 'accepted' RETURNING *;`;

    const values = ride_id ? [ride_id, driver_id] : [driver_id];
    const result = await pool.query(updateQuery, values);

    if (!result.rows || result.rows.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'Cannot mark as arrived. Active accepted ride not found.'
      });
    }

    const updatedRide = result.rows[0];

    // Exact payload shape used in server.js
    const payload = {
      rideId: updatedRide.id,
      ride_id: updatedRide.id,
      driverId: driver_id,
      driver_id: driver_id,
      passengerId: updatedRide.user_id,
      user_id: updatedRide.user_id,
      status: "arrived",
      timestamp: new Date().toISOString(),
      ride: updatedRide
    };

    // Broadcast via Socket.io matching server.js room targeting
    const ioServer = req.io || global.io;
    if (ioServer) {
      ioServer.to(`ride_${updatedRide.id}`).emit("driver_arrived", payload);
      ioServer.to(`ride_${updatedRide.id}`).emit("solo_ride_status_updated", payload);

      if (updatedRide.user_id) {
        ioServer.to(`user_${updatedRide.user_id}`).emit("driver_arrived", payload);
        ioServer.to(`user_${updatedRide.user_id}`).emit("solo_ride_status_updated", payload);
      }
      console.log(`📍 Driver ${driver_id} arrived for solo ride ${updatedRide.id} via HTTP -> Broadcasted to passenger ${updatedRide.user_id}`);
    } else {
      console.warn("⚠️ Socket server instance (req.io or global.io) not attached!");
    }

    return res.status(200).json({
      success: true,
      message: 'Passenger notified of arrival.',
      ride_id: updatedRide.id,
      ride: updatedRide
    });

  } catch (err) {
    console.error("❌ Error setting arrived status:", err.message);
    return res.status(500).json({
      success: false,
      message: 'Server error updating arrival status.',
      error: err.message
    });
  }
});

function notifyPassengerDriverArrived(updatedRide) {
  if (!io) {
    console.warn("⚠️ Socket.io server instance is not initialized.");
    return;
  }

  const passengerId = updatedRide.user_id || updatedRide.passenger_id;
  const rideId = updatedRide.id;

  const payload = {
    event: "driver_arrived",
    message: "Your driver has arrived at the pickup point!",
    ride_id: rideId,
    ride: updatedRide,
    timestamp: new Date().toISOString()
  };

  // 1. Target individual passenger user room
  if (passengerId) {
    io.to(`user_${passengerId}`).emit("driver_arrived", payload);
    console.log(`📡 Broadcasted driver arrival to room: user_${passengerId}`);
  }

  // 2. Also emit to specific ride channel (if ride rooms are used)
  io.to(`ride_${rideId}`).emit("driver_arrived", payload);
}

function notifyPassengerDriverAccepted(updatedRide) {
  if (!io) return;

  const passengerId = updatedRide.user_id || updatedRide.passenger_id;
  const rideId = updatedRide.id;

  const payload = {
    event: "solo_ride_accepted",
    message: "A driver has accepted your ride request!",
    rideId: rideId,
    driverId: updatedRide.driver_id,
    status: "accepted",
    ride: updatedRide,
    timestamp: new Date().toISOString()
  };

  // Broadcast to ride room, falling back to personal user room
  io.to(`ride_${rideId}`).to(`user_${passengerId}`).emit("solo_ride_accepted", payload);
}


// STANDARD TIRP IN PROGRESS FETCHING INFO


/**
 * 1. UPDATE RIDE STATUS TO 'in_trip'
 * Called when the driver taps 'Start Trip' in DriverEnRoute screen
 */
app.patch('/api/rides/solo/status', async (req, res) => {
  try {
    const { ride_id, status } = req.body;

    if (!ride_id || !status) {
      return res.status(400).json({ success: false, message: "Missing ride_id or status." });
    }

    const query = `
      UPDATE ride_request_solo 
      SET status = $1, updated_at = NOW() 
      WHERE id = $2 
      RETURNING *
    `;
    const result = await pool.query(query, [status, ride_id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "Ride request not found." });
    }

    const updatedRide = result.rows[0];

    // 🚀 EMIT SOCKET EVENTS TO THE RIDE ROOM
    const io = req.app.get('io'); // (Make sure io is set on your express app: app.set('io', io))
    if (io) {
      const room = `ride_${ride_id}`;
      
      if (status === 'arrived') {
        io.to(room).emit('solo_driver_arrived', { ride_id, status });
      } else if (status === 'in_trip') {
        io.to(room).emit('solo_trip_started', { ride_id, status });
      } else if (status === 'completed') {
        io.to(room).emit('trip_completed', { ride_id, status });
      }
      
      // Generic fallback event if you want a single listener
      io.to(room).emit('ride_status_changed', { ride_id, status });
    }

    res.json({ success: true, ride: updatedRide });
  } catch (err) {
    console.error("❌ Error updating ride_request_solo status:", err);
    res.status(500).json({ success: false, error: "Internal server error." });
  }
});



app.get('/api/pricing/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const query = `SELECT * FROM pricing_configs WHERE id = $1 LIMIT 1`;
    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: `Pricing configuration not found for ID: ${id}` });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("❌ Error fetching pricing config:", err);
    res.status(500).json({ error: "Internal server error while fetching pricing." });
  }
});


/**
 * 3. COMPLETE RIDE & FINALIZE METRICS
 * Called when the driver taps 'Complete Trip' in DriverStandardTripInProgress
 */
app.post('/api/rides/solo/complete', async (req, res) => {
  const { ride_id, actual_distance_km, elapsed_minutes } = req.body || {};

  if (!ride_id) {
    return res.status(400).json({ 
      success: false, 
      message: 'Missing ride_id parameter.' 
    });
  }

  try {
    // 1. Fetch the ride request
    const rideResult = await pool.query(
      "SELECT * FROM ride_request_solo WHERE id = $1 LIMIT 1",
      [ride_id]
    );

    if (rideResult.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Ride request not found.' });
    }

    const ride = rideResult.rows[0];

    // 2. Fetch active pricing configuration from database
    const pricingResult = await pool.query(
      "SELECT base_flag_down, rate_per_km, rate_per_minute, minimum_fare, fuel_multiplier, surge_multiplier, platform_commission_rate FROM pricing_configs WHERE city_code = 'ADDIS_ABABA' AND is_active = TRUE LIMIT 1"
    );

    if (pricingResult.rows.length === 0) {
      return res.status(500).json({ success: false, message: 'Active pricing configuration not found.' });
    }

    const config = pricingResult.rows[0];

    // 3. Use actual metrics passed from driver app (or fallback to ride record values)
    const finalDist = parseFloat(actual_distance_km !== undefined ? actual_distance_km : ride.distance_km);
    const finalMins = parseInt(elapsed_minutes !== undefined ? elapsed_minutes : Math.round(finalDist * 3.5), 10);

    const baseFee = parseFloat(config.base_flag_down);
    const kmRate = parseFloat(config.rate_per_km);
    const minRate = parseFloat(config.rate_per_minute);
    const fuelMult = parseFloat(config.fuel_multiplier);
    const surgeMult = parseFloat(config.surge_multiplier);
    const minFare = parseFloat(config.minimum_fare);
    const commissionRate = parseFloat(config.platform_commission_rate || 0.05);

    // 4. Compute metered fare formula
    let calculatedPrice = (baseFee + (finalDist * kmRate) + (finalMins * minRate)) * fuelMult * surgeMult;

    // Enforce minimum fare floor
    if (calculatedPrice < minFare) {
      calculatedPrice = minFare;
    }
    const finalPrice = Math.round(calculatedPrice);

    // 5. Calculate platform fee and driver net payout
    const platformFee = Math.round(finalPrice * commissionRate);
    const driverNet = finalPrice - platformFee;

    // 6. Update ride status in database to completed
    const updateQuery = `
      UPDATE ride_request_solo 
      SET status = 'completed', 
          price = $1, 
          distance_km = $2, 
          updated_at = NOW() 
      WHERE id = $3 
      RETURNING *;
    `;
    const updateResult = await pool.query(updateQuery, [finalPrice, finalDist, ride_id]);
    const completedRide = updateResult.rows[0];

    // 🚀 7. SOCKET.IO REAL-TIME BROADCAST INTEGRATION
    const io = req.io;
    if (io) {
      const payload = {
        rideId: completedRide.id,
        ride_id: completedRide.id,
        status: "completed",
        price: completedRide.price,
        distance_km: completedRide.distance_km,
        destination_name: completedRide.destination_name,
        timestamp: new Date().toISOString(),
      };

      // Broadcast to the ride monitoring room
      io.to(`ride_${ride_id}`).emit("trip_completed", payload);
      io.to(`ride_${ride_id}`).emit("ride_status_changed", payload);
      io.to(`ride_${ride_id}`).emit("solo_ride_status_updated", payload);

      // Broadcast directly to the passenger's personal channel
      if (completedRide.user_id) {
        io.to(`user_${completedRide.user_id}`).emit("trip_completed", payload);
        io.to(`user_${completedRide.user_id}`).emit("ride_status_changed", payload);
        io.to(`user_${completedRide.user_id}`).emit("solo_ride_status_updated", payload);
      }
      
      console.log(`🏁 Solo ride #${ride_id} marked completed and broadcasted via socket.`);
    }

    return res.status(200).json({
      success: true,
      message: 'Ride completed successfully.',
      ride: completedRide,
      fare_breakdown: {
        total_fare: finalPrice,
        actual_distance_km: finalDist,
        elapsed_minutes: finalMins,
        platform_fee: platformFee,
        driver_net_payout: driverNet
      }
    });

  } catch (err) {
    console.error("❌ Error completing ride:", err.message);
    return res.status(500).json({ 
      success: false, 
      message: 'Server error while completing ride.',
      error: err.message
    });
  }
});

// Passenger needs to know the drivers location while waiting for accepted standard 
app.get('/api/rides/:rideId/driver-location', async (req, res) => {
  const { rideId } = req.params;
  try {
    const query = `
      SELECT r.driver_id, s.lat, s.lng, s.heading, s.last_ping_at
      FROM ride_request_solo r
      JOIN active_user_sessions s ON r.driver_id = s.user_id
      WHERE r.id = $1;
    `;
    const result = await pool.query(query, [rideId]);
    
    if (result.rows.length === 0 || !result.rows[0].lat) {
      return res.status(404).json({ success: false, message: "Driver location not active yet." });
    }

    return res.json({ success: true, location: result.rows[0] });
  } catch (err) {
    console.error("❌ Error fetching initial driver location:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// driver payment details and other info when passenger completes standard trip 

app.get('/api/rides/:id', async (req, res) => {
  const { id } = req.params;

  if (!id) {
    return res.status(400).json({ success: false, message: "Missing ride id parameter." });
  }

  try {
    const query = `
      SELECT r.*, 
             u.full_name AS driver_name, 
             u.phone_number AS driver_phone, 
             u.rating AS driver_rating,
             v.model AS vehicle_model, 
             v.plate_number AS plate_number, 
             v.color AS vehicle_color,
             dp.bank_name, 
             dp.bank_account_number, 
             dp.telebirr_number
      FROM ride_request_solo r
      LEFT JOIN users u ON u.id = r.driver_id
      LEFT JOIN vehicles v ON v.driver_id = r.driver_id
      LEFT JOIN driver_profiles dp ON dp.driver_id = r.driver_id
      WHERE r.id = $1
      LIMIT 1;
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "Ride request not found." });
    }

    const ride = result.rows[0];

    res.status(200).json({
      success: true,
      id: ride.id,
      status: ride.status,
      destination_name: ride.destination_name || "Destination Arrival",
      price: ride.price || 0,
      distance_km: ride.distance_km || 0,
      pickup_lat: ride.pickup_lat,
      pickup_lng: ride.pickup_lng,
      destination_lat: ride.destination_lat,
      destination_lng: ride.destination_lng,
      driver: {
        name: ride.driver_name || "Captain",
        phone_number: ride.driver_phone || "",
        rating: ride.driver_rating || "5.00",
        vehicle_model: ride.vehicle_model || "Toyota Corolla",
        plate_number: ride.plate_number || "3-45678",
        vehicle_color: ride.vehicle_color || "White",
        bank_name: ride.bank_name || "Commercial Bank of Ethiopia (CBE)",
        bank_account_number: ride.bank_account_number || "1000847291024",
        telebirr_number: ride.telebirr_number || ride.driver_phone || "251998476704",
      }
    });

  } catch (err) {
    console.error("❌ Error fetching ride details:", err.message);
    res.status(500).json({ success: false, error: "Internal server error." });
  }
});




// POSTMAN TEST ROUTE: Simulate driver movement
app.post('/api/test/simulate-driver', async (req, res) => {
  const { driver_id, ride_id, user_id, latitude, longitude, heading } = req.body;

  try {
    const lat = latitude || 9.0105;
    const lng = longitude || 38.7613;
    const hdg = heading || 45.0;
    const dId = driver_id || 33;
    const rId = ride_id || 1;
    const uId = user_id || 27;

    // 1. Upsert into active_user_sessions table
    await pool.query(
      `INSERT INTO active_user_sessions (user_id, active_role, lat, lng, heading, status, last_ping_at)
       VALUES ($1, 'driver', $2, $3, $4, 'online', NOW())
       ON CONFLICT (user_id) 
       DO UPDATE SET lat = EXCLUDED.lat, 
                     lng = EXCLUDED.lng, 
                     heading = EXCLUDED.heading, 
                     status = 'online',
                     last_ping_at = NOW();`,
      [dId, lat, lng, hdg]
    );

    // 2. Broadcast via Socket.io to the passenger room
    const payload = { latitude: lat, longitude: lng, heading: hdg };
    io.to(`ride_${rId}`).emit("driver_location_update", payload);
    io.to(`user_${uId}`).emit("driver_location_update", payload);

    return res.json({ 
      success: true, 
      message: `Simulated driver movement sent for ride #${rId}`,
      coords: { lat, lng, heading: hdg }
    });

  } catch (err) {
    console.error("❌ Simulation error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});





/* ===========================================================================
   🏁 7. ENGINE DISPATCH INITIALIZER LISTEN
=========================================================================== */
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Corridor Engine active on port ${PORT}`);
});
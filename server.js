const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Client } = require('pg');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db'); // 👈 KEEP YOUR EXISTING DB POOL INTACT!

const app = express();
const PORT = process.env.PORT || 5000;

const multer = require("multer");
const path = require("path");

// ⚡ Global Parsers & Middlewares
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 🔌 Wrap Express app in HTTP server for WebSockets
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// 🔔 DEDICATED CLIENT FOR POSTGRES LISTEN / NOTIFY 
// (Uses process.env.DATABASE_URL or your database connection string)
const pgListener = new Client({
  connectionString: process.env.DATABASE_URL || `postgresql://postgres:D@ny_456@127.0.0.1:5432/corridor_pool`
});

pgListener.connect()
  .then(async () => {
    console.log("🔌 Dedicated Postgres client connected for LISTEN/NOTIFY");
    await pgListener.query("LISTEN route_pool_changed");
  })
  .catch(err => console.error("❌ Postgres listener connection failed:", err));

pgListener.on("notification", (msg) => {
  try {
    const payload = JSON.parse(msg.payload);
    console.log("🔔 Postgres Notification Caught on Server:", payload);
    
    // Broadcast instantly to all connected React Native clients
    io.emit("route_pool_updated", payload);
  } catch (err) {
    console.error("❌ Error parsing notification payload:", err);
  }
});

io.on("connection", (socket) => {
  console.log("⚡ Passenger/Driver connected via Socket.io:", socket.id);
});

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

    // 2. DYNAMIC LOOKUP: Resolve pickup_hub_id if missing
    let targetPickupHubId = pickup_hub_id;
    if (!targetPickupHubId) {
      const pickupLookup = await client.query(
        `SELECT hub_id FROM route_stops WHERE route_id = $1 ORDER BY stop_order ASC LIMIT 1;`,
        [parseInt(route_id, 10)]
      );
      targetPickupHubId = pickupLookup.rows[0]?.hub_id;
      if (!targetPickupHubId) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "MISSING_PICKUP_HUB",
          message: "pickup_hub_id is required and could not be resolved from route stops."
        });
      }
    }

    // 3. DYNAMIC LOOKUP: Resolve dropoff_hub_id if missing
    let finalDropoffHubId = dropoff_hub_id;
    if (!finalDropoffHubId) {
      const destinationLookup = await client.query(
        `SELECT hub_id FROM route_stops WHERE route_id = $1 ORDER BY stop_order DESC LIMIT 1;`,
        [parseInt(route_id, 10)]
      );
      finalDropoffHubId = destinationLookup.rows[0]?.hub_id || 1;
    }

    // 4. Insert Entry strictly into route_pool with 'searching' status
    const insertRes = await client.query(
      `INSERT INTO route_pool (user_id, route_id, pickup_hub_id, dropoff_hub_id, status, joined_at)
       VALUES ($1, $2, $3, $4, 'searching', NOW()) RETURNING id, status;`,
      [parseInt(user_id, 10), parseInt(route_id, 10), parseInt(targetPickupHubId, 10), parseInt(finalDropoffHubId, 10)]
    );

    const newPool = insertRes.rows[0];
    console.log(`✅ User ${user_id} registered into route_pool queue (searching). Triggering matching engine...`);

    // ⚡ 5. INVOKE UNIFIED MATCHING ENGINE INSTANTLY
    // (Make sure executeCorridorAutoMatch is imported at the top of your file!)
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

  // 1. OVERFLOW SWEEP
  const recoverySweep = await client.query(
    `UPDATE route_pool rp
     SET status = 'searching', matched_ride_offer_id = NULL
     FROM ride_offers ro
     WHERE rp.matched_ride_offer_id = ro.id
       AND TRIM(LOWER(ro.status)) IN ('accepted', 'arrived', 'completed')
       AND TRIM(LOWER(rp.status)) != 'declined'
       AND NOT (rp.user_id = ANY(ro.accepted_passengers))
     RETURNING rp.id, rp.user_id;`
  );

  // 2. FETCH ELIGIBLE PASSENGERS
  const waitingPassengersQuery = await client.query(
    `SELECT rp.id, rp.user_id, rp.status, 
            rs_pickup.stop_order AS pickup_stop_order,
            rs_dropoff.stop_order AS dropoff_stop_order
     FROM route_pool rp
     JOIN route_stops rs_pickup ON rs_pickup.route_id = rp.route_id AND rs_pickup.hub_id::int = rp.pickup_hub_id::int
     JOIN route_stops rs_dropoff ON rs_dropoff.route_id = rp.route_id AND rs_dropoff.hub_id::int = rp.dropoff_hub_id::int
     WHERE rp.route_id = $1 
       AND rs_dropoff.stop_order > rs_pickup.stop_order
       AND TRIM(LOWER(rp.status)) = 'searching'
     ORDER BY rs_pickup.stop_order ASC, rp.joined_at ASC 
     FOR UPDATE OF rp;`,
    [route_id]
  );

  let availableRiders = waitingPassengersQuery.rows;
  console.log(`👥 Searching Passengers Found: ${availableRiders.length}`);

  if (availableRiders.length === 0) return { status: "idle", matchedCount: 0 };

  // 3. FETCH PENDING RIDE OFFERS (Explicitly locking ONLY 'ro' to bypass outer join lock error)
  const openOffersQuery = await client.query(
    `SELECT ro.id, ro.driver_id, ro.total_seats, ro.available_seats,
            LEAST(rs_start.stop_order, rs_end.stop_order) AS start_stop_order,
            GREATEST(rs_start.stop_order, rs_end.stop_order) AS end_stop_order
     FROM ride_offers ro
     LEFT JOIN route_stops rs_start ON rs_start.route_id = ro.route_id AND TRIM(rs_start.hub_id::text) = TRIM(ro.start_location::text)
     LEFT JOIN route_stops rs_end ON rs_end.route_id = ro.route_id AND TRIM(rs_end.hub_id::text) = TRIM(ro.end_location::text)
     WHERE ro.route_id = $1 
       AND TRIM(LOWER(ro.status)) = 'pending' 
       AND ro.available_seats > 0
     ORDER BY ro.created_at ASC 
     FOR UPDATE OF ro;`, // 👈 Fixed: Explicitly targets 'ro' instead of locking nullable outer joins
    [route_id]
  );

  const openOffers = openOffersQuery.rows;
  console.log(`🚗 Pending Ride Offers Available: ${openOffers.length}`);

  let totalMatchedCount = 0;

  // 4. BULLETPROOF MATCHING PIPELINE
  for (const offer of openOffers) {
    let unassigned = [];
    let seatsAvailable = Number(offer.available_seats);
    let matchesMadeThisOffer = 0;

    const offerStartStop = offer.start_stop_order !== null ? Number(offer.start_stop_order) : 1;
    const offerEndStop = offer.end_stop_order !== null ? Number(offer.end_stop_order) : 999;

    for (const pax of availableRiders) {
      const paxPickup = Number(pax.pickup_stop_order);
      const paxDropoff = Number(pax.dropoff_stop_order);

      const isValidSegment = 
        paxPickup >= offerStartStop && 
        paxDropoff <= offerEndStop &&
        seatsAvailable > 0;

      console.log(`   [DEBUG] Rider ${pax.user_id}: Needs ${paxPickup}->${paxDropoff} | Driver #${offer.id} covers ${offerStartStop}->${offerEndStop} | Valid: ${isValidSegment}`);

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
        console.log(`   ⚡ MATCHED: Rider ${pax.user_id} assigned to Driver #${offer.driver_id}`);
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

/* ---------------- GET CORRIDORS FROM PICKUP HUB (NEW FLAT SCHEMA) ---------------- */
/* ---------------- GET CORRIDORS FROM PICKUP HUB (MATCHING EXISTING RIDE_OFFERS) ---------------- */
app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;
    
    const result = await pool.query(
      `SELECT 
        r.id AS route_id,
        r.hub_id AS parent_hub_id,
        r.recommended_base_price,
        r.pickup_hub_id,
        h_pickup.name AS pickup_hub_name,
        r.drop_off_hub_id,
        h_drop.name AS destination_name,
        r.pass_through_hub_id,
        h_pass.name AS pass_through_name,
        -- Hybrid Pricing: Use driver's custom price_per_seat if active, otherwise fallback to recommended base price
        COALESCE(latest_ro.price_per_seat, r.recommended_base_price) AS current_price,
        latest_ro.available_seats,
        latest_ro.total_seats,
        latest_ro.id AS ride_offer_id,
        latest_ro.status AS offer_status
      FROM routes r
      JOIN hubs h_pickup ON h_pickup.id = r.pickup_hub_id
      JOIN hubs h_drop ON h_drop.id = r.drop_off_hub_id
      LEFT JOIN hubs h_pass ON h_pass.id = r.pass_through_hub_id
      -- Attach the latest active driver ride offer matching this route
      LEFT JOIN LATERAL (
        SELECT id, price_per_seat, available_seats, total_seats, status
        FROM ride_offers 
        WHERE ride_offers.route_id = r.id 
          AND status IN ('pending', 'matched', 'active')
        ORDER BY id DESC 
        LIMIT 1
      ) latest_ro ON true
      WHERE LOWER(h_pickup.name) = LOWER($1)
      ORDER BY r.id;`,
      [pickupHub]
    );

    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching corridors from hub:", err.message);
    res.status(500).json({ error: "Server error" });
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
    // Ensures 'rejected', 'declined', 'cancelled', 'completed', and 'expired' offers NEVER block going online.
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

    // 3. Collect Route, Geometry, and Hub IDs
    const routeGeomQuery = await client.query(
      `SELECT r.base_price, 
              ARRAY_AGG(h.id ORDER BY rs.stop_order) AS hub_ids,
              ARRAY_AGG(h.name ORDER BY rs.stop_order) AS hub_names,
              (ARRAY_AGG(h.id ORDER BY rs.stop_order))[1] AS start_hub_id,
              (ARRAY_AGG(h.name ORDER BY rs.stop_order))[1] AS start_hub_name,
              (ARRAY_AGG(h.latitude ORDER BY rs.stop_order))[1] AS start_lat,
              (ARRAY_AGG(h.longitude ORDER BY rs.stop_order))[1] AS start_lng
       FROM routes r
       JOIN route_stops rs ON rs.route_id = r.id
       JOIN hubs h ON h.id = rs.hub_id
       WHERE r.id = $1 
       GROUP BY r.id, r.base_price;`,
      [parseInt(route_id, 10)]
    );
    
    const routeData = routeGeomQuery.rows[0];
    if (!routeData || !routeData.hub_ids || routeData.hub_ids.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: "INCOMPLETE_ROUTE", 
        message: "The chosen corridor route blueprint has an incomplete stop list." 
      });
    }

    // 🎯 4. Enforce saving Hub ID numbers (e.g., "1" and "4") instead of string names
    const finalStartLoc = start_location !== undefined && start_location !== null ? String(start_location) : String(routeData.hub_ids[0]);
    const finalEndLoc = end_location !== undefined && end_location !== null ? String(end_location) : String(routeData.hub_ids[routeData.hub_ids.length - 1]);
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
    const route = await pool.query(`SELECT capacity, base_price FROM routes WHERE id = $1`, [route_id]);
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

    await client.query("COMMIT");

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

    // 1. Find active route_pool record for the user (accepts 'pending' AND 'accepted' ride offer statuses)
    const poolCheckQuery = `
      SELECT rp.id, rp.matched_ride_offer_id, rp.status, ro.status AS ride_status, ro.accepted_passengers
      FROM route_pool rp
      JOIN ride_offers ro ON rp.matched_ride_offer_id = ro.id
      WHERE rp.user_id = $1 
        AND LOWER(TRIM(rp.status)) NOT IN ('completed', 'cancelled', 'idle', 'expired')
        AND LOWER(TRIM(ro.status)) IN ('pending', 'accepted', 'active', 'matched', 'in_progress')
        AND $1 = ANY(ro.accepted_passengers)
      ORDER BY rp.joined_at DESC
      LIMIT 1;
    `;

    const poolResult = await client.query(poolCheckQuery, [targetUserId]);

    if (poolResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: `No active ride pool match found for user ${targetUserId}.` 
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

    // 3. Fetch co-riders manifest
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

// 📊 DRIVER Get Corridor Pool Status OF PASSENGERS metrics for a specific pickup hub


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
  const { user_id, ride_offer_id } = req.body;

  if (!user_id || !ride_offer_id) {
    return res.status(400).json({ error: "Missing required user_id or ride_offer_id." });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Fetch details of the record being declined
    const currentMatchQuery = await client.query(
      `SELECT id, route_id, pickup_hub_id 
       FROM route_pool 
       WHERE user_id = $1 AND matched_ride_offer_id = $2 AND status = 'matched'
       FOR UPDATE;`,
      [parseInt(user_id, 10), parseInt(ride_offer_id, 10)]
    );

    const matchRecord = currentMatchQuery.rows[0];

    if (!matchRecord) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Active matching proposal record not found." });
    }

    const { route_id, pickup_hub_id } = matchRecord;

    // 2. Mark the declining passenger as 'cancelled'
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

    // 5. 🌟 DECOUPLED CALL: Invoke upgraded Matching Engine to resolve searching/stranded entries
    // Passing the transactional client down allows the recovery engine to execute within the same safe atomic space.
    await executeCorridorAutoMatch(client, route_id, pickup_hub_id);

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
    const offerQuery = await pool.query(
      `SELECT id, status, price_per_seat, pickup_lat, pickup_lng, route_id 
       FROM ride_offers 
       WHERE id = $1`,
      [rideOfferId]
    );

    if (offerQuery.rows.length === 0) {
      return res.status(404).json({ error: "Target Corridor Offer profile index not found." });
    }

    const offer = offerQuery.rows[0];

    // 🎯 UPGRADE: Join 'users' table to fetch real passenger names and contact info
    const passengersQuery = await pool.query(
      `SELECT rp.user_id, rp.status, u.full_name, u.phone_number 
       FROM route_pool rp
       JOIN users u ON rp.user_id = u.id
       WHERE rp.matched_ride_offer_id = $1
         AND TRIM(LOWER(rp.status)) IN ('matched', 'accepted', 'arrived', 'in_trip', 'intransit')`,
      [rideOfferId]
    );

    const trackingPayload = {
      rideOfferId: offer.id,
      rideStatus: offer.status, 
      status: offer.status, // Support both casing variations for frontend resilience
      route_id: offer.route_id,
      price_per_seat: offer.price_per_seat,
      driverLocation: {
        latitude: parseFloat(offer.pickup_lat) || 9.0106,
        longitude: parseFloat(offer.pickup_lng) || 38.7894
      },
      passengers: passengersQuery.rows.map(row => ({
        userId: parseInt(row.user_id, 10), 
        user_id: parseInt(row.user_id, 10), 
        full_name: row.full_name || `Rider ${row.user_id}`,
        phone_number: row.phone_number || "",
        status: row.status ? row.status.toString().trim().toLowerCase() : "matched" 
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
    // 1. Fetch active ride offer for this driver
    const proposalQuery = await pool.query(
      `SELECT id, route_id, status, start_location, end_location, departure_time, 
              total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng,
              accepted_passengers
       FROM ride_offers 
       WHERE driver_id = $1 
         AND status IN ('pending', 'accepted', 'arrived', 'matched', 'in_trip', 'inprogress')
       ORDER BY created_at DESC 
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
    // 1. Fetch active ride offer for the given driver
    const offerQuery = await pool.query(
      `SELECT id, route_id, status, start_location, end_location, departure_time, 
              total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng,
              accepted_passengers, driver_id
       FROM ride_offers 
       WHERE driver_id = $1 
         AND status IN ('pending', 'proposed', 'accepted', 'arrived', 'inprogress')
       ORDER BY created_at DESC 
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

    // 6. Return response payload
    return res.status(200).json({
      hasActiveProposal: true,
      id: offer.id,
      ride_offer_id: offer.id,
      route_id: offer.route_id,
      status: offer.status,
      start_location: offer.start_location || "Megenagna Departure Hub",
      end_location: offer.end_location || "Piassa",
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

    // 6. Remove driver from queue since run is locked
    const deleteQueueQuery = `DELETE FROM driver_queue WHERE driver_id = $1`;
    await client.query(deleteQueueQuery, [driver_id]);

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

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 1. Find the active 'accepted' ride offer for this driver and update status to 'arrived'
    const updateRideOfferQuery = await client.query(
      `UPDATE ride_offers 
       SET status = 'arrived' 
       WHERE driver_id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived')
       RETURNING id, driver_id, status, COALESCE(accepted_passengers, '{}') AS accepted_passengers;`,
      [targetDriverId]
    );

    if (updateRideOfferQuery.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: `No active accepted ride offer found for driver ID ${targetDriverId} to mark as arrived.` 
      });
    }

    const activeRide = updateRideOfferQuery.rows[0];
    const activeRideOfferId = activeRide.id;
    const acceptedPassengers = activeRide.accepted_passengers;

    let updatedPassengersCount = 0;

    // 2. Cascade status update to 'arrived' ONLY for active accepted passengers of THIS ride offer
    if (Array.isArray(acceptedPassengers) && acceptedPassengers.length > 0) {
      const updatePoolQuery = await client.query(
        `UPDATE route_pool 
         SET status = 'arrived' 
         WHERE matched_ride_offer_id = $1 
           AND user_id = ANY($2::int[])
           AND TRIM(LOWER(status)) IN ('accepted', 'arrived')
         RETURNING id, user_id, status;`,
        [activeRideOfferId, acceptedPassengers]
      );

      updatedPassengersCount = updatePoolQuery.rows.length;
    }

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Driver arrival state and associated active passenger statuses updated to arrived.",
      ride_offer_id: activeRideOfferId,
      driver_id: targetDriverId,
      status: "arrived",
      accepted_passengers_updated: updatedPassengersCount
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: ARRIVAL SYSTEM FAULT:", err.stack || err.message);
    return res.status(500).json({ error: `Internal execution error: ${err.message}` });
  } finally {
    client.release();
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
// FETCH AVAILABLE CORRIDORS FOR A HUB (WITH REAL-TIME SEAT COUNT)
// GET /api/routes/from/:hubName
// ==================================================================
app.get('/api/routes/from/:hubName', async (req, res) => {
  try {
    const { hubName } = req.params;

    const query = `
      SELECT 
        r.id AS route_id, 
        r.stops, 
        r.base_price,
        COALESCE(ro.total_seats, 4) AS capacity,
        COALESCE(ro.available_seats, 4) AS available_seats,
        -- 🧮 Compute filled seats directly in SQL so the frontend gets an exact number
        (COALESCE(ro.total_seats, 4) - COALESCE(ro.available_seats, 4)) AS filled_seats,
        ro.id AS ride_offer_id,
        ro.status AS offer_status
      FROM routes r
      JOIN hubs h ON r.pickup_hub_id = h.id
      LEFT JOIN ride_offers ro ON r.id = ro.route_id AND ro.status IN ('pending', 'matched', 'active')
      WHERE LOWER(h.name) = LOWER($1);
    `;

    const result = await pool.query(query, [hubName]);
    console.log(`📊 [API /routes/from/${hubName}] Returning corridors with filled_seats:`, result.rows);
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching active corridors:", err);
    res.status(500).json({ error: "Internal server error" });
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



/* ===========================================================================
   🏁 7. ENGINE DISPATCH INITIALIZER LISTEN
=========================================================================== */
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Corridor Engine active on port ${PORT}`);
});
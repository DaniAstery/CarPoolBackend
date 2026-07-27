const express = require('express');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;

const multer = require("multer");
const path = require("path");

// ⚡ Global Parsers & Middlewares
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));




app.use("/uploads", express.static(path.join(__dirname, "uploads")));

const { getDistanceFromLatLonInKm } = require('./utils/distance');

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


// Checking passenger Distance from requested hub 
app.post("/api/passenger/validate-location", async (req, res) => {
 //const { user_lat, user_lng, user_id } = req.body;
  const { user_id } = req.body;
  const user_lat = parseFloat(9.0106);
  const user_lng = parseFloat(38.7894);

  // 1. Fetch all hubs
  const hubs = await pool.query("SELECT id, name, latitude, longitude FROM hubs");

  // 2. Calculate Haversine distance for all hubs
  const validHubs = hubs.rows.filter(hub => {
    const distance = getDistanceFromLatLonInKm(
      user_lat, user_lng, 
      hub.latitude, hub.longitude
    );
    return distance <= 1.0; // 1km threshold
  });

  if (validHubs.length === 0) {
    return res.status(403).json({ 
      error: "OUT_OF_RANGE", 
      message: "You are not within 1km of any pickup corridor hub." 
    });
  }

  // 3. Return the nearest hub context
  res.json({ status: "VALID", nearestHub: validHubs[0] });
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

/* ---------------- SELECT HUB → GET VALID CORRIDORS ---------------- */
/* ---------------- SELECT HUB → GET SLICED SEPARATE CORRIDORS ---------------- */
app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;
    
    const result = await pool.query(
      `WITH pickup_info AS (
        SELECT 
          r.id AS route_id,
          r.capacity,
          r.base_price,
          rs.hub_id AS pickup_hub_id,
          rs.stop_order AS pickup_order
        FROM routes r
        JOIN route_stops rs ON rs.route_id = r.id
        JOIN hubs h ON h.id = rs.hub_id
        WHERE LOWER(h.name) = LOWER($1)
      )
      SELECT 
        pi.route_id,
        pi.capacity,
        pi.base_price,
        pi.pickup_hub_id AS start_hub_id,
        rs_drop.hub_id AS end_hub_id,
        h_drop.name AS destination_name,
        -- Dynamically slice the stops array from pickup up to this specific drop-off
        (
          SELECT ARRAY_AGG(h_all.name ORDER BY rs_all.stop_order)
          FROM route_stops rs_all
          JOIN hubs h_all ON h_all.id = rs_all.hub_id
          WHERE rs_all.route_id = pi.route_id
            AND rs_all.stop_order >= pi.pickup_order
            AND rs_all.stop_order <= rs_drop.stop_order
        ) AS stops
      FROM pickup_info pi
      -- Find all downstream drop-off stops that occur AFTER the pickup stop
      JOIN route_stops rs_drop ON rs_drop.route_id = pi.route_id 
        AND rs_drop.stop_order > pi.pickup_order
      JOIN hubs h_drop ON h_drop.id = rs_drop.hub_id
      ORDER BY pi.route_id, rs_drop.stop_order;`,
      [pickupHub]
    );

    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching corridors from hub:", err.message);
    res.status(500).json({ error: "Server error" });
  }
});


/* ---------------- PASSENGER SIDE: JOIN CORRIDOR WAITING POOL ---------------- */
/* ---------------- SELECT HUB → GET ALL ROUTES PASSING THROUGH ---------------- */
app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;
    
    const result = await pool.query(
      `SELECT
        r.id AS route_id,
        r.capacity,
        r.base_price,
        (SELECT rs_first.hub_id FROM route_stops rs_first WHERE rs_first.route_id = r.id ORDER BY rs_first.stop_order ASC LIMIT 1) AS start_hub_id,
        (SELECT rs_last.hub_id FROM route_stops rs_last WHERE rs_last.route_id = r.id ORDER BY rs_last.stop_order DESC LIMIT 1) AS end_hub_id,
        (
          SELECT ARRAY_AGG(h.name ORDER BY rs.stop_order)
          FROM route_stops rs
          JOIN hubs h ON h.id = rs.hub_id
          WHERE rs.route_id = r.id
        ) AS stops
      FROM routes r
      WHERE r.id IN (
        SELECT rs_check.route_id 
        FROM route_stops rs_check
        JOIN hubs h_check ON h_check.id = rs_check.hub_id
        WHERE LOWER(h_check.name) = LOWER($1)
      );`,
      [pickupHub]
    );

    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching routes from hub:", err.message);
    res.status(500).json({ error: "Server error" });
  }
});




/* ---------------- START TRIP LIFECYCLE CONTROLLER ---------------- */
// 🎯 Import or reference your matching engine runner here

app.post("/api/rides/start-trip", async (req, res) => {
  const client = await pool.connect();
  try {
    const { ride_offer_id } = req.body;
    const parsedRideId = parseInt(ride_offer_id, 10);

    console.log(`🎬 [START TRIP] Request received for Ride Offer ID: ${ride_offer_id} (Parsed: ${parsedRideId})`);

    if (!parsedRideId || isNaN(parsedRideId)) {
      return res.status(400).json({ error: "Missing or invalid ride_offer_id parameter format." });
    }

    await client.query("BEGIN");

    // 1. Update the master ride offer's state from 'arrived' to 'in_trip'
    const updateOffer = await client.query(
      `UPDATE ride_offers 
       SET status = 'in_trip',
           created_at = NOW()
       WHERE id = $1 AND TRIM(LOWER(status)) = 'arrived'
       RETURNING id, status, route_id, driver_id;`,
      [parsedRideId]
    );

    if (updateOffer.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`⚠️ [START TRIP REJECTION] Row #${parsedRideId} not found or status was not 'arrived'.`);
      return res.status(404).json({ 
        error: "Ride offer not found or current status is not 'arrived'. Cannot transition to trip state." 
      });
    }

    const activeOffer = updateOffer.rows[0];
    console.log(`✅ [START TRIP] ride_offers row #${activeOffer.id} successfully updated to [${activeOffer.status}]`);

    // 2. 🟢 COMMIT COMMITTED RIDERS: Transition 'accepted' or 'arrived' riders to 'in_trip'
    const poolUpdateInTrip = await client.query(
      `UPDATE route_pool 
       SET status = 'in_trip'
       WHERE matched_ride_offer_id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived')
       RETURNING id, user_id, status;`,
      [parsedRideId]
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
      [parsedRideId]
    );

    const recycledCount = poolResetMatched.rows.length;
    console.log(`♻️ [START TRIP] Recycled ${recycledCount} uncommitted ('matched') passengers back to 'searching'.`);

    // Commit the database transaction *before* calling the matching engine
    // to ensure the recycled users are safely back in 'searching' state in DB.
    await client.query("COMMIT");

    // 4. 🚀 TRIGGER MATCHING ENGINE
    // Run this asynchronously so it doesn't block the driver's client response.
    if (recycledCount > 0) {
      console.log(`⚡ [START TRIP] Triggering Corridor Matching Engine to re-assign ${recycledCount} passenger(s)...`);
      
      // OPTION A: Internal Function Call (Fastest & cleanest)
      if (typeof runMatchingEngine === "function") {
        runMatchingEngine().catch(err => {
          console.error("❌ [Matching Engine Process Error]:", err.message);
        });
      } 
      // OPTION B: Loopback API Trigger (Use if matching engine is mapped to an endpoint)
      else {
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



// POST /api/rides/start-trip
app.post("/api/rides/start-trip", async (req, res) => {
  const client = await pool.connect();
  try {
    const { ride_offer_id } = req.body;
    const parsedRideId = parseInt(ride_offer_id, 10);

    console.log(`🎬 [START TRIP] Request received for Ride Offer ID: ${ride_offer_id} (Parsed: ${parsedRideId})`);

    if (!parsedRideId || isNaN(parsedRideId)) {
      return res.status(400).json({ error: "Missing or invalid ride_offer_id parameter format." });
    }

    await client.query("BEGIN");

    // 🎯 UPGRADE 1: Trim and normalize state tracking strings, updating to standard 'in_trip'
    const updateOffer = await client.query(
      `UPDATE ride_offers 
       SET status = 'in_trip',
           created_at = NOW()
       WHERE id = $1 AND TRIM(LOWER(status)) = 'arrived'
       RETURNING id, status, route_id, driver_id;`,
      [parsedRideId]
    );

    if (updateOffer.rows.length === 0) {
      await client.query("ROLLBACK");
      console.log(`⚠️ [START TRIP REJECTION] Row #${parsedRideId} not found or status was not 'arrived'.`);
      return res.status(404).json({ 
        error: "Ride offer not found or current status is not 'arrived'. Cannot transition to trip state." 
      });
    }

    const activeOffer = updateOffer.rows[0];
    console.log(`✅ [START TRIP] ride_offers row #${activeOffer.id} successfully updated to [${activeOffer.status}]`);

    // 🎯 UPGRADE 2: Update route_pool records using clean, trim-safe casing matching
    const poolUpdate = await client.query(
      `UPDATE route_pool 
       SET status = 'in_trip',
           joined_at = NOW()
       WHERE matched_ride_offer_id = $1 AND TRIM(LOWER(status)) = 'matched'
       RETURNING id;`,
      [parsedRideId]
    );

    console.log(`👥 [START TRIP] Synchronized ${poolUpdate.rows.length} passenger pool entries to 'in_trip' matrix.`);

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Corridor trip successfully transitioned to transit matrix.",
      ride_offer: {
        id: activeOffer.id,
        status: activeOffer.status.trim(), // Strip any legacy column layout spacing safely
        routeId: activeOffer.route_id,
        driverId: activeOffer.driver_id
      }
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: START TRIP FAULT:", err.message);
    return res.status(500).json({ error: "Internal database state validation exception starting trip." });
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

    // 2. CONSTRAINT CHECK: Prevent overlapping active states for this driver
    const activeOfferCheck = await client.query(
      `SELECT id, status FROM ride_offers 
       WHERE driver_id = $1 AND TRIM(LOWER(status)) NOT IN ('completed', 'cancelled')
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

    // 🎯 4. Enforce saving Hub ID numbers (e.g., "1" and "4") instead of alphabet string names
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

    // 🎯 6. GENERATE THE NEW RIDE OFFER (Storing Hub ID numbers in start_location and end_location)
    const rideOfferQuery = await client.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng, status, accepted_passengers, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, 'pending', '{}', CURRENT_TIMESTAMP)
       RETURNING id AS offer_id, status, available_seats, price_per_seat;`,
      [
        parseInt(driver_id, 10), 
        resolvedVehicleId, 
        parseInt(route_id, 10), 
        finalStartLoc,     // Stores Hub ID number string (e.g., "1")
        finalEndLoc,       // Stores Hub ID number string (e.g., "4")
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
      message: "Corridor pool initialized with Hub ID numbers.",
      ride_id: activeOfferData.offer_id,
      status: activeOfferData.status,
      start_location: finalStartLoc, // Returns Hub ID number string
      end_location: finalEndLoc,     // Returns Hub ID number string
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
app.post("/api/rides/accept-proposal", async (req, res) => {
  const { ride_offer_id, user_id } = req.body;
  if (!ride_offer_id || !user_id) {
    return res.status(400).json({ error: "Missing ride_offer_id or user_id parameters." });
  }

  try {
    // 1. 🛡️ Data Integrity Check: Validate pool matching alignment records
    const poolCheck = await pool.query(
      `SELECT id, status 
       FROM route_pool 
       WHERE user_id = $1 AND matched_ride_offer_id = $2`,
      [user_id, ride_offer_id]
    );

    // If no row is returned, this user is attempting an unauthorized/invalid injection
    if (poolCheck.rows.length === 0) {
      return res.status(403).json({ 
        error: "Unauthorized acceptance. User is not matched to this specific ride offer route pool cluster." 
      });
    }

    // Optional optimization: Prevent executing a state change if they are already flagged as canceled/dropped out
    if (poolCheck.rows[0].status === 'dropped') {
      return res.status(400).json({ error: "Cannot accept. Passenger has already left or been dropped from this pool cluster." });
    }

    // 2. 📝 Execute Database Updates sequentially across the state transaction
    // Thread the user_id securely into the tracking array without duplicates
    await pool.query(
      `UPDATE ride_offers 
       SET accepted_passengers = array_append(
         array_remove(accepted_passengers, $2), $2
       ) 
       WHERE id = $1`,
      [ride_offer_id, user_id]
    );

    res.json({ 
      success: true, 
      message: "Pickup proposal accepted successfully and validated against route pool ledger." 
    });
  } catch (err) {
    console.error("❌ ERROR RECORDING PROPOSAL ACCEPTANCE:", err.stack);
    res.status(500).json({ error: "Failed to post acceptance state changes adjustment." });
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
  const { ride_offer_id, user_id } = req.body;

  if (!ride_offer_id || !user_id) {
    return res.status(400).json({ error: "Missing ride_offer_id or user_id mapping keys." });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const targetRideId = parseInt(ride_offer_id, 10);
    const targetUserId = parseInt(user_id, 10);

    // 🎯 ENFORCE: Only update if the current status is strictly 'accepted' or already 'arrived'
    const result = await client.query(
      `UPDATE route_pool 
       SET status = 'arrived' 
       WHERE user_id = $1 
         AND matched_ride_offer_id = $2 
         AND (TRIM(LOWER(status)) = 'accepted' OR TRIM(LOWER(status)) = 'arrived')
       RETURNING id, status;`,
      [targetUserId, targetRideId]
    );

    if (result.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: `State Transition Denied: No matching user tracking block found, or passenger status is not 'accepted'.` 
      });
    }

    // 2. Fetch driver's current macro state from ride_offers
    const driverQuery = await client.query(
      `SELECT TRIM(LOWER(status)) as status FROM ride_offers WHERE id = $1`,
      [targetRideId]
    );
    const driverStatus = driverQuery.rows[0]?.status || "accepted";

    // 3. Collect the updated passenger manifest for the UI co-rider matrix
    const passengersQuery = await client.query(
      `SELECT user_id, TRIM(LOWER(status)) as status FROM route_pool WHERE matched_ride_offer_id = $1`,
      [targetRideId]
    );

    await client.query("COMMIT");
    console.log(`📍 Presence Verification Success: User ${targetUserId} transitioned from accepted -> arrived.`);

    return res.status(200).json({ 
      success: true, 
      message: "Arrival presence validated cleanly.",
      driverStatus: driverStatus, 
      passengers: passengersQuery.rows.map(row => ({
        userId: parseInt(row.user_id, 10),
        status: row.status || "accepted"
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
      const isAccepted = acceptedIdsArray.includes(pId);

      return {
        user_id: pId,
        status: isAccepted ? "accepted" : (passenger.status || "matched"),
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




app.post("/api/rides/driver-accept", async (req, res) => {
  const { ride_offer_id, driver_id } = req.body;

  if (!ride_offer_id || !driver_id) {
    return res.status(400).json({ error: "Missing ride_offer_id or driver_id parameters." });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Fetch details using actual schema column fields
    const offerQuery = `
      SELECT id, route_id, accepted_passengers, total_seats 
      FROM ride_offers 
      WHERE id = $1 AND driver_id = $2 FOR UPDATE
    `;
    const offerRes = await client.query(offerQuery, [ride_offer_id, driver_id]);

    if (offerRes.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Active dispatch offer not found or driver allocation mismatch." });
    }

    const offer = offerRes.rows[0];
    const { route_id, total_seats } = offer;

    let confirmedPassengerIds = [];
    if (offer.accepted_passengers) {
      if (Array.isArray(offer.accepted_passengers)) {
        confirmedPassengerIds = offer.accepted_passengers.map(id => parseInt(id, 10));
      } else if (typeof offer.accepted_passengers === "string") {
        confirmedPassengerIds = offer.accepted_passengers
          .replace(/{|}/g, "")
          .split(",")
          .map(id => parseInt(id.trim(), 10))
          .filter(id => !isNaN(id));
      }
    }

    if (confirmedPassengerIds.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "Cannot accept dispatch. Minimum core passenger threshold not met." });
    }

    const finalAvailableSeats = total_seats - confirmedPassengerIds.length;
    
    // 1. Update the master ride offer state to 'accepted'
    const updateOfferQuery = `
      UPDATE ride_offers 
      SET status = 'accepted', available_seats = $1 
      WHERE id = $2
    `;
    await client.query(updateOfferQuery, [finalAvailableSeats, ride_offer_id]);

    // 2. 🎯 SYNCING STATUS: Also switch the active manifest pairs inside route_pool to 'accepted'
    if (confirmedPassengerIds.length > 0) {
      const updatePassengerPoolQuery = `
        UPDATE route_pool 
        SET status = 'accepted' 
        WHERE matched_ride_offer_id = $1 
        AND user_id IN (${confirmedPassengerIds.map((_, i) => `$${i + 2}`).join(",")})
      `;
      await client.query(updatePassengerPoolQuery, [ride_offer_id, ...confirmedPassengerIds]);
    }

    const deleteQueueQuery = `DELETE FROM driver_queue WHERE driver_id = $1`;
    await client.query(deleteQueueQuery, [driver_id]);

    // Dynamically resolve active hub assignment parameters from the route pool data
    let targetHubId = 1; 
    const hubLookupQuery = `SELECT pickup_hub_id FROM route_pool WHERE matched_ride_offer_id = $1 LIMIT 1`;
    const hubRes = await client.query(hubLookupQuery, [ride_offer_id]);
    if (hubRes.rows.length > 0 && hubRes.rows[0].pickup_hub_id) {
      targetHubId = parseInt(hubRes.rows[0].pickup_hub_id, 10);
    }

    // Recycle unaccepted entries inside route_pool table mapping relationships
    if (confirmedPassengerIds.length > 0) {
      const resetLeftoverPoolQuery = `
        UPDATE route_pool 
        SET status = 'searching', matched_ride_offer_id = NULL 
        WHERE matched_ride_offer_id = $1 
        AND user_id NOT IN (${confirmedPassengerIds.map((_, i) => `$${i + 2}`).join(",")})
        RETURNING user_id
      `;
      
      const resetRes = await client.query(resetLeftoverPoolQuery, [ride_offer_id, ...confirmedPassengerIds]);
      console.log(`[Corridor Dispatch] Locked ${confirmedPassengerIds.length} riders. Kicked out ${resetRes.rows.length} pending riders back to searching stack.`);
    }

    await client.query("COMMIT");

    // Invoke matching engine asynchronously
    executeCorridorAutoMatch(pool, route_id, targetHubId)
      .then(() => console.log(`[AutoMatch] Execution loop context processed cleanly for Route ${route_id}`))
      .catch((err) => console.error("[AutoMatch Error] Async dispatch node loop failure:", err));

    return res.status(200).json({
      success: true,
      message: "Manifest and passenger pool locked in as 'accepted'. Leftover riders recycled.",
      rideOfferId: ride_offer_id,
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

  try {
    const query = `
      UPDATE ride_offers
      SET status = 'rejected'
      WHERE id = $1 AND driver_id = $2
      RETURNING id;
    `;
    await pool.query(query, [ride_offer_id, driver_id]);
    
    res.json({ success: true, message: "Offer cluster declined cleanly." });
  } catch (err) {
    console.error("Error writing driver rejection metrics:", err);
    res.status(500).json({ error: "Internal Write Process Failure" });
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
  // 🛡️ GUARD: Prevent crash if req.body or req itself isn't parsed/populated
  if (!req || !req.body) {
    return res.status(400).json({ 
      error: "Malformed request payload. Make sure express.json() middleware is active and Headers include Content-Type: application/json" 
    });
  }

  const { ride_offer_id } = req.body;

  if (!ride_offer_id) {
    return res.status(400).json({ error: "Missing required parameter: ride_offer_id" });
  }

  const parsedOfferId = parseInt(ride_offer_id, 10);
  if (isNaN(parsedOfferId)) {
    return res.status(400).json({ error: "Invalid format: ride_offer_id must be an integer." });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 🎯 STRICT MODE: Only allows transition if the driver already accepted the ride ('accepted')
    const updateRideOfferQuery = await client.query(
      `UPDATE ride_offers 
       SET status = 'arrived' 
       WHERE id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived')
       RETURNING id, status;`,
      [parsedOfferId]
    );

    if (updateRideOfferQuery.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: `Ride offer ID ${parsedOfferId} not found or status cannot transition to arrived (Must be 'accepted' first).` 
      });
    }

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Driver arrival state updated and committed successfully.",
      ride_offer: updateRideOfferQuery.rows[0]
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: ARRIVAL SYSTEM FAULT:", err.message);
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
  const { ride_offer_id } = req.body;

  if (!ride_offer_id) {
    return res.status(400).json({ error: "Missing ride_offer_id parameters." });
  }

  try {
    // 1. Fetch base pricing configuration directly from the database row
    const offerCheck = await pool.query(
      "SELECT price_per_seat FROM ride_offers WHERE id = $1",
      [ride_offer_id]
    );

    if (offerCheck.rows.length === 0) {
      return res.status(404).json({ error: "Ride offer target profile instance not found." });
    }

    const pricePerSeat = parseFloat(offerCheck.rows[0].price_per_seat) || 0;

    // 2. Synchronize associated users to 'completed'
    const passengerUpdate = await pool.query(
      `UPDATE route_pool 
       SET status = 'completed'
       WHERE matched_ride_offer_id = $1
       RETURNING id, user_id, status`,
      [ride_offer_id]
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
      [calculatedFinalPrice, ride_offer_id]
    );

    console.log(`🏁 Transactional Lifecycle Sealed for Corridor Run ID #${ride_offer_id}:`);
    console.log(`   🔹 ride_offers status -> completed (final_price: ETB ${calculatedFinalPrice})`);
    console.log(`   🔹 route_pool passengers -> ${completedRidersCount} records synchronized`);

    return res.status(200).json({
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




/* ===========================================================================
   🏁 7. ENGINE DISPATCH INITIALIZER LISTEN
=========================================================================== */
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Corridor Engine active on port ${PORT}`);
});
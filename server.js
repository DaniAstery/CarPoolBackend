const express = require('express');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;

// ⚡ Global Parsers & Middlewares
app.use(cors());
app.use(express.json());

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



/* ===========================================================================
   👥 3. CENTRAL CONTINUOUS MATCHING ENGINE (Headcount-Based Allocation)
=========================================================================== */
/* ========================================================================= 
   🚀 ENGINE: UNIFIED CORRIDOR AUTO-MATCH & RECOVERY PIPELINE
   ========================================================================= */
/* ========================================================================= 
   🚀 ENGINE: DYNAMIC CAPACITY CORRIDOR AUTO-MATCH & RECOVERY
   ========================================================================= */
async function executeCorridorAutoMatch(client, route_id, hub_id, forcedRideOfferId = null) {
  console.log(`🎬 Dynamic Matching Engine Invoked: Route [${route_id}] | Hub [${hub_id}]`);

  // -------------------------------------------------------------------------
  // STEP 1: FETCH BOTH 'searching' AND DRIVERLESS 'matched' PASSENGERS (FIFO)
  // -------------------------------------------------------------------------
  // 🎯 UPGRADE: Included passengers who are already peer-matched but missing a driver!
  const waitingPassengersQuery = await client.query(
    `SELECT id, user_id, pickup_hub_id, dropoff_hub_id, status FROM route_pool 
     WHERE route_id = $1 
       AND pickup_hub_id = $2 
       AND (
         TRIM(LOWER(status)) = 'searching' 
         OR (TRIM(LOWER(status)) = 'matched' AND matched_ride_offer_id IS NULL)
       )
     ORDER BY joined_at ASC FOR UPDATE SKIP LOCKED;`,
    [route_id, hub_id]
  );

  let availableRiders = waitingPassengersQuery.rows;
  if (availableRiders.length === 0) {
    console.log(`ℹ️ No eligible passengers found at Hub [${hub_id}] for Route [${route_id}].`);
    return { status: "idle", message: "Passenger pool empty." };
  }

  // -------------------------------------------------------------------------
  // STEP 2: REACTIVE RECOVERY & TOP-UP FOR EXISTING RIDE OFFERS
  // -------------------------------------------------------------------------
  let openOffers = [];

  if (forcedRideOfferId) {
    const forcedOfferQuery = await client.query(
      `SELECT id, available_seats, total_seats FROM ride_offers WHERE id = $1 FOR UPDATE;`,
      [forcedRideOfferId]
    );
    if (forcedOfferQuery.rows.length > 0) {
      openOffers = forcedOfferQuery.rows;
    }
  } else {
    const openOffersQuery = await client.query(
      `SELECT id, available_seats, total_seats FROM ride_offers
       WHERE route_id = $1 AND TRIM(LOWER(status)) IN ('pending', 'accepted', 'arrived') AND available_seats > 0
       ORDER BY created_at ASC FOR UPDATE;`,
      [route_id]
    );
    openOffers = openOffersQuery.rows;
  }

  let passengerIndex = 0;

  for (const offer of openOffers) {
    let seatsAvailable = offer.available_seats; 
    let matchedInThisCycle = 0;

    while (seatsAvailable > 0 && passengerIndex < availableRiders.length) {
      const targetPassenger = availableRiders[passengerIndex];
      
      console.log(`⚡ Binding Driver: Pairing Passenger Row ID ${targetPassenger.id} into Ride Offer ${offer.id}`);

      // 🎯 Fixes your table parameters: Always keeps status as 'matched' and links the exact offer ID
      await client.query(
        `UPDATE route_pool 
         SET status = 'matched', matched_ride_offer_id = $1 
         WHERE id = $2;`,
        [offer.id, targetPassenger.id]
      );

      seatsAvailable--;
      passengerIndex++;
      matchedInThisCycle++;
    }

    if (matchedInThisCycle > 0) {
      const totalMatchedNow = offer.total_seats - seatsAvailable;
      const finalPrice = totalMatchedNow >= 3 ? 80 : 100; 

      await client.query(
        `UPDATE ride_offers 
         SET available_seats = $1, price_per_seat = $2 
         WHERE id = $3;`,
        [seatsAvailable, finalPrice, offer.id]
      );
    }

    if (passengerIndex >= availableRiders.length) break;
  }

  // Slice out remaining passengers who didn't get sorted into vacancies
  availableRiders = availableRiders.slice(passengerIndex);

  // If a forced driver injection loop just completed filling up, exit successfully early
  if (forcedRideOfferId && passengerIndex > 0) {
    return {
      status: "success",
      ride_id: forcedRideOfferId,
      matchedCount: passengerIndex,
      message: "Direct injection match successful within transaction."
    };
  }

  // -------------------------------------------------------------------------
  // STEP 3: INITIALIZE NEW CORRIDOR BATCH (Fallback for standard pooling)
  // -------------------------------------------------------------------------
  if (availableRiders.length < 2) {
    return { 
      status: "buffering", 
      matchedCount: availableRiders.length, 
      message: "Awaiting baseline pairs to call a new driver dispatch." 
    };
  }

  const topDriverQuery = await client.query(
    `SELECT id, driver_id, vehicle_id, capacity FROM driver_queue 
     WHERE route_id = $1 AND current_hub_id = $2 AND TRIM(LOWER(status)) IN ('queued', 'waiting')
     ORDER BY entered_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;`,
    [route_id, hub_id]
  );

  const activeDriver = topDriverQuery.rows[0];

  // DRIVERLESS PEER-MATCHING INTERCEPT:
  if (!activeDriver) {
    const peerBatchSize = Math.min(availableRiders.length, 3);
    const peerBatch = availableRiders.slice(0, peerBatchSize);
    const peerPassengerIds = peerBatch.map(p => p.id);

    console.log(`ℹ️ No driver queue found. Executing Driverless Peer Match for IDs: ${JSON.stringify(peerPassengerIds)}`);

    await client.query(
      `UPDATE route_pool 
       SET status = 'matched', matched_ride_offer_id = NULL 
       WHERE id = ANY($1::int[]);`,
      [peerPassengerIds]
    );

    return { 
      status: "peer_group_formed", 
      matchedCount: peerBatchSize, 
      message: "Passengers grouped successfully in peer standby matrix. Awaiting driver claim." 
    };
  }

  // -------------------------------------------------------------------------
  // STEP 4: STANDARD DISPATCH MATCHING (Driver is present in queue)
  // -------------------------------------------------------------------------
  const maxBatchSize = Math.min(activeDriver.capacity, 3); 
  const initialBatch = availableRiders.slice(0, maxBatchSize);
  const matchedCount = initialBatch.length;

  const routeMetaData = await client.query(`SELECT base_price FROM routes WHERE id = $1;`, [route_id]);
  const basePrice = parseFloat(routeMetaData.rows[0].base_price) || 100;

  const targetPickupId = initialBatch[0].pickup_hub_id;
  const targetDropoffId = initialBatch[0].dropoff_hub_id;
  
  const hubNamesQuery = await client.query(`SELECT id, name FROM hubs WHERE id IN ($1, $2);`, [targetPickupId, targetDropoffId]);
  const hubMap = {};
  hubNamesQuery.rows.forEach(row => { hubMap[row.id] = row.name; });
  
  const startLoc = hubMap[targetPickupId] || "Terminal Station Start";
  const endLoc = hubMap[targetDropoffId] || "Terminal Station End";

  const finalPricePerSeat = matchedCount === 3 ? 80 : basePrice;
  const remainingSeats = activeDriver.capacity - matchedCount;
  const passengerIds = initialBatch.map(p => p.id);

  // 🎯 FIX: Removed non-existent updated_at from the fallback query layout as well
  const matchTransactionQuery = await client.query(
    `WITH inserted_ride AS (
      INSERT INTO ride_offers (
        driver_id, vehicle_id, route_id, start_location, end_location, 
        total_seats, available_seats, price_per_seat, status, departure_time, created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW(), NOW())
      RETURNING id
    )
    UPDATE route_pool 
    SET status = 'matched', matched_ride_offer_id = (SELECT id FROM inserted_ride) 
    WHERE id = ANY($9::int[])
    RETURNING (SELECT id FROM inserted_ride) AS new_ride_offer_id;`,
    [
      activeDriver.driver_id, activeDriver.vehicle_id, route_id, startLoc, endLoc,
      activeDriver.capacity, remainingSeats, finalPricePerSeat, passengerIds
    ]
  );

  const generatedRideOfferId = matchTransactionQuery.rows[0]?.new_ride_offer_id;

  await client.query(
    `UPDATE driver_queue SET status = 'dispatched' WHERE vehicle_id = $1;`,
    [activeDriver.vehicle_id]
  );

  return {
    status: "success",
    ride_id: generatedRideOfferId,
    matchedCount: matchedCount,
    finalPrice: finalPricePerSeat
  };
}
/* ===========================================================================
   🎟️ 4. CORE PASSENGER PIPELINE MANIFESTS
=========================================================================== */

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
app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;
    const result = await pool.query(
      `SELECT
        r.id AS route_id,
        r.capacity,
        r.base_price,
        ARRAY_AGG(h2.name ORDER BY rs2.stop_order) AS stops
      FROM routes r
      JOIN route_stops rs ON rs.route_id = r.id
      JOIN hubs h ON h.id = rs.hub_id
      JOIN route_stops rs2 ON rs2.route_id = r.id
      JOIN hubs h2 ON h2.id = rs2.hub_id
      WHERE LOWER(h.name) = LOWER($1)
      GROUP BY r.id, r.capacity, r.base_price;`,
      [pickupHub]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching routes from hub:", err.message);
    res.status(500).json({ error: "Server error" });
  }
});


/* ---------------- PASSENGER SIDE: JOIN CORRIDOR WAITING POOL ---------------- */
// POST /api/routes/join
app.post("/api/routes/join", async (req, res) => {
  const { route_id, user_id, pickup_hub_id, dropoff_hub_id, ride_offer_id } = req.body;

  if (!route_id || !user_id) {
    return res.status(400).json({ error: "Missing required fields: route_id and user_id" });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 🛡️ STAGE 1: DETECT AND BLOCK DUPLICATE REGISTRATIONS FOR ACTIVE/ONGOING TRIPS ONLY
    // 💡 FIX: Added "AND TRIM(LOWER(status)) != 'completed'" so old finished records are safely ignored.
    const strictDuplicateCheck = await client.query(
      `SELECT id, status, route_id, pickup_hub_id, dropoff_hub_id, matched_ride_offer_id 
       FROM route_pool 
       WHERE user_id = $1 
         AND TRIM(LOWER(status)) != 'completed'
         AND (
           (matched_ride_offer_id = $2 AND matched_ride_offer_id IS NOT NULL) 
           OR 
           TRIM(LOWER(status)) IN ('searching', 'accepted', 'arrived', 'matched', 'in_trip', 'intransit')
         )
       LIMIT 1 FOR UPDATE;`, 
      [parseInt(user_id, 10), ride_offer_id ? parseInt(ride_offer_id, 10) : null]
    );

    if (strictDuplicateCheck.rows.length > 0) {
      const activeRecord = strictDuplicateCheck.rows[0];
      await client.query("ROLLBACK"); // Instantly kill transaction, freeing resource threads

      const normalizedStatus = activeRecord.status.trim().toLowerCase();
      console.log(`🛑 [Duplicate Guard] Blocked double registration. User ${user_id} already exists in active row ID: ${activeRecord.id} [State: ${normalizedStatus}]`);
      
      // Dynamic App State Recovery routing parameters based on current lifecycle stage
      let targetScreen = "MatchingScreen"; 
      if (["matched", "accepted", "arrived"].includes(normalizedStatus)) {
        targetScreen = "LiveTrackingScreen"; 
      } else if (["in_trip", "intransit"].includes(normalizedStatus)) {
        targetScreen = "TripInProgressPassengerScreen"; 
      }

      return res.status(200).json({
        message: "User registration already active in an ongoing corridor pool matrix.",
        alreadyActive: true,
        navigateTo: targetScreen, 
        pool: {
          ...activeRecord,
          status: normalizedStatus
        },
        match_pipeline: { 
          status: "recovered", 
          message: "Re-routed to active session state.",
          ride_offer_id: activeRecord.matched_ride_offer_id
        }
      });
    }

    // STAGE 2: RESOLVE HUB STATIONS IF MISSING FROM PAYLOAD
    let finalPickup = pickup_hub_id;
    let finalDropoff = dropoff_hub_id;

    if (!finalPickup || !finalDropoff) {
      const routeStops = await client.query(
        `SELECT hub_id FROM route_stops WHERE route_id = $1 ORDER BY stop_order ASC`,
        [route_id]
      );
      if (routeStops.rows.length >= 2) {
        finalPickup = finalPickup || routeStops.rows[0].hub_id;
        finalDropoff = finalDropoff || routeStops.rows[routeStops.rows.length - 1].hub_id;
      } else {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "Could not auto-map terminal checkpoints." });
      }
    }

    // STAGE 3: EXECUTE CLEAN POOL ENTRY GENERATION FOR THE NEW TRIP
    const newEntry = await client.query(
      `INSERT INTO route_pool (route_id, user_id, pickup_hub_id, dropoff_hub_id, status, joined_at)
       VALUES ($1, $2, $3, $4, 'searching', NOW())
       RETURNING *`,
      [route_id, user_id, finalPickup, finalDropoff]
    );

    // Fire matching automation engine
    const matchPipeline = await executeCorridorAutoMatch(client, route_id, finalPickup);

    await client.query("COMMIT");

    return res.status(201).json({
      message: "Success",
      alreadyActive: false,
      pool: newEntry.rows[0],
      match_pipeline: matchPipeline
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ JOIN CORRIDOR POOL ERROR:", err.message);
    return res.status(500).json({ error: `Internal matchmaking error: ${err.message}` });
  } finally {
    client.release();
  }
});



/* ---------------- START TRIP LIFECYCLE CONTROLLER ---------------- */
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

    // 1. Update the master ride offer's state
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

    // 🎯 FIX: Updated where clause to look for passengers who are 'accepted' or 'arrived' 
    // 🎯 NOTE: Ensure 'in_trip' (or 'inprogress') is whitelisted in your check_route_pool_status table constraint!
    const poolUpdate = await client.query(
      `UPDATE route_pool 
       SET status = 'in_trip'
       WHERE matched_ride_offer_id = $1 
         AND TRIM(LOWER(status)) IN ('accepted', 'arrived', 'matched')
       RETURNING id, status;`,
      [parsedRideId]
    );

    console.log(`👥 [START TRIP] Synchronized ${poolUpdate.rows.length} passenger pool matrix records to 'in_trip'.`);

    await client.query("COMMIT");

    return res.status(200).json({
      success: true,
      message: "Corridor trip successfully transitioned to transit matrix.",
      ride_offer: {
        id: activeOffer.id,
        status: activeOffer.status.trim(),
        routeId: activeOffer.route_id,
        driverId: activeOffer.driver_id
      },
      synchronizedRidersCount: poolUpdate.rows.length
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: START TRIP FAULT:", err.message);
    
    // Sniff for constraint failures to output exact debugging tips in logs
    if (err.message.includes("violates check constraint")) {
      console.error("💡 DB SCHEMA ALERT: You need to run an ALTER TABLE statement to add 'in_trip' to your constraint enum array!");
    }

    return res.status(500).json({ 
      error: "Internal database state validation exception starting trip.",
      details: err.message 
    });
  } finally {
    client.release();
  }
});
/* ---------------- PASSENGER JOINS THE POOL (BOOK A SEAT) ---------------- */
app.post("/api/rides/join-pool", async (req, res) => {
  const client = await pool.connect();
  try {
    const { ride_id, passenger_id, pickup_hub_id, dropoff_hub_id, seats_booked } = req.body;

    if (!ride_id || !passenger_id || !pickup_hub_id || !dropoff_hub_id || !seats_booked) {
      return res.status(400).json({ error: "Missing required booking details." });
    }

    await client.query("BEGIN");

    const rideCheck = await client.query(
      `SELECT available_seats, total_seats FROM rides WHERE id = $1 FOR UPDATE`,
      [ride_id]
    );

    if (rideCheck.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Active ride run not found." });
    }

    const { available_seats } = rideCheck.rows[0];

    if (available_seats < seats_booked) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "Not enough seats available in this pool." });
    }

    await client.query(
      `UPDATE rides SET available_seats = available_seats - $1 WHERE id = $2`,
      [seats_booked, ride_id]
    );

    const bookingResult = await client.query(
      `INSERT INTO ride_bookings (ride_id, passenger_id, pickup_hub_id, dropoff_hub_id, seats_booked, status)
       VALUES ($1, $2, $3, $4, $5, 'booked') RETURNING id AS booking_id, status;`,
      [ride_id, passenger_id, pickup_hub_id, dropoff_hub_id, seats_booked]
    );

    await client.query("COMMIT");
    res.status(201).json({
      message: "Successfully joined the corridor ride pool!",
      booking_id: bookingResult.rows[0].booking_id,
      status: bookingResult.rows[0].status
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error joining pool manifest:", err.message);
    res.status(500).json({ error: "Server error joining the pool." });
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

// POST /api/rides/create
// POST /api/rides/create
app.post("/api/rides/create", async (req, res) => {
  const client = await pool.connect();
  try {
    const { driver_id, route_id, total_seats } = req.body;

    if (!driver_id || !route_id) {
      return res.status(400).json({ error: "Missing driver_id or route_id parameters vectors." });
    }

    // 1. DYNAMIC PROFILE EXTRACTION
    const driverProfileQuery = await pool.query(
      `SELECT full_name FROM users WHERE id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );
    
    const vehicleQuery = await pool.query(
      `SELECT id, make, model, capacity FROM vehicles WHERE driver_id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );

    if (driverProfileQuery.rows.length === 0) {
      return res.status(404).json({ error: "No user profile matches this driver_id." });
    }

    const driverName = driverProfileQuery.rows[0].full_name;
    const resolvedVehicleId = vehicleQuery.rows[0]?.id || null;
    const carMake = vehicleQuery.rows[0]?.make || "Corridor";
    const carModel = vehicleQuery.rows[0]?.model || "Vehicle";
    const vehicleCapacity = vehicleQuery.rows[0]?.capacity || total_seats || 4;

    // 🚨 BEGIN TRANSACTION BLOCK
    await client.query("BEGIN");

    // 2. CONSTRAINT CHECK: Prevent overlapping running states for this driver
    const activeOfferCheck = await client.query(
      `SELECT id, status FROM ride_offers 
       WHERE driver_id = $1 AND TRIM(LOWER(status)) NOT IN ('completed', 'cancelled')
       LIMIT 1;`,
      [parseInt(driver_id, 10)]
    );

    if (activeOfferCheck.rows.length > 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: `Unacceptable overlap: ${driverName} already has an active corridor trip running (Offer ID #${activeOfferCheck.rows[0].id}).` 
      });
    }

    // 3. Collect Route, Geometry, and Hub Information
    const routeGeomQuery = await client.query(
      `SELECT r.base_price, 
              ARRAY_AGG(h.name ORDER BY rs.stop_order) AS stops,
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
    if (!routeData || !routeData.stops || routeData.stops.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "The chosen corridor route blueprint has an incomplete stop list." });
    }

    const startLoc = routeData.stops[0];
    const endLoc = routeData.stops[routeData.stops.length - 1];
    const pricePerSeat = routeData.base_price;

    // 4. SYNCHRONIZE DRIVER QUEUE STATE MATRIX AS 'QUEUED'
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

    // 5. 🎯 FIX: Removed non-existent updated_at column from fields list and values footprint
    const rideOfferQuery = await client.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng, status, accepted_passengers, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, 'pending', '{}', CURRENT_TIMESTAMP)
       RETURNING id AS offer_id, status, available_seats, price_per_seat;`,
      [parseInt(driver_id, 10), resolvedVehicleId, parseInt(route_id, 10), startLoc, endLoc, vehicleCapacity, vehicleCapacity, pricePerSeat, routeData.start_lat, routeData.start_lng]
    );
    
    let activeOfferData = rideOfferQuery.rows[0];

    // DIRECT INJECTION ASSIGNMENT
    console.log(`⚡ Driver online. Executing direct matching loop for Offer ID #${activeOfferData.offer_id}...`);
    const matchPipelineResult = await executeCorridorAutoMatch(
      client, 
      parseInt(route_id, 10), 
      routeData.start_hub_id, 
      activeOfferData.offer_id
    );

    // If the pipeline matched passengers into this specific offer, refresh returned state values
    if (matchPipelineResult.status === "success" && matchPipelineResult.ride_id === activeOfferData.offer_id) {
      console.log(`🎉 Instant Match Executed! ${matchPipelineResult.matchedCount} riders synchronized directly into the new trip run.`);
      
      const refreshedOfferQuery = await client.query(
        `SELECT available_seats, price_per_seat FROM ride_offers WHERE id = $1;`,
        [activeOfferData.offer_id]
      );
      if (refreshedOfferQuery.rows.length > 0) {
        activeOfferData.available_seats = refreshedOfferQuery.rows[0].available_seats;
        activeOfferData.price_per_seat = refreshedOfferQuery.rows[0].price_per_seat;
      }
    }

    await client.query("COMMIT");

    const filledCount = vehicleCapacity - activeOfferData.available_seats;

    return res.status(201).json({
      message: "Corridor pool initialized dynamically and driver queue synced.",
      ride_id: activeOfferData.offer_id,
      status: activeOfferData.status,
      start_location: startLoc,
      end_location: endLoc,
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
    return res.status(500).json({ error: "Internal server error deploying live ride offer matrix." });
  } finally {
    client.release();
  }
});// POST /api/rides/create
app.post("/api/rides/create", async (req, res) => {
  const client = await pool.connect();
  try {
    const { driver_id, route_id, total_seats } = req.body;

    if (!driver_id || !route_id) {
      return res.status(400).json({ error: "Missing driver_id or route_id parameters vectors." });
    }

    // 1. DYNAMIC PROFILE EXTRACTION
    const driverProfileQuery = await pool.query(
      `SELECT full_name FROM users WHERE id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );
    
    const vehicleQuery = await pool.query(
      `SELECT id, make, model, capacity FROM vehicles WHERE driver_id = $1 LIMIT 1`, 
      [parseInt(driver_id, 10)]
    );

    if (driverProfileQuery.rows.length === 0) {
      return res.status(404).json({ error: "No user profile matches this driver_id." });
    }

    const driverName = driverProfileQuery.rows[0].full_name;
    const resolvedVehicleId = vehicleQuery.rows[0]?.id || null;
    const carMake = vehicleQuery.rows[0]?.make || "Corridor";
    const carModel = vehicleQuery.rows[0]?.model || "Vehicle";
    const vehicleCapacity = vehicleQuery.rows[0]?.capacity || total_seats || 4;

    // 🚨 BEGIN TRANSACTION BLOCK
    await client.query("BEGIN");

    // 2. CONSTRAINT CHECK: Prevent overlapping running states for this driver
    const activeOfferCheck = await client.query(
      `SELECT id, status FROM ride_offers 
       WHERE driver_id = $1 AND TRIM(LOWER(status)) NOT IN ('completed', 'cancelled')
       LIMIT 1;`,
      [parseInt(driver_id, 10)]
    );

    if (activeOfferCheck.rows.length > 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ 
        error: `Unacceptable overlap: ${driverName} already has an active corridor trip running (Offer ID #${activeOfferCheck.rows[0].id}).` 
      });
    }

    // 3. Collect Route, Geometry, and Hub Information
    const routeGeomQuery = await client.query(
      `SELECT r.base_price, 
              ARRAY_AGG(h.name ORDER BY rs.stop_order) AS stops,
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
    if (!routeData || !routeData.stops || routeData.stops.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "The chosen corridor route blueprint has an incomplete stop list." });
    }

    const startLoc = routeData.stops[0];
    const endLoc = routeData.stops[routeData.stops.length - 1];
    const pricePerSeat = routeData.base_price;

    // 4. SYNCHRONIZE DRIVER QUEUE STATE MATRIX AS 'QUEUED'
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

    // 5. 🎯 FIX: Removed non-existent updated_at column from fields list and values footprint
    const rideOfferQuery = await client.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng, status, accepted_passengers, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, 'pending', '{}', CURRENT_TIMESTAMP)
       RETURNING id AS offer_id, status, available_seats, price_per_seat;`,
      [parseInt(driver_id, 10), resolvedVehicleId, parseInt(route_id, 10), startLoc, endLoc, vehicleCapacity, vehicleCapacity, pricePerSeat, routeData.start_lat, routeData.start_lng]
    );
    
    let activeOfferData = rideOfferQuery.rows[0];

    // DIRECT INJECTION ASSIGNMENT
    console.log(`⚡ Driver online. Executing direct matching loop for Offer ID #${activeOfferData.offer_id}...`);
    const matchPipelineResult = await executeCorridorAutoMatch(
      client, 
      parseInt(route_id, 10), 
      routeData.start_hub_id, 
      activeOfferData.offer_id
    );

    // If the pipeline matched passengers into this specific offer, refresh returned state values
    if (matchPipelineResult.status === "success" && matchPipelineResult.ride_id === activeOfferData.offer_id) {
      console.log(`🎉 Instant Match Executed! ${matchPipelineResult.matchedCount} riders synchronized directly into the new trip run.`);
      
      const refreshedOfferQuery = await client.query(
        `SELECT available_seats, price_per_seat FROM ride_offers WHERE id = $1;`,
        [activeOfferData.offer_id]
      );
      if (refreshedOfferQuery.rows.length > 0) {
        activeOfferData.available_seats = refreshedOfferQuery.rows[0].available_seats;
        activeOfferData.price_per_seat = refreshedOfferQuery.rows[0].price_per_seat;
      }
    }

    await client.query("COMMIT");

    const filledCount = vehicleCapacity - activeOfferData.available_seats;

    return res.status(201).json({
      message: "Corridor pool initialized dynamically and driver queue synced.",
      ride_id: activeOfferData.offer_id,
      status: activeOfferData.status,
      start_location: startLoc,
      end_location: endLoc,
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
    return res.status(500).json({ error: "Internal server error deploying live ride offer matrix." });
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




app.get("/api/rides/active-proposal/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ error: "Missing userId parameter path." });
  }

  try {
    // Step A: Find the active matched ride for this user from the route pool
    const poolQuery = await pool.query(
      `SELECT matched_ride_offer_id 
       FROM route_pool 
       WHERE user_id = $1 AND status = 'matched' 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(userId, 10)]
    );

    // If the passenger isn't matched yet, return a clean 404 so the app can handle the loading/waiting state
    if (poolQuery.rows.length === 0 || !poolQuery.rows[0].matched_ride_offer_id) {
      return res.status(404).json({ error: "No active matched trip allocation found for this user." });
    }

    const activeRideOfferId = poolQuery.rows[0].matched_ride_offer_id;

    // Step B: Pull the acceptance tracking array directly from the resolved ride offer row
    const rideQuery = await pool.query(
      `SELECT id, route_id, accepted_passengers 
       FROM ride_offers 
       WHERE id = $1`,
      [activeRideOfferId]
    );

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "The allocated ride run records are missing from the cluster." });
    }

    const { id, route_id, accepted_passengers } = rideQuery.rows[0];

    // Step C: Gather the complete passenger manifest group bound to this exact same trip
    const manifestQuery = await pool.query(
      `SELECT user_id 
       FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status = 'matched'`,
      [activeRideOfferId]
    );

    const manifestUserIds = manifestQuery.rows.map(row => row.user_id);

    // Return everything to the phone layout cleanly
    return res.json({
      rideOfferId: id,
      routeId: route_id,
      manifestPassengers: manifestUserIds,          // Array of all passenger IDs (e.g., [1, 2])
      acceptedPassengers: accepted_passengers || []  // Array of IDs who tapped accept (e.g., [2])
    });

  } catch (err) {
    console.error("❌ BACKEND STATE RESOLVER ERROR:", err.stack);
    return res.status(500).json({ error: "Internal transactional exception mapping execution matrices." });
  }
});


/* =========================================================================
   1. 📡 LIVE TRACKING ENGINE: Fetch Passenger Manifest Status & Driver GPS Coordinates
/* ========================================================================= 
   📦 LIVE TRACKING STREAM CONTROLLER (FIXED FOR ARRIVED SESSION RECOVERY)
   ========================================================================= */
app.get("/api/rides/active-proposal/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ error: "Missing userId parameter path." });
  }

  try {
    // 🎯 Step A: Updated to find active allocations whether status is 'matched' OR 'arrived'
    const poolQuery = await pool.query(
      `SELECT matched_ride_offer_id, status
       FROM route_pool 
       WHERE user_id = $1 AND status IN ('matched', 'arrived') 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(userId, 10)]
    );

    // If the passenger isn't matched or arrived yet, return a clean 404
    if (poolQuery.rows.length === 0 || !poolQuery.rows[0].matched_ride_offer_id) {
      return res.status(404).json({ error: "No active matched trip allocation found for this user." });
    }

    const activeRideOfferId = poolQuery.rows[0].matched_ride_offer_id;
    const userCurrentPoolStatus = poolQuery.rows[0].status;

    // Step B: Pull the acceptance tracking array directly from the resolved ride offer row
    const rideQuery = await pool.query(
      `SELECT id, route_id, accepted_passengers 
       FROM ride_offers 
       WHERE id = $1`,
      [activeRideOfferId]
    );

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "The allocated ride run records are missing from the cluster." });
    }

    const { id, route_id, accepted_passengers } = rideQuery.rows[0];

    // 🎯 Step C: Gather the complete passenger manifest group bound to this exact same trip (both matched and arrived riders)
    const manifestQuery = await pool.query(
      `SELECT user_id, status 
       FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status IN ('matched', 'arrived')`,
      [activeRideOfferId]
    );

    // Build a structured list of passengers with their real-time statuses so the frontend matrix can map them easily
    const passengersWithStatus = manifestQuery.rows.map(row => ({
      userId: row.user_id,
      status: row.status // Will return 'matched' or 'arrived'
    }));

    const manifestUserIds = manifestQuery.rows.map(row => row.user_id);

    // Return everything to the phone layout cleanly
    return res.json({
      rideOfferId: id,
      routeId: route_id,
      currentUserPoolStatus: userCurrentPoolStatus,  // 🌟 Tells frontend if 'this' specific user is already 'arrived'
      manifestPassengers: manifestUserIds,          
      acceptedPassengers: accepted_passengers || [],
      passengers: passengersWithStatus               // 🌟 Complete status ledger for the co-rider matrix rendering
    });

  } catch (err) {
    console.error("❌ BACKEND STATE RESOLVER ERROR:", err.stack);
    return res.status(500).json({ error: "Internal transactional exception mapping execution matrices." });
  }
});


// 📤 Arrival Signal Handshake Route Handler
app.post("/api/rides/signal-arrival", async (req, res) => {
  const { ride_offer_id, user_id } = req.body;
  
  try {
    // Flag the user's presence state directly within the pool table mapping for real-time aggregation checks
    await pool.query(
      `UPDATE route_pool 
       SET status = 'arrived' 
       WHERE user_id = $1 AND matched_ride_offer_id = $2 AND status = 'matched'`,
      [parseInt(user_id, 10), parseInt(ride_offer_id, 10)]
    );
    
    return res.json({ success: true, message: "Presence terminal handshake sequence verified." });
  } catch (err) {
    console.error("❌ HANDSHAKE WRITER BLOCKED:", err.stack);
    return res.status(500).json({ error: "Database rejected transaction loop writing arrival matrix." });
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

  try {
    const targetRideId = parseInt(ride_offer_id, 10);
    const targetUserId = parseInt(user_id, 10);

    // Atomic transaction updating the exact passenger node status context to 'arrived'
    const result = await pool.query(
      `UPDATE route_pool 
       SET status = 'arrived' 
       WHERE user_id = $1 AND matched_ride_offer_id = $2 
       RETURNING id, status`,
      [targetUserId, targetRideId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Passenger cluster matching context mismatch." });
    }

    console.log(`📍 Presence Verification Locked: User ${targetUserId} arrived at Hub via Ride ${targetRideId}`);
    return res.json({ success: true, message: "Arrival presence validated cleanly." });

  } catch (err) {
    console.error("❌ SIGNAL ARRIVAL TRANSACTION FAILURE:", err.stack);
    return res.status(500).json({ error: "Failed to log terminal point arrival verification." });
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


app.get("/api/rides/track-live/offer/:id", async (req, res) => {
  const rideOfferId = parseInt(req.params.id, 10);

  if (isNaN(rideOfferId)) {
    return res.status(400).json({ error: "Invalid structural tracking ID parameter vector." });
  }

  try {
    const offerQuery = await pool.query(
      `SELECT id, status, price_per_seat, pickup_lat, pickup_lng 
       FROM ride_offers 
       WHERE id = $1`,
      [rideOfferId]
    );

    if (offerQuery.rows.length === 0) {
      return res.status(404).json({ error: "Target Corridor Offer profile index not found." });
    }

    const offer = offerQuery.rows[0];

    // 🎯 FIXED: Added 'matched' into the string matrix query loop block
    const passengersQuery = await pool.query(
      `SELECT user_id, status 
       FROM route_pool 
       WHERE matched_ride_offer_id = $1
         AND TRIM(LOWER(status)) IN ('matched', 'accepted', 'arrived', 'in_trip', 'intransit')`,
      [rideOfferId]
    );

    const trackingPayload = {
      rideOfferId: offer.id,
      rideStatus: offer.status, 
      price_per_seat: offer.price_per_seat,
      driverLocation: {
        latitude: parseFloat(offer.pickup_lat) || 9.0106,
        longitude: parseFloat(offer.pickup_lng) || 38.7894
      },
      passengers: passengersQuery.rows.map(row => ({
        userId: parseInt(row.user_id, 10), 
        user_id: parseInt(row.user_id, 10), 
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
 * 🔄 1. GET ACTIVE ASSIGNMENT PROPOSAL FOR DRIVER
 * Route: GET /api/rides/driver-proposal/:driverId
 */
app.get('/api/rides/driver-proposal/:driverId', async (req, res) => {
  const { driverId } = req.params;
  
  try {
    const query = `
      SELECT 
        ro.id, ro.driver_id, ro.vehicle_id, ro.start_location, ro.end_location, 
        ro.total_seats, ro.available_seats, ro.price_per_seat, ro.status, ro.route_id,
        ro.accepted_passengers,
        COALESCE(
          json_agg(
            json_build_object(
              'user_id', rp.user_id::integer,
              'status', rp.status::text
            )
          ) FILTER (WHERE rp.user_id IS NOT NULL), '[]'
        ) AS route_pool_passengers
      FROM ride_offers ro
      LEFT JOIN route_pool rp ON rp.matched_ride_offer_id = ro.id
      WHERE ro.driver_id = $1 AND ro.status IN ('pending', 'accepted', 'inprogress')
      GROUP BY ro.id
      ORDER BY ro.created_at DESC
      LIMIT 1;
    `;
    
    const result = await pool.query(query, [parseInt(driverId, 10)]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ message: "No active dispatch assignment found for this driver." });
    }
    
    const offer = result.rows[0];
    
    let passengerArray = [];
    if (offer.accepted_passengers) {
      passengerArray = Array.isArray(offer.accepted_passengers) 
        ? offer.accepted_passengers.map(Number) 
        : offer.accepted_passengers.replace(/{|}/g, '').split(',').map(Number);
    }

    res.json({
      id: offer.id,
      driver_id: offer.driver_id,
      vehicle_id: offer.vehicle_id,
      start_location: offer.start_location,
      end_location: offer.end_location,
      total_seats: offer.total_seats,
      available_seats: offer.available_seats,
      price_per_seat: offer.price_per_seat,
      status: offer.status, 
      route_id: offer.route_id,
      accepted_passengers: passengerArray, 
      route_pool_passengers: offer.route_pool_passengers
    });

  } catch (err) {
    console.error("Error executing driver-proposal pool fetch:", err);
    res.status(500).json({ error: "Internal Database Sync Error" });
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

// 🎯 ADD THIS TO YOUR BACKEND CONTROLLERS


app.get("/api/rides/active-accepted-offer", async (req, res) => {
  const client = await pool.connect();
  try {
    // Looks for the most recent offer that has been accepted but hasn't started the trip yet
    const queryText = `
      SELECT id, route_id, user_id, status 
      FROM route_pool 
      WHERE TRIM(LOWER(status)) IN ('accepted', 'arrived')
      ORDER BY id DESC 
      LIMIT 1;
    `;
    const result = await client.query(queryText);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "No currently active accepted ride offers found." });
    }

    return res.status(200).json(result.rows[0]);
  } catch (err) {
    console.error("❌ DISCOVERY ENDPOINT FAULT:", err.message);
    return res.status(500).json({ error: "Internal database lookup exception for active allocations." });
  } finally {
    client.release();
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
   🚀 ENDPOINT 2: UPDATE STATUS TO ARRIVED (THE ARRIVAL BUTTON MATRIX)
   ========================================================================= */
app.post("/api/rides/arrive", async (req, res) => {
  const client = await pool.connect();
  try {
    const { ride_offer_id } = req.body;

    if (!ride_offer_id) {
      return res.status(400).json({ error: "Missing ride_offer_id parameter." });
    }

    await client.query("BEGIN");

        // 🎯 FIX: Atomically change column status value to 'arrived' for the target primary key row
      const updateRideOfferQuery = await client.query(
      `UPDATE ride_offers 
      SET status = 'arrived' 
      WHERE id = $1 AND (status = 'accepted' OR status = 'arrived')
      RETURNING id, status;`,
      [parseInt(ride_offer_id, 10)]
    );

    if (updateRideOfferQuery.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ 
        error: "Ride offer not found or status cannot transition to arrived (must be pending)." 
      });
    }

    await client.query("COMMIT");

    return res.status(200).json({
      message: "Driver arrival state initialized and committed successfully.",
      ride_offer: updateRideOfferQuery.rows[0]
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ TRANSACTION ARREST: ARRIVAL SYSTEM FAULT:", err.message);
    return res.status(500).json({ error: "Internal exception processing driver status updates." });
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
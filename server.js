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
  
  if (!userId || userId === "undefined" || userId === "null") {
    return res.status(400).json({ error: "Valid User ID parameter is required" });
  }

  const client = await pool.connect(); 
  
  try {
    // 1. Pull the most recent booking entry for this passenger
    const userStatus = await client.query(
      `SELECT status, route_id, pickup_hub_id, matched_ride_offer_id 
       FROM route_pool WHERE user_id = $1 
       ORDER BY joined_at DESC LIMIT 1`,
      [userId]
    );

    if (userStatus.rows.length === 0) {
      return res.json({ status: "idle", currentPoolCount: 0 });
    }

    const record = userStatus.rows[0];
    let finalPrice = null;

    // If the atomic transaction completed, fetch the locked fare from the generated offer
    if (record.status === "matched") {
      const priceQuery = await client.query(
        `SELECT price_per_seat FROM ride_offers WHERE id = $1`,
        [record.matched_ride_offer_id]
      );
      finalPrice = priceQuery.rows[0]?.price_per_seat || null;
    }

    // 2. Count how many total searching users are standing at this station platform
    const countQuery = await client.query(
      `SELECT COUNT(*) FROM route_pool 
       WHERE route_id = $1 AND pickup_hub_id = $2 AND status = 'searching'`,
      [record.route_id, record.pickup_hub_id]
    );

    const liveHubQueueCount = parseInt(countQuery.rows[0].count, 10);

    res.json({
      status: record.status, 
      matched_ride_offer_id: record.matched_ride_offer_id,
      finalPrice: finalPrice,
      currentPoolCount: record.status === 'matched' ? 2 : liveHubQueueCount
    });

  } catch (err) {
    console.error("❌ STATUS ENDPOINT ERROR:", err.stack);
    res.status(500).json({ error: err.message });
  } finally {
    client.release(); 
  }
});

/* ===========================================================================
   👥 3. THE CENTRAL MATCHING ENGINE FUNCTION (Atomic Transaction CTE)
=========================================================================== */
async function executeCorridorAutoMatch(client, route_id, hub_id) {
  // 1. Look for the top driver waiting at this exact hub pipeline
  const topDriverQuery = await client.query(
    `SELECT id, driver_id, vehicle_id, capacity FROM driver_queue 
     WHERE route_id = $1 AND current_hub_id = $2 AND status = 'queued'
     ORDER BY entered_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;`,
    [route_id, hub_id]
  );

  const activeDriver = topDriverQuery.rows[0];
  if (!activeDriver) return { status: "buffering", message: "No driver in queue." };

  // 2. Fetch up to 4 searching passengers standing at this EXACT pickup hub station
  const passengerPoolQuery = await client.query(
    `SELECT id, pickup_hub_id, dropoff_hub_id FROM route_pool 
     WHERE route_id = $1 AND pickup_hub_id = $2 AND status = 'searching' 
     ORDER BY joined_at ASC LIMIT 4 FOR UPDATE SKIP LOCKED;`,
    [route_id, hub_id]
  );
  
  const totalRiders = passengerPoolQuery.rows.length;

  // 🛑 POWER OF 2 RULE: Exit safely if we don't have our core pair yet
  if (totalRiders < 2) {
    return { status: "buffering", matchedCount: totalRiders };
  }

  // 📍 Get base price from route config
  const routeMetaData = await client.query(
    `SELECT base_price FROM routes WHERE id = $1;`, [route_id]
  );
  if (routeMetaData.rows.length === 0) {
    throw new Error(`Route configuration missing for ID ${route_id}`);
  }
  const basePrice = parseFloat(routeMetaData.rows[0].base_price);

  // 🏙️ RESOLVE HUB NAMES: Pull text descriptors directly from your hubs table
  const targetPickupId = passengerPoolQuery.rows[0].pickup_hub_id;
  const targetDropoffId = passengerPoolQuery.rows[0].dropoff_hub_id;

  const hubNamesQuery = await client.query(
    `SELECT id, name FROM hubs WHERE id IN ($1, $2);`,
    [targetPickupId, targetDropoffId]
  );

  const hubMap = {};
  hubNamesQuery.rows.forEach(row => { hubMap[row.id] = row.name; });
  
  const startLoc = hubMap[targetPickupId] || "Terminal Station Start";
  const endLoc = hubMap[targetDropoffId] || "Terminal Station End";

  // Dynamic pricing calculations based on final group density
  let finalPricePerSeat = basePrice;
  if (totalRiders === 3) finalPricePerSeat *= 0.90;
  if (totalRiders === 4) finalPricePerSeat *= 0.80;

  const remainingSeats = activeDriver.capacity - totalRiders;
  const passengerIds = passengerPoolQuery.rows.map(p => p.id);

  // ⚡ THE FIXED ATOMIC TRANSACTION: Insert ride and cascade update the passenger pool using a CTE
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
    WHERE id = ANY($9)
    RETURNING (SELECT id FROM inserted_ride) AS new_ride_offer_id;`,
    [
      activeDriver.driver_id, activeDriver.vehicle_id, route_id, startLoc, endLoc,
      activeDriver.capacity, remainingSeats, finalPricePerSeat, passengerIds
    ]
  );

  const generatedRideOfferId = matchTransactionQuery.rows[0]?.new_ride_offer_id;

  // Clear the driver from the active platform queue
  await client.query(
    `UPDATE driver_queue SET status = 'dispatched' WHERE vehicle_id = $1;`,
    [activeDriver.vehicle_id]
  );

  return {
    status: "success",
    ride_id: generatedRideOfferId,
    matchedCount: totalRiders,
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
app.post("/api/routes/join", async (req, res) => {
  const { route_id, user_id, pickup_hub_id, dropoff_hub_id } = req.body;

  if (!route_id || !user_id) {
    return res.status(400).json({ error: "Missing required fields: route_id and user_id" });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

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

    // Clear out any previous looking-for-ride states for this specific user
    await client.query(
      `UPDATE route_pool SET status = 'cancelled' WHERE user_id = $1 AND status = 'searching'`,
      [user_id]
    );

    // Insert passenger into queue line
    const newEntry = await client.query(
      `INSERT INTO route_pool (route_id, user_id, pickup_hub_id, dropoff_hub_id, status, joined_at)
       VALUES ($1, $2, $3, $4, 'searching', NOW())
       RETURNING *`,
      [route_id, user_id, finalPickup, finalDropoff]
    );

    // Fire the engine to see if match criteria are fulfilled
    const matchPipeline = await executeCorridorAutoMatch(client, route_id, finalPickup);

    await client.query("COMMIT");

    res.status(201).json({
      message: "Success",
      pool: newEntry.rows[0],
      match_pipeline: matchPipeline
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ JOIN CORRIDOR POOL ERROR:", err.message);
    res.status(500).json({ error: "Internal matchmaking error" });
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

/* ---------------- DRIVER GOES ONLINE → CREATE LIVE POOLED RUN ---------------- */
app.post("/api/rides/create", async (req, res) => {
  const client = await pool.connect();
  try {
    const { driver_id, vehicle_id, route_id, total_seats } = req.body;

    if (!driver_id || !route_id) {
      return res.status(400).json({ error: "Missing driver_id or route_id parameters." });
    }

    const driverProfileQuery = await pool.query(`SELECT full_name FROM users WHERE id = $1 LIMIT 1`, [driver_id]);
    const vehicleQuery = await pool.query(`SELECT make, model FROM vehicles WHERE driver_id = $1 LIMIT 1`, [driver_id]);

    const driverName = driverProfileQuery.rows[0]?.full_name || "Driver";
    const carMake = vehicleQuery.rows[0]?.make || "Corridor";
    const carModel = vehicleQuery.rows[0]?.model || "Vehicle";

    await client.query("BEGIN");

    const routeGeomQuery = await client.query(
      `SELECT r.base_price, ARRAY_AGG(h.name ORDER BY rs.stop_order) AS stops,
        (ARRAY_AGG(h.latitude ORDER BY rs.stop_order))[1] AS start_lat,
        (ARRAY_AGG(h.longitude ORDER BY rs.stop_order))[1] AS start_lng
      FROM routes r
      JOIN route_stops rs ON rs.route_id = r.id
      JOIN hubs h ON h.id = rs.hub_id
      WHERE r.id = $1 GROUP BY r.id, r.base_price;`,
      [route_id]
    );
    
    const routeData = routeGeomQuery.rows[0];
    if (!routeData || !routeData.stops || routeData.stops.length < 2) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "The chosen corridor route blueprint has an incomplete stop list." });
    }

    const startLoc = routeData.stops[0];
    const endLoc = routeData.stops[routeData.stops.length - 1];
    const pricePerSeat = routeData.base_price;
    const seatsCount = total_seats || 4;

    const rideOfferQuery = await client.query(
      `INSERT INTO ride_offers (driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, pickup_lat, pickup_lng, status, created_at)
       VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, 'pending', CURRENT_TIMESTAMP)
       RETURNING id AS offer_id, status;`,
      [driver_id, vehicle_id || null, route_id, startLoc, endLoc, seatsCount, seatsCount, pricePerSeat, routeData.start_lat, routeData.start_lng]
    );
    const newOffer = rideOfferQuery.rows[0];

    const matchQueueQuery = await client.query(
      `UPDATE route_pool SET status = 'matched' WHERE route_id = $1 AND status = 'searching' RETURNING id, user_id;`,
      [route_id]
    );

    const passengerCount = matchQueueQuery.rows.length;
    await client.query("COMMIT");

    res.status(201).json({
      message: "Corridor pool initialized dynamically.",
      ride_id: newOffer.offer_id,
      status: newOffer.status,
      start_location: startLoc,
      end_location: endLoc,
      total_seats: seatsCount,
      filled: passengerCount,
      price_per_seat: pricePerSeat,
      driver_name: driverName,
      make: carMake,
      model: carModel
    });

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error executing dynamic ride creator:", err.message);
    res.status(500).json({ error: "Internal server error deploying live ride offer matrix." });
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
      SELECT rp.id AS request_id, rp.route_id, rp.user_id AS passenger_id, u.name AS passenger_name,
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
    // Use array_append to thread the user_id into the tracking array without duplicates
    await pool.query(
      `UPDATE ride_offers 
       SET accepted_passengers = array_append(
         array_remove(accepted_passengers, $2), $2
       ) 
       WHERE id = $1`,
      [ride_offer_id, user_id]
    );

    res.json({ message: "Pickup proposal accepted successfully." });
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
   ========================================================================= */
app.get("/api/rides/track-live/:userId", async (req, res) => {
  const { userId } = req.params;

  try {
    // Step A: Find the active trip context row for this passenger
    const poolQuery = await pool.query(
      `SELECT matched_ride_offer_id 
       FROM route_pool 
       WHERE user_id = $1 AND status = 'matched' 
       ORDER BY joined_at DESC LIMIT 1`,
      [parseInt(userId, 10)]
    );

    if (poolQuery.rows.length === 0 || !poolQuery.rows[0].matched_ride_offer_id) {
      return res.status(404).json({ error: "No active trip tracked for this user identifier." });
    }

    const activeRideOfferId = poolQuery.rows[0].matched_ride_offer_id;

    // Step B: Fetch the real-time driver telemetry coordinates and metadata tracking structures
    const rideQuery = await pool.query(
      `SELECT id, route_id, pickup_lat, pickup_lng, status FROM ride_offers WHERE id = $1`,
      [activeRideOfferId]
    );

    if (rideQuery.rows.length === 0) {
      return res.status(404).json({ error: "Target deployment vehicle streaming profile missing." });
    }

    // Step C: Resolve the absolute walking and arrival state matrix of all grouped co-riders
    const manifestQuery = await pool.query(
      `SELECT user_id, status FROM route_pool 
       WHERE matched_ride_offer_id = $1 AND status IN ('matched', 'arrived')
       ORDER BY user_id ASC`,
      [activeRideOfferId]
    );

    const passengerManifest = manifestQuery.rows.map(row => ({
      userId: row.user_id,
      status: row.status // 'matched' indicates Walking, 'arrived' indicates at the hub
    }));

    return res.json({
      rideOfferId: rideQuery.rows[0].id,
      routeId: rideQuery.rows[0].route_id,
      rideStatus: rideQuery.rows[0].status,
      driverLocation: {
        latitude: parseFloat(rideQuery.rows[0].pickup_lat) || 9.0045, // Fallback safely to Megenagna route vectors
        longitude: parseFloat(rideQuery.rows[0].pickup_lng) || 38.7865,
      },
      passengers: passengerManifest
    });

  } catch (err) {
    console.error("❌ LIVE TRACKING MATRIX EXCEPTION:", err.stack);
    return res.status(500).json({ error: "Internal database tracking stream error." });
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










/* ===========================================================================
   🏁 7. ENGINE DISPATCH INITIALIZER LISTEN
=========================================================================== */
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Corridor Engine active on port ${PORT}`);
});
const express = require('express');
const cors = require('cors');
require('dotenv').config();
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// 1. Base Root Route
app.get('/', (req, res) => {
    res.json({ message: "Welcome to the Corridor Pool API Engine" });
});

/* ---------------- HUB SEARCH ---------------- */
app.get("/api/hubs/search/:term", async (req, res) => {
  try {
    const { term } = req.params;
    const result = await pool.query(
      `
      SELECT id, name, latitude, longitude
      FROM hubs
      WHERE LOWER(name) LIKE LOWER($1)
      ORDER BY name
      LIMIT 10
      `,
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
      `
      SELECT
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
      GROUP BY r.id, r.capacity, r.base_price;
      `,
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
    return res.status(400).json({ error: "Missing required properties: route_id and user_id" });
  }

  try {
    await pool.query("BEGIN");

    // 1. Resolve fallback layout hubs from configuration matrix if not explicitly passed
    let finalPickup = pickup_hub_id;
    let finalDropoff = dropoff_hub_id;

    if (!finalPickup || !finalDropoff) {
      const routeStops = await pool.query(
        `SELECT hub_id FROM route_stops WHERE route_id = $1 ORDER BY stop_order ASC`,
        [route_id]
      );
      if (routeStops.rows.length >= 2) {
        finalPickup = finalPickup || routeStops.rows[0].hub_id;
        finalDropoff = finalDropoff || routeStops.rows[routeStops.rows.length - 1].hub_id;
      } else {
        await pool.query("ROLLBACK");
        return res.status(400).json({ error: "Could not map corridor path checkpoints automatically." });
      }
    }

    // 2. Kill stale 'searching' sequences for this user to keep data cleanly constrained
    await pool.query(
      `UPDATE route_pool SET status = 'cancelled' WHERE user_id = $1 AND status = 'searching'`,
      [user_id]
    );

    // 3. Atomically register into our upgraded structural segment layer
    const newEntry = await pool.query(
      `INSERT INTO route_pool (route_id, user_id, pickup_hub_id, dropoff_hub_id, status, joined_at)
       VALUES ($1, $2, $3, $4, 'searching', NOW())
       RETURNING *`,
      [route_id, user_id, finalPickup, finalDropoff]
    );

    // 4. Calculate real-time pickup density metrics
    const checkDensity = await pool.query(
      `SELECT COUNT(*) FROM route_pool 
       WHERE route_id = $1 AND pickup_hub_id = $2 AND status = 'searching'`,
      [route_id, finalPickup]
    );

    const waitingInSegment = parseInt(checkDensity.rows[0].count, 10);

    await pool.query("COMMIT");

    res.status(201).json({
      message: "Successfully entered corridor waiting room pipeline.",
      pool: newEntry.rows[0],
      meta: {
        current_waiting_in_segment: waitingInSegment,
        demand_type: waitingInSegment >= 2 ? "BUNDLE" : "SINGLE"
      }
    });

  } catch (err) {
    await pool.query("ROLLBACK");
    console.error("❌ JOIN CORRIDOR POOL ERROR:", err.message);
    res.status(500).json({ error: "Failed to join live route pool" });
  }
});

/* ---------------- DRIVER SIDE: THE LIVE DEMAND RADAR FEED ---------------- */
app.get("/api/drivers/radar", async (req, res) => {
  try {
    const liveDemandQuery = `
      SELECT 
        rp.route_id,
        rp.pickup_hub_id,
        rp.dropoff_hub_id,
        h_start.name AS pickup_name,
        h_end.name AS dropoff_name,
        COUNT(rp.id) AS waiting_passengers,
        r.base_price * COUNT(rp.id) AS raw_value,
        CASE 
          WHEN COUNT(rp.id) >= 2 THEN 'BUNDLE'
          ELSE 'SINGLE'
        END as demand_type
      FROM route_pool rp
      JOIN routes r ON rp.route_id = r.id
      JOIN hubs h_start ON rp.pickup_hub_id = h_start.id
      JOIN hubs h_end ON rp.dropoff_hub_id = h_end.id
      WHERE rp.status = 'searching'
      GROUP BY rp.route_id, rp.pickup_hub_id, rp.dropoff_hub_id, h_start.name, h_end.name
      ORDER BY waiting_passengers DESC, rp.route_id ASC;
    `;

    const result = await pool.query(liveDemandQuery);

    // Inject your engine dynamic pricing structures straight to the driver dashboard
    const formattedDemands = result.rows.map(row => {
      const filled = parseInt(row.waiting_passengers, 10);
      const base_price = 180; // Default base price fallback
      
      let price_per_person = base_price;
      if (filled === 2) price_per_person = Math.round(base_price * 0.85);
      if (filled >= 3) price_per_person = Math.round(base_price * 0.75);

      const driver_payout = filled === 1 
        ? base_price 
        : Math.round(price_per_person * filled * 1.1);

      return {
        ...row,
        payout_text: `${driver_payout} ETB`,
        price_per_seat: price_per_person
      };
    });

    res.json({ active_demands: formattedDemands });
  } catch (err) {
    console.error("❌ RADAR ENGINE DATA FAILURE:", err.message);
    res.status(500).json({ error: "Failed to load live driver radar data stream" });
  }
});

/* ---------------- DRIVER SIDE: GO ONLINE / DEPLOY INDEPENDENT OFFER ---------------- */
app.post("/api/rides", async (req, res) => {
  const {
    driver_id,
    vehicle_id,
    route_id,
    start_location,
    end_location,
    total_seats,
    price_per_seat
  } = req.body;

  try {
    const result = await pool.query(
      `
      INSERT INTO ride_offers (
        driver_id, vehicle_id, route_id, start_location, end_location, 
        departure_time, total_seats, available_seats, price_per_seat, status, created_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), $6, $6, $7, 'pending', NOW())
      RETURNING *;
      `,
      [driver_id, vehicle_id, route_id, start_location, end_location, total_seats, price_per_seat]
    );

    res.status(201).json({
      message: "Driver is now online operating corridor path.",
      ride: result.rows[0],
    });
  } catch (err) {
    console.error("❌ Error deploying active ride offer:", err.message);
    res.status(500).json({ error: "Server error while launching corridor deployment." });
  }
});

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
      route_id,
      filled,
      capacity,
      status,
      price_per_person,
      driver_payout,
      remaining: Math.max(0, capacity - filled),
      is_ready: filled >= 2,
      profit_model: "dynamic_pooling_v1",
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Engine execution calculation crash" });
  }
});

// Legacy backward-compatibility endpoints preserved
app.get("/api/rides", async (req, res) => {
  try {
    const result = await pool.query(`SELECT * FROM ride_offers WHERE status = 'pending' ORDER BY created_at DESC;`);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Server error" });
  }
});

app.listen(5000, "0.0.0.0", () => {
  console.log("🚀 Corridor Engine active on port 5000");
});
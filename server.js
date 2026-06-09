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

// 2. GET ALL ACTIVE RIDES (The Passenger's Ride Feed)
app.get("/api/rides", async (req, res) => {
  const { start } = req.query;

  try {
    let query = `
      SELECT *
      FROM route_offers
    `;

    const params = [];

    if (start) {
      params.push(`%${start}%`);

      query += `
        WHERE start_location ILIKE $1
      `;
    }

    const result = await pool.query(query, params);

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

app.post("/api/rides", async (req, res) => {
  const {
    driver_id,
    vehicle_id,
    start_location,
    end_location,
    departure_time,
    total_seats,
    price_per_seat,
    pickup_lat,
    pickup_lng,
  } = req.body;

  try {
    const result = await pool.query(
      `
      INSERT INTO route_offers (
        driver_id,
        vehicle_id,
        start_location,
        end_location,
        departure_time,
        total_seats,
        available_seats,
        price_per_seat,
        pickup_lat,
        pickup_lng,
        filled,
        status
      )
      VALUES ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9,0,'waiting')
      RETURNING *;
      `,
      [
        driver_id,
        vehicle_id,
        start_location,
        end_location,
        departure_time,
        total_seats,
        price_per_seat,
        pickup_lat,
        pickup_lng,
      ]
    );

    res.status(201).json({
      message: "Ride created successfully",
      ride: result.rows[0],
    });
  } catch (err) {
    console.error("❌ Error creating ride:", err.message);
    res.status(500).json({ error: "Server error while creating ride" });
  }
});


// 4. POST A NEW BOOKING (Riders joining a ride pool)
app.post('/api/bookings', async (req, res) => {
    const { ride_offer_id, rider_id, seats_booked } = req.body;

    try {
        // Step A: Check if the ride exists and has enough available seats
        const rideCheck = await pool.query(
            'SELECT available_seats FROM ride_offers WHERE id = $1', 
            [ride_offer_id]
        );

        if (rideCheck.rows.length === 0) {
            return res.status(404).json({ error: 'Ride offer not found' });
        }

        const availableSeats = rideCheck.rows[0].available_seats;
        if (availableSeats < seats_booked) {
            return res.status(400).json({ error: 'Not enough available seats left on this ride!' });
        }

        // Step B: Insert the booking record
        const bookingQuery = `
            INSERT INTO bookings (ride_offer_id, rider_id, seats_booked) 
            VALUES ($1, $2, $3) 
            RETURNING *;
        `;
        const bookingResult = await pool.query(bookingQuery, [ride_offer_id, rider_id, seats_booked]);

        // Step C: Deduct the seats from the ride offer
        await pool.query(
            'UPDATE ride_offers SET available_seats = available_seats - $1 WHERE id = $2',
            [seats_booked, ride_offer_id]
        );

        res.status(210).json({
            message: "Ride booked successfully!",
            booking: bookingResult.rows[0]
        });

    } catch (err) {
        console.error('❌ Error processing booking:', err.message);
        res.status(500).json({ error: 'Server error while processing booking' });
    }
});




app.get("/api/routes/from/:pickupHub", async (req, res) => {
  try {
    const { pickupHub } = req.params;

    const result = await pool.query(
      `
                SELECT
                r.id AS route_id,
                ARRAY_AGG(h2.name ORDER BY rs2.stop_order) AS stops
                FROM routes r
                JOIN route_stops rs ON rs.route_id = r.id
                JOIN hubs h ON h.id = rs.hub_id

                JOIN route_stops rs2 ON rs2.route_id = r.id
                JOIN hubs h2 ON h2.id = rs2.hub_id

                WHERE LOWER(h.name) = LOWER($1)

                GROUP BY r.id;
      `,
      [pickupHub]
    );

    res.json(result.rows);
  } catch (err) {
    console.error("❌ Error fetching routes:", err.message);

    res.status(500).json({
      error: "Server error",
    });
  }
});

//searching hubs to before displaying available routes 
app.get("/api/hubs/search/:term", async (req, res) => {
  try {
    const { term } = req.params;

    const result = await pool.query(
      `
      SELECT id, name
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

    res.status(500).json({
      error: "Server error",
    });
  }
});

// users joining a corridor 

app.post("/api/routes/join", async (req, res) => {
  const { route_id, user_id } = req.body;

  try {
    await pool.query("BEGIN");

    // 1. Lock the row (prevents double booking)
    const rideRes = await pool.query(
      `SELECT * FROM route_offers
       WHERE id = $1
       FOR UPDATE`,
      [route_id]
    );

    if (rideRes.rows.length === 0) {
      await pool.query("ROLLBACK");
      return res.status(404).json({ error: "Ride not found" });
    }

    const ride = rideRes.rows[0];

    // 2. Check availability
    if (ride.filled >= ride.total_seats) {
      await pool.query("ROLLBACK");
      return res.status(400).json({ error: "Ride is full" });
    }

    // 3. Insert booking (optional but recommended)
    await pool.query(
      `INSERT INTO route_pool (route_id, user_id)
       VALUES ($1, $2)`,
      [route_id, user_id]
    );

    // 4. Update seat counters safely
    const updated = await pool.query(
      `UPDATE route_offers
       SET filled = filled + 1,
           available_seats = available_seats - 1,
           status = CASE
             WHEN filled + 1 >= total_seats THEN 'ready_to_depart'
             ELSE 'waiting'
           END
       WHERE id = $1
       RETURNING *`,
      [route_id]
    );

    await pool.query("COMMIT");

    return res.json({
      message: "Joined successfully",
      ride: updated.rows[0],
    });

  } catch (err) {
    await pool.query("ROLLBACK");
    console.error("JOIN ERROR:", err.message);
    res.status(500).json({ error: "Join failed" });
  }
});

// getting active booked routes 
app.get("/api/routes", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM route_offers
      ORDER BY created_at DESC;
    `);

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch routes" });
  }
});



//getting route status 
app.get("/api/routes/status/:route_id", async (req, res) => {
  const { route_id } = req.params;

  try {
    const route = await pool.query(
      `SELECT capacity FROM routes WHERE id = $1`,
      [route_id]
    );

    const count = await pool.query(
      `SELECT COUNT(*) FROM route_pool WHERE route_id = $1`,
      [route_id]
    );

    const filled = parseInt(count.rows[0].count);
    const capacity = route.rows[0]?.capacity || 3;

    res.json({
      route_id,
      filled_seats: filled,
      capacity,
      remaining: capacity - filled,
      is_ready: filled >= capacity,
    });
  } catch (err) {
    res.status(500).json({ error: "Status error" });
  }
});

// Pool logic 
app.get("/api/routes/engine/:route_id", async (req, res) => {
  const { route_id } = req.params;

  try {
    const route = await pool.query(
      `SELECT capacity, base_price FROM routes WHERE id = $1`,
      [route_id]
    );

    const count = await pool.query(
      `SELECT COUNT(*) FROM route_pool WHERE route_id = $1`,
      [route_id]
    );

    const filled = parseInt(count.rows[0].count);
    const capacity = route.rows[0].capacity || 3;
    const base_price = route.rows[0].base_price || 180;

    // ---------------- CORE LOGIC ----------------

    let status = "waiting";
    let price_per_person = base_price;

    // 2 passengers → activate market
    if (filled >= 2) {
      status = "active";

      // discount starts (pool efficiency)
      price_per_person = Math.round(base_price * 0.85);
    }

    // 3 passengers → optimized system
    if (filled >= 3) {
      status = "optimized";

      // best discount (max efficiency)
      price_per_person = Math.round(base_price * 0.75);
    }

    // driver incentive model
    const driver_payout =
      filled === 1
        ? base_price
        : Math.round(price_per_person * filled * 1.1);

    res.json({
      route_id,
      filled,
      capacity,
      status,
      price_per_person,
      driver_payout,
      profit_model: "dynamic_pooling_v1",
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "engine error" });
  }
});

function getPickupPoint(startHub) {
  const hubMap = {
    "Bole Atlas": { lat: 9.016, lng: 38.761 },
    "Megenagna": { lat: 9.030, lng: 38.803 },
    "Mexico": { lat: 9.012, lng: 38.746 },
  };

  return hubMap[startHub];
}











app.listen(5000, "0.0.0.0", () => {
  console.log("Server running on port 5000");
});
const { Client } = require("@googlemaps/google-maps-services-js");
const { Pool } = require("pg");

const client = new Client({});

// Configure your PostgreSQL connection
const pool = new Pool({
  user: "Postgres",
  host: "localhost",
  database: "corridor_pool",
  password: "D@ny_456",
  port: 5432,
});

async function populateCorridorHubs(origin, destination, corridorName) {
  try {
    // 1. Request driving directions from Google Maps API
    const response = await client.directions({
      params: {
        origin: origin,
        destination: destination,
        mode: "driving",
        region: "et",
        key: "AIzaSyDZo26RtMEWndm5gPqMO9M2bcCZHOdpqao",
      },
    });

    const routes = response.data.routes;
    if (!routes || routes.length === 0) {
      console.log(`No route found for ${corridorName}`);
      return;
    }

    const leg = routes[0].legs[0];
    const hubList = [];

    // Add starting terminal point
    hubList.push({
      name: `${corridorName} - Terminal (${leg.start_address.split(",")[0]})`,
      lat: leg.start_location.lat,
      lng: leg.start_location.lng,
    });

    // 2. Extract step-by-step navigation points along the asphalt road
    leg.steps.forEach((step, index) => {
      // Clean HTML tags from Google's instructions
      const cleanInstruction = step.html_instructions
        .replace(/<\/?[^>]+(>|$)/g, "")
        .split("<")[0];

      hubList.push({
        name: `${corridorName} - Stop ${index + 1}: ${cleanInstruction}`,
        lat: step.end_location.lat,
        lng: step.end_location.lng,
      });
    });

    // Add final destination terminal point
    hubList.push({
      name: `${corridorName} - Destination Terminal (${leg.end_address.split(",")[0]})`,
      lat: leg.end_location.lat,
      lng: leg.end_location.lng,
    });

    // 3. Insert extracted hubs into PostgreSQL
    const dbClient = await pool.connect();
    try {
      for (const hub of hubList) {
        await dbClient.query(
          `INSERT INTO hubs (name, latitude, longitude) 
           VALUES ($1, $2, $3) 
           ON CONFLICT DO NOTHING;`,
          [hub.name, hub.lat, hub.lng]
        );
      }
      console.log(`Successfully inserted ${hubList.length} micro-hubs for ${corridorName}!`);
    } finally {
      dbClient.release();
    }
  } catch (error) {
    console.error(`Error processing ${corridorName}:`, error.message);
  }
}

async function run() {
  // Execute for Addis Ababa's major corridors
  await populateCorridorHubs("Meskel Square, Addis Ababa", "Bole International Airport, Addis Ababa", "Bole Corridor");
  await populateCorridorHubs("Megenagna, Addis Ababa", "Ayat, Addis Ababa", "Megenagna-Ayat Corridor");
  await populateCorridorHubs("Mexico Square, Addis Ababa", "Jemo 3, Addis Ababa", "Mexico-Jemo Corridor");

  await pool.end();
}

run();
const { Client } = require("@googlemaps/google-maps-services-js");
const { Pool } = require("pg");
const polyline = require("polyline");
const axios = require("axios");

const mapsClient = new Client({});

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "corridor_pool",
  password: "D@ny_456",
  port: 5432,
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getDistanceFromLatLonInMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000; 
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

// Free OpenStreetMap Reverse Geocoder to fetch real local names, banks, cafes, and streets
async function getRealLocalLandmark(lat, lng, corridorName, fallbackIndex) {
  try {
    const url = `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&zoom=18&addressdetails=1`;
    
    // OpenStreetMap requires a custom User-Agent header
    const response = await axios.get(url, {
      headers: { "User-Agent": "AddisCorridorTransitApp/1.0" }
    });

    if (response.data && response.data.address) {
      const addr = response.data.address;
      
      // Extract specific local identifiers (amenity, shop, building, or road)
      const landmark = addr.amenity || addr.shop || addr.building || addr.office || addr.attraction;
      const street = addr.road || addr.suburb || addr.neighbourhood;

      if (landmark && street) {
        return `${corridorName} - ${landmark} (${street})`;
      } else if (landmark) {
        return `${corridorName} - Near ${landmark}`;
      } else if (street) {
        return `${corridorName} - ${street}`;
      }
    }
  } catch (error) {
    // Fallback gracefully if network hiccups occur
  }

  return `${corridorName} - Stop Point ${fallbackIndex} (~${fallbackIndex * 150}m)`;
}

const masterAddisGrid = [
  // 1. Bole Subcity
  { name: "Bole Main Corridor", origin: "Meskel Square, Addis Ababa", destination: "Bole International Airport, Addis Ababa" },
  { name: "Bole-Bulbula Extension", origin: "Bole Atlas, Addis Ababa", destination: "Bulbula, Addis Ababa" },

  // 2. Yeka Subcity
  { name: "Megenagna-Ayat Corridor", origin: "Megenagna, Addis Ababa", destination: "Ayat Adebabay, Addis Ababa" },
  { name: "Kotebe-Megenagna Axis", origin: "Kotebe Metal Works, Addis Ababa", destination: "Megenagna, Addis Ababa" },

  // 3. Lemi Kura Subcity & Expansion Zones
  { name: "Lemi Kura-Summit Corridor", origin: "Lemi Kura Subcity Administration, Addis Ababa", destination: "Summit Square, Addis Ababa" },
  { name: "Bole Arabsa-Ayat Axis", origin: "Bole Arabsa, Addis Ababa", destination: "Ayat Adebabay, Addis Ababa" },
  { name: "Yeka Abado-Megenagna Corridor", origin: "Yeka Abado Condominiums, Addis Ababa", destination: "Megenagna, Addis Ababa" },

  // 4. Kirkos Subcity
  { name: "Kazanchis-Bole Connector", origin: "Kazanchis, Addis Ababa", destination: "Bole Medhanialem, Addis Ababa" },
  { name: "Meskel Square-Olympia Axis", origin: "Meskel Square, Addis Ababa", destination: "Olympia, Addis Ababa" },

  // 5. Arada Subcity
  { name: "Piassa-Sidist Kilo Corridor", origin: "Piassa, Addis Ababa", destination: "Sidist Kilo, Addis Ababa" },
  { name: "Arat Kilo-Megenagna Axis", origin: "Arat Kilo, Addis Ababa", destination: "Megenagna, Addis Ababa" },

  // 6. Addis Ketema Subcity
  { name: "Merkato-Autobis Tera Corridor", origin: "Merkato, Addis Ababa", destination: "Autobis Tera, Addis Ababa" },
  { name: "Merkato-Tor Hailoch Axis", origin: "Merkato, Addis Ababa", destination: "Tor Hailoch, Addis Ababa" },

  // 7. Gulele Subcity
  { name: "Sidist Kilo-Entoto Axis", origin: "Sidist Kilo, Addis Ababa", destination: "Entoto Maryam, Addis Ababa" },
  { name: "Shiro Meda-Piassa Corridor", origin: "Shiro Meda, Addis Ababa", destination: "Piassa, Addis Ababa" },

  // 8. Lideta Subcity
  { name: "Lideta-Mexico Corridor", origin: "Lideta Church, Addis Ababa", destination: "Mexico Square, Addis Ababa" },
  { name: "Balcha Hospital Axis", origin: "Balcha Hospital, Addis Ababa", destination: "Tor Hailoch, Addis Ababa" },

  // 9. Kolfe Keranio Subcity
  { name: "Tor Hailoch-Asko Corridor", origin: "Tor Hailoch, Addis Ababa", destination: "Asko, Addis Ababa" },
  { name: "Hana Mariam Axis", origin: "Hana Mariam, Addis Ababa", destination: "Jemo, Addis Ababa" },

  // 10. Nifas Silk-Lafto Subcity
  { name: "Sarbet-Kality Corridor", origin: "Sarbet, Addis Ababa", destination: "Kality, Addis Ababa" },
  { name: "Lebu-Mekanissa Axis", origin: "Lebu, Addis Ababa", destination: "Mekanissa, Addis Ababa" },

  // 11. Akaki Kality Subcity & Southern Expansions (Koye Feche / Tulu Dimtu)
  { name: "Gotera-Akaki Corridor", origin: "Gotera Interchange, Addis Ababa", destination: "Akaki Kaliti, Addis Ababa" },
  { name: "Koye Feche-Tulu Dimtu Corridor", origin: "Koye Feche Condominiums, Addis Ababa", destination: "Tulu Dimtu, Addis Ababa" },
  { name: "Tulu Dimtu-Kality Axis", origin: "Tulu Dimtu, Addis Ababa", destination: "Kality Ring Road, Addis Ababa" },
  { name: "Kilinto-Gotera Expressway Axis", origin: "Kilinto Industrial Park, Addis Ababa", destination: "Gotera Interchange, Addis Ababa" }
];

async function generateRealNamedHubs(corridor) {
  try {
    console.log(`[Processing] Mapping route for ${corridor.name} ...`);
    
    // 1. Get precise driving path from Google Directions API (which already works for you)
    const response = await mapsClient.directions({
      params: {
        origin: corridor.origin,
        destination: corridor.destination,
        mode: "driving",
        region: "et",
        key: "AIzaSyDZo26RtMEWndm5gPqMO9M2bcCZHOdpqao",
      },
    });

    const routes = response.data.routes;
    if (!routes || routes.length === 0) return;

    const encodedPolyline = routes[0].overview_polyline.points;
    const decodedCoords = polyline.decode(encodedPolyline);

    const hubList = [];
    let currentDistance = 0.0;
    const targetInterval = 150.0; // 150-meter spacing
    let hubIndex = 1;

    let lastPoint = decodedCoords[0];
    
    console.log(`[Naming] Fetching local landmarks for ${corridor.name}...`);
    const startName = await getRealLocalLandmark(lastPoint[0], lastPoint[1], corridor.name, 0);
    hubList.push({ name: startName, lat: lastPoint[0], lng: lastPoint[1] });
    await sleep(1000); // 1-second polite delay for OpenStreetMap server rules

    for (let i = 1; i < decodedCoords.length; i++) {
      const currentPoint = decodedCoords[i];
      const dist = getDistanceFromLatLonInMeters(
        lastPoint[0],
        lastPoint[1],
        currentPoint[0],
        currentPoint[1]
      );
      currentDistance += dist;

      if (currentDistance >= targetInterval) {
        // Fetch real building, bank, cafe, or street name from OpenStreetMap
        const localName = await getRealLocalLandmark(currentPoint[0], currentPoint[1], corridor.name, hubIndex);
        
        hubList.push({
          name: localName,
          lat: currentPoint[0],
          lng: currentPoint[1],
        });

        hubIndex++;
        currentDistance = 0.0;
        lastPoint = currentPoint;
        await sleep(1000); // Respect OpenStreetMap rate limit (1 request per second)
      }
    }

    // Insert into PostgreSQL
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
      console.log(`[Success] Inserted ${hubList.length} landmark-backed hubs for ${corridor.name}!\n`);
    } finally {
      dbClient.release();
    }
  } catch (error) {
    console.error(`[Error] Failed processing ${corridor.name}:`, error.message);
  }
}

async function run() {
  for (const corridor of masterAddisGrid) {
    await generateRealNamedHubs(corridor);
  }
  await pool.end();
  console.log("=== ALL HUBS POPULATED WITH REAL ADDIS ABABA LANDMARKS ===");
}

run();
-- 1. USERS TABLE (Handles both Drivers and Riders)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    phone_number VARCHAR(20) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE,
    role VARCHAR(20) DEFAULT 'rider', -- 'rider', 'driver', 'admin'
    rating DECIMAL(3,2) DEFAULT 5.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. VEHICLES TABLE (Linked to Drivers)
CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    driver_id INT REFERENCES users(id) ON DELETE CASCADE,
    make VARCHAR(50) NOT NULL,       -- e.g., 'Toyota'
    model VARCHAR(50) NOT NULL,      -- e.g., 'Vitz' or 'Yaris'
    plate_number VARCHAR(20) UNIQUE NOT NULL,
    color VARCHAR(30),
    capacity INT NOT NULL,           -- Total available passenger seats
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. RIDE_OFFERS TABLE (Created by Drivers)
CREATE TABLE IF NOT EXISTS ride_offers (
    id SERIAL PRIMARY KEY,
    driver_id INT REFERENCES users(id) ON DELETE CASCADE,
    vehicle_id INT REFERENCES vehicles(id) ON DELETE SET NULL,
    start_location VARCHAR(255) NOT NULL, -- e.g., 'Bole Atlas'
    end_location VARCHAR(255) NOT NULL,   -- e.g., 'CMC'
    departure_time TIMESTAMP NOT NULL,
    total_seats INT NOT NULL,             -- Seats offered originally
    available_seats INT NOT NULL,         -- Remaining seats left
    price_per_seat DECIMAL(10,2) NOT NULL,-- e.g., 150.00 ETB
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'active', 'completed', 'cancelled'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. BOOKINGS TABLE (Created by Riders joining a ride)
CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    ride_offer_id INT REFERENCES ride_offers(id) ON DELETE CASCADE,
    rider_id INT REFERENCES users(id) ON DELETE CASCADE,
    seats_booked INT NOT NULL DEFAULT 1,
    booking_status VARCHAR(20) DEFAULT 'reserved', -- 'reserved', 'confirmed', 'cancelled'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
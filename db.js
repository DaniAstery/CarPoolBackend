const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const caCertPath = path.join(__dirname, 'global-bundle.pem');

const pool = new Pool({
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST || 'corridor.co72s6iqs2bu.us-east-1.rds.amazonaws.com',
    database: process.env.DB_DATABASE || 'corridor',
    port: parseInt(process.env.DB_PORT || '5432'),
    ssl: fs.existsSync(caCertPath) ? {
        rejectUnauthorized: true,
        ca: fs.readFileSync(caCertPath)
    } : { rejectUnauthorized: false }
});

// Test connection on startup
pool.query('SELECT NOW()', (err, res) => {
    if (err) {
        console.error('❌ Database connection failed:', err.stack);
    } else {
        console.log('✅ Database connected successfully at:', res.rows[0].now);
    }
});

module.exports = pool;
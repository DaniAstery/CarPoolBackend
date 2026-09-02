const { Pool } = require('pg');
const { parse } = require('csv-parse/sync'); // Optional, or use native string splitting

const pool = new Pool({
  user: 'postgres',
  host: '127.0.0.1',
  database: 'corridor_pool',
  password: 'D@ny_456',
  port: 5432,
});

const ROUTES_CSV_DATA = `route_short_name,route_id,route_long_name,route_type,agency_id,route_desc,route_color,route_text_color
TX341,10568476,Olympia ↔ Agona Cinema (Minibus),3,AA,,1779c2,ffffff
TX002,15633036,Total 3 kuter Mazoria ↔ Kera (Minibus),3,AA,,1779c2,ffffff
TX200,15676211,Wello Sefer ↔ Kera (Minibus),3,AA,,1779c2,ffffff
TX065,15676212,Safari (Summit) ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
TX005,15676213,Kality Menaheria ↔ 4 Kilo (Minibus),3,AA,,1779c2,ffffff
TX001,15676215,Arada ↔ 18 Mazoria (Minibus),3,AA,,1779c2,ffffff
TX006,15677019,Kara ↔ 4 Kilo (Children Theatre) (Minibus),3,AA,,1779c2,ffffff
TX008,15677252,Megenagna ↔ 6 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx Kal 001,15774596,Kality Menaheria ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Kal 002,15775112,Kality Total ↔ Bole Roundabout (Minibus),3,AA,,1779c2,ffffff
Tx Kal 006,15797527,Saris ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx Kal 003,15797528,Saris ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Kal 004,15797529,Saris Abo ↔ Bulbula (Minibus),3,AA,,1779c2,ffffff
Tx Kal 005,15797530,Kality Maseltegna ↔ Jemo (Minibus),3,AA,,1779c2,ffffff
Tx Bole 001,15830934,Saris Abo ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Kal 012,15833378,Kality Total ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Kal 013,15833518,Mexico ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Bole 002,15841640,Tulu Dimu ↔ Sefera Square (Minibus),3,AA,,1779c2,ffffff
Tx Bole 004,15841839,Bole Bridge ↔ 22 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Bole 005,15841978,Ayat ↔ 22 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Bole 003,15842386,Bole Bridge ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx Bole 006,15842687,Summit ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx Bole 014,15845949,Mulugeta Building ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Bole 011,15845950,Megenagna ↔ Bulbula (Minibus),3,AA,,1779c2,ffffff
Tx Bole 012,15846182,Bulbula ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Bole 009,15850226,Goro ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Bole 010,15850509,Safari ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Kal 011,15850623,Saris ↔ Derartu (Minibus),3,AA,,1779c2,ffffff
Tx Bole 015,15858008,Tekle Haimanot ↔ Bole Mickael (Minibus),3,AA,,1779c2,ffffff
Tx Bole 016,15858151,Bole Mickael ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Bole 018,15858269,Piassa Arada ↔ Bole Mickael (Minibus),3,AA,,1779c2,ffffff
Tx Bole 019,15858339,Piassa Arada ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Bole 020,15858404,Bole Bridge ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Bole 021,15858713,Tulu Dimtu ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx Bole 022,15858887,Saint Georgis ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 002,15858958,Ayer Tena ↔ Hana Mariam (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 003,15861781,Sefera ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 004,15861962,Gelan Condominium ↔ Haile Garment (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 008,15863862,Tulu Dimtu ↔ Saris (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 009,15864075,Sefera ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 010,15865443,Total 3 Kuter Mazoria ↔ Kara (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 011,15865947,Kera ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 012,15866774,Kera ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 013,15867049,Kality Maseltegna ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 014,15867151,Piassa Arada ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 015,15867420,Jemmo 1 ↔ Alem Gena (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 017,15867572,Piassa Arada ↔ German Square (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 018,15867651,Tekle Haimanot ↔ Gofa (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 021,15867786,Gofa ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 019,15869243,Alem Gena ↔ Gofa (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 020,15869312,Gofa ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 022,15869366,Kera ↔ German Square (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 023,15869889,Mexico ↔ German Square (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 024,15870075,Mexico ↔ Mekanisa Abo (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 025,15870224,Mekanisa ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 026,15870632,Sefera ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 027,15870793,Jemmo Mickael ↔ Autobis Tera (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 028,15870960,Kality Menaheria ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 031,15871080,Mebrathile ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 032,15871275,Mexico ↔ Mebrathile (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 001,15871624,Merab Hotel ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx Kolfe 003,15876625,Mexico ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 004,15876725,Alem Bank ↔ Mexico Shebelle Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 005,15876752,Torhayloch ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 006,15876870,Germen Square ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 007,15876935,Jemmo 1 ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 008,15877451,Mexico ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 009,15877500,Mexico Shebelle Hotel ↔ Betel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 015,15879148,Zenebewerk ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 010,15880133,Alem Bank ↔ 18 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 011,15880213,Merab Hotel ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 012,15880308,Merab Hotel ↔ Betel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 013,15880397,Meraba Hotel ↔ Kara Kore (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 014,15880659,Zenebewerk ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 016,15881004,Zenebewerk ↔ Mexico Shebelle Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 017,15881508,Zenebewerk Meneharia ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 018,15881684,Atena Tera ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 019,15882444,Mexico Shebelle Hotel ↔ Kara Kore (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 020,15882498,Tekle Haimanot ↔ Kara Kore (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 021,15882550,Kara Kore ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 022,15882638,Kara Kore ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
TX Kolfe 023,15883171,Ayer Tena ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 024,15885969,Yeshi Debele ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 025,15886152,Kara Kore ↔ 18 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 026,15887144,Yeshi Debele ↔ Sebategna (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 027,15887191,Yeshi Debele ↔ Winget (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 028,15887240,Yeshi Debele ↔ 18 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Kal 017,15898347,Leghar ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Gl 001,15900380,Atena Tera ↔ Addisu Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Gl 002,15900555,Addisu Gebeya ↔ Ertirea Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Gl 003,15900883,Piassa ↔ Kechenie (Minibus),3,AA,,1779c2,ffffff
Tx Gl 004,15901020,Shegole ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Gl 005,15901146,Megenagna (Marathon Motors) ↔ 6 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx Gl 007,15901995,Kazanchis ↔ 6 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx Gl 008,15902043,piassa ↔ Addisu Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Gl 009,15902171,Shiromeda ↔ Kazanchis (Minibus),3,AA,,1779c2,ffffff
Tx Gl 010,15902285,Shiromeda ↔ Raguel Church (Minibus),3,AA,,1779c2,ffffff
Tx Gl 011,15902321,Mexico ↔ Addisu Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Gl 012,15903001,Addisu Gebeya ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx Gl 013,15903091,Shegole ↔ Autobis Tera (Minibus),3,AA,,1779c2,ffffff
Tx LK 001,15903260,Summit Condominiums ↔ Aba Kiros (Minibus),3,AA,,1779c2,ffffff
Tx LK 002,15903402,Ayat ↔ Bole Arabsa (Minibus),3,AA,,1779c2,ffffff
Tx LK 003,15903454,Aba Kiros ↔ Bole Arabsa (Minibus),3,AA,,1779c2,ffffff
Tx LK 004,15903518,Megenagna ↔ Summit Condominiums (Minibus),3,AA,,1779c2,ffffff
Tx LK 005,15904627,Tafo Square ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx LK 006,15904691,Goro ↔ Bole Arabsa (Minibus),3,AA,,1779c2,ffffff
Tx LK 007,15904811,22 Mazoria ↔ Ayat Chefe Condominium (Minibus),3,AA,,1779c2,ffffff
Tx LK 008,15905759,Goro ↔ Aba Kiros (Minibus),3,AA,,1779c2,ffffff
Tx LK 009,15905796,Aba Kiros ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx LK 010,15905849,Megenagna ↔ Ayat Chefe Condominium (Minibus),3,AA,,1779c2,ffffff
Tx LK 013,15906133,Summit Condominiums ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx LK 014,15906163,Bole Arabsa ↔ Ayat (Minibus),3,AA,,1779c2,ffffff
Tx LID 003,15906876,Torhayloch ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx LID 004,15906946,Torhayloch ↔ Stadium (Minibus),3,AA,,1779c2,ffffff
Tx LID 009,15907058,Torhayloch ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx LID 010,15907234,Tekle Haimanot ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx LID 011,15907305,Asko ↔ Torhayloch (Minibus),3,AA,,1779c2,ffffff
Tx YK 001,15909909,Megenagna ↔ Addisu Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx YK 004,15909991,Megenagna ↔ Kara (Minibus),3,AA,,1779c2,ffffff
Tx YK 006,15910058,Shiromeda ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx YK 007,15910153,Ferensay ↔ 4 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx YK 009,15910213,Megenagna ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx YK 010,15910286,Megenagna ↔ Ayat (Minibus),3,AA,,1779c2,ffffff
Tx YK 011,15910343,Megenagna ↔ Ferensay (Minibus),3,AA,,1779c2,ffffff
Tx YK 012,15910407,St. George Total ↔ Ferensay (Minibus),3,AA,,1779c2,ffffff
Tx YK 013,15910507,Raguel Church ↔ Ferensay (Minibus),3,AA,,1779c2,ffffff
Tx YK 016,15910595,Mexico ↔ Lamberet Menaheria (Minibus),3,AA,,1779c2,ffffff
Tx YK 023,15912626,Kidane Mihret ↔ Shola Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx KRK 002,15912956,Mexico ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx KRK 003,15912995,Total 3 Kuter Mazoria ↔ Kera (Minibus),3,AA,,1779c2,ffffff
Tx KRK 004,15913024,Kazanchis ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx KRK 006,15913086,Stadium ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx KRK 007,15913109,Kazanchis ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx KRK 008,15913170,Legehar ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx KRK 009,15913211,Wello Sefer ↔ Kera (Minibus),3,AA,,1779c2,ffffff
Tx KRK 011,15913312,Stadium ↔ Kera (Minibus),3,AA,,1779c2,ffffff
Tx KRK 013,15913400,Shiromeda ↔ Buna Ena Shay (Minibus),3,AA,,1779c2,ffffff
Tx ARD 002,15913788,Piassa Arada ↔ Adey Abeba (Minibus),3,AA,,1779c2,ffffff
Tx ARD 003,15913797,Addisu Gebeya ↔ Piassa (Minibus),3,AA,,1779c2,ffffff
Tx ARD 004,15913851,Kality Total ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx ARD 005,15913986,Jemmo 1 ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx ARD 007,15916307,4 Kilo ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx ARD 006,15916364,Piassa Arada ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx ARD 008,15916401,Saris ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx ADK 002,15916629,Amanuel total ↔ Merab Hotel (Minibus),3,AA,,1779c2,ffffff
Tx ADK 004,15916756,Asko ↔ Autobis Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 008,15916839,Menlik Hospital ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 009,15916909,Abune Petros ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx ADK 011,15916957,Alem Bank ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 012,15917637,Haile Garment ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 013,15917697,Shiromeda ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 014,15917888,Kera ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 015,15917949,Kality Total ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 016,15917971,Jemmo 1 ↔ Autobis Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 017,15918070,Kechenie ↔ Eritrea Hotel (Minibus),3,AA,,1779c2,ffffff
Tx ADK 020,15919609,Bole Bridge ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx ADK 021,15919798,Cinema Ras ↔ Bole Mekael (Minibus),3,AA,,1779c2,ffffff
Tx ADK 022,15919880,Kera ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx ADK 026,15921312,Autobus Tera ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx ADK 028,15921454,Tekle Haimanot ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx ADK 030,15921591,Winget ↔ Torhayloch (Minibus),3,AA,,1779c2,ffffff
Tx ADK 031,15921656,Asco ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 038,15957904,Mekanisa ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx ADK 042,15958044,Megenagna ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx ADK 043,15958107,Yeshi Debele ↔ Merab Hotel (Minibus),3,AA,,1779c2,ffffff
Tx ADK 044,15958349,Merab Hotel ↔ Mekanisa (Minibus),3,AA,,1779c2,ffffff
Tx ADK 047,15958502,Saris Abo ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx ADK 048,15958564,Tekle Haimanot ↔ Saris Abo (Minibus),3,AA,,1779c2,ffffff
Tx ADK 052,15958618,Atena Tera ↔ Alem Bank (Minibus),3,AA,,1779c2,ffffff
Tx ADK 053,15958644,Bole Bridge ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 058,15958710,Raguel Church ↔ Menlik Hospital (Minibus),3,AA,,1779c2,ffffff
Tx ARD 011,15960815,Kality Menaheria ↔ 4 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx ARD 012,15960877,Piassa Arada ↔ Mekanisa (Minibus),3,AA,,1779c2,ffffff
Tx ARD 016,15960944,Bole Medhanialem ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Bole 023,15960977,Cinema Ras ↔ Bole Medhanialem (Minibus),3,AA,,1779c2,ffffff
Tx Gl 018,15961078,Addisu Gebeya ↔ Eritrea Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kal 032,15961305,Gojam Berenda ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Kal 033,15961341,Lamberet Menaheria ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Kal 037,15961494,Mexico ↔ Gelan Condominium (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 034,15961764,Weybla Mariam ↔ Merab Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 036,15961854,Stadium ↔ Zenebewerk (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 037,15961885,18 Mazoria ↔ 22 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx KRK 014,15961978,Saris ↔ Stadium (Minibus),3,AA,,1779c2,ffffff
Tx KRK 015,15962019,Saris Abo ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx KRK 016,15962065,Piassa Arada ↔ Kera (Minibus),3,AA,,1779c2,ffffff
Tx KRK 017,15962103,Stadium ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx KRK 020,15962159,Bole Mickael ↔ Buna Ena Shay (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 034,15963098,Merab Hotel ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 042,15963187,Gofa Gebriel ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
Tx LK 016,15963257,Ayat ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx ADK 039,15970722,Merkato ↔ Lukanda (Minibus),3,AA,,1779c2,ffffff
Tx ADK 040,15970883,Tero Mosque ↔ Merkato (Minibus),3,AA,,1779c2,ffffff
Tx ADK 045,15970955,Sebategna ↔ Luwkanda (Minibus),3,AA,,1779c2,ffffff
Tx ARD 013,15971057,Luwkanda ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Gl 015,15971140,Rufael ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Kolfe 035,15973776,Piassa Arada ↔ Selefia Mosque (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 042,15973832,Piassa Arada ↔ 18 Mazoria (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 038,15973924,Saris ↔ Akaki Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 039,15974268,Akaki Gebeya ↔ Adey Abeba (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 040,15974308,Lafto ↔ Jemmo 1 (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 043,15974328,Saris ↔ Yeshi Total (Minibus),3,AA,,1779c2,ffffff
Tx YK 002,15974746,Kotebe Gebriel ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx YK 008,15974793,Kotebe ↔ 4 Kilo (Minibus),3,AA,,1779c2,ffffff
Tx YK 014,15974906,Yeka Abado ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx Bole 007,15992541,22 Mazoria ↔ Summit (Minibus),3,AA,,1779c2,ffffff
Tx Bole 008,15992646,Summit ↔ Bole Bridge (Minibus),3,AA,,1779c2,ffffff
Tx Bole 017,15992742,Riventy ↔ Bole Mickael (Minibus),3,AA,,1779c2,ffffff
Tx ADK 005,15992882,Lomi Meda ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 006,15992969,Atena Tera ↔ Sebategna (Minibus),3,AA,,1779c2,ffffff
Tx ADK 024,15993132,Saris ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 023,15993248,Tourist Hotel ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx ADK 035,15993363,Mesalemia ↔ Coca Cola (Minibus),3,AA,,1779c2,ffffff
Tx ADK 037,15993376,Asko ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx ADK 055,15993527,Tikur Anbesa ↔ Tero Mosque (Minibus),3,AA,,1779c2,ffffff
Tx ADK 057,15993651,Raguel Church ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx ARD 001,15998238,Piassa ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx ARD 009,15998277,Ambasader ↔ Tourist Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Gl 006,15998427,Shiromeda ↔ Addisu Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Kal 021,15999097,Selam Building ↔ Saris (Minibus),3,AA,,1779c2,ffffff
Tx Kal 022,15999123,Saris Abo ↔ Kality Total (Minibus),3,AA,,1779c2,ffffff
Tx Kal 023,15999152,Gelan Condominium ↔ Saris (Minibus),3,AA,,1779c2,ffffff
Tx Kal 027,15999254,Bulbula ↔ Saris (Minibus),3,AA,,1779c2,ffffff
Tx Kal 008,15999429,Gelan Condominium ↔ Akaki Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx Kal 009,16003640,Akaki Gebeya ↔ Sefera Square (Minibus),3,AA,,1779c2,ffffff
Tx Kal 030,16004103,Akaki Gebeya ↔ Saris Abo (Minibus),3,AA,,1779c2,ffffff
Tx Kal 034,16004169,Saris Abo ↔ Akaki CBE (Minibus),3,AA,,1779c2,ffffff
Tx Kal 035,16004300,Merkato ↔ Akaki Gebeya (Minibus),3,AA,,1779c2,ffffff
Tx KRK 001,16004409,Lafto ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx KRK 012,16004442,Bole Bridge ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 031,16004670,Woyira Sefer ↔ Mexico Shebelle Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 033,16006993,Selefia Mosque ↔ Merab Hotel (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 038,16007055,Zenebewerk ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx Kolfa 041,16007115,Kara Kore ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 001,16007166,Gofa Gebriel ↔ Adey Abeba (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 016,16007247,Jemmo 1 ↔ Cinema Ras (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 030,16007282,Mexico Shebelle Hotel ↔ Lafto (Minibus),3,AA,,1779c2,ffffff
Tx LK 012,16007348,Kera ↔ Tafo Square (Minibus),3,AA,,1779c2,ffffff
Tx LID 002,16007491,Abenet ↔ Weleta (Minibus),3,AA,,1779c2,ffffff
Tx LID 005,16007565,Torhayloch ↔ Alem Gena (Minibus),3,AA,,1779c2,ffffff
Tx LID 007,16007600,Weleta ↔ Mexico (Minibus),3,AA,,1779c2,ffffff
Tx LID 008,16007791,Mexico ↔ Amanuel Church (Minibus),3,AA,,1779c2,ffffff
Tx LID 013,16007850,Alem Gena ↔ Abenet (Minibus),3,AA,,1779c2,ffffff
Tx YK 017,16007931,Raguel Church ↔ Megenagna (Minibus),3,AA,,1779c2,ffffff
Tx YK 019,16008090,Autobus Tera ↔ Lamberet MenaherIa (Minibus),3,AA,,1779c2,ffffff
Tx YK 021,16008308,Shola ↔ Piassa Arada (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 006,16028728,Mexico ↔ Hana Mariam (Minibus),3,AA,,1779c2,ffffff
Tx Lafto 007,16028886,Mexico ↔ Haile Garment (Minibus),3,AA,,1779c2,ffffff
Kolfe 042,17332228,Yeshi Debele ↔ Ayer Tena (Minibus),3,AA,,1779c2,ffffff
LK 015,17332229,Goro ↔ Abem Mebrat (Minibus),3,AA,,1779c2,ffffff
Lafto 044,17332230,Megenagna ↔ Hana Mariam (Minibus),3,AA,,1779c2,ffffff
Lafto 045,17332231,Kality Total ↔ Hana Mariam (Minibus),3,AA,,1779c2,ffffff
Tx KRK,20385422,Lafto ↔ Gofa Gebriel (Minibus),3,AA,,1779c2,ffffff
Tx Kal,20385488,Saris Abo ↔ Mariam Condominium (Minibus),3,AA,,1779c2,ffffff
Tx 002,20389398,Kazanchis ↔ Abuare Roundabout (Minibus),3,AA,,1779c2,ffffff
Tx 001,20389399,Kazanchis ↔ Balderas (Minibus),3,AA,,1779c2,ffffff
Tx 005,20389824,Winget ↔ Kechenie (Minibus),3,AA,,1779c2,ffffff
Tx 004,20389825,Mexico ↔ Kazanchis (Minibus),3,AA,,1779c2,ffffff
Tx 003,20389826,Kela ↔ Asko (Minibus),3,AA,,1779c2,ffffff
Tx 007,20389875,Medhanalem ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx 006,20389876,Torhayloch ↔ Atena Tera (Minibus),3,AA,,1779c2,ffffff
Tx 008,20389877,Tikur Anbesa ↔ Autobus Tera (Minibus),3,AA,,1779c2,ffffff
Tx 009,20406757,Jemmo 1 ↔ Gofa Gebriel,3,AA,,1779c2,ffffff
Tx 010,20406755,Ayer Tena ↔ Haile Garment,3,AA,,1779c2,ffffff`;

async function importRoutes() {
  const client = await pool.connect();
  try {
    // Fetch existing hubs to build a name-to-ID lookup map
    const hubsResult = await client.query('SELECT id, name FROM hubs;');
    const hubsMap = {};
    hubsResult.rows.forEach(row => {
      hubsMap[row.name.trim().toLowerCase()] = row.id;
    });

    // Parse CSV data using standard library or simple line splitting
    const lines = ROUTES_CSV_DATA.trim().split('\n');
    const headers = lines[0].split(',');
    
    let insertedCount = 0;
    let skippedCount = 0;

    for (let i = 1; i < lines.length; i++) {
      const currentLine = lines[i].split(',');
      const row = {};
      headers.forEach((h, index) => {
        row[h.trim()] = currentLine[index]?.trim();
      });

      const routeId = row['route_id'];
      const routeLongName = row['route_long_name'];

      if (routeLongName && routeLongName.includes('↔')) {
        const terminals = routeLongName.split('↔');
        const pickupRaw = terminals[0].replace('(Minibus)', '').trim();
        const dropoffRaw = terminals[1].replace('(Minibus)', '').split('(')[0].trim();

        const pickupHubId = hubsMap[pickupRaw.toLowerCase()];
        const dropoffHubId = hubsMap[dropoffRaw.toLowerCase()];

        if (pickupHubId && dropoffHubId) {
          await client.query(
            `INSERT INTO routes (id, hub_id, pickup_hub_id, drop_off_hub_id, pass_through_hub_id)
             VALUES ($1, $2, $3, $4, NULL)
             ON CONFLICT (id) DO UPDATE 
             SET pickup_hub_id = EXCLUDED.pickup_hub_id, 
                 drop_off_hub_id = EXCLUDED.drop_off_hub_id;`,
            [routeId, pickupHubId, pickupHubId, dropoffHubId]
          );
          insertedCount++;
        } else {
          skippedCount++;
        }
      }
    }

    console.log(`Successfully processed routes: ${insertedCount} inserted/updated, ${skippedCount} skipped.`);
  } finally {
    client.release();
    await pool.end();
  }
}

importRoutes().catch(err => console.error('Error executing import:', err));
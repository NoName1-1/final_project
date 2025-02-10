CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    visit_date DATE NOT NULL,
    visit_time TIME,
    visit_number INTEGER NOT NULL,
    utm_source VARCHAR,
    utm_medium VARCHAR,
    utm_campaign VARCHAR,
    utm_adcontent VARCHAR,
    utm_keyword VARCHAR,
    device_category VARCHAR,
    device_os VARCHAR,
    device_brand VARCHAR,
    device_model VARCHAR,
    device_screen_resolution VARCHAR,
    device_browser VARCHAR,
    geo_country VARCHAR,
    geo_city VARCHAR
);

CREATE TABLE IF NOT EXISTS hits (
    hit_id SERIAL PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    hit_date DATE NOT NULL,
    hit_time TIME,
    hit_number INTEGER NOT NULL,
    hit_type VARCHAR NOT NULL,
    hit_referer VARCHAR,
    hit_page_path VARCHAR NOT NULL,
    event_category VARCHAR NOT NULL,
    event_action VARCHAR NOT NULL,
    event_label VARCHAR,
    event_value FLOAT,
    CONSTRAINT fk_session FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
);

from db_local_api import read, write

"""
Instructions for using this module:

1. Use the `read` function to execute SELECT queries.
   - The result will be printed in a table format by default.
   - To suppress the table output, pass `verbose=False` as an argument.

2. Use the `write` function for executing INSERT, UPDATE, DELETE, or other non-SELECT SQL statements.

3. Note: The current user 'ds_user' does not have permissions to modify the table structure (e.g., ALTER TABLE). 
   Ensure you have the necessary permissions or consult the database administrator before attempting structural changes.

Example usage:

query = "INSERT INTO users (user_id, ...) VALUES (...);"
write(query)

data_frame = read("SELECT * FROM users WHERE ...;")

"""

query = """
drop table if exists readings;
create table readings (
    time timestamptz not null,
    city varchar(255) not null,
    lat double precision not null check (lat >= -90 and lat <= 90),
    lon double precision not null check (lon >= -180 and lon <= 180),
    timezone varchar(50) not null,
    timezone_offset int not null,
    dt int not null,
    sunrise int not null,
    sunset int not null,
    temp double precision not null,
    feels_like double precision not null,
    pressure double precision not null,
    humidity double precision not null check (humidity >= 0 and humidity <= 100),
    dew_point double precision not null,
    uvi double precision not null,
    clouds double precision not null,
    visibility double precision not null,
    wind_speed double precision not null,
    wind_deg double precision not null,
    wind_gust double precision not null,
    weather_type_code int not null,
    main varchar(25) not null,
    description varchar(60) not null,
    icon varchar(10) not null,
    rain_1h double precision not null,
    snow_1h double precision not null,
    primary key (time, lat, lon)
);
"""

write(query)
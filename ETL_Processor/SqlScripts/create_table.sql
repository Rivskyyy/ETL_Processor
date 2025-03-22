CREATE TABLE ProcessedData (
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT NULL,
    trip_distance FLOAT,
    store_and_fwd_flag VARCHAR(3),
    PULocationID INT,
    DOLocationID INT,
    fare_amount DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2)
);
GO

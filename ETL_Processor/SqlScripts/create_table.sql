USE SampleDB;
GO

CREATE TABLE ProcessedData (
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INT NULL,
    trip_distance FLOAT NULL,
    store_and_fwd_flag VARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    fare_amount DECIMAL(10, 2) NULL,
    tip_amount DECIMAL(10, 2) NULL,
    PRIMARY KEY (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count)
);
GO
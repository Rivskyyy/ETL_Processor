USE SampleDB;
GO

CREATE INDEX IX_PULocationID_TipAmount ON ProcessedData (PULocationID, tip_amount);
CREATE INDEX IX_TripDistance ON ProcessedData (trip_distance DESC);
CREATE INDEX IX_TripDuration ON ProcessedData (DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) DESC);
CREATE INDEX IX_PULocationID ON ProcessedData (PULocationID);
GO
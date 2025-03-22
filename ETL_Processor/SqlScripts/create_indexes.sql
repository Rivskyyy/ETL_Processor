
CREATE INDEX IX_PULocationID_AvgTipAmount
ON ProcessedData (PULocationID)
INCLUDE (tip_amount);

CREATE INDEX IX_TripDistance
ON ProcessedData (trip_distance DESC);

CREATE INDEX IX_PULocationID
ON ProcessedData (PULocationID);

CREATE INDEX IX_StoreAndFwdFlag
ON ProcessedData (store_and_fwd_flag);

CREATE INDEX IX_PickupDropoffDateTime
ON ProcessedData (tpep_pickup_datetime, tpep_dropoff_datetime);

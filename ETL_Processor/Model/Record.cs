﻿using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_Processor.Model
{
    public class Record
    {
        [Name("tpep_pickup_datetime")]
        public DateTime PickupDateTime { get; set; }

        [Name("tpep_dropoff_datetime")]
        public DateTime DropoffDateTime { get; set; }

        [Name("passenger_count")]
        public int? PassengerCount { get; set; }

        [Name("trip_distance")]
        public double TripDistance { get; set; }

        [Name("store_and_fwd_flag")]
        public string StoreAndFwdFlag { get; set; }

        [Name("PULocationID")]
        public int PULocationID { get; set; }

        [Name("DOLocationID")]
        public int DOLocationID { get; set; }

        [Name("fare_amount")]
        public decimal FareAmount { get; set; }

        [Name("tip_amount")]
        public decimal TipAmount { get; set; }

    }

}

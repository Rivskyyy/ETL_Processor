﻿using ETL_Processor.Model;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;

namespace ETL_Processor.Services
{
    public class TransformService
    {
        public IEnumerable<Record> TransformData(IEnumerable<Record> records)
        {
            var tzdb = DateTimeZoneProviders.Tzdb;
            var estZone = tzdb["America/New_York"];

            foreach (var record in records)
            {
                record.StoreAndFwdFlag = record.StoreAndFwdFlag?.Trim();
                record.StoreAndFwdFlag = record.StoreAndFwdFlag == "Y" ? "Yes" :
                              (record.StoreAndFwdFlag == "N" ? "No" : record.StoreAndFwdFlag);

                var pickupInstant = Instant.FromDateTimeUtc(record.PickupDateTime.ToUniversalTime());
                var dropoffInstant = Instant.FromDateTimeUtc(record.DropoffDateTime.ToUniversalTime());

                var pickupZonedDateTime = pickupInstant.InZone(estZone);
                var dropoffZonedDateTime = dropoffInstant.InZone(estZone);

                record.PickupDateTime = pickupZonedDateTime.ToInstant().ToDateTimeUtc();
                record.DropoffDateTime = dropoffZonedDateTime.ToInstant().ToDateTimeUtc();

                yield return record;
            }
        }
    }
}


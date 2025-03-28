﻿using CsvHelper.Configuration;
using CsvHelper;
using ETL_Processor.Model;
using System.Globalization;

namespace ETL_Processor.Services
{
    public class DuplicatesRemoveService
    {
        public async Task<IEnumerable<Record>> RemoveDuplicatesAsync(IAsyncEnumerable<Record> records)
        {
            var recordList = new List<Record>();
            await foreach (var record in records)
            {
                recordList.Add(record);
            }

            var uniqueRecords = recordList
         .DistinctBy(r => Tuple.Create(r.PickupDateTime, r.DropoffDateTime, r.PassengerCount))
         .ToList();

            return uniqueRecords;

        }
        public async Task<IEnumerable<Record>> GetDuplicatesAsync(
          IAsyncEnumerable<Record> transformedRecords,
          IEnumerable<Record> recordsWithoutDuplicates)
        {
            var transformedList = new List<Record>();
            await foreach (var record in transformedRecords)
            {
                transformedList.Add(record);
            }

            var seenSet = new HashSet<Tuple<DateTime, DateTime, int?>>();
            var duplicatesList = new List<Record>();

            foreach (var record in transformedList)
            {
                var recordKey = Tuple.Create(record.PickupDateTime, record.DropoffDateTime, record.PassengerCount);

           
                if (seenSet.Contains(recordKey))
                {
                    duplicatesList.Add(record);
                }
                else
                {
                    seenSet.Add(recordKey); 
                }
            }

            Console.WriteLine($"Total duplicates found: {duplicatesList.Count}");
            return duplicatesList;
        }

        public async Task WriteDuplicatesToCsvAsync(IEnumerable<Record> duplicates, string filePath)
        {
            using (var writer = new StreamWriter(filePath))
            using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                await csv.WriteRecordsAsync(duplicates);
            }
        }
    }
}

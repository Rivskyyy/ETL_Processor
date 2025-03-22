using CsvHelper.Configuration;
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

            return recordList.GroupBy(r => new { r.PickupDateTime, r.DropoffDateTime, r.PassengerCount })
                             .Select(g => g.First());
        }
        public async Task<IEnumerable<Record>> GetDuplicatesAsync(IAsyncEnumerable<Record> transformedRecords, IEnumerable<Record> recordsWithoutDuplicates)
        {
            var transformedList = new List<Record>();
            var duplicatesList = new List<Record>();

   
            await foreach (var record in transformedRecords)
            {
                transformedList.Add(record);
            }

            foreach (var record in recordsWithoutDuplicates)
            {
                if (transformedList.Contains(record))
                {
                    duplicatesList.Add(record);
                }
            }

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

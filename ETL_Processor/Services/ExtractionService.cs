using CsvHelper.Configuration;
using CsvHelper;
using ETL_Processor.Model;
using System.Globalization;

namespace ETL_Processor.Services
{
    public class ExtractionService
    {
        public async IAsyncEnumerable<Record> ExtractDataAsync(string filePath)
        {
            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
            var records = csv.GetRecordsAsync<Record>();

            await foreach (var record in records)
            {
                yield return record;
            }
        }
    }
}
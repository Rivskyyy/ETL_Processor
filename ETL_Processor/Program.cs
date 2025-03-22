using ETL_Processor.Services;
using Microsoft.Extensions.Configuration;

namespace ETL_Processor
{
    internal class Program
    {
        private const string CSV_FILE_PATH = "RawData/sample-cab-data.csv";
        private const string CSV_FILE_DUPLICATES_PATH = "Duplicates/duplicates.csv";
       
        static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddUserSecrets<Program>()
            .Build();

            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var extractionService = new ExtractionService();
            var transformService = new TransformService();
            var duplicatesRemoveService = new DuplicatesRemoveService();
            var loadingService = new LoadingService(connectionString);

            Console.WriteLine("Extracting data...");
            var records = extractionService.ExtractDataAsync(CSV_FILE_PATH);
            var transformedRecords = transformService.TransformDataAsync(records);
            var recordsWithoutDuplicates = await duplicatesRemoveService.RemoveDuplicatesAsync(transformedRecords);

            Console.WriteLine("Loading data into the database...");
            await loadingService.LoadDataAsync(recordsWithoutDuplicates);

            var duplicates = await duplicatesRemoveService.GetDuplicatesAsync(transformedRecords, recordsWithoutDuplicates);
            Console.WriteLine("Saving duplicates to CSV...");
            await duplicatesRemoveService.WriteDuplicatesToCsvAsync(duplicates, CSV_FILE_DUPLICATES_PATH);

            Console.WriteLine("Process completed.");

        }
    }
}

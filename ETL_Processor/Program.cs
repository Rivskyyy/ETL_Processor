using ETL_Processor.Services;
using Microsoft.Extensions.Configuration;

namespace ETL_Processor
{
    internal class Program
    {
        private const string CSV_FILE_PATH = "RawData/sample-cab-data.csv";
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory()) 
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true) 
            .Build();

            var connectionString = configuration.GetConnectionString("DefaultConnection");

            var extractionService = new ExtractionService();
            var transformService = new TransformService();
            var loadingService = new LoadingService(connectionString);

            var extractedRecords = extractionService.ExtractData(CSV_FILE_PATH);
            var transformedRecords = transformService.TransformData(extractedRecords);
            loadingService.LoadData(transformedRecords);

        }
    }
}

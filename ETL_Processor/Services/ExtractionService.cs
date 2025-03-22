using CsvHelper.Configuration;
using CsvHelper;
using ETL_Processor.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_Processor.Services
{
    public class ExtractionService
    {
        public IEnumerable<Record> ExtractData(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                return csv.GetRecords<Record>();
            }
        }
    }
}

using ETL_Processor.Model;
using Microsoft.Data.SqlClient;
using System.Data;

namespace ETL_Processor.Services
{
    public class LoadingService
    {
        private readonly string _connectionString;

        public LoadingService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task LoadDataAsync(IEnumerable<Record> records)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync().ConfigureAwait(false);

            using var transaction = connection.BeginTransaction();

            try
            {
                using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                bulkCopy.DestinationTableName = "ProcessedData";

                var dataTable = new DataTable();
                dataTable.Columns.Add("PickupDateTime", typeof(DateTime));
                dataTable.Columns.Add("DropoffDateTime", typeof(DateTime));
                dataTable.Columns.Add("PassengerCount", typeof(int));
                dataTable.Columns.Add("TripDistance", typeof(double));
                dataTable.Columns.Add("StoreAndFwdFlag", typeof(string));
                dataTable.Columns.Add("PULocationID", typeof(int));
                dataTable.Columns.Add("DOLocationID", typeof(int));
                dataTable.Columns.Add("FareAmount", typeof(decimal));
                dataTable.Columns.Add("TipAmount", typeof(decimal));

                foreach (var record in records)
                {
                    dataTable.Rows.Add(
                        record.PickupDateTime,
                        record.DropoffDateTime,
                        record.PassengerCount,
                        record.TripDistance,
                        record.StoreAndFwdFlag,
                        record.PULocationID,
                        record.DOLocationID,
                        record.FareAmount,
                        record.TipAmount
                    );
                }

                await bulkCopy.WriteToServerAsync(dataTable).ConfigureAwait(false);

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new Exception("An error occurred while loading data into the database", ex);
            }
        }
    }
}

using ETL_Processor.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_Processor.Services
{
    public class LoadingService
    {
        private readonly string _connectionString;

        public LoadingService(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void LoadData(IEnumerable<Record> records)
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            foreach (var record in records)
            {
                var command = new SqlCommand("INSERT INTO ProcessedData (PickupDateTime, DropoffDateTime, PassengerCount, TripDistance, StoreAndFwdFlag, PULocationID, DOLocationID, FareAmount, TipAmount) " +
                                     "VALUES (@PickupDateTime, @DropoffDateTime, @PassengerCount, @TripDistance, @StoreAndFwdFlag, @PULocationID, @DOLocationID, @FareAmount, @TipAmount)", connection);

                command.Parameters.AddWithValue("@PickupDateTime", record.PickupDateTime);
                command.Parameters.AddWithValue("@DropoffDateTime", record.DropoffDateTime);
                command.Parameters.AddWithValue("@PassengerCount", record.PassengerCount);
                command.Parameters.AddWithValue("@TripDistance", record.TripDistance);
                command.Parameters.AddWithValue("@StoreAndFwdFlag", record.StoreAndFwdFlag);
                command.Parameters.AddWithValue("@PULocationID", record.PULocationID);
                command.Parameters.AddWithValue("@DOLocationID", record.DOLocationID);
                command.Parameters.AddWithValue("@FareAmount", record.FareAmount);
                command.Parameters.AddWithValue("@TipAmount", record.TipAmount);

                command.ExecuteNonQuery();
            }
        }
    }
}

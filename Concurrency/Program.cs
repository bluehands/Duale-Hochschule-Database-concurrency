using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = @"Data Source=.;Initial Catalog=Concurrency;Integrated Security=True";
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var customerV1 = GetCustomer(connection, 1);
                var customerV2 = GetCustomer(connection, 1);
                customerV1.Name = "changed";
                customerV2.Name = "changedV2";
                UpdateCustomer(connection, customerV1);
                UpdateCustomer(connection, customerV2); //This line throw a DBConcurrencyException 
            }
        }

        private static void UpdateCustomer(SqlConnection connection, Customer customer)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "UPDATE [Customer] SET [Name] = @name,[Street] = @street,[ZipCode] = @zipCode,[City] =@city WHERE CustomerId=@id and [Version]=@version";
                cmd.Parameters.Add("@name", SqlDbType.NVarChar).Value = customer.Name;
                cmd.Parameters.Add("@street", SqlDbType.NVarChar).Value = customer.Street;
                cmd.Parameters.Add("@zipCode", SqlDbType.NVarChar).Value = customer.ZipCode;
                cmd.Parameters.Add("@city", SqlDbType.NVarChar).Value = customer.City;
                cmd.Parameters.Add("@id", SqlDbType.Int).Value = customer.Id;
                cmd.Parameters.Add("@version", SqlDbType.Timestamp).Value = customer.Version;
                var affectedRows = cmd.ExecuteNonQuery();
                if (affectedRows == 0)
                {
                    throw new DBConcurrencyException("Version mismatch");
                }

            }
        }

        private static Customer GetCustomer(SqlConnection connection, int id)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT [CustomerId] ,[Name],[Street],[ZipCode],[City],[Version] FROM [Customer] where [CustomerId]=@id";
                cmd.Parameters.Add("@id", SqlDbType.Int).Value = id;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var customer = new Customer();
                        customer.Id = reader.GetInt32(0);
                        customer.Name = reader.GetString(1);
                        customer.Street = reader.GetString(2);
                        customer.ZipCode = reader.GetString(3);
                        customer.City = reader.GetString(4);
                        customer.Version = reader.GetFieldValue<byte[]>(5);
                        return customer;
                    }
                }
            }
            return null;
        }
    }

    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Street { get; set; }
        public string ZipCode { get; set; }
        public string City { get; set; }
        public byte[] Version { get; set; }
    }
}

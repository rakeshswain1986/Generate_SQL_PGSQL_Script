using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using Npgsql;

namespace SqlScriptGenerator
{
    class Program
    {
        static void Main(string[] args)
        {

            try {
                
            string source = ""; // SQL Server connection string
            string target = ""; // PostgreSQL connection string
            
            List<ColumnDetail> sourceTables = new List<ColumnDetail>();
            List<ColumnDetail> sourceData = new List<ColumnDetail>();
            List<ColumnDetail> destinationData = new List<ColumnDetail>();
            List<ColumnDetail> missingColumnData = new List<ColumnDetail>();

            string query =$"select TABLE_NAME  column_name,'' IS_NULLABLE,'' DATA_TYPE,'' NUMERIC_PRECISION,'' NUMERIC_SCALE from INFORMATION_SCHEMA.TABLES";
            sourceTables = GetSqlServerSchema(source, query);
            foreach (var item in sourceTables)
            {
                query =$"SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{item.ColumnName.ToLower()}'";
                sourceData = GetSqlServerSchema(source, query);
                destinationData = GetPostgreSqlSchema(target, tableName:item.ColumnName.ToLower());
               // If both source and destination has table 
                if(sourceData.Count > 0 && destinationData.Count > 0)
                {
                    missingColumnData = CompareSchemas(sourceData, destinationData);
                    if(missingColumnData.Count > 0)
                    {
                        string sqlScript = GenerateSqlScript(item.ColumnName.ToLower(), missingColumnData);
                        Console.WriteLine(sqlScript);
                    }
                }else
                {
                    Console.WriteLine($"Table {item.ColumnName.ToLower()} not found in destination");
                }
                
            }
            } catch (Exception ex) { Console.WriteLine(ex.Message); }
        }
        static List<ColumnDetail> GetSqlServerSchema(string connectionString, string query)
        {
            List<ColumnDetail> columns = new List<ColumnDetail>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var command = new SqlCommand(query, connection);
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    columns.Add(
                        new ColumnDetail
                        {
                            ColumnName = reader["COLUMN_NAME"].ToString(),
                            IsNullable = reader["IS_NULLABLE"].ToString(),
                            DataType = reader["DATA_TYPE"].ToString(),
                            NumericPrecision = reader["NUMERIC_PRECISION"].ToString(),
                            NumericScale = reader["NUMERIC_SCALE"].ToString()
                        }
                    );
                }
                connection.Close();
                return columns;
            }
        }

        static List<ColumnDetail>  GetPostgreSqlSchema(string connectionString, string tableName)
        {
            List<ColumnDetail> columns = new List<ColumnDetail>();
            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();
                var command = new NpgsqlCommand($"SELECT COLUMN_NAME,IS_NULLABLE,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{tableName}'", connection);
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    columns.Add(
                        new ColumnDetail
                        {
                            ColumnName = reader["COLUMN_NAME"].ToString(),
                            IsNullable = reader["IS_NULLABLE"].ToString(),
                            DataType = reader["DATA_TYPE"].ToString(),
                            NumericPrecision = reader["NUMERIC_PRECISION"].ToString(),
                            NumericScale = reader["NUMERIC_SCALE"].ToString()
                        }
                    );
                }
                connection.Close();

                return columns;
            }
        }

        static List<ColumnDetail>  CompareSchemas(List<ColumnDetail> sourceData, List<ColumnDetail> destinationData)
        {
            List<ColumnDetail> notInDestination = new List<ColumnDetail>();
            foreach (var item in sourceData)
            {
                var column = destinationData.Find(x => x.ColumnName.ToLower() == item.ColumnName.ToLower());
                if (column == null)
                {
                   notInDestination.Add(item);
                }
            }
            return notInDestination;
        }
        static string GenerateSqlScript(string tableName, List<ColumnDetail> columns)
        {
            var sb = new StringBuilder();

            foreach (var column in columns)
            {
                string nullability = column.IsNullable == "YES" ? "NULL" : "NOT NULL";
                string precisionScale = column.DataType.ToLower() == "numeric" ? 
                    $"({column.NumericPrecision}, {column.NumericScale})" : 
                    (column.DataType.ToLower() == "decimal" ? 
                    $"({column.NumericPrecision}, {column.NumericScale})" : string.Empty);

                sb.AppendLine($"ALTER TABLE {tableName} ADD {column.ColumnName} {column.DataType} {precisionScale} {nullability};");
            }

            return sb.ToString();
        }
    }

    class ColumnDetail
    {
        public string ColumnName { get; set; }
        public string IsNullable { get; set; }
        public string DataType { get; set; }
        public string NumericPrecision { get; set; }
        public string NumericScale { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using Npgsql;
using System.Collections;

namespace SqlScriptGenerator
{
    class Program
    {
        static void Main(string[] args)
        {

            try {
            Console.WriteLine("Starting the process...");    
            string source = ""; // SQL Server connection string
            string target = ""; // PostgreSQL connection string
            File.WriteAllText("alterscript.txt", "");
            File.WriteAllText("newTable.txt", "");
            List<ColumnDetail> sourceTables = new List<ColumnDetail>();
            List<ColumnDetail> sourceData = new List<ColumnDetail>();
            List<ColumnDetail> destinationData = new List<ColumnDetail>();
            List<ColumnDetail> missingColumnData = new List<ColumnDetail>();

            string query =$"select TABLE_NAME  column_name,'' IS_NULLABLE,'' DATA_TYPE,'' CHARACTER_MAXIMUM_LENGTH,'' NUMERIC_PRECISION,'' NUMERIC_SCALE from INFORMATION_SCHEMA.TABLES";
            sourceTables = GetSqlServerSchema(source, query);
            foreach (var item in sourceTables)
            {
                query =$"SELECT lower(COLUMN_NAME) COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{item.ColumnName.ToLower()}' and COLUMN_NAME not like '%RowId%' and COLUMN_NAME not like '%Row_Id' AND COLUMN_NAME!= 'SysStartTime' AND COLUMN_NAME!= 'SysEndTime' and COLUMN_NAME!= 'rowvalue' ";
                sourceData = GetSqlServerSchema(source, query);
                destinationData = GetPostgreSqlSchema(target, tableName:item.ColumnName.ToLower());
               // If both source and destination has table 
                if(sourceData.Count > 0 && destinationData.Count > 0)
                {
                    missingColumnData = CompareSchemas(sourceData, destinationData);
                    if(missingColumnData.Count > 0)
                    {
                        string sqlScript = GenerateSqlScript(item.ColumnName.ToLower(), missingColumnData);
                        File.AppendAllText("alterscript.txt", sqlScript + Environment.NewLine);
                    }
                }else
                {
                   if(!item.ColumnName.ToLower().StartsWith("du_") || item.ColumnName.ToLower().StartsWith("st_"))
                   {
                     string sqlScript = GenerateSqlScript(item.ColumnName.ToLower(), sourceData,isAlter:false);
                    File.AppendAllText("newTable.txt", sqlScript + Environment.NewLine);
                   }
                } 
            }
             Console.WriteLine("End the process...");  
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
                            NumericScale = reader["NUMERIC_SCALE"].ToString(),
                            Length = reader["CHARACTER_MAXIMUM_LENGTH"].ToString()
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
        static string GenerateSqlScript(string tableName, List<ColumnDetail> columns,bool isAlter = true)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"-- changeset ivycpg:{System.DateTime.Now.Date.ToString("yyyyMMdd")}-1 labels:CPG_Missing_Columns_Add");
            if(!isAlter) sb.AppendLine($"create table {tableName} (");
            foreach (var column in columns)
            {
                string precisionScale = string.Empty;
                string nullability = column.IsNullable == "YES" ? "null" : "not null" + ((column.DataType.ToLower() == "numeric" || column.DataType.ToLower() == "bigint" || column.DataType.ToLower() == "int" || column.DataType.ToLower() == "smallint")  ? " default 0" : string.Empty);
                switch(column.DataType.ToLower()){
                  case "bit":
                    column.DataType = "boolean";
                    break;
                  case "nvarchar":
                    column.DataType = column.Length == "-1" ? "text" :"varchar";
                    precisionScale = column.Length != "-1"? $"({column.Length})":string.Empty;
                    break;
                  case "varchar":
                    column.DataType = column.Length == "-1" ? "text" :"varchar";
                    precisionScale = column.Length != "-1"? $"({column.Length})":string.Empty;
                    break;
                  case "char":
                    column.DataType = "varchar";
                    precisionScale = $"(1)";
                    break;
                  case "datetime":
                    column.DataType = "timestamp";
                    break;
                  case "bigint":
                    column.DataType = "int8";
                    break;
                  case "int":
                    column.DataType = "int4";
                    break;
                  case "smallint":
                    column.DataType = "int2";
                    break;
                  case "decimal":
                    column.DataType = "numeric";
                    precisionScale = $"({column.NumericPrecision}, {column.NumericScale})";
                    break;};
                if(isAlter) 
                sb.AppendLine($"alter table {tableName} add {column.ColumnName} {column.DataType} {precisionScale} {nullability};");
                else
                sb.AppendLine($"{column.ColumnName} {column.DataType} {precisionScale} {nullability},");
            }
            if(!isAlter) sb.AppendLine(");");
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
        public string Length { get; set; }
    }
}

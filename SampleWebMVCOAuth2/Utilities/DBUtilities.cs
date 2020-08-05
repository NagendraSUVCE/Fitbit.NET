    using System;
    using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
    using System.Web;

    namespace SampleWebMVCOAuth2.Utilities
    {

    public class DBUtilities
    {
        public static string ConnectionString { get; set; }
        public static void Migrate(DataSet ds)
        {
            string colsUnique = string.Empty;
            string[] colsUniqueArray = null;
            List<string> lstUnique = new List<string>();
            foreach (DataTable item in ds.Tables)
            {
                CreateTableStructure(item);
                if (item != null && item.Columns.Contains("Identity") && item.Rows.Count > 0 && item.Rows[0]["Identity"] != null)
                {
                    colsUnique = item.Rows[0]["Identity"].ToString();
                    colsUniqueArray = colsUnique.Split(',');
                    lstUnique = colsUniqueArray.ToList();
                }
                else
                {
                    lstUnique = item.Columns.Cast<DataColumn>()
                                 .Where(y => y.ColumnName != "CreatedDate")
                                  .Select(x => x.ColumnName)
                                  .ToList();
                }

                MigrateToExistingTable(item, lstUnique);
            }
        }

        public static void CreateTableStructure(DataTable dt)
        {

            //connect to sql server and appropriate database see if table already present, if not present create new table after that load data from dataset to sql using bulkcopy
            try
            {
                SqlConnection conn = new SqlConnection(ConnectionString);
                conn.Open();
                //check if table is present or not
                string exists = null;
                try
                {
                    SqlCommand cmd = new SqlCommand("SELECT * FROM sysobjects where name = '" + dt.TableName + "'", conn);
                    exists = cmd.ExecuteScalar().ToString();
                }
                catch (Exception exce)
                {
                    exists = null;
                }
                //SqlBulkCopy bulk = new SqlBulkCopy(conn);
                //bulk.DestinationTableName = "[" + dt.TableName + "]";
                foreach (DataColumn dc in dt.Columns)
                {
                    //bulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                    string type = "";
                    //Getting right data type for column is very importatnt as it can create problem later if wrong data type is chosen
                    //for mapping we are using below key value pair
                    Dictionary<string, string> typemappings = new Dictionary<string, string>();
                    typemappings.Add("Decimal", "Numeric(6,2)");
                    typemappings.Add("String", "nvarchar(max) COLLATE Latin1_General_BIN");
                    typemappings.Add("Int32", "Int");
                    typemappings.TryGetValue(dc.DataType.FullName.Split('.')[1], out type);
                    if (type == null)
                        type = "nvarchar(max) COLLATE Latin1_General_BIN";
                    if (exists == null)
                    {
                        SqlCommand createtable = new SqlCommand("CREATE TABLE [dbo].[" + dt.TableName + "] ([" + dc.ColumnName + "] " + type + ")", conn);
                        createtable.ExecuteNonQuery();
                        exists = dt.TableName;
                    }
                    else
                    {
                        try
                        {
                            SqlCommand addcolumn = new SqlCommand("ALTER TABLE [dbo].[" + dt.TableName + "] ADD [" + dc.ColumnName + "] " + type, conn);
                            addcolumn.ExecuteNonQuery();
                        }
                        catch (Exception ex2)
                        {
                            //Console.WriteLine(ex2.Message.ToString() + "\n" + ex2.StackTrace.ToString());
                        }
                    }

                    //bulk.DestinationTableName = string.Format("[{0}].[{1}]", "dbo", dt.TableName);
                }
                //load data in sql
                //bulk.WriteToServer(dt);
                Console.WriteLine("Bulk Insert completed table:" + dt.TableName);
                conn.Close();
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString() + "\n" + ex.StackTrace.ToString());
            }
            finally
            {
            }
        }

        public static void MigrateToExistingTable(DataTable dt, List<string> ColumnsForUniqueCheck)
        {

            try
            {
                string uniqueQuery = string.Empty; object o = null;
                SqlConnection conn = new SqlConnection(ConnectionString);
                conn.Open();
                SqlBulkCopy bulk = new SqlBulkCopy(conn);
                bulk.DestinationTableName = "[" + dt.TableName + "]";
                foreach (DataColumn dc in dt.Columns)
                {
                    bulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                }
                DataTable dtClone = dt.Clone();
                int rowCount = 0;
                foreach (DataRow item in dt.Rows)
                {
                    rowCount++;
                    uniqueQuery = "SELECT * FROM [dbo].[" + dt.TableName + "] WHERE 1 = 1 AND";
                    foreach (var colUnique in ColumnsForUniqueCheck)
                    {
                        uniqueQuery = uniqueQuery + " " + colUnique + " = '" + item[colUnique] + "'" + " AND ";
                    }
                    uniqueQuery = uniqueQuery + " 1=1";
                    using (SqlCommand cmd = new SqlCommand(uniqueQuery, conn))
                    {
                        try
                        {

                            o = cmd.ExecuteScalar();
                            if (o == null)
                            {
                                System.Diagnostics.Debug.WriteLine("INSERTED rownumber    " + rowCount.ToString());
                                System.Diagnostics.Debug.WriteLine("QUERY " + uniqueQuery);
                                dtClone.ImportRow(item);
                            }
                            else
                            {

                                System.Diagnostics.Debug.WriteLine("NOT INSERTED. Row Already Exists. rownumber    " + rowCount.ToString());
                                System.Diagnostics.Debug.WriteLine("QUERY " + uniqueQuery);
                            }
                        }
                        catch (Exception ex)
                        {
                            dtClone.ImportRow(item);
                        }
                    }
                }
                if (dtClone.Rows.Count > 0)
                {
                    bulk.DestinationTableName = string.Format("[{0}].[{1}]", "dbo", dt.TableName);
                    bulk.WriteToServer(dtClone);
                }
            }
            catch (Exception ex)
            {

            }
        }

        public static DataSet ExecuteDataSet(string sqlText, string datasetName)
        {
            DataSet ds = null;
            try
            {// Assumes that connection is a valid SqlConnection object.
                SqlDataAdapter adapter = new SqlDataAdapter(sqlText, ConnectionString);

                ds = new DataSet();
                adapter.Fill(ds, datasetName);

            }
            catch (Exception ex)
            {

            }
            return ds;
        }

        public static void DeleteTableFromDatabase(DataTable dt)
        {
            string tableName = dt.TableName;
            if (tableName.ToLowerInvariant().StartsWith("table"))
            {

            }
            else
            {
                SqlConnection conn = new SqlConnection(ConnectionString);
                conn.Open();
                //check if table is present or not
                string exists = null;
                try
                {
                    SqlCommand cmd = new SqlCommand("SELECT * FROM sysobjects where name = '" + dt.TableName + "'", conn);
                    exists = cmd.ExecuteScalar().ToString();

                    if (exists != null)
                    {
                        SqlCommand cmdDel = new SqlCommand("DROP TABLE [dbo].[" + dt.TableName + "]", conn);
                        cmdDel.ExecuteNonQuery();
                    }
                }
                catch (Exception exce)
                {
                    exists = null;
                }
            }
        }
        public static void InsertDatatableToDatabase(DataTable dt)
        {

            //connect to sql server and appropriate database see if table already present, if not present create new table after that load data from dataset to sql using bulkcopy
            try
            {
                SqlConnection conn = new SqlConnection(ConnectionString);
                conn.Open();
                //check if table is present or not
                string exists = null;
                try
                {
                    SqlCommand cmd = new SqlCommand("SELECT * FROM sysobjects where name = '" + dt.TableName + "'", conn);
                    exists = cmd.ExecuteScalar().ToString();
                }
                catch (Exception exce)
                {
                    exists = null;
                }
                // selecting each column of the datatable to create a table in the database
                Console.WriteLine("Bulk Insert Started table:" + dt.TableName);
                SqlBulkCopy bulk = new SqlBulkCopy(conn);
                bulk.BulkCopyTimeout = 0;
                bulk.DestinationTableName = "[" + dt.TableName + "]";
                foreach (DataColumn dc in dt.Columns)
                {
                    bulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                    string type = "";
                    //Getting right data type for column is very importatnt as it can create problem later if wrong data type is chosen
                    //for mapping we are using below key value pair
                    Dictionary<string, string> typemappings = new Dictionary<string, string>();
                    typemappings.Add("Decimal", "Numeric(12,2)");
                    typemappings.Add("String", "nvarchar(max) COLLATE Latin1_General_BIN");
                    typemappings.Add("Int32", "Int");
                    typemappings.Add("double", "Int");
                    typemappings.Add("DateTime", "DateTime");
                    typemappings.TryGetValue(dc.DataType.FullName.Split('.')[1], out type);
                    if (type == null)
                        type = "nvarchar(max) COLLATE Latin1_General_BIN";
                    if (exists == null)
                    {
                        SqlCommand createtable = new SqlCommand("CREATE TABLE [dbo].[" + dt.TableName + "] ([" + dc.ColumnName + "] " + type + ")", conn);
                        createtable.ExecuteNonQuery();
                        exists = dt.TableName;
                    }
                    else
                    {
                        try
                        {
                            SqlCommand addcolumn = new SqlCommand("ALTER TABLE [dbo].[" + dt.TableName + "] ADD [" + dc.ColumnName + "] " + type, conn);
                            addcolumn.ExecuteNonQuery();
                        }
                        catch (Exception ex2)
                        {
                            //Console.WriteLine(ex2.Message.ToString() + "\n" + ex2.StackTrace.ToString());
                        }
                    }

                    bulk.DestinationTableName = string.Format("[{0}].[{1}]", "dbo", dt.TableName);
                }
                //load data in sql
                bulk.WriteToServer(dt);
                Console.WriteLine("Bulk Insert completed table:" + dt.TableName);
                conn.Close();
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString() + "\n" + ex.StackTrace.ToString());
            }
            finally
            {
            }
        }

        public static void Migrate(DataTable dt)
        {

            //connect to sql server and appropriate database see if table already present, if not present create new table after that load data from dataset to sql using bulkcopy
            try
            {
                SqlConnection conn = new SqlConnection(ConnectionString);
                conn.Open();
                //check if table is present or not
                string exists = null;
                try
                {
                    SqlCommand cmd = new SqlCommand("SELECT * FROM sysobjects where name = '" + dt.TableName + "'", conn);
                    exists = cmd.ExecuteScalar().ToString();
                }
                catch (Exception exce)
                {
                    exists = null;
                }
                // selecting each column of the datatable to create a table in the database
                Console.WriteLine("Bulk Insert Started table:" + dt.TableName);
                SqlBulkCopy bulk = new SqlBulkCopy(conn);
                bulk.DestinationTableName = "[" + dt.TableName + "]";
                foreach (DataColumn dc in dt.Columns)
                {
                    bulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                    string type = "";
                    //Getting right data type for column is very importatnt as it can create problem later if wrong data type is chosen
                    //for mapping we are using below key value pair
                    Dictionary<string, string> typemappings = new Dictionary<string, string>();
                    typemappings.Add("Decimal", "Numeric(6,2)");
                    typemappings.Add("String", "nvarchar(max) COLLATE Latin1_General_BIN");
                    typemappings.Add("Int32", "Int");
                    typemappings.TryGetValue(dc.DataType.FullName.Split('.')[1], out type);
                    if (type == null)
                        type = "nvarchar(max) COLLATE Latin1_General_BIN";
                    if (exists == null)
                    {
                        SqlCommand createtable = new SqlCommand("CREATE TABLE [dbo].[" + dt.TableName + "] ([" + dc.ColumnName + "] " + type + ")", conn);
                        createtable.ExecuteNonQuery();
                        exists = dt.TableName;
                    }
                    else
                    {
                        try
                        {
                            SqlCommand addcolumn = new SqlCommand("ALTER TABLE [dbo].[" + dt.TableName + "] ADD [" + dc.ColumnName + "] " + type, conn);
                            addcolumn.ExecuteNonQuery();
                        }
                        catch (Exception ex2)
                        {
                            //Console.WriteLine(ex2.Message.ToString() + "\n" + ex2.StackTrace.ToString());
                        }
                    }

                    bulk.DestinationTableName = string.Format("[{0}].[{1}]", "dbo", dt.TableName);
                }
                //load data in sql
                bulk.WriteToServer(dt);
                Console.WriteLine("Bulk Insert completed table:" + dt.TableName);
                conn.Close();
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString() + "\n" + ex.StackTrace.ToString());
            }
            finally
            {
            }
        }

        public static object GetSingleValue(string ConnectionString, string SQL)
        {
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = SQL;
            cmd.CommandTimeout = 0;

            conn.Open();
            object o = cmd.ExecuteScalar();
            conn.Close();

            return o;
        }
    }
}
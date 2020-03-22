using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using System.Data.Entity;

namespace Bulk
{
    public static class BulkOperation
    {
        private static int ConnectionTimeout { get; set; }

        private static List<Tuple<string, int>> primaryKeys = new List<Tuple<string, int>>();
        private static List<string> excludeFields = new List<string>();
        private static List<string> includeFields = new List<string>();
        private static List<string> joinKey = new List<string>();

        #region ConnectionTimeout

        public static DbContext SetConnectionTimeout<T>(this DbContext context, int timeout)
        {
            ConnectionTimeout = timeout;
            return context;
        }

        public static IEnumerable<T> SetConnectionTimeout<T>(this IEnumerable<T> ele, int timeout)
        {
            ConnectionTimeout = timeout;
            return ele;
        }
        #endregion

        #region exludeColumns

        public static DbContext excludeColumns<TSource, TRes>(this DbContext context, Expression<Func<TSource, TRes>> selector)
        {
            excludeColumns(selector);
            return context;
        }
        public static SqlConnection excludeColumns<TSource, TRes>(this SqlConnection connection, Expression<Func<TSource, TRes>> selector)
        {
            excludeColumns(selector);
            return connection;
        }
        private static void excludeColumns<TSource, TRes>(Expression<Func<TSource, TRes>> selector)
        {
            if (selector != null)
            {
                excludeFields = (((selector) as LambdaExpression).Body as NewExpression).Members.Select(s => s.Name).ToList();
            }
        }
        #endregion

        #region IncludeColumns
        public static DbContext includeColumns<TSource, TRes>(this DbContext context, Expression<Func<TSource, TRes>> selector)
        {
            includeColumns(selector);
            return context;
        }
        public static SqlConnection includeColumns<TSource, TRes>(this SqlConnection connection, Expression<Func<TSource, TRes>> selector)
        {
            includeColumns(selector);
            return connection;
        }
        private static void includeColumns<TSource, TRes>(Expression<Func<TSource, TRes>> selector)
        {
            if (selector != null)
            {
                includeFields = (((selector) as LambdaExpression).Body as NewExpression).Members.Select(s => s.Name).ToList();
            }
        }
        #endregion

        #region JoinColumns 
        public static DbContext joinColumns<TSource, TRes>(this DbContext context, Expression<Func<TSource, TRes>> selector)
        {
            joinColumns(selector);
            return context;
        }
        public static SqlConnection joinColumns<TSource, TRes>(this SqlConnection connection, Expression<Func<TSource, TRes>> selector)
        {
            joinColumns(selector);
            return connection;
        }
        private static void joinColumns<TSource, TRes>(Expression<Func<TSource, TRes>> selector)
        {
            if (selector != null)
            {
                joinKey = (((selector) as LambdaExpression).Body as NewExpression).Members.Select(s => s.Name).ToList();
            }
        }
        #endregion

        #region Create Table 
        public static void CreateTable<TSource>(this DbContext context, string tableName, bool dropIfExist = false)
        {
            using (SqlConnection conn = new SqlConnection(context.Database.Connection.ConnectionString))
            {
                SqlTransaction trans = null;
                try
                {
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    trans = dropIfExist ? conn.BeginTransaction() : null;

                    CreateTable<TSource>(conn, tableName, dropIfExist, trans);
                    if (trans != null) trans.Commit();
                }
                catch (Exception)
                {
                    if (trans != null) trans.Rollback();

                    throw;
                }
            }
        }
        public static void CreateTable<TSource>(this SqlConnection conn, string tableName, bool dropIfExist = false, SqlTransaction trans = null)
        {
            List<string> contentCreate = new List<string>();
            var fieldsCreate = GetProperties<TSource>();

            //Drop Table If Exist
            if (dropIfExist)
            {
                conn.DropTable<TSource>(tableName, trans, true);
            }

            using (SqlCommand command = new SqlCommand("", conn, trans))
            {
                //Creating temp table on database
                string pks = string.Join(",", primaryKeys.OrderBy(p => p.Item2).Select(s => s.Item1));
                contentCreate.AddRange(fieldsCreate.Select(e => $"[{e.Item2}] {e.Item3} {(e.Item4 ? "NULL" : "NOT NULL")}"));
                if (!string.IsNullOrEmpty(pks))
                {
                    contentCreate.Add($" PRIMARY KEY({pks})");
                }

                string commandCreate = $"CREATE TABLE [{tableName}] ({string.Join(", ", contentCreate)})";
                command.CommandText = commandCreate;
                command.Transaction = trans;
                command.ExecuteNonQuery();
            }
        }

        #endregion

        #region DropTable
        public static void DropTable<TSource>(this DbContext context, string tableName, bool useTransaction = false, bool useIfExist = false)
        {
            using (SqlConnection conn = new SqlConnection(context.Database.Connection.ConnectionString))
            {
                SqlTransaction trans = null;
                if (conn.State == ConnectionState.Closed) conn.Open();
                trans = useTransaction ? conn.BeginTransaction() : null;

                conn.DropTable<TSource>(tableName, trans, useIfExist);
            }
        }
        public static SqlConnection DropTable<TSource>(this SqlConnection conn, string tableName, SqlTransaction transaction = null, bool useIfExist = false)
        {
            using (SqlCommand command = new SqlCommand("", conn, transaction))
            {
                command.CommandText = $"DROP TABLE {(useIfExist ? "IF EXISTS" : "")} [{tableName}]";
                command.ExecuteNonQuery();
            }
            return conn;
        }


        public static DbContext BulkUpdate<TSource, TResult>(this DbContext context, IEnumerable<TSource> list, string TableName, Expression<Func<TSource, TResult>> fieldToUpdate)
        {
            using (SqlConnection conn = new SqlConnection(context.Database.Connection.ConnectionString))
            {
                if (conn.State == ConnectionState.Closed) conn.Open();
                SqlTransaction trans = conn.BeginTransaction();
                try
                {
                    conn.BulkUpdate(list, TableName, trans, fieldToUpdate);
                    trans.Commit();
                }
                catch (Exception)
                {
                    trans.Rollback();
                    throw;
                }
            }
            return context;
        }
        public static SqlConnection BulkUpdate<TSource, TResult>(this SqlConnection conn, IEnumerable<TSource> list, string TableName, SqlTransaction trans, Expression<Func<TSource, TResult>> fieldToUpdate)
        {
            list.BulkUpdate(TableName, conn, trans, fieldToUpdate);
            return conn;
        }

        #endregion

        public static IEnumerable<TSource> BulkUpdate<TSource, TResult>(this IEnumerable<TSource> list, string TableName, SqlConnection conn, SqlTransaction trans, Expression<Func<TSource, TResult>> fieldsToUpdate)// where TSource : class, new() 
        {
            string intTable = $"#{DateTime.Now.ToString("yyyyMMddHHmmssfff")}";

            string join = "";

            var fieldsCreate = GetProperties<TSource>();

            //Bulk insert into temp table
            BulkInsert(list, intTable, conn, trans, true, true);

            if (joinKey.Any())
            {
                join = string.Join(" AND ", joinKey.Select(s => $" [ExtendedRes].{s} = [ExtendedTemp].{s}"));
            }
            else if (primaryKeys.Any())
            {
                join = string.Join(" AND ", primaryKeys.Select(s => $" [ExtendedRes].{s.Item1} = [ExtendedTemp].{s.Item1}"));
            }
            else
            {
                throw new Exception("Nessuna chiave di confronto impostata,la tabella non ha una PrimaryKey, popolare il metodo 'joinColumns'");
            }
            var fieldsUpdatelist = GetFields(fieldsToUpdate);
            var fieldsUpdate = "";
            if (fieldsUpdatelist.Any())
            {
                fieldsUpdate = string.Join(" , ", fieldsUpdatelist.Select(s => $" {s} = [ExtendedTemp].{s}"));
            }
            using (SqlCommand command = new SqlCommand("", conn, trans))
            {
                // Updating destination table, and dropping temp table
                command.CommandTimeout = (ConnectionTimeout == 0 ? conn.ConnectionTimeout : ConnectionTimeout);
                command.CommandText = $"UPDATE [{TableName}] SET {fieldsUpdate} FROM [{intTable}] as [ExtendedTemp] INNER JOIN [{TableName}] as [ExtendedRes] on {join}; DROP TABLE [{intTable}];";
                command.ExecuteNonQuery();
            }
            return list;
        }

        public static void BulkInsert<T>(this IEnumerable<T> getDatareader, string tableName, SqlConnection conn,
          SqlTransaction trans = null, bool createTableIfNotExist = false, bool dropTableIfExist = false)
        {
            var fieldsCreate = GetProperties<T>();

            if (dropTableIfExist)
            {
                conn.DropTable<T>(tableName, trans, true);
            }

            if (createTableIfNotExist)
            {
                conn.CreateTable<T>(tableName, false, trans);
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, trans))
            {
                bulkCopy.DestinationTableName = $"[{tableName}]";
                bulkCopy.BulkCopyTimeout = conn.ConnectionTimeout;
                bulkCopy.ColumnMappings.Clear();
                fieldsCreate.Select(e => bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(e.Item2, e.Item2)));

                using (GenericListDataReader<T> dataReader = new GenericListDataReader<T>(getDatareader, fieldsCreate.Select(s => s.Item1).ToList()))
                {
                    bulkCopy.WriteToServer(dataReader);
                }
            }
        }

        #region "Utils" 
        private static List<Tuple<PropertyInfo, string, string, bool>> GetProperties<TSource>()
        {
            primaryKeys.Clear();
            List<Tuple<PropertyInfo, string, string, bool>> fieldsCreate = new List<Tuple<PropertyInfo, string, string, bool>>();
            foreach (PropertyInfo fieldinfo in typeof(TSource).GetProperties(
                BindingFlags.Instance |
                BindingFlags.Public)
                .Where(p => !p.GetGetMethod().IsVirtual))
            {
                //Skip Fields non contenute
                if (excludeFields.Any() && !excludeFields.Contains(fieldinfo.Name)) continue;
                else if (includeFields.Any() && !includeFields.Contains(fieldinfo.Name)) continue;

                //PrimaryKey
                if (GetAttributeFrom<KeyAttribute>(fieldinfo, fieldinfo.Name).FirstOrDefault() != null)
                {
                    var orderAttr = GetAttributeFrom<ColumnAttribute>(fieldinfo, "Order").FirstOrDefault();
                    int position = (orderAttr != null) ? position = orderAttr.Order : 0;
                    primaryKeys.Add(Tuple.Create(fieldinfo.Name, position));
                }

                //Fields da creare
                fieldsCreate.Add(Tuple.Create(fieldinfo, fieldinfo.Name, GetTypeSqlType(fieldinfo.GetGetMethod().ReturnType), fieldinfo.GetType().IsNullable()));
            }
            return fieldsCreate;
        }
        public static List<string> GetFields<T, TResult>(Expression<Func<T, TResult>> exp)
        {
            MemberExpression body = exp.Body as MemberExpression;
            var fields = new List<string>();
            if (body == null)
            {
                NewExpression ubody = exp.Body as NewExpression;
                if (ubody != null)
                    foreach (var arg in ubody.Arguments)
                    {
                        fields.Add((arg as MemberExpression).Member.Name);
                    }
            }
            else
            {
                fields.Add(body.Member.Name);
            }

            return fields;
        }
        private static T[] GetAttributeFrom<T>(PropertyInfo property, string propertyName) where T : Attribute
        {
            var attrType = typeof(T);
            return (T[])property.GetCustomAttributes(attrType, false);
        }
        private static string GetTypeSqlType(Type t)
        {
            switch (t.Name)
            {
                case "Int64":
                    return "bigint";
                case "Byte":
                    return "tinyint";
                case "Guid":
                    return "uniqueidentifier";
                case "TimeSpan":
                    return "time";
                case "Byte[]":
                    return "varbinary(max)";
                case "Boolean":
                    return "bit";
                case "DateTimeOffset":
                    return "datetimeoffset";
                case "Decimal":
                    return "decimal";
                case "Double":
                    return "float";
                case "Char[]":
                    return "nvarchar(max)";
                case "Single":
                    return "real";
                case "Int16":
                    return "smallint";
                case "Int32":
                    return "int";
                case "string":
                case "String":
                    return "varchar(max)";
                case "DateTime":
                    return "datetime";
                case "Xml":
                    return "xml";
                default:
                    throw new Exception("Type non valido");
            }
        }
        public static bool IsNullable(this Type type)
        {
            return Nullable.GetUnderlyingType(type) != null;
        }
        private class GenericListDataReader<T> : IDataReader //where T : class, new()
        {
            private IEnumerator<T> list = null;
            private List<PropertyInfo> fields;

            public GenericListDataReader(IEnumerable<T> listElements, List<PropertyInfo> fields)
            {
                list = listElements.GetEnumerator();
                this.fields = fields;
            }

            public void Dispose()
            {
                list.Dispose();
            }

            public string GetName(int i)
            {
                return fields[i].Name;
            }

            public string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public Type GetFieldType(int i)
            {
                return fields[i].PropertyType;
            }

            public object GetValue(int i)
            {
                return fields[i].GetValue(list.Current);

            }

            public int GetValues(object[] values)
            {
                throw new NotImplementedException();
            }

            public int GetOrdinal(string name)
            {
                return fields.FindIndex(p => p.Name == name);
            }

            public bool GetBoolean(int i)
            {
                throw new NotImplementedException();
            }

            public byte GetByte(int i)
            {
                throw new NotImplementedException();
            }

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public char GetChar(int i)
            {
                throw new NotImplementedException();
            }

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public Guid GetGuid(int i)
            {
                throw new NotImplementedException();
            }

            public short GetInt16(int i)
            {
                throw new NotImplementedException();
            }

            public int GetInt32(int i)
            {
                throw new NotImplementedException();
            }

            public long GetInt64(int i)
            {
                throw new NotImplementedException();
            }

            public float GetFloat(int i)
            {
                throw new NotImplementedException();
            }

            public double GetDouble(int i)
            {
                throw new NotImplementedException();
            }

            public string GetString(int i)
            {
                throw new NotImplementedException();
            }

            public decimal GetDecimal(int i)
            {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i)
            {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i)
            {
                throw new NotImplementedException();
            }

            public bool IsDBNull(int i)
            {
                throw new NotImplementedException();
            }

            public int FieldCount
            {
                get { return fields.Count; }
            }

            public object this[int i] => throw new NotImplementedException();

            public object this[string name] => throw new NotImplementedException();

            public void Close()
            {
                list.Dispose();
            }

            public DataTable GetSchemaTable()
            {
                throw new NotImplementedException();
            }

            public bool NextResult()
            {
                throw new NotImplementedException();
            }

            public bool Read()
            {
                return list.MoveNext();
            }

            public int Depth { get; }
            public bool IsClosed { get; }
            public int RecordsAffected { get; }
        }
        #endregion

    }


}

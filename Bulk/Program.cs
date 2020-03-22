using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulk
{
    class Program
    {
        static void Main(string[] args)
        {
            List<DataTest> data = new List<DataTest>();
            Random rdn = new Random(1);
            Console.WriteLine("Start");
            //try
            //{
            //    for (long i = 0; i < 1054300; i++)
            //    {
            //        data.Add(new DataTest() { value = rdn.Next(1, 1000000000).ToString(), stato = 0 });
            //        Console.WriteLine($"Prepare i : {i}");
            //    }

            //    Console.WriteLine($"StartBulk");

            //    using (SqlConnection conn = new SqlConnection(new ApplicationContext().Database.Connection.ConnectionString))
            //    {
            //        if (conn.State == System.Data.ConnectionState.Closed) { conn.Open(); }
            //        SqlTransaction trans = conn.BeginTransaction();

            //        new BulkOperation().BulkInsert<DataTest>("dataTest", conn, data, trans);

            //        trans.Commit();
            //    }
            //    Console.WriteLine($"End Bulk");
            //}
            //catch (Exception ex)
            //{

            //    Console.WriteLine(ex.Message);
            //}
            using (ApplicationContext ctx = new ApplicationContext())
            {
                data = ctx.dataTest.Where(w => w.stato == 0).Take(1000000).ToList().Select(s =>
                {
                    s.stato = 10;
                    return s;
                }).ToList();
            }

            using (SqlConnection conn = new SqlConnection(new ApplicationContext().Database.Connection.ConnectionString))
            {
                if (conn.State == System.Data.ConnectionState.Closed) { conn.Open(); }
                SqlTransaction trans = conn.BeginTransaction();

                //data.BulkInsert("dataTest", conn, trans);
                data.BulkUpdate("DataTests", conn, trans, s => s.stato);

                trans.Commit();
            }

        }


    }
}

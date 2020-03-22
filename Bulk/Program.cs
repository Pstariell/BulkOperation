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
            DateTime dtS =  DateTime.Now;
            Random rdn = new Random(1);
            try
            {
                Console.WriteLine($"StartBulk");
                //for (long i = 0; i < 1054300; i++)
                //{
                //    data.Add(new DataTest() { value = rdn.Next(1, 1000000000).ToString(), stato = 0 });
                //    Console.WriteLine($"Prepare i : {i}");
                //}


                //using (SqlConnection conn = new SqlConnection(new ApplicationContext().Database.Connection.ConnectionString))
                //{
                //    if (conn.State == System.Data.ConnectionState.Closed) { conn.Open(); }
                //    SqlTransaction trans = conn.BeginTransaction();

                //    conn.BulkInsert(data, "dataTest", trans);

                //    trans.Commit();
                //}

                using (ApplicationContext ctx = new ApplicationContext())
                {
                    data = ctx.dataTest.Where(w =>  w.stato == 0).Take(1000000).ToList().Select(s =>
                    {
                        s.stato = 10;
                        s.value = "0";
                        return s;
                    }).ToList();
                    ctx.BulkUpdate(data, "DataTests", s => new { s.stato, s.value });
                }

                DateTime dtE = DateTime.Now;
                Console.WriteLine($"End Bulk {TimeSpan.FromSeconds((dtE - dtS).TotalSeconds)} seconds");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
                Console.ReadLine();
        }


    }
}

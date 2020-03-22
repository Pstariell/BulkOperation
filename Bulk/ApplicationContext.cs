using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulk
{
    public class ApplicationContext : DbContext
    {
        public ApplicationContext() : base("ApplicationContext")
        {

            ((IObjectContextAdapter)this).ObjectContext.CommandTimeout = 500000000;
        }

        public DbSet<DataTest> dataTest { get; set; }
    }
}

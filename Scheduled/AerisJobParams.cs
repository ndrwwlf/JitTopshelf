using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Scheduled
{
    public class AerisJobParams
    {
        //public string AerisClientId { get; set; }
        //public string AerisClientSecret { get; set; }
        //public string JitWeatherConnectionString { get; set; }
        //public string JitWebData3ConnectionString { get; set; }

        public string AerisClientId = "vgayNZkz1o2JK6VRhOTBZ";
        public string AerisClientSecret = "8YK1bmJlOPJCIO2darWs48qmXPKzGxQHdWWzWmNg";

        public string JitWeatherConnectionString
            = "Data Source=.\\SQLEXPRESS;Initial Catalog=Weather;User ID=WorkWeeksql;Password=Jon23505#sql; MultipleActiveResultSets=true";
        public string JitWebData3ConnectionString
            = "Data Source = .\\SQLEXPRESS; Initial Catalog = JitWebData3; User ID = WorkWeeksql; Password = Jon23505#sql; MultipleActiveResultSets=true";

        //public string JitWeatherConnectionString
        //    = "Data Source=JITSQL02;Initial Catalog=Weather;User ID=WorkWeeksql;Password=Jon23505#sql; MultipleActiveResultSets=true";
        //public string JitWebData3ConnectionString
        //    = "Data Source=JITSQL02 ; Initial Catalog=JitWebData3 ; User ID=WorkWeeksql;  Password=Jon23505#sql ; MultipleActiveResultSets=true";
    }
}
//"MyConnectionString": "Data Source=WINDEV1805EVAL\\SQLEXPRESS ; Initial Catalog=Weather ; User ID=foo; Password=bar ; MultipleActiveResultSets=true",
//    "JitWebData3ConnectionString": "Data Source=JITSQL02 ; Initial Catalog=JitWebData3 ; User ID=WorkWeeksql;  Password=Jon23505#sql ; MultipleActiveResultSets=true",
//    "RealJitWeatherConnection": "Data Source=JITSQL02;Initial Catalog=Weather;User ID=WorkWeeksql;Password=Jon23505#sql; MultipleActiveResultSets=true"
//  }
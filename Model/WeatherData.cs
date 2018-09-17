using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Model
{
    public class WeatherData
    {
        public int WdID { get; set; }

        public string StationID { get; set; }

        public string ZipCode { get; set; }
        
        public string RDate { get; set; }

        public int? HighTmp { get; set; }

        public int? LowTmp { get; set; }

        public double? AvgTmp { get; set; }
    }
}

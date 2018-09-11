using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Dao
{
    public class ReadingsQueryResult
    {
        public int RdngID { get; set; }
        public int AccID { get; set; }
        public int UtilID { get; set; }
        public int UnitID { get; set; }
        public string Zip { get; set; }
        public DateTime DateStart { get; set; }
        public DateTime DateEnd { get; set; }
        public int Days { get; set; }
        public int? Units { get; set; }
        public decimal ExpUsage { get; set; }
        public double B1 { get; set; }
        public double B2 { get; set; }
        public int B3 { get; set; }
        public double B4 { get; set; }
        public int B5 { get; set; }
        public double? R2 { get; set; }
    }
}

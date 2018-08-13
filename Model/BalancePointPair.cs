using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Model
{
    public class BalancePointPair
    {
        public int HeatingBalancePoint { get; set; }
        public int CoolingBalancePoint { get; set; }
        public double CoolingDegreeDays { get; set; }
        public double HeatingDegreeDays { get; set; }
        public string ZipCode { get; set; }
        public int DaysInReading { get; set; }
        public int ActualUsage { get; set; }
        public decimal ExpUsage { get; set; }
        public int DaysInYear { get; set; }
    }
}

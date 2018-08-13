using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Dao
{
    public class WNRdngData
    {
        public int AccID { get; set; }
        public int UtilID { get; set; }
        public int MoID { get; set; }
        public int EMoID { get; set; }
        public int MoCt { get; set; }
        public DateTime DateStart { get; set; }
        public DateTime DateEnd { get; set; }
        public int WstID { get; set; }
        public string Zip { get; set; }
        public int UnitID { get; set; }
        public int Units { get; set; }
        public decimal Cost { get; set; }
    }
}

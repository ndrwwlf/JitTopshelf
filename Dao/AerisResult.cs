using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Dao
{
    public class AerisResult
    {
        public bool Success { get; set; }
        public object Error { get; set; }
        public IList<Response> Response { get; set; }
    }

    public class Response
    {
        public string Id { get; set; }
        public IList<Period> Periods { get; set; }
    }

    public class Temp
    {
        public int? MaxF { get; set; }
        public int? MinF { get; set; }
        public double? AvgF { get; set; }
    }

    public class Dewpt
    {
        public double? AvgF { get; set; }
    }

    public class Summary
    {
        public DateTime DateTimeISO { get; set; }
        public Temp Temp { get; set; }
        public Dewpt Dewpt { get; set; }
    }

    public class Period
    {
        public Summary Summary { get; set; }
    }
}

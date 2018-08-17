using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Model
{
    class BadWNRdngDataException: Exception
    {
        public BadWNRdngDataException(string message): base(message)
        {
            
        }
    }
}

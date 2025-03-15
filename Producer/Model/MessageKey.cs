using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producer.Model;

public class MessageKey
{
    public string PrimaryId { get; set; }
    public string SecondaryId { get; set; }
    public long Timestamp { get; set; }
}

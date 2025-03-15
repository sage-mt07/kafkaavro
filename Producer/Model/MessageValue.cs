using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producer.Model;

internal class MessageValue
{
    public string TransactionId { get; set; }
    public string ProductCode { get; set; }
    public decimal Amount { get; set; }  // Decimal型の使用
    public DateTimeOffset TransactionDate { get; set; } // DateTimeOffset型の使用
    public decimal UnitPrice { get; set; }
    public int Quantity { get; set; }
    public Dictionary<string, string> Metadata { get; set; }
}

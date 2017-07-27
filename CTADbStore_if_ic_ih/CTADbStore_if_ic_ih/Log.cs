using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace CTADbStore_if_ic_ih
{
    class Log
    {

        public static void AppendALLText(string contents)
        {
            File.AppendAllText(@"Log.txt", contents);
        }

        public static void AppendAllLines(IEnumerable<string> contents)
        {
            try
            {
                File.AppendAllLines(@"Log.txt", contents);
            }
            catch(Exception e)
            {
                Guid guid = Guid.NewGuid();
                File.AppendAllLines(@"Logs\Log" +guid.ToString()+ @".txt", contents);
            }

        }

        public static void AppendAllLines(string path, IEnumerable<string> contents)
        {
            try
            {
                File.AppendAllLines(path, contents);
            }
            catch (Exception e)
            {
                Guid guid = Guid.NewGuid();
                File.AppendAllLines(@"Logs\Log" + guid.ToString() + @".txt", contents);
            }

        }
    }
}

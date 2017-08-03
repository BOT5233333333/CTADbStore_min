using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
namespace DeleteUserlessLogs
{
    class Program
    {
        static void Main(string[] args)
        {
            DirectoryInfo dir = new DirectoryInfo(@"E:\ProgramWorkSpace\Git Lib\CTADbStore\BatchInsert\BatchInsert\bin\Release\Logs");
            foreach(var file in dir.GetFiles())
            {
                FileStream fs = file.OpenRead();
                StreamReader sr = new StreamReader(fs, Encoding.UTF8);
                string line = null;
                while((line = sr.ReadLine()) !=null)
                {
                    if(line.Contains("导入完成"))
                    {
                        fs.Close();
                        sr.Close();
                        file.Delete();
                        break;
                    }
                }
                fs.Close();
                sr.Close();
            }
        }
    }
}

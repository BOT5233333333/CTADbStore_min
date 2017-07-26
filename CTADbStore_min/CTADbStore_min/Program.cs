using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CTADbStore_min
{
    class Program
    {
        static void Main(string[] args)
        {
            string rootPath = @"G:\期货行情_导入";
            DirectoryInfo root = new DirectoryInfo(rootPath);

            AppHelper.numAllFiles = 0;
            foreach (var monthDir in root.GetDirectories())
            {
                foreach (var dir in monthDir.GetDirectories())
                {
                    AppHelper.numAllFiles += dir.GetFiles().Length;
                }
            }
            SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "CTAHisDBSPFT2016", AppHelper.numAllFiles);
            //SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "ctatest_deng", AppHelper.numAllFiles);
            mainFunc.MainFunc();
        }
    }
}

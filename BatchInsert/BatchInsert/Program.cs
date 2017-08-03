using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BatchInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            string rootPath = @"G:\CTA\2013\导入";
            DirectoryInfo root = new DirectoryInfo(rootPath);

            AppHelper.numAllFiles = 0;
            foreach(var monthDir in root.GetDirectories())
            {
                foreach(var dir in monthDir.GetDirectories())
                {
                    AppHelper.numAllFiles += dir.GetFiles().Length;
                }
            }
            SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "CTAHisDBSPFT2013", AppHelper.numAllFiles);
            //SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "ctatest_deng", AppHelper.numAllFiles);
            mainFunc.MainFunc();
            
        }
    }
}

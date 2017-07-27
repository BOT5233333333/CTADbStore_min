using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CTADbStore_Load
{
    class Program
    {
        static void Main(string[] args)
        {
            string rootPath = @"E:\期货行情_测试";
            DirectoryInfo root = new DirectoryInfo(rootPath);
            AppHelper.numAllFiles = 0;
            foreach (var monthDir in root.GetDirectories())
            {
                foreach(var dir in monthDir.GetDirectories())
                {
                    AppHelper.numAllFiles += dir.GetFiles().Length;
                }
            }

            SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, AppHelper.numAllFiles);
            //SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "STHisDBTick_deng", AppHelper.numAllFiles);
            mainFunc.MainFunc();
        }

    }
}

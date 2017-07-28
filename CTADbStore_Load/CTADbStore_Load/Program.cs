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
            //路径必须不能带有中文字符
            string rootPath = @"E:\Test";
            DirectoryInfo root = new DirectoryInfo(rootPath);
            AppHelper.numAllFiles = 0;
            foreach (var monthDir in root.GetDirectories())
            {
                foreach(var dir in monthDir.GetDirectories().Where(d => d.Name != "if" && d.Name != "tf" && d.Name != "t" && d.Name != "ic" && d.Name != "ih" && d.GetFiles().Length > 0))
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

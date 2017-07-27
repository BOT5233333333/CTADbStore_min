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
            //string rootPath = @"G:\期货行情_导入";
            //DirectoryInfo root = new DirectoryInfo(rootPath);

            //AppHelper.numAllFiles = 0;
            //foreach (var monthDir in root.GetDirectories())
            //{
            //    foreach (var dir in monthDir.GetDirectories())
            //    {
            //        AppHelper.numAllFiles += dir.GetFiles().Length;
            //    }
            //}
            //SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "CTAHisDBSPFT2016", AppHelper.numAllFiles);
            ////SPTxtToSqlClass mainFunc = new SPTxtToSqlClass(rootPath, "ctatest_deng", AppHelper.numAllFiles);
            //mainFunc.MainFunc();
            Console.WriteLine(DateTime.Now);
            string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=sthisdbtick_deng;CharSet=utf8";
            DBConnection conn = new DBConnection(connStr);
            string query = string.Format("LOAD DATA LOCAL INFILE '{0}' REPLACE INTO TABLE {1} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' ignore 1 lines (marketid,stockcode,tdatetime,lastpx,zbs,turnover ,volume,direction,b1,b2,b3,b4,b5 ,s1,s2,s3,s4,s5,bv1,bv2,bv3,bv4,bv5,sv1,sv2,sv3,sv4,sv5)"
                , "F:\\sh600111.csv"
                , "test_tbl");
            conn.executeSqlNoPrm(query);
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();
        }
    }
}

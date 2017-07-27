using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CTADbStore_Load_if_ic_ih
{
    class SPTxtToSqlClass
    {
        private string rootPath;
        private string dbName;
        private DBConnection conn;

        private int numAllFiles;
        private static int numFilesFinish;

        private string sqlOutPutPath = @"E:\sql_load.txt";

        //private string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=ctatest_deng;CharSet=utf8";
        private string connStr = "Server=192.168.2.181;User ID=root;Password=123456;Database=CTAHisDBSPFT;CharSet=utf8";
        public SPTxtToSqlClass(string rootPath, int numAllFiles)
        {
            this.rootPath = rootPath;
            this.dbName = "CTAHisDBSPFT";
            this.conn = new DBConnection(connStr);
            numFilesFinish = 0;
            this.numAllFiles = numAllFiles;
        }

        internal void MainFunc()
        {
            List<string> querys = new List<string>();
            string query = "";

            DirectoryInfo rootDir = new DirectoryInfo(this.rootPath);


            foreach (var monthDir in rootDir.GetDirectories())
            {
                List<Task<int>> tasks = new List<Task<int>>();
                Console.WriteLine("*************");
                Console.WriteLine("文件夹:" + monthDir.FullName + "开始");
                Console.WriteLine("时间:" + DateTime.Now);
                Console.WriteLine("*************");
                foreach (var dir in monthDir.GetDirectories()
                    .Where(d => (d.Name == "if" ||  d.Name == "ic" || d.Name == "ih") && d.GetFiles().Length > 0))
                {
                    foreach (var file in dir.GetFiles())
                    {
                        string tableName = "CTA_" + file.Name.Substring(0, 4 + dir.Name.Count()).ToUpper() + "_TBL";
                        TableProcess(tableName, this.dbName);
                        query = string.Format("LOAD DATA LOCAL INFILE '{0}' REPLACE INTO TABLE {1} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
                            + "(IFCD, TDATE ,TTIME ,UPDATEMILLISEC ,CP , CHG , CHGPCT , CQ ,CM, OC , "
                            + "S5 , S4 , S3 , S2 , S1, B1 , B2 , B3 , B4 , B5 ,"
                            + "SV5 , SV4 , SV3 , SV2 , SV1 , BV1 , BV2 , BV3 , BV4 , BV5 , BS , BSRATIO ,"
                            + "PRECLOSEPRC , OPENPRC , HP , LP , CLOSEPRC , UPPERLMTPRC , LOWERLMTPRC , "
                            + "TQ , TM , PREOPNINTRST , OPNINTRST , PRESTLMTPRC , STLMTPRC , PREDELTA , DELTA ,"
                            + "SETTLEGROUPID , SETTLEID , IFLXID , IFLXNAME, UNIX, MATKET);"
                               , file.FullName.Replace(@"\", @"\\")
                               , tableName);
                        querys.Add(query);
                        Console.WriteLine(string.Format("{0}/{1}", ++numFilesFinish, numAllFiles));
                    }
                }
                //@dummy
                Console.WriteLine("*************");
                Console.WriteLine("文件夹:" + monthDir.FullName + "结束");
                Console.WriteLine("时间:" + DateTime.Now);
                Console.WriteLine("*************");

                File.WriteAllLines(this.sqlOutPutPath, querys);
            }
        }

        private void TableProcess(string tableName, string dbName)
        {
            string SqlQryTbl = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA =" + "\"" + dbName + "\""
                    + " and TABLE_NAME = " + "\"" + tableName + "\"";
            Object res = this.conn.ExecuteScalar(SqlQryTbl);
            string SqlCreateTbl = "";
            if (res == null)
            {
                Console.WriteLine("创建表:" + tableName);
                SqlCreateTbl = "CREATE TABLE " + tableName + "(recordID INT NOT NULL auto_increment,IFCD NVARCHAR(6),TDATE NVARCHAR(8),"
                            + "TTIME NVARCHAR(6),UPDATEMILLISEC INT,CP DOUBLE, CHG DOUBLE, CHGPCT DOUBLE, CQ INT,CM DOUBLE, OC NVARCHAR(9), "
                            +"S5 DOUBLE, S4 DOUBLE, S3 DOUBLE, S2 DOUBLE, S1 DOUBLE, B1 DOUBLE, B2 DOUBLE, B3 DOUBLE, B4 DOUBLE, B5 DOUBLE,"
                            +"SV5 INT, SV4 INT, SV3 INT, SV2 INT, SV1 INT, BV1 INT, BV2 INT, BV3 INT, BV4 INT, BV5 INT, BS NVARCHAR(1), BSRATIO DOUBLE,"
                            + "PRECLOSEPRC DOUBLE, OPENPRC DOUBLE, HP DOUBLE, LP DOUBLE, CLOSEPRC DOUBLE, UPPERLMTPRC DOUBLE, LOWERLMTPRC DOUBLE, "
                            + "TQ INT, TM DOUBLE, PREOPNINTRST INT, OPNINTRST INT, PRESTLMTPRC DOUBLE, STLMTPRC DOUBLE, PREDELTA DOUBLE, DELTA DOUBLE,"
                            + "SETTLEGROUPID NVARCHAR(9), SETTLEID INT, IFLXID NVARCHAR(6), IFLXNAME NVARCHAR(8), UNIX BIGINT, MATKET CHAR(5)"
                            +", PRIMARY KEY (recordID)) ENGINE = InnoDB DEFAULT CHARSET = utf8; ";
            }

            this.conn.ExecuteNonQuery(SqlCreateTbl);
        }
    }


}

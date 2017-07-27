using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CTADbStore_Load
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
        private string connStr = "Server=192.168.2.181;User ID=root;Password=123456;Database=CTAHisDBSPFT2016;CharSet=utf8";
        public SPTxtToSqlClass(string rootPath, int numAllFiles)
        {
            this.rootPath = rootPath;
            this.dbName = "CTAHisDBSPFT2016";
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
                    .Where(d => d.Name != "if" && d.Name != "tf" && d.Name != "t" && d.Name != "ic" && d.Name != "ih" && d.GetFiles().Length > 0))
                {
                    foreach (var file in dir.GetFiles())
                    {
                        string tableName = "CTA_HSFT_" + file.Name.Substring(0, 11 + dir.Name.Count()).ToUpper() + "_TBL";
                        TableProcess(tableName, this.dbName);
                        query = string.Format("LOAD DATA LOCAL INFILE '{0}' REPLACE INTO TABLE {1} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' ignore 1 lines "
                            + "(contractid,contractname,tdatetime,lastpx,highpx,lowpx ,cq,tq,lastqty,initopenints,openints,intschg "
                            + ",turnover ,riselimit,falllimit,presettle,preclose,s1,b1,sv1,bv1,openpx,closepx,settlementpx,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,mflxid,"
                            + "s5,s4,s3,s2,b2,b3,b4,b5,sv5,sv4,sv3,sv2,bv2,bv3,bv4,bv5,predelta,currdelta,@dummy,@dummy,chg,chgpct,@dummy,@dummy)"
                               , file.FullName
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
                SqlCreateTbl = "CREATE TABLE " + tableName + "(recordID INT NOT NULL auto_increment,contractid CHAR(6),contractname CHAR(15),"
                            + "tdatetime DATETIME,lastpx DOUBLE,highpx DOUBLE,lowpx DOUBLE,cq INT,tq INT,lastqty INT,initopenints INT,openints INT,"
                            + "intschg INT,turnover DOUBLE,riselimit DOUBLE,falllimit DOUBLE,presettle DOUBLE,preclose DOUBLE,s1 DOUBLE,b1 DOUBLE,"
                            + "sv1 INT,bv1 INT,openpx DOUBLE,closepx DOUBLE,settlementpx DOUBLE,mflxid CHAR(10),s5 DOUBLE,s4 DOUBLE,s3 DOUBLE,"
                            + "s2 DOUBLE,b2 DOUBLE,b3 DOUBLE,b4 DOUBLE,b5 DOUBLE,sv5 INT,sv4 INT,sv3 INT,sv2 INT,bv2 INT,bv3 INT,bv4 INT,bv5 INT,"
                            + "predelta DOUBLE,currdelta DOUBLE,chg DOUBLE,chgpct DOUBLE,PRIMARY KEY (recordID)) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            }

            this.conn.ExecuteNonQuery(SqlCreateTbl);
        }
    }


}

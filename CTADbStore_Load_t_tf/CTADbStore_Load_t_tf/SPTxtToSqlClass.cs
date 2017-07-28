using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CTADbStore_Load_t_tf
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
        private string connStr = "Server=192.168.2.181;User ID=root;Password=123456;Database=CTAHisDBSPFT_tf;CharSet=utf8";
        public SPTxtToSqlClass(string rootPath, int numAllFiles)
        {
            this.dbName = "CTAHisDBSPFT_tf";
            //this.dbName = "ctatest_deng";
            this.rootPath = rootPath;
            
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
                    .Where(d => (d.Name == "tf" ||  d.Name == "t" ) && d.GetFiles().Length > 0))
                {
                    foreach (var file in dir.GetFiles())
                    {
                        string tableName = "CTA_HSFT_" + file.Name.Substring(0, 4 + dir.Name.Count()).ToUpper() + "_TBL";
                        TableProcess(tableName, this.dbName);
                        query = string.Format("LOAD DATA LOCAL INFILE '{0}' REPLACE INTO TABLE {1} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
                            + "(Symbol,Businestime,OpenPrice,Lastprice,Highprice,Lowprice,SettlePrice,PreSettle,ClosePrice,PreClose,Cq,Volume,Cm,Amount,PrePosition,Position,PositionChange,"
                            +"LimitUp,LimitDown,Side,OC,B01,B02,B03,B04,B05,S01,S02,S03,S04,S05,BV01,BV02,BV03,BV04,BV05,SV01,SV02,SV03,SV04,SV05,CurrDelta,PreDelta,SettlementGroupID"
                            +",SettlementID,`Change`,ChangeRatio,Continuesign,TradingDate,`LocalTime`,RecTime,ExchangeCode,ID,Unix);"
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
                SqlCreateTbl = "CREATE TABLE " + tableName + "(recordID INT NOT NULL auto_increment,Symbol VARCHAR(6), Businestime DATETIME(6)"
                + ",OpenPrice DOUBLE, Lastprice DOUBLE, Highprice DOUBLE,Lowprice DOUBLE,SettlePrice DOUBLE,PreSettle DOUBLE,ClosePrice DOUBLE"
                +",PreClose DOUBLE,Cq INT,Volume INT,Cm DOUBLE,Amount DOUBLE,PrePosition INT,Position INT,PositionChange INT,LimitUp DOUBLE"
                + ",LimitDown DOUBLE,Side INT,OC INT,B01 DOUBLE,B02 DOUBLE,B03 DOUBLE,B04 DOUBLE,B05 DOUBLE,S01 DOUBLE,S02 DOUBLE,S03 DOUBLE,S04 DOUBLE,S05 DOUBLE"
                + ",BV01 INT,BV02 INT,BV03 INT,BV04 INT,BV05 INT,SV01 INT,SV02 INT,SV03 INT,SV04 INT,SV05 INT,CurrDelta DOUBLE,PreDelta DOUBLE"
                + ",SettlementGroupID INT,SettlementID INT,`Change` DOUBLE,ChangeRatio DOUBLE,Continuesign INT,TradingDate DATETIME,`LocalTime` DATETIME"
                + ",RecTime DATETIME,ExchangeCode VARCHAR(5),ID INT,Unix BIGINT"
                + ", PRIMARY KEY (recordID)) ENGINE = InnoDB DEFAULT CHARSET = utf8; ";
            }

            this.conn.ExecuteNonQuery(SqlCreateTbl);
        }
    }

}

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BatchInsert
{
    class SPTxtToSqlClass
    {
        private string rootPath;
        private string dbName;
        private DBConnection conn;

        private int step; //一次同时导入几个月份
        private int numAllFiles;
        private static int numFilesFinish;

        //private string folderFinishRecordPath = @"E:\测试记录\文件夹记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        //private string fileFinishRecordPath = @"E:\测试记录\文件记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        private string folderFinishRecordPath = @"E:\期货行情数据导入完成记录\文件夹记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        private string fileFinishRecordPath = @"E:\期货行情数据导入完成记录\文件记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";

        //private string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=ctatest_deng;CharSet=utf8";
        private string connStr = "Server=192.168.2.180;User ID=root;Password=123456;Database=CTAHisDBSPFT2013;CharSet=utf8";
        public SPTxtToSqlClass(string rootPath, string dbName,int numAllFiles)
        {
            this.rootPath = rootPath;
            this.dbName = dbName;
            this.conn = new DBConnection(connStr);
            numFilesFinish = 0;
            this.numAllFiles = numAllFiles;
            this.step = 1;
        }

        public SPTxtToSqlClass(string rootPath, string dbName, int numAllFiles, int step)
        {
            this.rootPath = rootPath;
            this.dbName = dbName;
            this.conn = new DBConnection(connStr);
            numFilesFinish = 0;
            this.numAllFiles = numAllFiles;
            this.step = step;
        }

        internal void MainFunc()
        {
            try
            {
                DirectoryInfo rootDir = new DirectoryInfo(this.rootPath);

                //List<Task<int>> tasks = new List<Task<int>>();
                //int curStep = 1;
                foreach (var monthDir in rootDir.GetDirectories())
                {
                    List<Task<int>> tasks = new List<Task<int>>();
                    Console.WriteLine("*************");
                    Console.WriteLine("文件夹:" + monthDir.FullName + "开始");
                    Console.WriteLine("时间:" + DateTime.Now);
                    Console.WriteLine("*************");
                    foreach (var dir in monthDir.GetDirectories()
                        .Where(d=> d.Name!="if" && d.Name!="tf" && d.Name != "t" && d.Name != "ic" && d.Name != "ih" && d.GetFiles().Length>0) )
                    {
                        Task<int> t = new Task<int>(new Func<object, int>(DataProcessDir), dir, TaskCreationOptions.LongRunning);
                        t.Start();
                        tasks.Add(t);
                    }
                    Task.WaitAll(tasks.ToArray());
                    Log.AppendAllLines(this.folderFinishRecordPath, new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "导入完成:" + monthDir.FullName, "-----------------------------------" });
                    //if(curStep < this.step)
                    //{
                    //    ++curStep;
                    //}
                    //else
                    //{
                    //    curStep = 0;
                    //    Task.WaitAll(tasks.ToArray());
                    //    tasks.Clear();
                    //}
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("There is a exception." + e.Message);
                Log.AppendAllLines(new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "-----------------------------------" });
            }
        }

        private int DataProcessDir(object obj)
        {
            DirectoryInfo dir = (DirectoryInfo)obj;
            FileInfo[] files = dir.GetFiles();
            if (files.Length > 0)
            {
                foreach (var file in files)
                {
                    try
                    {
                        DataProcessFunc(file, file.Name, dir.Name);
                    }
                    catch (IndexOutOfRangeException e)
                    {
                        Console.WriteLine("There is a exception." + e.Message);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("There is a exception." + e.Message);
                        Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "文件:" + file.FullName, e.Message, "-----------------------------------" });
                    }

                }
            }
            return 0;
        }


        private void DataProcessFunc(FileInfo file, string fName, string dirName)
        {
            FileStream fs = file.Open(FileMode.Open, FileAccess.Read);
            StreamReader sr = new StreamReader(fs, Encoding.UTF8);

            string tableName = "CTA_HSFT_" + fName.Substring(0,11 + dirName.Count()).ToUpper() + "_TBL";
            Console.WriteLine("Table Name is: " + tableName);
            TableProcess(tableName);

            Console.WriteLine("--------");
            Console.WriteLine("开始导入该文件的数据：" + file.FullName);
            Console.WriteLine("--------");
            List<string> querys = new List<string>();
            string line = null;

            int count = 0;
            string query = InitSqlString(tableName);
            while((line = sr.ReadLine()) != null)
            {
                //line = line.Replace("None", "null");

                string[] tmpList = line.Split(',');
                if (tmpList.Length != 55) {

                    //Guid guid = Guid.NewGuid();
                    //Log.AppendAllLines(this.columnErrLogDir + "Log" + guid.ToString() + @".txt", new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(),"文件:"+file.FullName, "出错数据:" + line, "-----------------------------------" });
                    continue;
                }

                CTASPWordDefine DataUnit = new CTASPWordDefine();

                DataUnit.contractid = tmpList[0];
                // 合约代码
                DataUnit.contractname = tmpList[1];
                // 合约名称
                DataUnit.tdatetime = tmpList[2];
                // 日期时间
                DataUnit.lastpx = tmpList[3];
                // 最新价
                DataUnit.highpx = tmpList[4];
                // 最高价
                DataUnit.lowpx = tmpList[5];
                // 最低价
                DataUnit.cq = tmpList[6];
                // 瞬时成交量
                DataUnit.tq = tmpList[7];
                // 累计成交量
                DataUnit.lastqty = tmpList[8];
                // 最新成交量
                DataUnit.initopenints = tmpList[9];
                // 初始持仓量
                DataUnit.openints = tmpList[10];
                // 持仓量
                DataUnit.intschg = tmpList[11];
                // 持仓量变化
                DataUnit.turnover = tmpList[12];
                // 成交额
                DataUnit.riselimit = tmpList[13];
                // 涨停板
                DataUnit.falllimit = tmpList[14];
                // 跌停板
                DataUnit.presettle = tmpList[15];
                // 昨结算
                DataUnit.preclose = tmpList[16];
                // 昨收盘
                DataUnit.s1 = tmpList[17];
                // 卖一
                DataUnit.b1 = tmpList[18];
                // 买一
                DataUnit.sv1 = tmpList[19];
                // 卖量一
                DataUnit.bv1 = tmpList[20];
                // 买量一
                DataUnit.openpx = tmpList[21];
                // 开盘价
                DataUnit.closepx = tmpList[22];
                // 收盘价
                DataUnit.settlementpx = tmpList[23];
                // 结算价
                DataUnit.lifelow = tmpList[24];
                // 历史最低价
                DataUnit.lifehigh = tmpList[25];
                // 历史最高价
                DataUnit.avgpx = tmpList[26];
                // 当日均价
                DataUnit.bidimplyqty = tmpList[27];
                // 申买推导量
                DataUnit.askimplyqty = tmpList[28];
                // 申卖推导量
                DataUnit.side = tmpList[29];
                // 买卖方向
                DataUnit.mflxid = tmpList[30];
                // 连续合约代码
                DataUnit.s5 = tmpList[31];
                // 卖五
                DataUnit.s4 = tmpList[32];
                // 卖四
                DataUnit.s3 = tmpList[33];
                // 卖三
                DataUnit.s2 = tmpList[34];
                // 卖二
                DataUnit.b2 = tmpList[35];
                // 买二
                DataUnit.b3 = tmpList[36];
                // 买三
                DataUnit.b4 = tmpList[37];
                // 买四
                DataUnit.b5 = tmpList[38];
                // 买五
                DataUnit.sv5 = tmpList[39];
                // 卖量五
                DataUnit.sv4 = tmpList[40];
                // 卖量四
                DataUnit.sv3 = tmpList[41];
                // 卖量三
                DataUnit.sv2 = tmpList[42];
                // 卖量二  
                DataUnit.bv2 = tmpList[43];
                // 买量二
                DataUnit.bv3 = tmpList[44];
                // 买量三
                DataUnit.bv4 = tmpList[45];
                // 买量四
                DataUnit.bv5 = tmpList[46];
                // 买量五
                DataUnit.predelta = tmpList[47];
                // 昨虚实度
                DataUnit.currdelta = tmpList[48];
                // 今虚实度
                DataUnit.localtime = tmpList[49];
                // 本地时间
                DataUnit.market = tmpList[50];
                // 交易所
                DataUnit.chg = tmpList[51];
                // 涨跌
                DataUnit.chgpct = tmpList[52];
                // 涨跌幅
                DataUnit.varieties = tmpList[53];
                // 品种
                DataUnit.unix = 0;
                // 时间戳

                if(++count >= 200)
                {
                    query += SqlStringConcat(DataUnit);
                    querys.Add(query);
                    query = InitSqlString(tableName);
                    count = 0;
                }
                else
                {
                    query += SqlStringConcat(DataUnit) + ",";
                }

                //string excutSql = SqlStringConcat(DataUnit, tableName);
                //querys.Add(excutSql);
                //this.conn.executeSqlNoPrm(excutSql);
            }

            if(count != 0)
            {
                query = query.TrimEnd(',');
                querys.Add(query);
            }
            
            BatchInsert_MySql bm = new BatchInsert_MySql(this.connStr);
            bm.ExecuteSqlTran(querys);
            Console.WriteLine("-------------");
            Console.WriteLine("导入完成：" + file.FullName);
            Log.AppendAllLines(this.fileFinishRecordPath, new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "导入完成:" + file.FullName, "-----------------------------------" });
            ++numFilesFinish;
            Console.WriteLine("导入进度：" + ((double)numFilesFinish/(double)numAllFiles)*100 + "%");
            Console.WriteLine("-------------");
        }
        
        private string InitSqlString(string tableName)
        {
            return "INSERT INTO " + tableName
                 + "(contractid,contractname,tdatetime,lastpx,highpx,lowpx ,cq,tq,lastqty,initopenints,openints,intschg "
                 + ",turnover ,riselimit,falllimit,presettle,preclose,s1,b1,sv1,bv1,openpx,closepx,settlementpx,mflxid,"
                 + "s5,s4,s3,s2,b2,b3,b4,b5,sv5,sv4,sv3,sv2,bv2,bv3,bv4,bv5,predelta,currdelta,chg,chgpct)VALUES";
        }

        private string SqlStringConcat(CTASPWordDefine DataUnit)
        {
            return "(\""
                 + DataUnit.contractid + "\",\" " + DataUnit.contractname + "\",\"" + DataUnit.tdatetime + "\"," + DataUnit.lastpx
                 + "," + DataUnit.highpx + "," + DataUnit.lowpx + "," + DataUnit.cq + "," + DataUnit.tq + "," + DataUnit.lastqty
                 + "," + DataUnit.initopenints + "," + DataUnit.openints + "," + DataUnit.intschg + "," + DataUnit.turnover
                 + "," + DataUnit.riselimit + "," + DataUnit.falllimit + "," + DataUnit.presettle + "," + DataUnit.preclose
                 + "," + DataUnit.s1 + "," + DataUnit.b1 + "," + DataUnit.sv1 + "," + DataUnit.bv1 + "," + DataUnit.openpx
                 + "," + DataUnit.closepx + "," + DataUnit.settlementpx + ",\"" + DataUnit.mflxid + "\"," + DataUnit.s5 + "," + DataUnit.s4
                 + "," + DataUnit.s3 + "," + DataUnit.s2 + "," + DataUnit.b2 + "," + DataUnit.b3 + "," + DataUnit.b4 + "," + DataUnit.b5
                 + "," + DataUnit.sv5 + "," + DataUnit.sv4 + "," + DataUnit.sv3 + "," + DataUnit.sv2 + "," + DataUnit.bv2
                 + "," + DataUnit.bv3 + "," + DataUnit.bv4 + "," + DataUnit.bv5 + "," + DataUnit.predelta + "," + DataUnit.currdelta
                 + "," + DataUnit.chg + "," + DataUnit.chgpct + ")";
        }

        private string SqlStringConcat(CTASPWordDefine DataUnit, string tableName)
        {
            string ResStr = "INSERT INTO " + tableName
                 + "(contractid,contractname,tdatetime,lastpx,highpx,lowpx ,cq,tq,lastqty,initopenints,openints,intschg "
                 + ",turnover ,riselimit,falllimit,presettle,preclose,s1,b1,sv1,bv1,openpx,closepx,settlementpx,mflxid,"
                 + "s5,s4,s3,s2,b2,b3,b4,b5,sv5,sv4,sv3,sv2,bv2,bv3,bv4,bv5,predelta,currdelta,chg,chgpct)values(\""
                 + DataUnit.contractid + "\",\" " + DataUnit.contractname + "\",\"" + DataUnit.tdatetime + "\"," + DataUnit.lastpx
                 + "," + DataUnit.highpx + "," + DataUnit.lowpx + "," + DataUnit.cq + "," + DataUnit.tq + "," + DataUnit.lastqty
                 + "," + DataUnit.initopenints + "," + DataUnit.openints + "," + DataUnit.intschg + "," + DataUnit.turnover
                 + "," + DataUnit.riselimit + "," + DataUnit.falllimit + "," + DataUnit.presettle + "," + DataUnit.preclose
                 + "," + DataUnit.s1 + "," + DataUnit.b1 + "," + DataUnit.sv1 + "," + DataUnit.bv1 + "," + DataUnit.openpx
                 + "," + DataUnit.closepx + "," + DataUnit.settlementpx + ",\"" + DataUnit.mflxid + "\"," + DataUnit.s5 + "," + DataUnit.s4
                 + "," + DataUnit.s3 + "," + DataUnit.s2 + "," + DataUnit.b2 + "," + DataUnit.b3 + "," + DataUnit.b4 + "," + DataUnit.b5
                 + "," + DataUnit.sv5 + "," + DataUnit.sv4 + "," + DataUnit.sv3 + "," + DataUnit.sv2 + "," + DataUnit.bv2
                 + "," + DataUnit.bv3 + "," + DataUnit.bv4 + "," + DataUnit.bv5 + "," + DataUnit.predelta + "," + DataUnit.currdelta
                 + "," + DataUnit.chg + "," + DataUnit.chgpct + ")";
            //Console.WriteLine("ResStr is: " + ResStr);
            return ResStr;
        }

        private void TableProcess(string tableName)
        {
            string SqlQryTbl = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA =" + "\"" + this.dbName + "\"" 
                    +" and TABLE_NAME = " + "\"" + tableName + "\"";
            Object res = this.conn.ExecuteScalar(SqlQryTbl);
            string SqlCreateTbl = "";
            if (res == null)
            {
                Console.WriteLine("创建表:" + tableName);
                SqlCreateTbl = "CREATE TABLE " + tableName + "(recordID INT NOT NULL auto_increment,contractid CHAR(6),contractname CHAR(15),"
                            + "tdatetime DATETIME(3),lastpx DOUBLE,highpx DOUBLE,lowpx DOUBLE,cq INT,tq INT,lastqty INT,initopenints INT,openints INT,"
                            + "intschg INT,turnover DOUBLE,riselimit DOUBLE,falllimit DOUBLE,presettle DOUBLE,preclose DOUBLE,s1 DOUBLE,b1 DOUBLE,"
                            + "sv1 INT,bv1 INT,openpx DOUBLE,closepx DOUBLE,settlementpx DOUBLE,mflxid CHAR(10),s5 DOUBLE,s4 DOUBLE,s3 DOUBLE,"
                            + "s2 DOUBLE,b2 DOUBLE,b3 DOUBLE,b4 DOUBLE,b5 DOUBLE,sv5 INT,sv4 INT,sv3 INT,sv2 INT,bv2 INT,bv3 INT,bv4 INT,bv5 INT,"
                            + "predelta DOUBLE,currdelta DOUBLE,chg DOUBLE,chgpct DOUBLE,PRIMARY KEY (recordID)) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            }
            
            //Console.WriteLine("SqlCreateTbl is: " + SqlCreateTbl);
            this.conn.executeSqlNoPrm(SqlCreateTbl);
        }
    }
}

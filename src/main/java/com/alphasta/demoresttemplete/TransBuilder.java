package com.alphasta.demoresttemplete;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

/**
 * 转换测试类
 * .
 * @author gsj
 */
public class TransBuilder
{
    public static final String[] databasesXML = {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<connection>\n" +
        "    <name>target</name>\n" +
        "    <server>192.168.16.187</server>\n" +
        "    <type>MYSQL</type>\n" +
        "    <access>Native</access>\n" +
        "    <database>test2</database>\n" +
        "    <port>3306</port>\n" +
        "    <username>root</username>\n" +
        "    <password>Encrypted 2be98afc86aa7f2e4cb79ce10cc9da0ce</password>\n" +
        "    <servername />\n" +
        "    <data_tablespace />\n" +
        "    <index_tablespace />\n" +
        "    <attributes>\n" +
        "      <attribute>\n" +
        "        <code>FORCE_IDENTIFIERS_TO_LOWERCASE</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>FORCE_IDENTIFIERS_TO_UPPERCASE</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>IS_CLUSTERED</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>PORT_NUMBER</code>\n" +
        "        <attribute>3306</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>PRESERVE_RESERVED_WORD_CASE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>QUOTE_ALL_FIELDS</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>STREAM_RESULTS</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>SUPPORTS_BOOLEAN_DATA_TYPE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>SUPPORTS_TIMESTAMP_DATA_TYPE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>USE_POOLING</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "    </attributes>\n" +
        "  </connection>",
          
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<connection>\n" +
        "    <name>source</name>\n" +
        "    <server>192.168.16.187</server>\n" +
        "    <type>MYSQL</type>\n" +
        "    <access>Native</access>\n" +
        "    <database>test1</database>\n" +
        "    <port>3306</port>\n" +
        "    <username>root</username>\n" +
        "    <password>Encrypted 2be98afc86aa7f2e4cb79ce10cc9da0ce</password>\n" +
        "    <servername />\n" +
        "    <data_tablespace />\n" +
        "    <index_tablespace />\n" +
        "    <attributes>\n" +
        "      <attribute>\n" +
        "        <code>FORCE_IDENTIFIERS_TO_LOWERCASE</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>FORCE_IDENTIFIERS_TO_UPPERCASE</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>IS_CLUSTERED</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>PORT_NUMBER</code>\n" +
        "        <attribute>3306</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>PRESERVE_RESERVED_WORD_CASE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>QUOTE_ALL_FIELDS</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>STREAM_RESULTS</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>SUPPORTS_BOOLEAN_DATA_TYPE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>SUPPORTS_TIMESTAMP_DATA_TYPE</code>\n" +
        "        <attribute>Y</attribute>\n" +
        "      </attribute>\n" +
        "      <attribute>\n" +
        "        <code>USE_POOLING</code>\n" +
        "        <attribute>N</attribute>\n" +
        "      </attribute>\n" +
        "    </attributes>\n" +
        "  </connection>"
    };

    /**
     * Creates a new Transformation using input parameters such as the tablename to read from.
     * 创建一个新的转换对象
     * @param transformationName 转换的名称
     * @param sourceDatabaseName 源数据库的名称
     * @param sourceTableName 源表的名称
     * @param sourceFields 源表的字段
     * @param targetDatabaseName 目标数据库的名称
     * @param targetTableName 目标表的名称
     * @param targetFields 目标表的字段
     * @return 新的转换对象
     * @throws KettleException 异常信息
     */
    public static final TransMeta buildCopyTable(String transformationName, String sourceDatabaseName, String sourceTableName, String[] sourceFields, String targetDatabaseName, String targetTableName, String[] targetFields) throws KettleException
    {
        // 环境初始化
        EnvUtil.environmentInit();
        try
        {
            //
            // 创建一个新的转换...
            //
            TransMeta transMeta = new TransMeta();
            transMeta.setName(transformationName);
            
            // 添加数据库连接
            for (int i=0;i<databasesXML.length;i++)
            {
                DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[i]);
                transMeta.addDatabase(databaseMeta);
            }
            DatabaseMeta sourceDBInfo = transMeta.findDatabase(sourceDatabaseName);
            DatabaseMeta targetDBInfo = transMeta.findDatabase(targetDatabaseName);

            
            //
            // 添加一个注释
            //
            String note = "Reads information from table [" + sourceTableName+ "] on database [" + sourceDBInfo + "]" + Const.CR;
            note += "After that, it writes the information to table [" + targetTableName + "] on database [" + targetDBInfo + "]";
            NotePadMeta ni = new NotePadMeta(note, 150, 10, -1, -1);
            transMeta.addNote(ni);

            // 
            // 创建源 Step ...
            //
            String fromstepname = "read from [" + sourceTableName + "]";
            TableInputMeta tii = new TableInputMeta();
            tii.setDatabaseMeta(sourceDBInfo);
            String selectSQL = "SELECT "+ Const.CR;
            for (int i=0;i<sourceFields.length;i++)
            {
                if (i>0) selectSQL+=", "; else selectSQL+="  ";
                selectSQL+=sourceFields[i]+Const.CR;
            }
            selectSQL+="FROM "+sourceTableName;
            tii.setSQL(selectSQL);

            // TODO: StepLoader变化
            // tepLoader steploader = StepLoader.getInstance();
            // String fromstepid = steploader.getStepPluginID(tii);
            // 变为如下方式：
            PluginRegistry registry = PluginRegistry.getInstance();
            String fromstepid = registry.getPluginId(tii);

            // TODO： StepMeta 变化
            // StepMeta fromstep = new StepMeta(log, fromstepid, fromstepname, (StepMetaInterface) tii);
            // 变为如下：
            StepMeta fromstep = new StepMeta(fromstepid, fromstepname, (StepMetaInterface) tii);

            fromstep.setLocation(150, 100);
            fromstep.setDraw(true);
            fromstep.setDescription("Reads information from table [" + sourceTableName + "] on database [" + sourceDBInfo + "]");
            transMeta.addStep(fromstep);

            //
            // 添加一个字段重命名 逻辑 Step
            // Use metadata logic in SelectValues, use SelectValueInfo...
            //
            SelectValuesMeta svi = new SelectValuesMeta();
            svi.allocate(0, 0, sourceFields.length);
            // FIXME: 字段名称复制，这块有点问题
//            for (int i = 0; i < sourceFields.length; i++)
//            {
//                svi.getMetaName()[i] = sourceFields[i];
//                svi.getMetaRename()[i] = targetFields[i];
//            }
            svi.setSelectName(sourceFields);
            svi.setSelectRename(targetFields);

            String selstepname = "Rename field names";
            String selstepid = registry.getPluginId(svi);
            StepMeta selstep = new StepMeta(selstepid, selstepname, (StepMetaInterface) svi);

            selstep.setLocation(350, 100);
            selstep.setDraw(true);
            selstep.setDescription("Rename field names");
            transMeta.addStep(selstep);

            TransHopMeta shi = new TransHopMeta(fromstep, selstep);
            transMeta.addTransHop(shi);
            fromstep = selstep;

            // 
            // 创建目标 Step...
            //
            //
            // 添加 TableOutputMeta step...
            //
            String tostepname = "write to [" + targetTableName + "]";
            TableOutputMeta toi = new TableOutputMeta();
            toi.setDatabaseMeta(targetDBInfo);
            toi.setTableName(targetTableName);

            toi.setCommitSize(200);
            toi.setTruncateTable(true);
            String tostepid = registry.getPluginId(toi);
            StepMeta tostep = new StepMeta(tostepid, tostepname, (StepMetaInterface) toi);

            tostep.setLocation(550, 100);
            tostep.setDraw(true);
            tostep.setDescription("Write information to table [" + targetTableName + "] on database [" + targetDBInfo + "]");
            transMeta.addStep(tostep);

            //
            // 在两个step之间添加一个hop...
            //
            TransHopMeta hi = new TransHopMeta(fromstep, tostep);
            transMeta.addTransHop(hi);

            // 返回转换对象.
            return transMeta;
        }
        catch (Exception e)
        {
            throw new KettleException("An unexpected error occurred creating the new transformation", e);
        }
    }

    /**
     * 1) 创建一个新的转换
     * 2) 保存转换为 XML 文件
     * 3) 生成创建目标表的 sql
     * 4) 执行转换
     * 5) 丢弃目标表，便于重复测试
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        // 环境初始化 - 注册插件
        KettleEnvironment.init();

        // 环境初始化 ??
    	EnvUtil.environmentInit();

        // 转换的参数
        String fileName = "NewTrans.xml";
        String transformationName = "Test Transformation";

        // 源参数
        String sourceDatabaseName = "source";
        String sourceTableName = "Customer";
        String sourceFields[] = { 
                "customernr",
                "Name",
                "firstname",
                "lang",
                "sex",
                "street",
                "housnr",
                "bus",
                "zipcode",
                "location",
                "country",
                "date_of_birth"
            };

        // 目标参数
        String targetDatabaseName = "target";
        String targetTableName = "Cust";
        String targetFields[] = { 
                "CustNo",
                "LastName",
                "FirstName",
                "Lang",
                "gender",
                "Street",
                "Housno",
                "busno",
                "ZipCode",
                "City",
                "Country",
                "BirthDate"
            };

        // 创建转换
        TransMeta transMeta = TransBuilder.buildCopyTable(
                transformationName,
                sourceDatabaseName,
                sourceTableName,
                sourceFields,
                targetDatabaseName,
                targetTableName,
                targetFields
                );
        
        // 保存转换为文件
        String xml = transMeta.getXML();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(fileName)));
        dos.write(xml.getBytes("UTF-8"));
        dos.close();
        System.out.println("Saved transformation to file: "+fileName);

        // 获取我们需要生成目标标的sql
        String sql = transMeta.getSQLStatementsString();
        
        // 在目标表中执行sql语句:
        Database targetDatabase = new Database(transMeta.findDatabase(targetDatabaseName));
        targetDatabase.connect();
        targetDatabase.execStatements(sql);
        
        // 现在执行转换...
        Trans trans = new Trans(transMeta);
        trans.execute(null);
        trans.waitUntilFinished();
        
        // 为了便于重复测试，我们删除目标表
        targetDatabase.execStatement("drop table "+targetTableName);

        // 断开目标数据库连接
        targetDatabase.disconnect();
    }


}

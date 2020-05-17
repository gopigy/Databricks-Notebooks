# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Flow:
# MAGIC     
# MAGIC       This notebook reads all the .xml files from a folder in ADLS. 
# MAGIC       Parse xml at session tags and creates multiple dataframes. 
# MAGIC       These dataframes are writen as delimited files into ADLS landing for further processing.

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Connection to XML source
#Connect to the data source
#Inputs
inputLoc=dbutils.widgets.get("inputLoc")
outputLoc=dbutils.widgets.get("outputLoc")
snapshot_year_month = dbutils.widgets.get("snapshot_year_month")
snapshot_day = dbutils.widgets.get("snapshot_day")
logPath = dbutils.widgets.get("logpath")
archivePath = dbutils.widgets.get("archivepath")
#------------------------------------------------------------
xmlFileName='*.xml'
logFileName ='parsing_count_0.log'
opdelimeter = "\u0001"
outputCodec ="none"
vhash="md5(concat_ws('',"
#-----------------------------------------------------------------------------
xmlFileName=inputLoc+xmlFileName
dlypath= '/snapshot_year_month='+ snapshot_year_month +'/snapshot_day='+ snapshot_day + "/"

# COMMAND ----------

# DBTITLE 1,Define Schema
# MAGIC %run ./defineSchema

# COMMAND ----------

logLines = []

def log(msg):  
  logLines.append(msg)

def writeLog(file, yearmonth, day ):
  #outFile = file + "landing_count_0.log"
  outFile = file + "/" + yearmonth + "/" + day + "/" + "landing_count_0.log"  
  outS = ""
  for log in logLines:
    outS += log + "\n"
  dbutils.fs.put(outFile, outS, True)

# COMMAND ----------

# DBTITLE 1,Session Tag
##Create DF with all the .xml filenames in the folder
#Failing when the folder is missing
dfAll=spark.createDataFrame(dbutils.fs.ls(inputLoc))
dfAll=dfAll.select(col('path').alias('all_file_path'),col('name').alias('file_name')).where(instr(dfAll.name,'.xml') > 0 )

#Create DF for at session tag for all files in the folder
dfXML = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='root').load(xmlFileName,schema=rootSchema)
print('Dataframe created with all XML files')
dfXML=dfXML.withColumn('file_path', input_file_name())

dfXML=dfXML.join(dfAll, dfXML.file_path == dfAll.all_file_path, 'outer').select('*').drop('all_file_path','file_path')

#Session & Transaction ID
#sid =[col('entry.session._id').alias('session_id'),col('entry.session.data.policy.CurrentTransactionID').alias('current_transaction_id')]
sid =[col('entry.session._id').alias('session_id'),col('entry.session.data.policy.CurrentTransactionID').alias('current_transaction_id'),
      col('entry._type').alias('type'),col('entry._transGroup').alias('transgroup'),col('entry._index').alias('index')]

dfXMLSn=dfXML.select(explode('entry').alias("entry"),'file_name')

clist=['rtrim(entry.session._id) as session_id','entry.session.data.policy.CurrentTransactionID as current_transaction_id',
       'entry._type as type','entry._transGroup as transgroup','entry._index as index',
        'entry.session._clientId as clientid','file_name', 'entry._transHistoryId as transHistoryId']

dfSn=dfXMLSn.selectExpr(clist)

vspk=vhash+"session_id,current_transaction_id,type,transgroup,index)) as session_pk"

clist1=[vspk,"session_id",'current_transaction_id','clientid','type','transgroup','index','file_name as xml_filename',
        "case when nullif(session_id,'') is NULL then 'Missing Session XPATH' else '' end as err_desc", 'transHistoryId'] 

clist2=['session_pk', 'session_id','current_transaction_id', 'clientid', 'type','transgroup','index','xml_filename', 'err_desc', 'err_ind', 'transHistoryId']

err_ind = when((col('err_desc') == ''), 'N').otherwise('Y')

dfSn=dfSn.selectExpr(clist1).withColumn('err_ind',err_ind).selectExpr(clist2)

print('Dataframe created for session')

# COMMAND ----------

# DBTITLE 1,InstallmentSchedule Tag
#Billing Installments
#InstallmentSchedule_Installment

clist=['session_id','policynumber','ins._PolicyTermId as policytermid',
        'ins._BillItemId as billitemid','ins._DetailId as detailid',
        'ins._TransactionDate as transactiondate','ins._DueDate as duedate',
        "ifnull(nullif(ins._ItemAmount,''),'0') as itemamount",'installmentdate',
        "ifnull(nullif(ins._InstallmentAmount,''),'0') as installmentamount",
        "'' as servicechargeamount",
        'ins._TransactionTypeCode as transactiontypecode',
        'ins._ReceivableTypeCode as receivabletypecode',
        'ins._DetailTypeCode as detailtypecode',
        'file_name as xml_filename',
        'ins._ItemEffectiveDate as itemeffectivedate',
        'ins._ClosedToCash as closedtocash',
        'ins._ClosedToCredit as closedtocredit',
        'ins._ClosedWriteOff as closedwritedff',
        'ins._ClosedRedistributed as closedredistributed',
        'ins._Balance as balance',
        "'' as err_desc",'historyid',
        'ins._InstallmentTypeCode as installmenttypecode', 
        'ins._InvoiceStatus as InvoiceStatus',
        'ins._ScheduleItemLevelTypeCode as ScheduleItemLevelTypeCode',
        'index as index_id']

clist2=['session_id','policynumber','policytermid','billitemid','detailid','transactiondate','duedate','itemamount','installmentdate',
        'installmentamount','servicechargeamount','transactiontypecode','receivabletypecode','detailtypecode','xml_filename','itemeffectivedate',
        'closedtocash','closedtocredit','closedwritedff','closedredistributed','balance','err_desc','err_ind','historyid', 'installmenttypecode', 
        'InvoiceStatus', 'ScheduleItemLevelTypeCode','index_id']

colList=['transactiondate','duedate','itemamount','installmentdate','installmentamount','itemeffectivedate','closedtocash','closedtocredit','closedwritedff','closedredistributed','balance']
typList=['date','date','float','date','float','date','float','float','float','float','float']

dfinsStg=colListtypchk(dfXML
       .select('InstallmentSchedule',explode('entry').alias("entry"),'file_name')
       .select('InstallmentSchedule',col('entry.session._id').alias('session_id'),'entry.session.data.policy.PolicyNumber',
               col('entry.session.data.policy.paymentInformation.downpayment').alias('balance'),
               col('entry._index').alias('index'),'file_name').distinct()
       .select('session_id','PolicyNumber', 'balance','index', explode('InstallmentSchedule').alias("insSch"),'file_name')
       .select('session_id','PolicyNumber', 'balance','index', explode('insSch.InstallmentDate').alias("ins_dt"),
               'file_name',col('insSch._historyId').alias('historyid'))
       .select('session_id','PolicyNumber', 'balance','index', col('ins_dt._InstallmentDate').alias('installmentdate'),
                     explode('ins_dt.SubGroup').alias("sub"),'file_name','historyid')
       .select('session_id','PolicyNumber', 'balance', 'index',
               'installmentdate',explode('sub.Installment').alias("ins"),'file_name','historyid')
       .selectExpr(clist),colList,typList).withColumn('err_ind',err_ind).select(clist2)
print('Dataframe created for installmentschedule_installment')

# COMMAND ----------

# DBTITLE 1,Data Tag
dfXMLSn.cache()
did =[col('entry.session.data._id').alias("data_id")]
#Account
vTag='entry.session.data.account'
clist =  sid + did +[col(vTag+'._id').alias("account_id"),
                      col(vTag+'.FEIN._VALUE').alias('fein'),
                      col(vTag+'.SSN._VALUE').alias('ssn'),
                      vTag+'.primaryphone']

vkey="session_id,current_transaction_id,type,transgroup,index,data_id,account_id"
vpk=vkey+")) as account_pk"

clist2 = [vhash+vpk,vspk,'session_id','current_transaction_id','data_id','account_id','fein','ssn','primaryphone',
      "concat(case when nullif(account_id,'') is NULL then 'Missing Session/data/account XPATH | ' else '' end,\
       case when nullif(FEIN,'') is NULL and nullif(SSN,'') is NULL then 'Missing FED_TAX_ID | ' else '' end) as err_desc"]
#dval ={'SSN':'NO-SSN','FEIN':''}
dfAccnt=dfXMLSn.select(clist).selectExpr(clist2).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_account')
#--------------------------------------------------------------------
#Account Address
vTag='entry.session.data.account.address'
vpk=vkey+",address_id)) as address_pk"
vfk=vkey+")) as account_pk"
clist= sid + did + [col('entry.session.data.account._id').alias("account_id"),
                col(vTag+'._id').alias("address_id"),                               
                vTag+'.address1',
                vTag+'.address2',
                vTag+'.city',
                vTag+'.county',
                vTag+'.state',
                vTag+'.zipcode']
clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','account_id','address_id','address1','address2','city','county','state','zipcode',
        "'' as err_desc"]
dfAccntAddr=dfXMLSn.select(clist).selectExpr(clist2).withColumn('err_ind',err_ind) 
print('Dataframe created for session_data_account_address')
#--------------------------------------------------------------------
#Account Location Address

clist=sid + did +[col('entry.session.data.account._id').alias("account_id"),
               explode('entry.session.data.account.location').alias("loc")]

vpk=vkey+",loc._id,loc.address._id)) as laddress_pk"
vfk=vkey+")) as account_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','account_id','loc._id as location_id',
        'loc.address._id as address_id','loc.address.address1 as address1','loc.address.address2 as address2','loc.address.city as city',
        'loc.address.county as county','loc.address.state as state','loc.address.zipcode as zipcode',
        "ifnull(nullif(loc.Number,''),'0') as location_number",'loc.MaximumNumberOfEmployeesAtOneTime as maximumnumberofemployeesatonetime',
        'loc.Description as description',"'' as err_desc",'loc._deleted as deleted_ind']  

clist3=['laddress_pk','account_pk','session_pk','session_id','current_transaction_id','data_id','account_id','location_id',
         'address_id','address1','address2','city',
        'county','state','zipcode','location_number','maximumnumberofemployeesatonetime',
        'description','err_desc','err_ind','deleted_ind'] 

dfAccntLocAddr=coltypchk(dfXMLSn.select(clist).selectExpr(clist2),'location_number','int').withColumn('err_ind',err_ind).selectExpr(clist3)
print('Dataframe created for session_data_account_location_address')

# COMMAND ----------

# DBTITLE 1,Policy Tag
#Policy
vTag='entry.session.data.policy'
pid=[col(vTag+'._id').alias("pol_node_id")]
clist=sid + did + pid + [vTag+'.CurrentTransactionID',
                      vTag+'.EffectiveDate',
                      vTag+'.ExpirationDate',
                      vTag+'.InsuredName',
                      vTag+'.PolicyNumber',
                      vTag+'.PremiumChange',
                      vTag+'.PurePremiumChange',
                      vTag+'.SICCode',
                      vTag+'.Status',
                      vTag+'.TaxesSurchargesChange',
                      vTag+'.DBA',
                      vTag+'._PolicyId',
                      vTag+'.PrimaryRatingState',
                      vTag+'.NAICSCode',
                      vTag+'.QuoteNumber']

vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"

clist2 = [vhash+vpk,vspk,'session_id','data_id','pol_node_id',
          'case when _PolicyId is null then session_id else _PolicyId end as policyid',
          'currenttransactionid as current_transaction_id','effectivedate','expirationdate','insuredname','policynumber',
          "ifnull(nullif(premiumchange,''),'0') as premiumchange",
          "ifnull(nullif(purepremiumchange,''),'0') as purepremiumchange",
          'siccode','status',
          "ifnull(nullif(taxessurchargeschange,''),'0') as taxessurchargeschange",
          'dba','primaryratingstate','naicscode',"QuoteNumber as quote_number",
      "concat(case when nullif(pol_node_id,'') is NULL then 'Missing Session/data/policy XPATH | ' else '' end,\
     case when nullif(PolicyNumber,'') is NULL then 'Missing POL_NBR | ' else '' end,\
     case when nullif(EffectiveDate,'') is NULL then 'Missing POL_EFF_DT | ' else '' end,\
     case when nullif(ExpirationDate,'') is NULL then 'Missing POL_EXPI_DT | ' else '' end) as err_desc"]

colList=['EffectiveDate','ExpirationDate','PremiumChange','PurePremiumChange','TaxesSurchargesChange']
typList=['date','date','float','float','float']

clist3=['policy_pk','session_pk','session_id','data_id','pol_node_id','policyid','current_transaction_id','effectivedate','expirationdate',
        'insuredname','policynumber','premiumchange','purepremiumchange','siccode','status','taxessurchargeschange','dba','primaryratingstate',
        'err_desc','err_ind','naicscode', 'quote_number']

dfPol=colListtypchk(dfXMLSn.select(clist).selectExpr(clist2),colList,typList).withColumn('err_ind',err_ind).select(clist3)
print('Dataframe created for session_data_policy')
#--------------------------------------------------------------------
#Billing Payment Information
#session_data_policy_paymentInformation
vTag='entry.session.data.policy.paymentInformation'
clist=sid + did + pid + [col(vTag+'._id').alias("payment_info_id"),
                      vTag+'.DownPayment',
                      vTag+'.PaymentPlan']

vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,payment_info_id)) as payment_info_pk"
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','data_id','pol_node_id','payment_info_id',
        "ifnull(nullif(downpayment,''),'0') as downpayment",
        'paymentplan','current_transaction_id',"'' as err_desc"]

dfPolPymtInfo=coltypchk(dfXMLSn.select(clist).selectExpr(clist2),'downpayment','float').withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_paymentinformation')

# COMMAND ----------

# DBTITLE 1,policyAdmin Tag
#Policy Admin Transactions
vTag='entry.session.data.policyAdmin.transactions.transaction'
clist=sid + did + [col('entry.session.data.policyAdmin._id').alias("policyadmin_id"),
                   col('entry.session.data.policyAdmin.transactions._id').alias("transactions_id"),
                   col(vTag+'._id').alias("transaction_id"),
                   explode(vTag).alias('tran')]

vpk="session_id,current_transaction_id,type,transgroup,index,data_id,policyadmin_id,transactions_id,tran._id)) as transaction_pk"

clist2 = [vhash+vpk,vspk,'session_id','current_transaction_id','data_id','policyadmin_id','transactions_id',
          'tran._id as transaction_id','tran.type','tran.cancellationdate','tran.effectivedate',
          'tran.expirationdate','tran.reasons.reason.Code as reason_cd','tran.historyid','tran.OriginalID','tran.HistoryIDOriginal','tran.IssuedDate',
    "concat(case when nullif(policyAdmin_id,'') is NULL then 'Missing Session/data/policyAdmin XPATH | ' else '' end,\
     case when nullif(tran.type,'') is NULL then 'Missing TRANS_TYP_CD | ' else '' end,\
     case when nullif(tran.effectivedate,'') is NULL then 'Missing TRAN_EFF_DT | ' else '' end,\
     case when nullif(tran.expirationdate,'') is NULL then 'Missing TRAN_EXPI_DT | ' else '' end) as err_desc"]

dfPolTran=dfXMLSn.select(clist).selectExpr(clist2).withColumn('err_ind',err_ind)

clist = ['transaction_pk','session_pk','session_id','current_transaction_id','data_id','policyadmin_id','transactions_id',
          'type','cancellationdate','effectivedate','expirationdate','reason_cd','historyid','err_desc','err_ind', 'originalid', 'historyidoriginal', 'issueddate']

dfPolTran=dfPolTran.select(clist).where(dfPolTran.current_transaction_id==dfPolTran.transaction_id)
print('Dataframe created for session_data_policyadmin')

# COMMAND ----------

# DBTITLE 1,Line Tag
#WC  Line
#session_data_policy_line
vTag='entry.session.data.policy.line'
clist=sid + did + pid + [explode(vTag).alias('line'),
    col('entry.session.data.policy.PurePremiumValues._change').alias('totalpurepremium_change'),
    col('entry.session.data.policy.PrimaryRatingState').alias('PrimaryRatingState')]

dfLineStg=dfXMLSn.select(clist)

dfLineStg=dfLineStg.where(dfLineStg.line.type =='WorkersCompensation')

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id)) as line_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id',
       'line._id as line_id','line.type',
       "ifnull(nullif(line.minimumpremium,''),'0') as minimumpremium",
       "ifnull(nullif(totalpurepremium_change,''),'0') as totalpurepremium_change",
       "ifnull(nullif(line.TaxesSurchargesValues._change,''),'0') as tfs_change",
       "'' as err_desc"]

colList=['MinimumPremium','totalpurepremium_change','tfs_change']
typList=['float','float','float']

dfLine=colListtypchk(dfLineStg.selectExpr(clist),colList,typList).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line')
#--------------------------------------------------------------------
#WC ExperienceRating
#session_data_policy_line_lineexperiencerating
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id)) as line_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id,line.lineExperienceRating._id)) as line_experience_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id',
       'line._id as line_id','line.lineExperienceRating._id as line_experience_id',
       "ifnull(nullif(line.lineExperienceRating.experiencemodificationfactor,''),'0') as experiencemodificationfactor",
       "'' as err_desc"]
dfExperienceRating=coltypchk(dfLineStg.selectExpr(clist).where(dfLineStg.line.lineExperienceRating._id !=''),'experiencemodificationfactor','float').withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_lineexperiencerating')
#--------------------------------------------------------------------
#Policy Limit
#session_data_policy_line_limit
vTag='line.limit'
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id',col('line._id').alias('line_id'),explode(vTag).alias('limit')]

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id)) as line_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,limit._id)) as limit_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id',
        'limit._id as limit_id','limit.type','limit.sValue as pol_limit',"'' as err_desc"]
dfLineLimit=dfLineStg.select(clist).selectExpr(clist2).withColumn('err_ind',err_ind)

dfLineLimit=dfLineLimit.where(dfLineLimit.type=='Policy')
print('Dataframe created for session_data_policy_line_limit')
#--------------------------------------------------------------------
#Line State
#session_data_policy_line_linestate
vTag='line.linestate'

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id)) as line_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id,line.linestate._id)) as linestate_pk"


clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line._id as line_id',
       vTag+'._id as linestate_id',
       "ifnull(nullif("+vTag+".PurePremiumValues._change,''),'0') as linestate_premium_change",
       "ifnull(nullif("+vTag+".TaxesSurchargesValues._change,''),'0') as linestate_tfs_change",
       "'' as err_desc"]

colList=['linestate_premium_change','linestate_tfs_change']
typList=['float','float']

dfLineSt=colListtypchk(dfLineStg.selectExpr(clist).where(
  dfLineStg.line.linestate._id !=''),colList,typList).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_linestate')

#--------------------------------------------------------------------
#StateTaxesSurcharges
vTag='line.linestate.stateTaxSurcharge'

vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,sTax._id)) as tfs_pk"
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"

clist=['session_id', 'current_transaction_id','type','transgroup', 'index', 'data_id','pol_node_id', 'line'] + [explode(vTag).alias('sTax')]

clist1=[vhash+vpk, vhash+vfk, vspk, 'session_id', 'current_transaction_id','data_id','pol_node_id', 
        'line._id as line_id', 'line.linestate._id as linestate_id', 
        "'' as state",'sTax.type as type',
        "ifnull(nullif(sTax.statCode.sValue,''),'') as stat_cd",'sTax._id as tfs_id',
        "ifnull(nullif(sTax.change,''),'0') as change",
        "ifnull(nullif(sTax.rate,''),'0') as rate","'' as err_desc",'line']

clist2=[ 'tfs_pk','policy_pk','session_id','current_transaction_id','data_id','pol_node_id',
        'tfs_id','type','state','change','err_desc', 'session_pk', 'line_id','linestate_id','stat_cd','rate'] + [explode('line.linestate.exposure').alias('exp')]

clist3=['tfs_pk','policy_pk','session_id','current_transaction_id','data_id','pol_node_id',
        'tfs_id','type','exp.sValue as state','change','err_desc','err_ind', 'session_pk', 'line_id','linestate_id','stat_cd','rate']

colList=['change','rate']
typList=['float','float']

dfPolStTaxS=colListtypchk(dfLineStg.select(clist).where(dfLineStg.line.linestate._id != '').selectExpr(clist1).where(col('change') != '0').select(clist2).where(col("exp.Type") =="State"),colList,typList).withColumn('err_ind',err_ind).selectExpr(clist3)
print('Dataframe created for session_data_policy_statetaxsurcharge')
#-----------------------------------------------------------------------------------------------------------------------------------------
vTag='line.CarrierUnderwriting.CarrierUnderwritingValue.declineReasons.message'

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"
vpcuk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id, line.CarrierUnderwriting._id)) as cuw_pk"

clist=['session_id', 'current_transaction_id', 'type', 'transgroup', 'index', 'data_id','pol_node_id', 'line',
       col('line._id').alias('line_id'),
       col('line.CarrierUnderwriting._id').alias('carrier_underwriting_id'), 
       col('line.CarrierUnderwriting.CarrierUnderwritingValue.Status').alias('status')]

clist1=['session_id', 'current_transaction_id', 'type', 'transgroup', 'index', 'data_id','pol_node_id', 'line',
      'line_id','carrier_underwriting_id', 'status', col(vTag).alias('c'), explode(vTag).alias('cuWtr')]

clist2=[ vhash+vpcuk, vhash+vfk, vspk, 'session_id', 'data_id','pol_node_id', 'line_id',
        'carrier_underwriting_id', 'status', 
        'cuWtr._MessageUnderwriter as messageunderwriter', 
        'cuWtr._CarrierDecision as carrierdecision', 
        'cuWtr._RuleType as ruletype', 
        'cuWtr._RuleID as ruleid',
        'cuWtr._RuleValues as rulevalues', "'' as err_desc"]

dfLineCrUWtr=dfLineStg.select(clist).select(clist1).selectExpr(clist2).withColumn('err_ind',err_ind)

print('Dataframe created for session_data_policy_carrierunderwriting')
#--------------------------------------------------------------------
#Line State Term
#session_data_policy_line_linestate_linestateterm
vTag='line.linestate.linestateterm'

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id,line.linestate._id)) as linestate_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line._id,line.linestate._id,line.linestate.linestateterm._id)) as linestateterm_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id',
       vTag+'._id as linestateterm_id',
       vTag+'.RateEffectiveDate as termrateeffectivedate',
       vTag+'.PurePremiumValues._change as linestateterm_premium_change',
       vTag+'.TaxesSurchargesValues._change as linestateterm_tfs_change',"'' as err_desc"]
dfLineStTerm=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_linestate_linestateterm')
#---------------------------------------------------------------------------------------------------------------------------------------------
#Line State Term - OtherTaxesandAssessments
#session_data_policy_line_linestate_linestateterm_OtherTaxesandAssessments
vTag='line.linestate.linestateterm'

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id)) as policy_pk"
#vfk="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id, line.linestate.linestateterm._id)) as linestateterm_pk"
vpk1="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id,  'OtherTaxesAndAssessments1')) as ota_pk"
clist=[vhash+vpk1,vhash+vfk,vspk,'session_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id', 
       vTag+'._id as linestateterm_id', "'OtherTaxesAndAssessments1' as type_caption",
       vTag+'.OtherTaxesAndAssessments1Caption as tfs_Name', "'' as err_desc"]

dfOtherTaxesandAssessments1=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)

vpk2="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id,  'OtherTaxesAndAssessments2')) as ota_pk"
clist=[vhash+vpk2,vhash+vfk,vspk,'session_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id', 
       vTag+'._id as linestateterm_id', "'OtherTaxesAndAssessments2' as type_caption",
       vTag+'.OtherTaxesAndAssessments2Caption as tfs_Name', "'' as err_desc"]

dfOtherTaxesandAssessments2=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)

vpk3="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id,  'OtherTaxesAndAssessments3')) as ota_pk"
clist=[vhash+vpk3,vhash+vfk,vspk,'session_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id', 
       vTag+'._id as linestateterm_id', "'OtherTaxesAndAssessments3' as type_caption",
       vTag+'.OtherTaxesAndAssessments3Caption as tfs_Name', "'' as err_desc"]

dfOtherTaxesandAssessments3=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)

vpk4="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id,  'OtherTaxesAndAssessments4')) as ota_pk"
clist=[vhash+vpk4,vhash+vfk,vspk,'session_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id', 
       vTag+'._id as linestateterm_id', "'OtherTaxesAndAssessments4' as type_caption",
       vTag+'.OtherTaxesAndAssessments4Caption as tfs_Name', "'' as err_desc"]

dfOtherTaxesandAssessments4=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)

vpk5="session_id, current_transaction_id, type, transgroup, index, data_id, pol_node_id, line._id, line.linestate._id,  'OtherTaxesAndAssessments5')) as ota_pk"
clist=[vhash+vpk5,vhash+vfk,vspk,'session_id','data_id','pol_node_id',
       'line._id as line_id','line.linestate._id as linestate_id', 
       vTag+'._id as linestateterm_id', "'OtherTaxesAndAssessments5' as type_caption",
       vTag+'.OtherTaxesAndAssessments5Caption as tfs_Name', "'' as err_desc"]

dfOtherTaxesandAssessments5=dfLineStg.selectExpr(clist).where(dfLineStg.line.linestate.linestateterm._id !='').withColumn('err_ind',err_ind)

dfLineStTermOtherTaxSurch = dfOtherTaxesandAssessments1.union(dfOtherTaxesandAssessments2).union(dfOtherTaxesandAssessments3).union(dfOtherTaxesandAssessments4).union(dfOtherTaxesandAssessments5)

print('Dataframe created for session_data_policy_line_linestate_linestateterm_OtherTaxesandAssessments')
#----------------------------------------------------------------------------------------------------------------------------------
#Line State Term Coverage
#session_data_policy_line_linestate_linestateterm_coverage
vTag='line.linestate.linestateterm.coverage'
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id',col('line._id').alias('line_id'),
       col('line.linestate._id').alias('linestate_id'),
       col('line.linestate.linestateterm._id').alias('linestateterm_id'),
       explode(vTag).alias('lcoverage'),'PrimaryRatingState']
dfLineStTermCovgstg=dfLineStg.select(clist)

#dfLineStTermCovgstg=dfLineStTermCovgstg.where(dfLineStTermCovgstg.lcoverage.change!=0)

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id)) as linestateterm_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id,lcoverage._id)) as coverage_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id',
       'linestate_id','linestateterm_id',
       'lcoverage._id as coverage_id','lcoverage.type as coverage_type',
       "ifnull(nullif(lcoverage.change,''),'0') as class_premium_change",
       "ifnull(nullif(lcoverage.prior,''),'0') as class_premium_prior",
       "ifnull(nullif(lcoverage.written,''),'0') as class_premium_written",
       "ifnull(nullif(lcoverage.BaseRate,''),'0') as class_baserate",
       "'' as err_desc"] 
#"case when nullif(lcoverage.change,'') is NULL then 'Missing class_premium_change | ' else '' end as err_desc"

clist1=['coverage_pk','linestateterm_pk','session_pk','session_id','current_transaction_id','data_id','pol_node_id','line_id',
        'linestate_id','linestateterm_id','coverage_id','coverage_type','class_premium_change','class_baserate',
        'err_desc', 'err_ind', 'class_premium_prior','class_premium_written']

colList=['class_premium_change','class_baserate','class_premium_prior', 'class_premium_written']
typList=['float','float', 'float','float']

dfLineStTermCovg=colListtypchk(dfLineStTermCovgstg.selectExpr(clist)
                               .where((dfLineStTermCovgstg.lcoverage.change!=0)|(dfLineStTermCovgstg.lcoverage.written!=0)),colList,typList).withColumn('err_ind',err_ind).select(clist1)

print('Dataframe created for session_data_policy_line_linestate_linestateterm_coverage')
#--------------------------------------------------------------------
#Line State Term Coverage statCode
#session_data_policy_line_linestate_linestateterm_coverage_statcode
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id','line_id',
       'linestate_id','linestateterm_id',
       col('lcoverage._id').alias('coverage_id'),explode('lcoverage.statCode').alias('cstatCode')]

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id,coverage_id)) as coverage_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id,coverage_id,cstatCode._id)) as statcode_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id',
       'linestate_id','linestateterm_id','coverage_id','cstatCode._id as statcode_id',
        'cstatCode.Type as coverage_type','cstatCode.sValue as class_code',"'' as err_desc"]
dfLineStTermCovgStCD=dfLineStTermCovgstg.select(clist).where((dfLineStTermCovgstg.lcoverage.change!=0)|(dfLineStTermCovgstg.lcoverage.written!=0)).selectExpr(clist2).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_linestate_linestateterm_coverage_statcode')
#--------------------------------------------------------------------
#Line State Term Coverage Deductible
#session_data_policy_line_linestate_linestateterm_coverage_deductible
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id','line_id','linestate_id',
       'linestateterm_id',col('lcoverage._id').alias('coverage_id'),
       explode('lcoverage.deductible').alias('deductible'),'PrimaryRatingState',col('lcoverage.Type').alias('coverage_type')]
dfStCovDed=dfLineStTermCovgstg.select(clist)

vfk2="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id)) as line_pk"
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id,coverage_id)) as coverage_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,linestate_id,linestateterm_id,coverage_id,deductible._id)) as deductible_pk"

clist=[vhash+vpk,vhash+vfk,vhash+vfk2,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','linestate_id',
       'linestateterm_id','coverage_id','deductible._id as deductible_id',
       'deductible.Type as ded_type','deductible.scope',
       "ifnull(nullif(deductible.ivalue,''),'0') as ivalue",
       "ifnull(nullif(deductible.svalue,''),'') as svalue",
       'coverage_type',
       'PrimaryRatingState',"'' as err_desc"]

clist1=["deductible_pk","coverage_pk","session_pk","session_id",
        "current_transaction_id","data_id","pol_node_id",
        "line_id","linestate_id","linestateterm_id","coverage_id",
        "deductible_id","ded_type","scope","ivalue","err_desc","err_ind","line_pk"]

#ALLSTATES EXCEPT TX and FL(deductible type='SmallDeductibleCredit'and primaryratingstate!='FL','TX' and coverage_type='SmallDeductibleCredit')

dfLineStTermCovgILDedAmt=coltypchk(dfStCovDed.selectExpr(clist).where((col("coverage_type") == "SmallDeductibleCredit") & (col("ded_type") == "SmallDeductibleCredit")& (col("PrimaryRatingState")!="TX") & (col("PrimaryRatingState")!="FL")),'ivalue','int').withColumn('err_ind',err_ind).selectExpr(clist1)

#FL (Florida):(deductible type='Deductible' and scope='FL' and primaryratingstate='FL' and coverage_type='SmallDeductibleCredit')

dfLineStTermCovgFLDedAmt=coltypchk(dfStCovDed.selectExpr(clist).where((col("coverage_type") == "SmallDeductibleCredit") & (col("ded_type") == "Deductible") & (col("PrimaryRatingState") == "FL")& (col("scope") == "FL")),'ivalue','int').withColumn('err_ind',err_ind).selectExpr(clist1)

# TX (TEXAS):(deductible type='DeductibleCreditSelection' and scope='TX' and primaryratingstate='TX' and coverage_type='SmallDeductibleCredit') 

dfLineStTermCovgTXDedtemp=coltypchk(dfStCovDed.selectExpr(clist).where((col("coverage_type") == "SmallDeductibleCredit") &(col("PrimaryRatingState") == "TX")& (col("scope") == "TX")),'svalue','string').withColumn('err_ind',err_ind)

clistDedCredSeltx=['deductible_id as deductible_id_1','deductible_pk as deductible_pk_1', 'coverage_pk as coverage_pk_1','ded_type as ded_type_1','scope as scope_1',"concat_ws('',svalue, 's') as svalue_1"]

dfLineStTermCovgTxDedCredSelTmp =dfLineStTermCovgTXDedtemp.where(( col("ded_type") == "DeductibleCreditSelection")& (col("scope") == "TX")).selectExpr(clistDedCredSeltx)

dfLineStTermCovgTxDedAmtJoin =dfLineStTermCovgTxDedCredSelTmp.join(dfLineStTermCovgTXDedtemp,
                                             (dfLineStTermCovgTXDedtemp.coverage_pk==dfLineStTermCovgTxDedCredSelTmp.coverage_pk_1)&
                                             (dfLineStTermCovgTXDedtemp.scope==dfLineStTermCovgTxDedCredSelTmp.scope_1) &
                                             (trim(dfLineStTermCovgTXDedtemp.ded_type) == (trim(dfLineStTermCovgTxDedCredSelTmp.svalue_1)))
                                             ,'inner').select('*').where((dfLineStTermCovgTXDedtemp.ded_type=="PerClaimDeductibles")|(dfLineStTermCovgTXDedtemp.ded_type=="PerAccidentDeductibles")|(dfLineStTermCovgTXDedtemp.ded_type=="MedicalOnlyDeductibles"))

dfLineStTermCovgTxDedAmt=dfLineStTermCovgTxDedAmtJoin.selectExpr(clist1)

dfStCovDed = dfLineStTermCovgTxDedAmt.union(dfLineStTermCovgILDedAmt).union(dfLineStTermCovgFLDedAmt)
print('Dataframe created for session_data_policy_line_linestate_linestateterm_coverage_deductible')

# COMMAND ----------

# DBTITLE 1,Risk Tag
#Risk
#session_data_policy_line_risk
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id',
       col('line._id').alias('line_id'),explode('line.risk').alias('risk')]
dfLineRiskStg=dfLineStg.select(clist)

#dfLineRiskStg=dfLineRiskStg.where((dfLineRiskStg.risk.classCode.Type == 'Risk') & (dfLineRiskStg.risk.coverageterm.PurePremiumValues._change!=0) )
dfLineRiskStg=dfLineRiskStg.where(dfLineRiskStg.risk.classCode.Type == 'Risk')

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id)) as line_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id)) as risk_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id',
       'risk._id as risk_id','risk.locationid',
       "ifnull(nullif(risk.PurePremiumValues._change,''),'0') as purepremium_change",
       "ifnull(nullif(risk.TaxesSurchargesValues._change,''),'0') as tfs_change",
       "'' as err_desc"]

colList=['PurePremium_change','tfs_change']
typList=['float','float']

#dfLineRisk=colListtypchk(dfLineRiskStg.selectExpr(clist).where(dfLineRiskStg.risk.coverageterm.PurePremiumValues._change!=0),colList,typList).withColumn('err_ind',err_ind)
dfLineRisk=colListtypchk(dfLineRiskStg.selectExpr(clist),colList,typList).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_risk')
#--------------------------------------------------------------------
#Occupational Class Code
#session_data_policy_line_risk_classcode
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id)) as risk_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id,risk.classCode._id)) as classcode_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','risk._id as risk_id',
       'risk.classCode._id as classcode_id','risk.classCode.type',
       'risk.classCode.sValue as class_code',
       "case when nullif(risk.classCode.sValue,'') is NULL then 'Missing CLS_CD | ' else '' end as err_desc"]
dfLineClassCode=dfLineRiskStg.selectExpr(clist).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_risk_classcode')
#--------------------------------------------------------------------
#Coverge Term
#session_data_policy_line_risk_coverageterm
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id)) as risk_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id,risk.coverageterm._id)) as coverageterm_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','risk._id as risk_id',
       'risk.coverageterm._id as coverageterm_id',
       'risk.coverageterm.termrateeffectivedate',
       "ifnull(nullif(risk.coverageterm.PurePremiumValues._change,''),'0') as purepremium_change",
       "ifnull(nullif(risk.coverageterm.TaxesSurchargesValues._change,''),'0') as tfs_change",
       "'' as err_desc"]

colList=['PurePremium_change','TFS_change']
typList=['float','float']

dfLineCovgTerm=colListtypchk(dfLineRiskStg.selectExpr(clist),colList,typList).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_risk_coverageterm')
#--------------------------------------------------------------------
#Coverge
#session_data_policy_line_risk_coverageterm_coverage
vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id,risk.coverageterm._id)) as coverageterm_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk._id,risk.coverageterm._id,risk.coverageterm.coverage._id)) as coverage_pk"

clist=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','risk._id as risk_id',
       'risk.coverageterm._id as coverageterm_id',
       'risk.coverageterm.coverage._id as coverage_id',
       'risk.coverageterm.coverage.type',
       "ifnull(nullif(risk.coverageterm.coverage.change,''),'0') as class_premium_change",
       "ifnull(nullif(risk.coverageterm.coverage.BaseRate,''),'0') as class_baserate",
       "ifnull(nullif(risk.coverageterm.coverage.prior,''),'0') as class_premium_prior",
       "ifnull(nullif(risk.coverageterm.coverage.written,''),'0') as class_premium_written",
       "'' as err_desc"]

clist1=['coverage_pk','coverageterm_pk','session_pk','session_id','current_transaction_id','data_id','pol_node_id','line_id','risk_id',
       'coverageterm_id','coverage_id','type','class_premium_change','class_baserate','err_desc', 'err_ind', 'class_premium_prior', 'class_premium_written']

colList=['class_premium_change', 'class_baserate', 'class_premium_prior', 'class_premium_written']
typList=['float','float','float','float']

dfLineCovg=colListtypchk(dfLineRiskStg.selectExpr(clist).where(
  dfLineRiskStg.risk.coverageterm.coverage.Type == lit('ManualPremium')),colList,typList).withColumn('err_ind',err_ind).select(clist1)
print('Dataframe created for session_data_policy_line_risk_coverageterm_coverage')
#--------------------------------------------------------------------
#Occupational Class Exposure
#session_data_policy_line_risk_coverageterm_coverage_limit
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id','line_id',col('risk._id').alias('risk_id'),
       col('risk.coverageterm._id').alias('coverageterm_id'),
       col('risk.coverageterm.coverage._id').alias('coverage_id'),
       explode("risk.coverageterm.coverage.limit").alias("covlist")]

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk_id,coverageterm_id,coverage_id)) as coverage_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk_id,coverageterm_id,coverage_id,covlist._id)) as limit_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','risk_id',
        'coverageterm_id','coverage_id','covlist._id as limit_id','covlist.type',
        'covlist.sValue as exposure',"'' as err_desc"]

dfLineClsExpo=dfLineRiskStg.select(clist).where(dfLineRiskStg.risk.coverageterm.coverage.Type == lit('ManualPremium'))
dfLineClsExpo=dfLineClsExpo.selectExpr(clist2).where(dfLineClsExpo.covlist.Type == lit('UnitsOfExposure')).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_risk_coverageterm_coverage_limit')
#--------------------------------------------------------------------
#Coverge Exposure
#session_data_policy_line_risk_coverageterm_coverage_exposure
clist=['session_id','current_transaction_id','type','transgroup','index','data_id','pol_node_id','line_id',col('risk._id').alias('risk_id'),
       col('risk.coverageterm._id').alias('coverageterm_id'),
       col('risk.coverageterm.coverage._id').alias('coverage_id'),
       explode("risk.coverageterm.coverage.exposure").alias("covexp"),
      col('risk.coverageterm.coverage.TotalEmployees').alias('totalemployees')]

vfk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk_id,coverageterm_id,coverage_id)) as coverage_pk"
vpk="session_id,current_transaction_id,type,transgroup,index,data_id,pol_node_id,line_id,risk_id,coverageterm_id,coverage_id,covexp._id)) as exposure_pk"

clist2=[vhash+vpk,vhash+vfk,vspk,'session_id','current_transaction_id','data_id','pol_node_id','line_id','risk_id',
        'coverageterm_id','coverage_id','covexp._id as exposure_id','covexp.type','totalemployees',"'' as err_desc"]

dfCovgExpo=dfLineRiskStg.select(clist)
dfCovgExpo=dfCovgExpo.selectExpr(clist2).where(dfCovgExpo.covexp.type == lit('NumberOfEmployees')).withColumn('err_ind',err_ind)
print('Dataframe created for session_data_policy_line_risk_coverageterm_coverage_exposure')

# COMMAND ----------

dfins=(dfPolTran
        .selectExpr('session_id as ASessionid','type','session_pk','effectivedate as Aeffectivedate','historyid')
        .where((dfPolTran.type == 'New') | (dfPolTran.type == 'Renew') | (dfPolTran.type == 'Rewrite'))
        .join(dfPol,dfPolTran.session_pk==dfPol.session_pk, 'inner')
        .join(dfPolPymtInfo,dfPolPymtInfo.session_pk==dfPol.session_pk, 'inner')
        .join(dfSn.select('session_pk','xml_filename','index'),dfSn.session_pk==dfPol.session_pk, 'inner')
        .selectExpr('ASessionid as session_id','policynumber','0 as policytermid','0 as billitemid','0 as detailid',
                    'Aeffectivedate as transactiondate','effectivedate as duedate','purepremiumchange as itemamount',
                    'effectivedate as installmentdate','downpayment as installmentamount','0 as servicechargeamount',
                    'type as transactiontypecode',"'PREM' as receivabletypecode","'N/A' as detailtypecode",'xml_filename',
                    'effectivedate as itemeffectivedate',' 0 as closedtocash','0 as closedtocredit','0 as closedwritedff',
                    '0 as closedredistributed','downpayment as balance',"'' as err_desc","'N' as err_ind",'historyid',
                    "'I' as installmenttypecode", "'N' as InvoiceStatus", "'I' as ScheduleItemLevelTypeCode",'index as index_id')
        .union(dfinsStg))
print('Generate row to reflect the Billing down payment')

# COMMAND ----------

dfERR=dfSn.selectExpr('session_pk','err_desc').where(dfSn.err_ind == lit('Y'))

dfERR=dfERR.union(dfAccnt.selectExpr('session_pk','err_desc').where(dfAccnt.err_ind == lit('Y')))
dfERR=dfERR.union(dfAccntAddr.selectExpr('session_pk','err_desc').where(dfAccntAddr.err_ind == lit('Y')))
dfERR=dfERR.union(dfAccntLocAddr.selectExpr('session_pk','err_desc').where(dfAccntLocAddr.err_ind == lit('Y')))

dfERR=dfERR.union(dfPol.selectExpr('session_pk','err_desc').where(dfPol.err_ind == lit('Y')))
dfERR=dfERR.union(dfPolPymtInfo.selectExpr('session_pk','err_desc').where(dfPolPymtInfo.err_ind == lit('Y')))

dfERR=dfERR.union(dfPolTran.selectExpr('session_pk','err_desc').where(dfPolTran.err_ind == lit('Y')))

dfERR=dfERR.union(dfLine.selectExpr('session_pk','err_desc').where(dfLine.err_ind == lit('Y')))
dfERR=dfERR.union(dfExperienceRating.selectExpr('session_pk','err_desc').where(dfExperienceRating.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineLimit.selectExpr('session_pk','err_desc').where(dfLineLimit.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineSt.selectExpr('session_pk','err_desc').where(dfLineSt.err_ind == lit('Y')))

dfERR=dfERR.union(dfPolStTaxS.selectExpr('session_pk','err_desc').where(dfPolStTaxS.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineCrUWtr.selectExpr('session_pk','err_desc').where(dfLineCrUWtr.err_ind == lit('Y')))

dfERR=dfERR.union(dfLineStTerm.selectExpr('session_pk','err_desc').where(dfLineStTerm.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineStTermOtherTaxSurch.selectExpr('session_pk','err_desc').where(dfLineStTermOtherTaxSurch.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineStTermCovg.selectExpr('session_pk','err_desc').where(dfLineStTermCovg.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineStTermCovgStCD.selectExpr('session_pk','err_desc').where(dfLineStTermCovgStCD.err_ind == lit('Y')))
dfERR=dfERR.union(dfStCovDed.selectExpr('session_pk','err_desc').where(dfStCovDed.err_ind == lit('Y')))

dfERR=dfERR.union(dfLineRisk.selectExpr('session_pk','err_desc').where(dfLineRisk.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineClassCode.selectExpr('session_pk','err_desc').where(dfLineClassCode.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineCovgTerm.selectExpr('session_pk','err_desc').where(dfLineCovgTerm.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineCovg.selectExpr('session_pk','err_desc').where(dfLineCovg.err_ind == lit('Y')))
dfERR=dfERR.union(dfLineClsExpo.selectExpr('session_pk','err_desc').where(dfLineClsExpo.err_ind == lit('Y')))
dfERR=dfERR.union(dfCovgExpo.selectExpr('session_pk','err_desc').where(dfCovgExpo.err_ind == lit('Y'))).distinct()

dfERR = dfERR.groupBy("session_pk").agg(concat_ws("", collect_list("err_desc")).alias('err_desc'))

dfPremChg=dfPol.selectExpr('session_pk',"'pol_prem' as lvl",'cast(premiumchange as double) as prem').union(
  dfPol.selectExpr('session_pk',"'covg_prem' as lvl",
                        'cast(TaxesSurchargesChange as double) as prem').groupBy("session_pk",'lvl').sum('prem')).union(
  dfLineCovg.selectExpr('session_pk',"'covg_prem' as lvl",
                        'cast(class_premium_change as double) as prem').groupBy("session_pk",'lvl').sum('prem')).union(
  dfLineStTermCovg.selectExpr('session_pk',"'covg_prem' as lvl",
                              'cast(class_premium_change as double) as prem').groupBy("session_pk",'lvl').sum('prem')
).groupBy('session_pk').pivot('lvl',['covg_prem','pol_prem']).sum('prem')

dfERR=(dfERR
       .join(dfSn.select('session_pk','xml_filename'),'session_pk', 'outer')
       .join(dfPremChg.selectExpr('*'), 'session_pk', 'outer')
       .selectExpr('xml_filename','session_pk','ifnull(covg_prem,0) as wc_covg_tot_chgd_prem_amt','ifnull(pol_prem,0) as pol_acty_tot_chgd_prem_amt',"'' AS bal_ind", 'err_desc',"CASE WHEN err_desc is null THEN 'N' ELSE 'Y' END err_ind")
       .union(dfins.selectExpr('xml_filename','session_id','0 as c1','0 as c2',"'' as c3",'err_desc'
                              ).groupBy('xml_filename','session_id','c1','c2',"c3").agg(concat_ws("", collect_list("err_desc")).alias('err_desc')
                                                                                        ).withColumn('err_ind',err_ind))
       .join(dfPol.select('session_pk',lit('Q').alias('sts_ind')).where(dfPol.status == lit('Quote')),'session_pk', 'outer').selectExpr('xml_filename','session_pk','wc_covg_tot_chgd_prem_amt','pol_acty_tot_chgd_prem_amt','bal_ind',"case when bal_ind='' then 'I' when sts_ind is NULL then 'S' else sts_ind end as sts_ind", 'err_desc','err_ind')
      )
dfERR=dfERR.join(dfERR.select('session_pk',col('err_ind').alias('err')).where(dfERR.err_ind == lit('Y')).distinct(),'session_pk', 'outer').selectExpr('session_pk','wc_covg_tot_chgd_prem_amt','pol_acty_tot_chgd_prem_amt','bal_ind','sts_ind','xml_filename', 'err_desc','case when err is null then err_ind else err end as err_ind')

print('Dataframe created for dovetail_control')
writedf(dfERR,'dovetail_control')

# COMMAND ----------

writedf(dfSn,'session')
writedf(dfins,'installmentschedule_installment')

writedf(dfAccnt,'session_data_account')
writedf(dfAccntAddr.drop('session_pk'),'session_data_account_address')
writedf(dfAccntLocAddr.drop('session_pk'),'session_data_account_location_address')

writedf(dfPol,'session_data_policy')
writedf(dfPolPymtInfo.drop('session_pk'),'session_data_policy_paymentinformation')
writedf(dfPolTran,'session_data_policyadmin')

# COMMAND ----------

#WC  Line
writedf(dfLine.drop('session_pk'),'session_data_policy_line')

#Policy Limit
writedf(dfLineLimit.drop('session_pk'),'session_data_policy_line_limit')

#carrier under writing
writedf(dfLineCrUWtr.drop('session_pk'),'session_data_policy_carrierunderwriting')

#state tax surcharge
writedf(dfPolStTaxS.drop('session_pk'),'session_data_policy_statetaxsurcharge')

#Line State
writedf(dfLineSt.drop('session_pk'),'session_data_policy_line_linestate')

#Line State Term
writedf(dfLineStTerm.drop('session_pk'),'session_data_policy_line_linestate_linestateterm')

#Line State Term Other Taxesand Assessments
writedf(dfLineStTermOtherTaxSurch.drop('session_pk'),'session_data_policy_line_linestate_linestateterm_othertaxesandassessments')

#Line State Term Coverage
writedf(dfLineStTermCovg.drop('session_pk'),'session_data_policy_line_linestate_linestateterm_coverage')

#Line State Term Coverage statCode
writedf(dfLineStTermCovgStCD.drop('session_pk'),'session_data_policy_line_linestate_linestateterm_coverage_statcode')

#WC ExperienceRating 
writedf(dfExperienceRating.drop('session_pk'),'session_data_policy_line_lineexperiencerating')

#Line State Term Coverage Deductible
writedf(dfStCovDed.drop('session_pk'),'session_data_policy_line_linestate_linestateterm_coverage_deductible')
#------------------------------
#Risk
writedf(dfLineRisk.drop('session_pk'),'session_data_policy_line_risk')

#Occupational Class Code
writedf(dfLineClassCode.drop('session_pk'),'session_data_policy_line_risk_classcode')

#Occupational Class Exposure
writedf(dfLineClsExpo.drop('session_pk'),'session_data_policy_line_risk_coverageterm_coverage_limit')

#Coverge Term
writedf(dfLineCovgTerm.drop('session_pk'),'session_data_policy_line_risk_coverageterm')

#Coverge
writedf(dfLineCovg.drop('session_pk'),'session_data_policy_line_risk_coverageterm_coverage')

#Coverge Exposure
writedf(dfCovgExpo.drop('session_pk'),'session_data_policy_line_risk_coverageterm_coverage_exposure')

# COMMAND ----------

# DBTITLE 1,Export file names for Archive
#inputLoc='adl://storadls01datahubdv02.azuredatalakestore.net/data/provsn/landing/dovetail/20190107/'
#archivePath='wasb://schema@stor02spark36dtlkmigdv01.blob.core.windows.net/dovetail/default/default/201901/07/archive_filelist.txt'

dfFiles=spark.createDataFrame(dbutils.fs.ls(inputLoc))
dfFiles=dfFiles.select(col('name').alias('file_name')).where( (instr(dfFiles.name,'.txt') > 0) | (instr(dfFiles.name,'.xml') > 0) )
rfileNames=dfFiles.collect()
comSepfileNames=""
temp=""
for counter in range(len(rfileNames)):
  temp+=rfileNames[counter].file_name
  temp=temp+","
  comSepfileNames=temp

fileList=comSepfileNames.strip(',')
print(fileList)
dbutils.fs.put(archivePath,fileList, True)


# COMMAND ----------

# DBTITLE 1,Logging
print("============== Log file Started ==============")
log("input_count=0")
log("output_count=0")

writeLog(logPath, snapshot_year_month, snapshot_day)
print("============== Log file Ended ==============")

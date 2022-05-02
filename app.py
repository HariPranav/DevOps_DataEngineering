import time
from flask import Flask, request, render_template
import awswrangler as wr
# Flask constructor
app = Flask(__name__)


@app.route('/', methods=["GET", "POST"])
def gfg():
    if request.method == "POST":
        first_ornament=""
        second_ornament=""
        third_ornament=""
        fourth_ornament=""
        fifth_ornament=""

        first_ornament = request.form.get("ornament1") 
        uppercount1 = request.form.get("uppercount1")
        lowercount1 = request.form.get("lowercount1")
        grossweightlowerrange1 = request.form.get("grossweightlowerrange1")
        grossweightupperrange1 = request.form.get("grossweightupperrange1")

    

        second_ornament = request.form.get("ornament2")
        uppercount2 = request.form.get("uppercount2")
        lowercount2 = request.form.get("lowercount2")
        grossweightlowerrange2 = request.form.get("grossweightlowerrange2")
        grossweightupperrange2 = request.form.get("grossweightupperrange2")

        third_ornament = request.form.get("ornament3")
        uppercount3 = request.form.get("uppercount3")
        lowercount3 = request.form.get("lowercount3")
        grossweightlowerrange3 = request.form.get("grossweightlowerrange3")
        grossweightupperrange3 = request.form.get("grossweightupperrange3")

        fourth_ornament = request.form.get("fourthornament_4")
        uppercount4 = request.form.get("fourthornamentuppercount_4")
        lowercount4 = request.form.get("fourthornamentlowercount_4")
        grossweightlowerrange4 = request.form.get("fourthornamentgrossweightlowerrange_4")
        grossweightupperrange4 = request.form.get("fourthornamentgrossweightupperrange_4")

        fifth_ornament = request.form.get("fifthornament_5")
        uppercount5 = request.form.get("fifthornamentuppercount_5")
        lowercount5 = request.form.get("fifthornamentlowercount_5")
        grossweightlowerrange5 = request.form.get("fifthornamentgrossweightlowerrange_5")
        grossweightupperrange5 = request.form.get("fifthornamentgrossweightupperrange_5")
        # df = wr.s3.read_csv(path="s3://mfl-development/mfl-development/mfl_barun/Unsaved/2021/07/12/76345fad-0478-4a7d-9122-7804732ea245.csv")
        # print(df)
        # wr.catalog.create_database(
        # name='my_wrangler_db',
        # exist_ok=True
        # )
        # dbs = wr.catalog.get_databases()
        # for db in dbs:
        #     print("Database name: " + db['Name'])
        
       
        #B8,2,5,9,70,H4,2,5,4,50,G1,2,5,4,120
        #, database="mfl_datalake_sor"
        #select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+lowercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = b.coltrl_pd_brn_code and a.coltrl_pd_ln_no = b.coltrl_pd_ln_no  join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+third_ornament+"%' and (coltrl_pd_orn_count >= "+lowercount3+"  and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+"  and coltrl_pd_gross_wt <= "+grossweightlowerrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = c.coltrl_pd_brn_code and a.coltrl_pd_ln_no = c.coltrl_pd_ln_no ) order by cust_pr_unique_cust_id, coltrl_pd_ln_no) 
## execute first ornament

        if (first_ornament=="") and (second_ornament=="") and (third_ornament=="") and (fourth_ornament=="") and (fifth_ornament=="") :
            return "Input a single value to proceed"

        elif (second_ornament=="") and (third_ornament=="") and (fourth_ornament=="") and (fifth_ornament=="") :
            df = wr.athena.read_sql_query("select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select cust_pr_unique_cust_id,coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%" + first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ) order by cust_pr_unique_cust_id, coltrl_pd_ln_no)", database="mfl_datalake_sor")
            df2 = wr.athena.read_sql_query("select distinct cust_pr_unique_cust_id from (select distinct cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ) order by cust_pr_unique_cust_id)", database="mfl_datalake_sor")
            print ("the loan values" + df)
            print("the customer values"+df2)
            #create a bucket
            bucket = 'cloudauditresults'
            bucket2='cloudauditcus'
            path1 = f"s3://{bucket}/file1.csv"
            path2=f"s3://{bucket2}/file1.csv"
            if(len(df.index) == 0 or len(df.columns) == 0):
                return "the input for loan "
            if (len(df2.index) == 0 or len(df2.columns) == 0):
                return "input for customer has returned empty values"
            wr.s3.to_csv(df, path1, index=False)
            wr.s3.to_csv(df2, path2, index=False)
            results_final=df.to_html()
            results_final2=df2.to_html()
            
            return "The loan details are" + results_final + " The customer details are" + results_final2


        
## execute second ornament function
        if  (third_ornament=="") and (fourth_ornament=="") and (fifth_ornament=="") :
            df = wr.athena.read_sql_query("select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = b.coltrl_pd_brn_code and a.coltrl_pd_ln_no = b.coltrl_pd_ln_no) order by cust_pr_unique_cust_id, coltrl_pd_ln_no) ", database="mfl_datalake_sor")
            df2 = wr.athena.read_sql_query("select distinct cust_pr_unique_cust_id from (select distinct cust_pr_unique_cust_id from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id ) order by cust_pr_unique_cust_id) ", database="mfl_datalake_sor")
            print ("the loan values" + df)
            print("the customer values"+df2)
            #create a bucket
            bucket = 'cloudauditresults'
            bucket2='cloudauditcus'
            path1 = f"s3://{bucket}/file1.csv"
            path2=f"s3://{bucket2}/file1.csv"
            if(len(df.index) == 0 or len(df.columns) == 0):
                return "the input for loan "
            if (len(df2.index) == 0 or len(df2.columns) == 0):
                return "input for customer has returned empty values"
            wr.s3.to_csv(df, path1, index=False)
            wr.s3.to_csv(df2, path2, index=False)
            results_final=df.to_html()
            results_final2=df2.to_html()
              
            return "The loan details are" + results_final + " The customer details are" + results_final2
## third ornament
        elif (fourth_ornament == "") and (fifth_ornament==""):
            df = wr.athena.read_sql_query("select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select a.* from ((select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = b.coltrl_pd_brn_code and a.coltrl_pd_ln_no = b.coltrl_pd_ln_no join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+" and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+" and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = c.coltrl_pd_brn_code and a.coltrl_pd_ln_no = c.coltrl_pd_ln_no ) order by cust_pr_unique_cust_id, coltrl_pd_ln_no) ", database="mfl_datalake_sor")
            df2 = wr.athena.read_sql_query("select distinct cust_pr_unique_cust_id from (select distinct cust_pr_unique_cust_id from (select a.* from ((select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id join (select cust_pr_unique_cust_id,  coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+" and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+" and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id ) order by cust_pr_unique_cust_id) ", database="mfl_datalake_sor")
            print ("the loan values" + df)
            print("the customer values"+df2)
            #create a bucket
            bucket = 'cloudauditresults'
            bucket2='cloudauditcus'
            path1 = f"s3://{bucket}/file1.csv"
            path2=f"s3://{bucket2}/file1.csv"
            if(len(df.index) == 0 or len(df.columns) == 0):
                return "the input for loan "
            if (len(df2.index) == 0 or len(df2.columns) == 0):
                return "input for customer has returned empty values"
            wr.s3.to_csv(df, path1, index=False)
            wr.s3.to_csv(df2, path2, index=False)
            results_final=df.to_html()
            results_final2=df2.to_html()
              
            return "The loan details are" + results_final + " The customer details are" + results_final2

## execute fourth ornament function
        elif fifth_ornament=="":
            df = wr.athena.read_sql_query("select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from(select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no,coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = b.coltrl_pd_brn_code and a.coltrl_pd_ln_no = b.coltrl_pd_ln_no join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no,coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+"  and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+"  and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = c.coltrl_pd_brn_code and a.coltrl_pd_ln_no = c.coltrl_pd_ln_no join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt,coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fourth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount4+"  and coltrl_pd_orn_count <= "+uppercount4+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange4+"  and coltrl_pd_gross_wt <= "+grossweightupperrange4+" )) d on a.cust_pr_unique_cust_id = d.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = d.coltrl_pd_brn_code and a.coltrl_pd_ln_no = d.coltrl_pd_ln_no)order by cust_pr_unique_cust_id, coltrl_pd_ln_no) ", database="mfl_datalake_sor")
            df2 = wr.athena.read_sql_query("select distinct cust_pr_unique_cust_id from(select distinct cust_pr_unique_cust_id from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join( select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id join  ( select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+"  and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+"  and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id  join  ( select cust_pr_unique_cust_id,  coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt,coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fourth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount4+"  and coltrl_pd_orn_count <= "+uppercount4+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange4+"  and coltrl_pd_gross_wt <= "+grossweightupperrange4+" )) d on a.cust_pr_unique_cust_id = d.cust_pr_unique_cust_id)order by cust_pr_unique_cust_id)", database="mfl_datalake_sor")
            print ("the loan values" + df)
            print("the customer values"+df2)
            #create a bucket
            bucket = 'cloudauditresults'
            bucket2='cloudauditcus'
            path1 = f"s3://{bucket}/file1.csv"
            path2=f"s3://{bucket2}/file1.csv"
            if(len(df.index) == 0 or len(df.columns) == 0):
                return "the input for loan "
            if (len(df2.index) == 0 or len(df2.columns) == 0):
                return "input for customer has returned empty values"
            wr.s3.to_csv(df, path1, index=False)
            wr.s3.to_csv(df2, path2, index=False)
            results_final=df.to_html()
            results_final2=df2.to_html()
             
            return "The loan details are" + results_final + " The customer details are" + results_final2 
## execute fifth query
        else:
            df = wr.athena.read_sql_query("select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no from (select a.*  from ((select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = b.coltrl_pd_brn_code and a.coltrl_pd_ln_no = b.coltrl_pd_ln_no join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+"  and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+"  and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = c.coltrl_pd_brn_code and a.coltrl_pd_ln_no = c.coltrl_pd_ln_no join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fourth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount4+"  and coltrl_pd_orn_count <= "+uppercount4+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange4+"  and coltrl_pd_gross_wt <= "+grossweightupperrange4+" )) d on a.cust_pr_unique_cust_id = d.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = d.coltrl_pd_brn_code and a.coltrl_pd_ln_no = d.coltrl_pd_ln_no join  ( select cust_pr_unique_cust_id, coltrl_pd_brn_code, coltrl_pd_ln_no, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fifth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount5+"  and coltrl_pd_orn_count <= "+uppercount5+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange5+"  and coltrl_pd_gross_wt <= "+grossweightupperrange5+" )) e on a.cust_pr_unique_cust_id = e.cust_pr_unique_cust_id and a.coltrl_pd_brn_code = e.coltrl_pd_brn_code and a.coltrl_pd_ln_no = e.coltrl_pd_ln_no) order by cust_pr_unique_cust_id, coltrl_pd_ln_no)", database="mfl_datalake_sor")
            df2 = wr.athena.read_sql_query("select cust_pr_unique_cust_id from (select cust_pr_unique_cust_id from (select a.*  from ((select cust_pr_unique_cust_id,  coltrl_pd_orn_code,coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+ first_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount1+" and coltrl_pd_orn_count <= "+uppercount1+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange1+" and coltrl_pd_gross_wt <= "+grossweightupperrange1+" ))) as a join ( select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails where coltrl_pd_orn_code like '%"+second_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount2+" and coltrl_pd_orn_count <= "+uppercount2+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange2+" and coltrl_pd_gross_wt <= "+grossweightupperrange2+" )) b on a.cust_pr_unique_cust_id = b.cust_pr_unique_cust_id  join  ( select cust_pr_unique_cust_id, coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+third_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount3+"  and coltrl_pd_orn_count <= "+uppercount3+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange3+"  and coltrl_pd_gross_wt <= "+grossweightupperrange3+" )) c on a.cust_pr_unique_cust_id = c.cust_pr_unique_cust_id  join  ( select cust_pr_unique_cust_id,  coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fourth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount4+"  and coltrl_pd_orn_count <= "+uppercount4+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange4+"  and coltrl_pd_gross_wt <= "+grossweightupperrange4+" )) d on a.cust_pr_unique_cust_id = d.cust_pr_unique_cust_id  join  ( select cust_pr_unique_cust_id,  coltrl_pd_orn_code, coltrl_pd_orn_count, coltrl_pd_net_wt, coltrl_pd_gross_wt, coltrl_pd_gold_purity from ucic_packetdetails  where coltrl_pd_orn_code like '%"+fifth_ornament+"%' and contr_pld_cl_dt is null and (coltrl_pd_orn_count >= "+lowercount5+"  and coltrl_pd_orn_count <= "+uppercount5+" ) and (coltrl_pd_gross_wt >= "+grossweightlowerrange5+"  and coltrl_pd_gross_wt <= "+grossweightupperrange5+" )) e on a.cust_pr_unique_cust_id = e.cust_pr_unique_cust_id ) order by cust_pr_unique_cust_id) ", database="mfl_datalake_sor")
            print ("the loan values" + df)
            print("the customer values"+df2)
            #create a bucket
            bucket = 'cloudauditresults'
            bucket2='cloudauditcus'
            path1 = f"s3://{bucket}/file1.csv"
            path2=f"s3://{bucket2}/file1.csv"
            if(len(df.index) == 0 or len(df.columns) == 0):
                return "the input for loan "
            if (len(df2.index) == 0 or len(df2.columns) == 0):
                return "input for customer has returned empty values"
            wr.s3.to_csv(df, path1, index=False)
            wr.s3.to_csv(df2, path2, index=False)
            results_final=df.to_html()
            results_final2=df2.to_html()
            
            return "The loan details are" + results_final + " The customer details are" + results_final2
        
    return render_template("index.html")

#"SELECT * FROM "anonymised_poc_pii"."cross_account_table" where ornament1='code of audit team' + ornametcount(lower and upper) + weight(upper and lower) -> loan number this is joined with second loand details 
# this will be joined with the third one.

if __name__ == '__main__':
    app.run()
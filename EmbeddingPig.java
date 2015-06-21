@@ -0,0 +1,171 @@
import java.io.IOException;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class EmbeddingPig {
   public static void main(String[] args) {      
        
      try {
    	  PigServer pigServer = new PigServer(ExecType.LOCAL);
    	  runMyQuery(pigServer, args);
        }  catch (ExecException e1) {
    		// TODO Auto-generated catch block
    		e1.printStackTrace();
    	}
      catch (IOException e) {
         e.printStackTrace();
        }
   }
   
   public static void runMyQuery(PigServer pigServer, String[] args) throws IOException {  
	   /*------Removal of Redlinks----------*/
       /*pigServer.registerQuery("data = load '" + inputFile + "'using PigStorage(',') AS (field1:chararray,field2:chararray);");
       pigServer.registerQuery("data_dup = foreach data generate field1 AS field_1, field2 AS field_2;");
       pigServer.registerQuery("data_join = join data by field2 , data_dup by field_1; ");
       //pigServer.debugOn();
       pigServer.registerQuery("data_table = FOREACH data_join GENERATE $0,$1;");
       pigServer.registerQuery("distinct_data = DISTINCT data_table;");
       pigServer.store("data_join", "P---roject");*/
       
       /*------Calculation of total number of pages--------*/
	   pigServer.registerQuery("data = load '" + args[0] + "'using PigStorage(',') AS (field1:chararray,field2:chararray);");
	   pigServer.registerQuery("distinct_data = DISTINCT data;");
	   pigServer.registerQuery("links_group = group distinct_data by field1;");
       pigServer.registerQuery("grp_all_pages = group links_group ALL;");
       pigServer.registerQuery("page_count = foreach grp_all_pages generate group, COUNT($1) AS cnt_of_N_pages;");
       //pigServer.store("page_count", "Project");
       
       /*Compute the table with all values*/
       pigServer.registerQuery("outgoing_count = foreach links_group generate group, COUNT($1) as outcount;");
       pigServer.registerQuery("i_join = join outgoing_count by $0,data by $0;");
       pigServer.registerQuery("in_table = foreach i_join generate $3 as pi,$2 as pj,$1 as outcount;");
       pigServer.registerQuery("cross_jn = CROSS in_table,page_count;");
       pigServer.registerQuery("initial_table = foreach cross_jn generate $0 as pi,$1 as pj,$2 as outcount,(double)1/$4 as PR,$4 as N;");       
       //pigServer.store("initial_table", "Project");
       
       /*First Iteration*/
       pigServer.registerQuery("div_pages_1 = foreach initial_table generate pi, pj,outcount,PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_1 = group div_pages_1 by pi;");
       pigServer.registerQuery("sum_div_pages_1 = foreach grp_div_pages_1 generate group,0.85*SUM(div_pages_1.$3) as sum_pr,(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_1 = CROSS sum_div_pages_1,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_1 = foreach cross_div_jn_1 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_1 = foreach sum_div_pages_dup_1 generate $0,$1 as sum_pr_1,($2/$3) as const,$3;");
       pigServer.registerQuery("iter1 = foreach sum_div_pages_dupp_1 generate $0,(const+sum_pr_1);");
       pigServer.registerQuery("join_iter1 = join initial_table by $1,iter1 by $0;");
       pigServer.registerQuery("iter1_table = foreach join_iter1 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
     pigServer.store("iter1_table", "Project");
       
       /*Second Iteration*/
       pigServer.registerQuery("div_pages_2 = foreach iter1_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_2 = group div_pages_2 by pi;");
       pigServer.registerQuery("sum_div_pages_2 = foreach grp_div_pages_2 generate group,0.85*SUM(div_pages_2.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_2 = CROSS sum_div_pages_2,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_2 = foreach cross_div_jn_2 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_2 = foreach sum_div_pages_dup_2 generate $0,$1 as sum_pr_2,($2/$3) as const,$3;");
       pigServer.registerQuery("iter2 = foreach sum_div_pages_dupp_2 generate $0,(const+sum_pr_2);");
       pigServer.registerQuery("join_iter2 = join iter1_table by $1,iter2 by $0;");
       pigServer.registerQuery("iter2_table = foreach join_iter2 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       
       //pigServer.store("iter2", "Project");
       
       /*Third Iteration*/
       pigServer.registerQuery("div_pages_3 = foreach iter2_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_3 = group div_pages_3 by pi;");
       pigServer.registerQuery("sum_div_pages_3 = foreach grp_div_pages_3 generate group,0.85*SUM(div_pages_3.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_3 = CROSS sum_div_pages_3,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_3 = foreach cross_div_jn_3 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_3 = foreach sum_div_pages_dup_3 generate $0,$1 as sum_pr_3,($2/$3) as const,$3;");
       pigServer.registerQuery("iter3 = foreach sum_div_pages_dupp_3 generate $0,(const+sum_pr_3);");
       pigServer.registerQuery("join_iter3 = join iter2_table by $1,iter3 by $0;");
       pigServer.registerQuery("iter3_table = foreach join_iter3 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter3", "Project");
       
       /*Fourth Iteration*/
       pigServer.registerQuery("div_pages_4 = foreach iter3_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_4 = group div_pages_4 by pi;");
       pigServer.registerQuery("sum_div_pages_4 = foreach grp_div_pages_4 generate group,0.85*SUM(div_pages_4.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_4 = CROSS sum_div_pages_4,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_4 = foreach cross_div_jn_4 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_4 = foreach sum_div_pages_dup_4 generate $0,$1 as sum_pr_4,($2/$3) as const,$3;");
       pigServer.registerQuery("iter4 = foreach sum_div_pages_dupp_4 generate $0,(const+sum_pr_4);");
       pigServer.registerQuery("join_iter4 = join iter3_table by $1,iter4 by $0;");
       pigServer.registerQuery("iter4_table = foreach join_iter4 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter4", "Project");
       
       /*Fifth Iteration*/
       pigServer.registerQuery("div_pages_5 = foreach iter4_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_5 = group div_pages_5 by pi;");
       pigServer.registerQuery("sum_div_pages_5 = foreach grp_div_pages_5 generate group,0.85*SUM(div_pages_5.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_5 = CROSS sum_div_pages_5,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_5 = foreach cross_div_jn_5 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_5 = foreach sum_div_pages_dup_5 generate $0,$1 as sum_pr_5,($2/$3) as const,$3;");
       pigServer.registerQuery("iter5 = foreach sum_div_pages_dupp_5 generate $0,(const+sum_pr_5);");
       pigServer.registerQuery("join_iter5 = join iter4_table by $1,iter5 by $0;");
       pigServer.registerQuery("iter5_table = foreach join_iter5 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter5", "Project");
       
       /*Six Iteration*/
       pigServer.registerQuery("div_pages_6 = foreach iter5_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_6 = group div_pages_6 by pi;");
       pigServer.registerQuery("sum_div_pages_6 = foreach grp_div_pages_6 generate group,0.85*SUM(div_pages_6.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_6 = CROSS sum_div_pages_6,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_6 = foreach cross_div_jn_6 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_6 = foreach sum_div_pages_dup_6 generate $0,$1 as sum_pr_6,($2/$3) as const,$3;");
       pigServer.registerQuery("iter6 = foreach sum_div_pages_dupp_6 generate $0,(const+sum_pr_6);");
       pigServer.registerQuery("join_iter6 = join iter5_table by $1,iter6 by $0;");
       pigServer.registerQuery("iter6_table = foreach join_iter6 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter6", "Project");
       
       /*Seventh Iteration*/
       pigServer.registerQuery("div_pages_7 = foreach iter6_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_7 = group div_pages_7 by pi;");
       pigServer.registerQuery("sum_div_pages_7 = foreach grp_div_pages_7 generate group,0.85*SUM(div_pages_7.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_7 = CROSS sum_div_pages_7,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_7 = foreach cross_div_jn_7 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_7 = foreach sum_div_pages_dup_7 generate $0,$1 as sum_pr_7,($2/$3) as const,$3;");
       pigServer.registerQuery("iter7 = foreach sum_div_pages_dupp_7 generate $0,(const+sum_pr_7);");
       pigServer.registerQuery("join_iter7 = join iter6_table by $1,iter7 by $0;");
       pigServer.registerQuery("iter7_table = foreach join_iter7 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter7", "Project");
       
       /*Eighth Iteration*/
       pigServer.registerQuery("div_pages_8 = foreach iter7_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_8 = group div_pages_8 by pi;");
       pigServer.registerQuery("sum_div_pages_8 = foreach grp_div_pages_8 generate group,0.85*SUM(div_pages_8.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_8 = CROSS sum_div_pages_8,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_8 = foreach cross_div_jn_8 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_8 = foreach sum_div_pages_dup_8 generate $0,$1 as sum_pr_8,($2/$3) as const,$3;");
       pigServer.registerQuery("iter8 = foreach sum_div_pages_dupp_8 generate $0,(const+sum_pr_8);");
       pigServer.registerQuery("join_iter8 = join iter7_table by $1,iter8 by $0;");
       pigServer.registerQuery("iter8_table = foreach join_iter8 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter8", "Project");
       
       /*ninth Iteration*/
       pigServer.registerQuery("div_pages_9 = foreach iter8_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_9 = group div_pages_9 by pi;");
       pigServer.registerQuery("sum_div_pages_9 = foreach grp_div_pages_9 generate group,0.85*SUM(div_pages_9.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_9 = CROSS sum_div_pages_9,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_9 = foreach cross_div_jn_9 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_9 = foreach sum_div_pages_dup_9 generate $0,$1 as sum_pr_9,($2/$3) as const,$3;");
       pigServer.registerQuery("iter9 = foreach sum_div_pages_dupp_9 generate $0,(const+sum_pr_9);");
       pigServer.registerQuery("join_iter9 = join iter8_table by $1,iter9 by $0;");
       pigServer.registerQuery("iter9_table = foreach join_iter9 generate $0 as pi,$1 as pj,$2 as outcount,$6 as PR,$4 as N;");
       //pigServer.store("iter8", "Project");
       
       /*Tenth Iteration*/
       pigServer.registerQuery("div_pages_10 = foreach iter9_table generate pi, pj,outcount,(double)PR/outcount,N;");
       pigServer.registerQuery("grp_div_pages_10 = group div_pages_10 by pi;");
       pigServer.registerQuery("sum_div_pages_10 = foreach grp_div_pages_10 generate group,0.85*SUM(div_pages_10.$3),(1-0.85) as num;");
       pigServer.registerQuery("cross_div_jn_10 = CROSS sum_div_pages_10,page_count;"); 
       pigServer.registerQuery("sum_div_pages_dup_10 = foreach cross_div_jn_10 generate $0,$1,$2,$4;");       
       pigServer.registerQuery("sum_div_pages_dupp_10 = foreach sum_div_pages_dup_10 generate $0,$1 as sum_pr_10,($2/$3) as const,$3;");
       pigServer.registerQuery("iter10 = foreach sum_div_pages_dupp_10 generate $0,(const+sum_pr_10);");
       
       pigServer.registerQuery("desc_order = ORDER iter10 BY $1 DESC;");
       pigServer.registerQuery("filtering = FILTER desc_order BY  $0 MATCHES  'A*';");
       pigServer.registerQuery("lim = LIMIT filtering 100;");
       pigServer.store("lim", args[1]);
   }
}
\ No newline at end of file

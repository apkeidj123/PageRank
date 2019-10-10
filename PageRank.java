package org.apache.hadoop.examples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	public static double Beta = 0.8;
	public static double Node_num = 10876.0;
	public static int ITERATIONS = 20;
	//public static Set<String> NODES = new HashSet<String>();
	public static String LINKS_SEPARATOR = "|";
	//public static double normalizedPageRank = 0.0;
	//public static String tempkey = "k";
	
    public static class PRMapper1
        extends Mapper<LongWritable, Text, Text, Text>{
		//extends Mapper<Object, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context
	//public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		/* Job#1 mapper will simply parse a line of the input graph creating a map with key-value(s) pairs.
         * Input format is the following (separator is TAB):
         * 
         *     <nodeA>    <nodeB>
         * 
         * which denotes an edge going from <nodeA> to <nodeB>.
         * We would need to skip comment lines (denoted by the # characters at the beginning of the line).
         * We will also collect all the distinct nodes in our graph: this is needed to compute the initial 
         * pagerank value in Job #1 reducer and also in later jobs.
         */
        StringTokenizer itr = new StringTokenizer(value.toString());
		String nodeA = "";
		String nodeB = "";
		while(itr.hasMoreTokens()){
			nodeA = itr.nextToken();
			nodeB = itr.nextToken();
		}
		/*
		if(split.length>1{
			String nodeA = split[0];
			String nodeB = split[1];
		}
        else{
			String split2 = value.toString().split(" ");
			String nodeA = split2[0];
			String nodeB = split2[1];
		}
		*/
        context.write(new Text(nodeA), new Text(nodeB));
            
            // add the current source node to the node list so we can 
            // compute the total amount of nodes of our graph in Job#2
        //PageRank.NODES.add(nodeA);
            // also add the target node to the same list: we may have a target node 
            // with no outlinks (so it will never be parsed as source)
        //PageRank.NODES.add(nodeB);
    }
}

	public static class PRReducer1
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException {
		/* Job#1 reducer will scroll all the nodes pointed by the given "key" node, constructing a
         * comma separated list of values and initializing the page rank for the "key" node.
         * Output format is the following (separator is TAB):
         * 
         *     <title>    <page-rank>  @  <link1>,<link2>,<link3>,<link4>,...,<linkN>
         *     
         * As for the pagerank initial value, early version of the PageRank algorithm used 1.0 as default, 
         * however later versions of PageRank assume a probability distribution between 0 and 1, hence the 
         * initial valus is set to DAMPING FACTOR / TOTAL NODES for each node in the graph.   
         */
        boolean first = true;
        String links = ( 1 / PageRank.Node_num) + "@";

        for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }

        context.write(key, new Text(links));
    }
}

	public static class PRMapper2
        extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
		/* PageRank calculation algorithm (mapper)
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>  @  <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * Output has 2 kind of records:
         * One record composed by the collection of links of each page:
         *     
         *     <title>   |<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record composed by the linked page, the page rank of the source page 
         * and the total amount of out links of the source page:
         *  
         *     <link>    <page-rank>  /  <total-links>
         */	

		String[] split = value.toString().split("\t");
		String page = "";	
		String pageRank = "";
		String links = "";
		int len = 0 ;
		page = split[0];
		String[] split2 = split[1].split("@");
		pageRank = split2[0];
		String[] allOtherPages ={};
		len = split2.length;
		if(len>1){
			links = split2[1];
			context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
			allOtherPages = links.split(",");
		}	
		
		double PR = Double.parseDouble(pageRank);	
		
		for (String otherPage : allOtherPages) { 
			//Text pageRankWithTotalLinks = new Text(pageRank + "@" + allOtherPages.length);
			double PRdsize = PR/allOtherPages.length;
			String pageRankWithTotalLinks = String.valueOf(PRdsize);
			context.write(new Text(otherPage), new Text(pageRankWithTotalLinks)); 
			
		}
		/*
		String[] split = value.toString().split("\t");
		String page = "";
	
		String pageRank = "";
		String links = "";
		String[] allOtherPages ={};
		int len = 0 ;
		if(split.length>1){
			page = split[0];
			String[] split2 = split[1].split("@");
			pageRank = split2[0];
			len = split2.length;
			if(len>1){
				links = split2[1];
			}	
		}
		else{
			String[] split0 = value.toString().split(" ");
			page = split0[0];
			String[] split2 = split0[1].split("@");
			pageRank = split2[0];
			len = split2.length;
			if(len>1){
				links = split2[1];
			}	
		}
		
		if(len>1){
			context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
			allOtherPages = links.split(",");
		}
		
		double PR = Double.parseDouble(pageRank);	
		
		for (String otherPage : allOtherPages) { 
			//Text pageRankWithTotalLinks = new Text(pageRank + "@" + allOtherPages.length);
			double PRdsize = PR/allOtherPages.length;
			String pageRankWithTotalLinks = String.valueOf(PRdsize);
			context.write(new Text(otherPage), new Text(pageRankWithTotalLinks)); 
			
		}
		*/
			
    }
}

public static class PRReducer2
        extends Reducer<Text, Text, Text, Text> {
		
    public void reduce(Text key, Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException {
		String links = "";					
		double sumShareOtherPageRanks = 0.0;
		for (Text value : values) {
			
            String content = value.toString();
            
            if (content.startsWith(PageRank.LINKS_SEPARATOR)) {
                // if this value contains node links append them to the 'links' string
                // for future use: this is needed to reconstruct the input for Job#2 mapper
                // in case of multiple iterations of it.
                links += content.substring(PageRank.LINKS_SEPARATOR.length());
            } else {				
                // extract tokens
                double pageRank = Double.parseDouble(content);

                // add the contribution of all the pages having an outlink pointing 
                // to the current node: we will add the DAMPING factor later when recomputing
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPageRanks += pageRank ;
            }

        }
		
		double newRank = PageRank.Beta * sumShareOtherPageRanks + (1 - PageRank.Beta)/Node_num;
		

		String newpageRank = String.valueOf(newRank);
		context.write(key, new Text(newpageRank + "@" + links));
    }
}

public static class PRMapper3
        extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			
		String[] split = value.toString().split("\t");
		String temp = "";
		temp += split[0] + "@" + split[1];
		String tmpkey = "K";
		context.write(new Text(tmpkey), new Text(temp));
		
		//String ha = value.toString();
		//context.write(new Text(tempkey), new Text(ha));	
		
		/*
		String tempkey = "k";
		
		String[] split = value.toString().split("\t");
		String page = "";
		String pageRank = "";
		String links = "";
		if(split.length>1){
			page = split[0];
			String[] split2 = split[1].split("@");
			pageRank = split2[0];
			if(split2.length>1){
				links = split2[1];
			}		
		}
		else{
			String[] split0 = value.toString().split(" ");
			page = split0[0];
			String[] split2 = split0[1].split("@");
			pageRank = split2[0];
			if(split2.length>1){
				links = split2[1];
			}
		}		
		String savepage = page + "&" + pageRank + "@" + links;
		context.write(new Text(tempkey), new Text(savepage));
		*/
			
    }
}

public static class PRReducer3
		extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException { 
		
		double sumPageRank = 0.0;
		
		String tmp = "";
	
		for (Text value : values){
			String content = value.toString();
			tmp += content+ "\t";
			String[] split = content.split("@");
			double pageRank = Double.parseDouble(split[1]);
			sumPageRank += pageRank;
		}
		String[] allValue = tmp.split("\t");
		
		//String WTF = String.valueOf(sumPageRank);
		//context.write(key,new Text(WTF));
		
		for (String alv : allValue){			
			String[] split2 = alv.split("@");
			String page = split2[0];
			String pageRank = split2[1];
			double pRank = Double.parseDouble(pageRank);
			String links = "";	
			if(split2.length>2){
				links = split2[2];
			}		
			double TruepageRank = pRank +(1-sumPageRank)/PageRank.Node_num;
			pageRank = String.valueOf(TruepageRank);
			/*
			if(split2.length>1){
				links = split2[1];
			}
			*/
			context.write(new Text(page), new Text(pageRank + "@" + links));
		}
	
		/*
		for (Text value : values){
			String content = value.toString();
			String[] split = content.split("&");
			String[] split2 = split[1].split("@");
			double pageRank = Double.parseDouble(split2[0]);
			sumPageRank += pageRank;
		}
		
		//String WTF = String.valueOf(sumPageRank);
		//context.write(key,new Text(WTF));
		
		for (Text value : values){
			String content = value.toString();
			String[] split = content.split("&");
			String page = split[0];
			String[] split2 = split[1].split("@");
			double pageRank = Double.parseDouble(split2[0]);
			double TruepageRank = pageRank +(1-sumPageRank)/PageRank.Node_num;
			String savePage = String.valueOf(TruepageRank);
			
			if(split2.length>1){
				links = split2[1];
			}
			
			context.write(new Text(page), new Text(savePage + "@" + links));
		}*/
		
		/*
		for(Text value: values){		
			context.write(key,new Text(value));
		}
		*/
		
		/*
		String page = "";
		String pageRank = "";	
		double pageRanks = 0.0;	
			
		String SHIT = "";
		
		for (Text value : values){
			String content = value.toString();
			String[] split = content.split("\t");
			if(split.length>1){
				String[] split2 = split[1].split("@");
				pageRanks = Double.parseDouble(split2[0]);		
				
			}
			else{
				String[] split0 = content.split(" ");
				String[] split2 = split0[1].split("@");
				pageRanks = Double.parseDouble(split2[0]);			
			}
			sumPageRank += pageRanks;
		}
		String WTF = String.valueOf(sumPageRank);
		context.write(key,new Text(WTF));
		for (Text value : values){
			String content = value.toString();
			String[] split = content.split("\t");
			if(split.length>1){
				page = split[0];
				String[] split2 = split[1].split("@");
				pageRanks = Double.parseDouble(split2[0]);
				double TruepageRank = pageRanks +(1-sumPageRank)/PageRank.Node_num;
				SHIT = String.valueOf(TruepageRank);
				if(split2.length>1){
					links = split2[1];
				}		
			}
			else{
				String[] split0 = content.split(" ");
				page = split0[0];
				String[] split2 = split0[1].split("@");
				pageRanks = Double.parseDouble(split2[0]);
				double TruepageRank = pageRanks +(1-sumPageRank)/PageRank.Node_num;
				SHIT = String.valueOf(TruepageRank);
				SHIT += "#" + sumPageRank;
				if(split2.length>1){
					links = split2[1];
				}
			}
			String FUCK = String.valueOf(sumPageRank);
			context.write(new Text(page), new Text(FUCK + "$" + SHIT + "@" + links));
		}
		*/
					
			
		//double TruepageRank = pageRank +(1-sumPageRank)/PageRank.Node_num;
		//String SHIT = String.valueOf(TruepageRank);
		//SHIT += "#" + sumPageRank ;
		//context.write(new Text(page), new Text(SHIT));
		//context.write(new Text(page), new DoubleWritable(TruepageRank));
    }
}

public static class PRMapper4
        extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		//extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			
		String[] split = value.toString().split("\t");
		String page = "";
		String pageRank = "";
		if(split.length>1){
			page = split[0];
			String[] split2 = split[1].split("@");
			pageRank = split2[0];
		
		}
		else{
			String[] split0 = value.toString().split(" ");
			page = split0[0];
			String[] split2 = split0[1].split("@");
			pageRank = split2[0];
		}	
		double FPR = Double.parseDouble(pageRank); 
		context.write(new DoubleWritable(FPR),new Text(page));
		//context.write(new Text(page),new DoubleWritable(FPR));	
    }
}

public static class PRReducer4
		extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException { 					
		for (Text truekey : values) {
			context.write(truekey,key);
        }
    }
}

public static class myComparator extends Comparator {
        
        public int compare( WritableComparable a,WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	String[] iter = new String[ITERATIONS*2+1];
	for(int i = 0 ; i < ITERATIONS*2+1 ; i++){
		iter[i]="";
		iter[i]+="v"+i;
		iter[i]+="";
	}
    if (otherArgs.length != 2) {
        System.err.println("Usage: PageRank <in> <out>");
        System.exit(2);
    }
		
		
		//job1
		Job job = new Job(conf, "PageRank1");
		job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		job.setMapperClass(PRMapper1.class);
		job.setReducerClass(PRReducer1.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"v0"));
		
		job.waitForCompletion(true);
		
		//job2
		int count = 0;
		
		for(int runs = 0 ; runs < ITERATIONS*2; runs+=2){
			
			Configuration conf2 = new Configuration();
			Job job2 = new Job(conf2, "PageRank2");
			job2.setJarByClass(PageRank.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setMapperClass(PRMapper2.class);
			job2.setReducerClass(PRReducer2.class);			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+iter[runs]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+iter[runs+1]));
					
			//FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"v0"));
			//FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"v1"));
					
			job2.waitForCompletion(true);
				
			Configuration conf3 = new Configuration();
			Job job3 = new Job(conf3, "PageRank3");
			job3.setJarByClass(PageRank.class);           
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setMapperClass(PRMapper3.class);   
			job3.setReducerClass(PRReducer3.class);		
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setInputFormatClass(TextInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);
				
			FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+iter[runs+1]));
			FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+iter[runs+2]));
			//FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+"v1"));
			//FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"v2"));
						
			job3.waitForCompletion(true);
				
			count=runs+2;
        }
		
		
		//job4	

		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4, "PageRank4");
		job4.setJarByClass(PageRank.class);           
        job4.setMapperClass(PRMapper4.class); 
		job4.setReducerClass(PRReducer4.class);
		job4.setMapOutputKeyClass(DoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);		

		//job4.setOutputKeyClass(DoubleWritable.class);
		job4.setOutputKeyClass(Text.class);
        //job4.setOutputValueClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		
		job4.setSortComparatorClass(myComparator.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job4, new Path(otherArgs[1]+iter[count]));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]+"vf"));
		
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		
		//System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}



/*
ssh root@127.0.0.1 -p 2222

mvn clean package

cd PageRank/target 
yarn jar PageRank-FV.jar org.apache.hadoop.examples.PageRank /user/root/data/input.txt output/out_
hadoop fs -cat /user/root/output/out_vf/*

hadoop fs -copyFromLocal input2.txt /user/root/data/


*/
/*true iter1
[[0.17333333333333334]
 [0.11999999999999998]
 [0.11999999999999998]
 [0.33333333333333337]
 [0.09333333333333331]]

[[0.20533333333333334]
 [0.152]
 [0.152]
 [0.3653333333333334]
 [0.12533333333333332]]
*/
/* true iter 10
[[0.14049063638292908]
 [0.11725833890878451]
 [0.11725833890878451]
 [0.2764114434145622]
 [0.08530693567721104]]
 
[[0.1931454977244748]
 [0.16991320025033024]
 [0.16991320025033024]
 [0.32906630475610793]
 [0.13796179701875677]]

*/

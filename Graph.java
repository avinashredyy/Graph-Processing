package edu.uta.cse6331;


import java.io.*;
import org.apache.hadoop.fs.Path;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


public class Graph
{
	public static class  Mapr1 extends Mapper<LongWritable,Text,Text,Text>
	{
		@Override
		public void map(LongWritable ky, Text line, Context cntxt) throws IOException, InterruptedException {

			String splitString = line.toString();

			String value = null;
			String jkey = null;
			StringTokenizer st = new StringTokenizer(splitString, "\\n");
			while (st.hasMoreElements()) {
				value = st.nextElement().toString();
				System.out.println(value);
				String [] key = value.split(",");
				jkey = key[0];
			}

			cntxt.write(new Text(jkey),new Text(value));


		}

	}
	public static class Redcr1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce( Text ky, Iterable<Text> vl, Context cntxt) throws IOException, InterruptedException {


			String old_key = ky.toString();
			String new_key = old_key +","+"0";
			String value = null;
			for(Text val: vl)	
			{
				value = val.toString();

			}
			String new_val = value;
			cntxt.write(new Text(new_key),new Text(new_val));

		}
	}
	public static class Mapr2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable ky, Text vl, Context cntxt) throws IOException, InterruptedException {


			Multimap<String, String> myMultimap = ArrayListMultimap.create();
			System.out.println("OUT");
			String [] val_old = vl.toString().split("\\t");
			System.out.println(val_old.length);
			String[] key_o = val_old[0].split(",");
			System.out.println(key_o.length);
			String new_key = key_o[0];
			String [] stack = val_old[1].split(",");
			for (int i=0;i<stack.length;i++)
			{
				myMultimap.put(key_o[0],stack[i]);
			}
			Collection<String> nodes = myMultimap.get(key_o[0]);
			System.out.println("key = " + key_o); 
			System.out.println("values = " + nodes); 
		}
	}


	public static void main ( String[] args ) throws Exception 
	{
		Job jb1 = Job.getInstance();

		jb1.setJobName("JOB 1");
		jb1.setJarByClass(Graph.class);
		jb1.setMapperClass(Mapr1.class);
		jb1.setReducerClass(Redcr1.class);
		jb1.setInputFormatClass(TextInputFormat.class);
		jb1.setOutputFormatClass(TextOutputFormat.class);
		jb1.setOutputKeyClass(Text.class);
		jb1.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(jb1,new Path(args[0]));
		FileOutputFormat.setOutputPath(jb1,new Path(args[1]));
		jb1.waitForCompletion(true);


		Job jb2 = Job.getInstance();
		jb2.setJobName("JOB 2");
		jb2.setJarByClass(Graph.class);
		jb2.setMapperClass(Mapr2.class);
		jb2.setInputFormatClass(TextInputFormat.class);
		jb2.setOutputFormatClass(TextOutputFormat.class);
		jb2.setOutputKeyClass(Text.class);
		jb2.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(jb2,new Path(args[1]));
		FileOutputFormat.setOutputPath(jb2,new Path(args[2]));
		System.exit(jb2.waitForCompletion(true) ? 0 : 1);
	}

}


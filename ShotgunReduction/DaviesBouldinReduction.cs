using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.HdfsExtras.Hdfs;
namespace ShotgunReduction
{
    class DaviesBouldinReduction : HadoopJob<DBReduxMapper, DBReduxReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            var config = new HadoopJobConfiguration();
            config.InputPath = "output/clustering/shotgun/dbIndeces";
            config.OutputFolder = "output/clustering/shotgun/final";
            return config;
        }
        
    }
    class DBReduxMapper : MapperBase 
    {
        public override void Map(string inputLine, MapperContext context)
        {
            string[] split = inputLine.Split('\t');
            string[] keySplit = split[0].Split(',');

            context.EmitKeyValue(keySplit[0], keySplit[1] + ", " + split[1]);
        }
    }


    class DBReduxReducer : ReducerCombinerBase
    {
        List<List<ClusterPoint>> centroids = new List<List<ClusterPoint>>();

        public void processLine(String input, ReducerCombinerContext context)
        {
            List<ClusterPoint> cluster = new List<ClusterPoint>();
            //context.Log(input);
            String[] points = input.Split('\t');
            foreach (String point in points)
            {
                try
                {

                    String[] split = point.Split(',');

                    double x = double.Parse(split[0]);

                    double y = double.Parse(split[1]);
                    ClusterPoint cp = new ClusterPoint(x, y);
                    
                    //context.Log(cp.ToString());
                    cluster.Add(cp);
                }

                catch (System.IndexOutOfRangeException e)
                {
                    context.Log("malformed input: " + point);
                }
                catch (System.FormatException f)
                {
                    context.Log("malformed input: " + point);
                }
            }
            centroids.Add(cluster);

        }

        public override void Initialize(ReducerCombinerContext context)
        {
            String[] stats = HdfsFile.ReadAllLines("/user/Aegon/output/clustering/shotgun/part-00000");
            foreach (String line in stats)
            {
                processLine(line, context);
            }
            base.Initialize(context);
        }

        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            int clusterNumber = int.Parse(key);
            List<ClusterPoint> cluster = centroids[clusterNumber];
            double[] s = new double[cluster.Count()];
            double[,] r = new double[cluster.Count(),cluster.Count()];
            double[,] m = new double[cluster.Count(), cluster.Count()];
            double[] d = new double[cluster.Count()];
            context.Log("Number of centroids: " + cluster.Count().ToString());
            foreach (string value in values)
            {
                //context.Log("reading input from mapper");
                //context.Log("line: " + value);
                string[] split = value.Split(',');
                int index = int.Parse(split[0]);
                double distance = double.Parse(split[1]);
                //context.Log(cluster.Count().ToString());
                //context.Log("parsed data");
                
                s[index] = distance;
                context.Log(s[index].ToString());
            }
            for (int i = 0; i < cluster.Count(); i++)
            {
                //context.Log("creating [][]");
                double maxR =-1;
                for (int j = 0; j < cluster.Count(); j++)
                {
                    if (i == j)
                    {
                        continue;
                    }
                    m[i,j] = cluster[i].SquaredDistance(cluster[j]);
                    //context.Log("Dist between centroids: "+m[i,j].ToString());
               
                    r[i, j] = (s[i] + s[j]) / m[i, j];
                   
              
                    if (r[i, j] > maxR)
                    {
                        maxR = r[i, j];
                    }
                }
                d[i] = maxR;
                context.Log("D[i]: " + d[i]);
            }
            double dbIndex = 0;
            foreach (double val in d)
            {
                dbIndex += val;
            }
            context.Log(dbIndex.ToString());
            context.Log(cluster.Count().ToString());
            context.Log(d.Length.ToString());
            dbIndex = dbIndex / cluster.Count();
            

            context.EmitKeyValue(dbIndex.ToString(), cluster.Count().ToString());
            

        }
    }
}

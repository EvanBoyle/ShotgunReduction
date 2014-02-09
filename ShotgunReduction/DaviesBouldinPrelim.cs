using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.HdfsExtras.Hdfs;

namespace ShotgunReduction
{
    //colects prelim information for DB index, computing s[] for each clustering
    class DaviesBouldinPrelim : HadoopJob<DBMapper, DBCombiner, DBReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            var config = new HadoopJobConfiguration();
            config.InputPath = "input/clustering";
            config.OutputFolder = "output/clustering/shotgun/dbIndeces";

            return config;
        }
    }

    class DBMapper : MapperBase
    {
        List<List<ClusterPoint>> centroids = new List<List<ClusterPoint>>();

        public void processLine(String input, MapperContext context)
        {
            List<ClusterPoint> cluster = new List<ClusterPoint>();
            String[] points = input.Split('\t');
            foreach (String point in points)
            {
                try
                {
                    String[] split = point.Split(',');
                    
                    double x = double.Parse(split[0]);

                    double y = double.Parse(split[1]);
                    ClusterPoint cp = new ClusterPoint(x, y);
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

        public override void Initialize(MapperContext context)
        {
            String[] stats = HdfsFile.ReadAllLines("/user/Aegon/output/clustering/shotgun/part-00000");
            foreach(String line in stats){
                processLine(line, context);
            }
            base.Initialize(context);
        }

        public override void Map(string inputLine, MapperContext context)
        {
            string[] split = inputLine.Split(',');
            var x = double.Parse(split[0]);
            var y = double.Parse(split[0]);
            ClusterPoint current = new ClusterPoint(x, y);
            int clusterNumber = 0;
            foreach (List<ClusterPoint> cluster in centroids)
            {   
                
                double minDist = double.MaxValue;
                ClusterPoint closest = null;
                int index = 0;
                int count = 0;
                foreach (ClusterPoint centroid in cluster)
                {
                    if (centroid.SquaredDistance(current) < minDist)
                    {
                        minDist = centroid.SquaredDistance(current);
                        closest = centroid;
                        index = count;
                    }
                    count++;
                }
                context.EmitKeyValue(clusterNumber.ToString() + ", "+count.ToString(), minDist.ToString()+ ", 1");
                clusterNumber++;
            }
            
        }

    }

    class DBReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            double dbDist = 0;
            double dbCount = 0;
            foreach (string value in values)
            {
                string[] split = value.Split(',');
                double dist = double.Parse(split[0]);
                double count = double.Parse(split[1]);
                dbDist = dbDist / (dbCount + count) + (count * dist) / (dbCount + count);
                dbCount += count;
            }
            context.EmitKeyValue(key, dbDist.ToString());
        }
    }

    class DBCombiner : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            double dbDist = 0;
            double dbCount = 0;
            foreach (string value in values)
            {
                string[] split = value.Split(',');
                double dist = double.Parse(split[0]);
                double count = double.Parse(split[1]);
                dbDist = dbDist / (dbCount + count) + (count * dist) / (dbCount + count);
                dbCount += count;
            }
            context.EmitKeyValue(key, dbDist.ToString() + ", " + dbCount.ToString());
        }
    }
}

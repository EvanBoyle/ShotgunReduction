using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop;
using Microsoft.Hadoop.MapReduce;

namespace ShotgunReduction
{
    class MeanStDev : HadoopJob<StatMapper, StatReducer>
    {
        //Job calculates the mean and stdev of the distances between points in 100 samples
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            var config = new HadoopJobConfiguration();
            config.InputPath = "input/clustering";
            config.OutputFolder = "output/clustering/prelimStats";
            config.DeleteOutputFolder = true;


            return config;

        }
    }


    class StatMapper : MapperBase
    {
        Random rGen = new Random();

        public override void Map(string inputLine, MapperContext context)
        {

            //only testing 10% of points
            int rand = rGen.Next(0, 10);
            if (rand == 0)
            {
                //creating 100 maps 1mil distance calculations per map for this data set
                rand = rGen.Next(0, 100);

                context.Log(rand.ToString());

                context.EmitKeyValue(rand.ToString(), inputLine);

            }



        }

    }

    class StatReducer : ReducerCombinerBase
    {
        public double count = 0;
        public double sum = 0;
        public double m = 0;
        public double s = 0;
        public double tmpM = 0;
        public List<ClusterPoint> points = new List<ClusterPoint>();

        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            foreach (string value in values)
            {
                string[] split = value.Split(',');
                double xval = double.Parse(split[0]);
                double yval = double.Parse(split[1]);
                ClusterPoint current = new ClusterPoint(xval, yval);
                points.Add(current);

            }
            //stat calculations
            for (int i = 0; i < points.Count - 1; i++)
            {
                for (int j = i + 1; j < points.Count; j++)
                {
                    ClusterPoint p1 = points[i];
                    ClusterPoint p2 = points[j];
                    double distance = p1.SquaredDistance(p2);
                    count++;
                    sum += distance;
                    tmpM = m;
                    m += (distance - tmpM) / count;
                    s += (distance - tmpM) * (distance - m);

                }
            }
            double stDev = Math.Sqrt((s / count));
            double mean = sum / count;
            //write out stats
            context.EmitKeyValue("StDev", stDev.ToString());
            context.EmitKeyValue("Mean", mean.ToString());
            count = 0;
            sum = 0;
            m = 0;
            s = 0;
            tmpM = 0;

        }


    }
}

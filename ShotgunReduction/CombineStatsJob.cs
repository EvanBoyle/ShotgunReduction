using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace ShotgunReduction
{

    //takes the 100 sample Means and StDevs from MeanStDev.cs and averages those values together
    //not sure if this is the best approach, will reexamine later, time permitting
    //could add a combiner to this to make it faster
    class CombineStatsJob : HadoopJob<CombineStatsMapper, CombineStatsReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            var config = new HadoopJobConfiguration();
            config.InputPath = "output/clustering/prelimStats";
            config.OutputFolder = "output/clustering/finalStats";
            return config;
        }
    }

    class CombineStatsMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            string[] split = inputLine.Split('\t');
            context.EmitKeyValue(split[0], split[1]);
        }

    }

    class CombineStatsReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            double sum = 0;
            double count = 0;
            foreach (string value in values)
            {
                count++;
                sum += double.Parse(value);

            }
            double avg = sum / count;
            context.EmitKeyValue(key, avg.ToString());
        }

    }
}

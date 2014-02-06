using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;
namespace ShotgunReduction
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            //var result = hadoop.MapReduceJob.ExecuteJob<MeanStDev>();
            //var result = hadoop.MapReduceJob.ExecuteJob<CombineStatsJob>();
            var result = hadoop.MapReduceJob.ExecuteJob<ShotgunJob>();
        }
    }
}

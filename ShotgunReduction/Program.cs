using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.Client;
using System.Threading;
namespace ShotgunReduction
{
    class Program
    {

        private static void WaitForJobCompletion(JobCreationResults jobResults, IJobSubmissionClient client)
        {
            JobDetails jobInProgress = client.GetJob(jobResults.JobId);
            while (jobInProgress.StatusCode != JobStatusCode.Completed && jobInProgress.StatusCode != JobStatusCode.Failed)
            {
                jobInProgress = client.GetJob(jobInProgress.JobId);
                Thread.Sleep(TimeSpan.FromSeconds(10));
            }
        }
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            //var result = hadoop.MapReduceJob.ExecuteJob<MeanStDev>();
            //var result = hadoop.MapReduceJob.ExecuteJob<CombineStatsJob>();
            //var result = hadoop.MapReduceJob.ExecuteJob<ShotgunJob>();
            //var result = hadoop.MapReduceJob.ExecuteJob<DaviesBouldinPrelim>();
            var result = hadoop.MapReduceJob.ExecuteJob<DaviesBouldinReduction>();
            
        }
    }
}

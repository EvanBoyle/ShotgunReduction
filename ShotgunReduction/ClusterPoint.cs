using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShotgunReduction
{
    public class ClusterPoint
    {
        //clusterPoint class with double precision coordinates and distance function
        public double x;
        public double y;
        public double count;

        public void AdjustCentroid(ClusterPoint other)
        {

            x = (count * x) / (count + 1) + other.x / (count + 1);
            y = (count * y) / (count + 1) + other.y / (count + 1);
            count++;
        }

        public ClusterPoint(double xVal, double yVal)
        {
            x = xVal;
            y = yVal;
            count = 1;

        }

        public double SquaredDistance(ClusterPoint other)
        {
            double xComp = x - other.x;
            double yComp = y - other.y;
            return xComp * xComp + y * yComp;
        }
    }
}

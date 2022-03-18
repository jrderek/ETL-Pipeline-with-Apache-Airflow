# ----------------------------------------------------------------------
#                  get resources from aws s3
# ----------------------------------------------------------------------
# place in your s3 bucket name: s3://your-bucket-name/
aws s3 cp s3://your-bucket-name/capstone/python/ $HOME/python/ --recursive
aws s3 cp s3://your-bucket-name/capstone/resources/ $HOME/resources/ --recursive


# ----------------------------------------------------------------------
#                Set important Environment Variables
# ----------------------------------------------------------------------
export SPARK_HOME=/usr/lib/spark
export PYSPARK_PYTHON=/usr/bin/python3


# ----------------------------------------------------------------------
#                Apply important configs
# ----------------------------------------------------------------------
#sudo sh -c "sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh"

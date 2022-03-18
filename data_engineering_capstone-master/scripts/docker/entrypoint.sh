#!/usr/bin/env bash


AIRFLOW_HOME="/opt/airflow"
CMD="airflow"


echo "Copy scripts to AWS S3 ..."
exec aws s3 cp /opt/scripts/ s3://$AWS_S3_BUCKET/capstone/scripts/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers --recursive &
exec aws s3 cp /opt/python/ s3://$AWS_S3_BUCKET/capstone/python/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers --recursive &
exec aws s3 cp /opt/resources/ s3://$AWS_S3_BUCKET/capstone/resources/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers --recursive &
sleep 10


## update airflow config - fernet key
#sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg


## start airflow
if [ "$1" = "initdb" ]
then
    echo "Airflow is on version ..."
    exec $CMD version &
    sleep 5
    echo "Initialize database ..."
    exec $CMD initdb &
    sleep 5
    echo "Create connections & variables..."
    exec $CMD connections -a --conn_id aws_credentials --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY --conn_type aws &
    exec $CMD connections -a --conn_id aws_emr_ssh --conn_login $AWS_EMR_SSH_USER --conn_password $AWS_EMR_SSH_PW --conn_type ssh --conn_extra '{"key_file": "/opt/'$AWS_SSH_KEY_FILE'"}' &
    exec $CMD connections -a --conn_id aws_redshift_db --conn_login $AWS_REDSHIFT_USER --conn_password $AWS_REDSHIFT_PW --conn_type postgres --conn_port 5439 --conn_schema $AWS_REDSHIFT_SCHEMA &

    exec $CMD variables -i /opt/airflow/airflow_variables.json

elif [ "$1" = "webserver" ]
then
    echo "Airflow is on version ..."
    exec $CMD version &
    sleep 5
    echo "Starting webserver ..."
    exec $CMD webserver

elif [ "$1" = "scheduler" ]
then
    echo "Airflow is on version ..."
    exec $CMD version &
    sleep 5
    echo "Starting scheduler ..."
    exec $CMD scheduler
fi

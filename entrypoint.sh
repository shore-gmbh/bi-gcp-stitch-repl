#!/bin/bash

# Initialize the database
airflow db init

# Reset the database
#airflow db reset

#supervisord -c /path/to/supervisord.conf

# Run the supervisor to manage the Airflow processes
exec supervisord -c /etc/supervisord.conf

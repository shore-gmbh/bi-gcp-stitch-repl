#!/bin/bash

# Initialize the database
#airflow db init

# Reset the database
#airflow db reset

#supervisord -c /path/to/supervisord.conf

#airflow users create -u franc_23 -f francis -l dcunha -r Admin -e francis.dcunha@shore.com -p 321aydc11be

# Run the supervisor to manage the Airflow processes
exec supervisord -c /etc/supervisord.conf

# Spark functions for identifying user sessions

This module provides functions for user session identification based on timestamps, user_ids, product_ids, and event_ids
and assignment the same user_session_ids to the table's rows representing the same user session.
The user_session_ids have format user_id#product_code#timestamp. 

This implementation supports 2 policies (tb and sc) of user session identification.

The example of the module's work with different user session identification policies 
is presented in the `./data` folder.

### How to create environment and run
Create a virtual environment and install necessary python packages
```shell
apt-get install python3.9-dev python3.9-venv
python3.9 -m venv venv
source venv/bin/activate
pip3 install wheel
pip3 install -r requirements.txt

# (optional) TEST the spark functions WITH THE DEFAULT DATA
python3 compute_session_id.py  # write handled test tables to ./data
```

Compute session id for your own data
```shell
python3 main.py \
  -i <input file path> \
  -o <output file path> \
  -p <user session definition policy: "tb" (time bounded) or "sc" (start & close)> \
  -t <time threshold for the tb policy in seconds>
```

The handler function takes as input a table with the following columns:  
`user_id` – a user’s anonymized identifier;  
`event_id` – identifier of an event that happened inside an IDE.  
Each event corresponds to action either of a user or an IDE itself. 
For the sake of simplicity we assume that an event is a user action if event_id in (‘a’, ‘b’, ‘c’);  
`timestamp` in "yyyy-MM-dd HH:mm:ss" format;  
`product_code` – shortened name of an IDE;


### User session definition
There are different versions of what to consider a user session.
This module provides an implementation for 2 user session identification policies.

**Policy 1: time-bounded actions (tb).**  
A user session is a set of actions performed by a user or an IDE
with a short time interval between these actions.  
With this policy, all the rows get a user session id.

**Policy 2: actions between start and close (sc).**  
A user session is a set of all events for a distinct user that happened 
between the events 'ide.start' and 'ide.close'. 
If an ide was opened but was not yet closed, this set of actions is also considered as a session.  
With this policy, some rows do not belong to any user session.

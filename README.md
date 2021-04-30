# CSE541 Spring'21 - Two-Phase Commit Data Store

## To run
1. Make sure Python 3.9 and pipenv are installed on your system.
2. Go into the project directory and run `pipenv install`
3. Once the dependencies have finished installing, run `pipenv shell`
4. If running 3 servers locally for testing, make sure `server.py`, `server2.py`, and `server3.py` are all being launched on different ports by looking at line number 217.
5. Then you can run `python3 <server/server1/server2>.py` to run the server. Enter a server ID matching the order of the server inside the config.
6. If you're running a client instead, then run `python <client_file_name>.py`
7. The config file should contain the addresses of the servers, in order of the server IDs given to each server.

## Client Specific considerations
1. We offer 5 different clients in this project: `db_client.py`, `mongo_client.py`, `azure_client.py`, `cockroach_client.py`, and `yugabyte_client.py`.
2. For `db_client.py`, just make sure that the config file is updated with the IP addresses of the 2PC servers.
3. For `mongo_client.py`, double check that the connection string points to your replica set.
3. Similarly, for `azure_client.py` check that the connection string matches what is shown on the Azure CosmosDB portal.
4. For `cockroach_client.py` the database connection string has to be passed as a command line parameter.
5. Finally, for `yugabyte_client.py` lines 19 and 22 contain auth credentials and cluster master IP which need to be updated to point to your cluster.
6. Each client outputs performance metrics and also generates a CSV file containing the latencies of each request.
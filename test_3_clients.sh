python3 abd_client_api.py&python3 abd_client_api2.py&python3 abd_client_api3.py
sort -u --files0-from=<(printf '%s\0' ./*.edn) -o output
rm -rf *abd_log.edn
cut -f3 output
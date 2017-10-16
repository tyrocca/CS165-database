# db_manager.c
- clean up file
- find a home for get commands

# General
- try to determine a good way to use status and message status (right now status contains all of the things for message status. It may be a good idea to make it so we can track each query...

### Loading
Todos:
- Loading in the CSV is tricky
- Currently we read all of the table in - what should be done instead?
- Shutdown - what should I do here - I'm a bit confused

Notes:
- The current system can support multiple databases. Each database is stored in the databases.bin
- Why do we check the status on the i/o (in the client.c file)

### Improvements
- Should I use mmap for storage
    - Could we make better checks for creating tables etc...
- Improve the lookup for table names (figure out a way to speed up processing here)
- Do something around the multiple databases that I support

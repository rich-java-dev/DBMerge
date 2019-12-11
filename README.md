# DBMerge

## Tool designed to stream-line data migrations, particularly partial data installs/merging data on top of an existing 'live' database.

    * Select tables from a source database, and migrate into another.
    * Stores database tables into flat files, and then (optionally) imports them into a target database.
    * Supports MSSQL, MYSQL, Oracle, and AS400 JDBC connections
    * Uses MSSQL/MYSQL import, or traditional 'INSERT' statement syntax


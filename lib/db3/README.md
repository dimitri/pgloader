# DB3

A lib to read dbf files version 3.

## Database file structure

The structure of a dBASE III database file is composed of a header
and data records.  The layout is given below.

### dBASE III DATABASE FILE HEADER:

    +---------+-------------------+---------------------------------+
    |  BYTE   |     CONTENTS      |          MEANING                |
    +---------+-------------------+---------------------------------+
    |  0      |  1 byte           | dBASE III version number        |
    |         |                   |  (03H without a .DBT file)      |
    |         |                   |  (83H with a .DBT file)         |
    +---------+-------------------+---------------------------------+
    |  1-3    |  3 bytes          | date of last update             |
    |         |                   |  (YY MM DD) in binary format    |
    +---------+-------------------+---------------------------------+
    |  4-7    |  32 bit number    | number of records in data file  |
    +---------+-------------------+---------------------------------+
    |  8-9    |  16 bit number    | length of header structure      |
    +---------+-------------------+---------------------------------+
    |  10-11  |  16 bit number    | length of the record            |
    +---------+-------------------+---------------------------------+
    |  12-31  |  20 bytes         | reserved bytes (version 1.00)   |
    +---------+-------------------+---------------------------------+
    |  32-n   |  32 bytes each    | field descriptor array          |
    |         |                   |  (see below)                    | --+
    +---------+-------------------+---------------------------------+   |
    |  n+1    |  1 byte           | 0DH as the field terminator     |   |
    +---------+-------------------+---------------------------------+   |
                                                                        |
                                                                        |
    A FIELD DESCRIPTOR:      <------------------------------------------+
    
    +---------+-------------------+---------------------------------+
    |  BYTE   |     CONTENTS      |          MEANING                |
    +---------+-------------------+---------------------------------+
    |  0-10   |  11 bytes         | field name in ASCII zero-filled |
    +---------+-------------------+---------------------------------+
    |  11     |  1 byte           | field type in ASCII             |
    |         |                   |  (C N L D or M)                 |
    +---------+-------------------+---------------------------------+
    |  12-15  |  32 bit number    | field data address              |
    |         |                   |  (address is set in memory)     |
    +---------+-------------------+---------------------------------+
    |  16     |  1 byte           | field length in binary          |
    +---------+-------------------+---------------------------------+
    |  17     |  1 byte           | field decimal count in binary   |
    +---------+-------------------+---------------------------------+
    |  18-31  |  14 bytes         | reserved bytes (version 1.00)   |
    +---------+-------------------+---------------------------------+

### The data records are layed out as follows:

  1. Data records are preceeded by one byte that is a space (20H) if the
     record is not deleted and an asterisk (2AH) if it is deleted.

  2. Data fields are packed into records with no field separators or record
     terminators.

  3. Data types are stored in ASCII format as follows:

   DATA TYPE      DATA RECORD STORAGE
   ---------      --------------------------------------------
   Character      (ASCII characters)
   Numeric        - . 0 1 2 3 4 5 6 7 8 9
   Logical        ? Y y N n T t F f  (? when not initialized)
   Memo           (10 digits representing a .DBT block number)
   Date           (8 digits in YYYYMMDD format, such as
                    19840704 for July 4, 1984)
   -----------------------------------------------------------



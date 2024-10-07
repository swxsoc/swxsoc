.. _retrieving_data:

**************************************
Retrieving and Downloading Data
**************************************

The `~swxsoc.util.util.SWXSOCClient` class in the `swxsoc` package is a FIDO client used for querying and downloading data from the SWxSOC buckets. Below are some examples demonstrating how to use this client for searching and downloading data.

.. warning::

    To download the data, you must have access to the SWxSOC buckets. If you do not have access, please contact the SWxSOC team. The client utilizes either presigned S3 URLs via the `boto3` package to download the data if you have AWS Credentials or it can download via S3 URLs if you have access to the SWxSOC buckets via allowed IP addresses.

Attributes for Searching Data
=============================
The `~swxsoc.util.util.SWXSOCClient` class supports the following attributes for searching data:

- `Instrument`: The instrument name for the data (e.g., 'EEA', 'SHARP').
- `Level`: The data level (e.g., 'L1', 'L2').
- `SearchTime`: The time range for the data (e.g., `('2021-01-01T00:00:00Z', '2021-01-02T00:00:00Z')`).
- `DevelopmentBucket`: Whether to search in the development bucket (e.g., `True`, `False`).

Examples for Searching Data
===========================
Below are some examples demonstrating how to search for data using the `SWXSOCClient` class for different attributes.


Example 1: Search by a combination of Attributes
------------------------------------------------

To search for data based on multiple attributes such as instrument, level, and time::

    # Import necessary modules
    from swxsoc.util.util import SWXSOCClient, AttrAnd, Instrument, Level, SearchTime, DevelopmentBucket

    # Initialize the FIDO client
    fido_client = SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Level("l0"),
            Instrument("eea"),
        ]
    )
    # Search for data
    results = fido_client.search(query)
    print(results)
    >>> instrument mode  test           time          level version ...   size       bucket                     etag                storage_class    last_modified   
    >>>                                                            ...   byte                                                                                       
    >>> ---------- ---- ----- ----------------------- ----- ------- ... ------- ---------------- ---------------------------------- ------------- -------------------
    >>> meddea None False 2024-03-27T13:46:16.000    l0       1 ...  4648.0 dev-padre-meddea "8fca00048426ec8a114750a4de80c161"      STANDARD 2024-08-09 17:12:09
    >>> meddea None  True 2024-03-27T13:46:16.000    l1   0.1.0 ... 25920.0 dev-padre-meddea "4b9c15fc55e8d05dd9b8414e146c51c3"      STANDARD 2024-08-09 17:12:24

Example 2: Search by a single Attribute
----------------------------------------

To search for data based on a single attribute such as instrument::

    # Import necessary modules
    from swxsoc.util.util import SWXSOCClient, AttrAnd, Instrument

    # Initialize the SWxSOC client
    fido_client = util.SWXSOCClient()

    # Test search with a query for specific instrument
    query = AttrAnd([Instrument("meddea")])
    results = fido_client.search(query)
    print(results)
    >>> instrument mode  test           time          level version ...   size      bucket                   etag                storage_class    last_modified   
    >>>                                                             ...   byte                                                                                    
    >>> ---------- ---- ----- ----------------------- ----- ------- ... -------- ------------ ---------------------------------- ------------- -------------------
    >>> meddea None False 2012-04-29T00:00:00.000    l0       1 ... 219672.0 padre-meddea "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-10 18:17:34
    >>> meddea None False 2023-04-30T00:00:00.000    l0       1 ... 219672.0 padre-meddea "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-01 15:07:52
    >>> meddea None False 2012-04-29T00:00:00.000    l1   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:17:58
    >>> meddea None False 2023-04-30T00:00:00.000    l1   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:01
    >>> meddea None False 2012-04-29T00:00:00.000    ql   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:18:01
    >>> meddea None False 2023-04-30T00:00:00.000    ql   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:05


Example 3: Search all data
--------------------------

To search for all data::

    # Import necessary modules
    from swxsoc.util.util import SWXSOCClient

    # Initialize the SWxSOC client
    fido_client = SWXSOCClient()

    # Test search with a query for all data
    results = fido_client.search()
    print(results)
    >>> instrument mode  test           time          level version ...   size      bucket                   etag                storage_class    last_modified   
    >>>                                                             ...   byte                                                                                    
    >>> ---------- ---- ----- ----------------------- ----- ------- ... -------- ------------ ---------------------------------- ------------- -------------------
    >>> meddea None False 2012-04-29T00:00:00.000    l0       1 ... 219672.0 padre-meddea "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-10 18:17:34
    >>> meddea None False 2023-04-30T00:00:00.000    l0       1 ... 219672.0 padre-meddea "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-01 15:07:52
    >>> meddea None False 2012-04-29T00:00:00.000    l1   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:17:58
    >>> meddea None False 2023-04-30T00:00:00.000    l1   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:01
    >>> meddea None False 2012-04-29T00:00:00.000    ql   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:18:01
    >>> meddea None False 2023-04-30T00:00:00.000    ql   0.0.1 ...      0.0 padre-meddea "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:05
    >>> sharp  None False 2012-04-29T00:00:00.000    l0       1 ... 219672.0 padre-sharp  "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-10 18:17:34
    >>> sharp  None False 2023-04-30T00:00:00.000    l0       1 ... 219672.0 padre-sharp  "c1f165f22d8d4190323894a4df26cda4"      STANDARD 2024-07-01 15:07:52
    >>> sharp  None False 2012-04-29T00:00:00.000    l1   0.0.1 ...      0.0 padre-sharp  "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:17:58
    >>> sharp  None False 2023-04-30T00:00:00.000    l1   0.0.1 ...      0.0 padre-sharp  "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:01
    >>> sharp  None False 2012-04-29T00:00:00.000    ql   0.0.1 ...      0.0 padre-sharp  "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-10 18:18:01
    >>> sharp  None False 2023-04-30T00:00:00.000    ql   0.0.1 ...      0.0 padre-sharp  "d41d8cd98f00b204e9800998ecf8427e"      STANDARD 2024-07-01 15:08:05

Downloading Data
================
The `~swxsoc.util.util.SWXSOCClient` class also supports downloading data from the SWxSOC buckets. Below are some examples demonstrating how to queue the download of data using this client. Note this requires the `~swxsoc.util.util.SWXSOCClient` class to have already been used to search for data as well as a parfive Downloader object to be initialized.

For more information on the `parfive` package, see the `parfive documentation <https://parfive.readthedocs.io/en/latest/api/parfive.Downloader.html>`_.

Example to Download Data
------------------------
Below is an example demonstrating how to download data using the `~swxsoc.util.util.SWXSOCClient` class::

    # Import necessary modules
    from swxsoc.util.util import SWXSOCClient, AttrAnd, Instrument, Level, SearchTime, DevelopmentBucket
    from parfive import Downloader

    # Initialize the SWxSOC client
    fido_client = SWXSOCClient()

    # Test search with a query for specific instrument, level, and time
    query = AttrAnd(
        [
            SearchTime("2024-01-01", "2025-01-01"),
            DevelopmentBucket(False),
            Level("l0"),
            Instrument("eea"),
        ]
    )

    # Search for data
    results = fido_client.search(query)

    # Initialize a parfive Downloader object
    dl = Downloader()

    # Queue the download of the data to specific path
    fido_client.fetch(query_results=results, downloader=dl, path="path/to/download")

    # Start the download
    dl.download()
    >>> Files Downloaded: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:00<00:00,  2.59file/s]
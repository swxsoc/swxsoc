.. _retrieving_data:

**************************************
Retrieving and Downloading Data
**************************************



The SWxSOC package provides two main FIDO-compatible data access clients for searching and downloading data:

- :class:`~swxsoc.util.data_access.S3DataClient`: For accessing data in the SWxSOC S3 buckets (requires AWS credentials and access).
- :class:`~swxsoc.util.data_access.HTTPDataClient`: For accessing data in the public SWxSOC HTTP archive (no credentials required).


Both clients provide the same interface for searching and downloading data, and can be used interchangeably in your code. 
The S3 client is primarily intended for automated pipelines and users with AWS access, while the HTTP client is intended for researchers and the general public.


**File Management:**

SWxSOC data is distributed as CDF or FITS files. 
Both clients allow you to search for and download these files. 
You can use the returned results with your preferred CDF or FITS file readers (e.g., :mod:`spacepy.pycdf`, :mod:`astropy.io.fits`).



.. warning::
    To use the :class:`~swxsoc.util.data_access.S3DataClient`, you must have access to the SWxSOC S3 buckets and valid AWS credentials. 
    If you do not have access, please contact the SWxSOC team. 
    The :class:`~swxsoc.util.data_access.HTTPDataClient` does not require credentials and is suitable for most research and public use cases.



Attributes for Searching Data
=============================
Both :class:`~swxsoc.util.data_access.S3DataClient` and :class:`~swxsoc.util.data_access.HTTPDataClient` support the following search attributes:

- :class:`~swxsoc.util.data_access.Instrument`: The instrument name for the data (e.g., 'EEA', 'SHARP').
- :class:`~swxsoc.util.data_access.Level`: The data level (e.g., 'L1', 'L2', 'raw', etc.).
- :class:`~swxsoc.util.data_access.SearchTime`: The time range for the data (e.g., `('2021-01-01T00:00:00Z', '2021-01-02T00:00:00Z')`).
- :class:`~swxsoc.util.data_access.DataType`: The data type or descriptor (e.g., 'housekeeping', 'spectrum', 'photon').
- :class:`~swxsoc.util.data_access.DevelopmentBucket`: Whether to search in the development bucket (S3 only; e.g., `True`, `False`).


Examples for Searching Data
===========================
Below are some examples demonstrating how to search for data using either the `S3DataClient` or `HTTPDataClient` classes. The interface is the same for both clients.



Example 1: Search by a combination of Attributes
------------------------------------------------

To search for data based on multiple attributes such as instrument, level, time, and data type::

    from swxsoc.util.data_access import S3DataClient, HTTPDataClient, AttrAnd, Instrument, Level, SearchTime, DataType, DevelopmentBucket

    # Choose your client:
    # For S3 (requires AWS credentials):
    client = S3DataClient()
    # For HTTP (public archive):
    # client = HTTPDataClient()

    query = AttrAnd([
        SearchTime("2024-01-01", "2025-01-01"),
        DevelopmentBucket(False),  # Only for S3DataClient
        Level("l0"),
        Instrument("eea"),
        DataType("housekeeping"),
    ])
    results = client.search(query)
    print(results)
    >>> instrument mode  test           time          level version ...   size       bucket                     etag                storage_class    last_modified   
    >>>                                                            ...   byte                                                                                       
    >>> ---------- ---- ----- ----------------------- ----- ------- ... ------- ---------------- ---------------------------------- ------------- -------------------
    >>> meddea None False 2024-03-27T13:46:16.000    l0       1 ...  4648.0 dev-padre-meddea "8fca00048426ec8a114750a4de80c161"      STANDARD 2024-08-09 17:12:09
    >>> meddea None  True 2024-03-27T13:46:16.000    l1   0.1.0 ... 25920.0 dev-padre-meddea "4b9c15fc55e8d05dd9b8414e146c51c3"      STANDARD 2024-08-09 17:12:24

Example 2: Search by a single Attribute
---------------------------------------

To search for data based on a single attribute such as instrument::

    from swxsoc.util.data_access import HTTPDataClient, AttrAnd, Instrument

    client = HTTPDataClient()
    query = AttrAnd([Instrument("meddea")])
    results = client.search(query)
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

    from swxsoc.util.data_access import HTTPDataClient

    client = HTTPDataClient()
    results = client.search()
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
Both `S3DataClient` and `HTTPDataClient` support downloading data using the same interface. Below is an example demonstrating how to queue and download data using either client. Note that you must first search for data and initialize a `parfive.Downloader` object.

For more information on the `parfive` package, see the `parfive documentation <https://parfive.readthedocs.io/en/latest/api/parfive.Downloader.html>`_.

Example to Download Data
------------------------

Below is an example demonstrating how to download data using the client class::

    from swxsoc.util.data_access import HTTPDataClient, AttrAnd, Instrument, Level, SearchTime, DataType
    from parfive import Downloader

    client = HTTPDataClient()
    query = AttrAnd([
        SearchTime("2024-01-01", "2025-01-01"),
        Level("l0"),
        Instrument("eea"),
        DataType("housekeeping"),
    ])
    results = client.search(query)

    dl = Downloader()
    client.fetch(query_results=results, downloader=dl, path="path/to/download")
    dl.download()
    >>> Files Downloaded: 100% 2/2 [00:00<00:00,  2.59file/s]
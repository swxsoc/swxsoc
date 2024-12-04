.. _grafana_annotation_management:

Managing Grafana Annotations with the `swxsoc` Package
======================================================

This guide provides a detailed overview of how to use the new annotation management functions in the `swxsoc` package to interact with Grafana. These functions allow you to query, create, and remove annotations for solar events such as flares or any other events of interest, enabling seamless integration with dashboards.

Overview
--------
The following functions are introduced for managing Grafana annotations:

- `~swxsoc.util.util.query_annotations`: Retrieve annotations within a specified timeframe and optionally filter by tags, dashboard, and panel names.
- `~swxsoc.util.util.create_annotation`: Create a new annotation with custom details like start time, end time, tags, and descriptive text.
- `~swxsoc.util.util.remove_annotation_by_id`: Remove annotations by their unique ID.

Prerequisites
-------------
Ensure the following environment variables are set before using these functions:

- **SWXSOC_MISSION**: Automatically set when testing from an instrument package. Specifies the mission context for annotations.
- **GRAFANA_API_KEY**: Used to authenticate requests to the Grafana API.

Getting a Grafana API Key via a Service Account
===============================================

To manage annotations in Grafana using the `swxsoc` package, you need a **service account API key**. Grafana uses service accounts to provide secure, role-based access for automation purposes. Follow the steps below to obtain and securely store your service account API key.

Creating a Service Account API Key
----------------------------------
1. **Log in to Grafana**:

   - Navigate to your organization's Grafana instance (e.g., `https://grafana.<mission>.swsoc.smce.nasa.gov/`).
   
   - Use your credentials to log in.

2. **Access the Service Accounts Section**:

   - In the left-hand menu, go to **"Administration"** > **Users and Access** > **"Service Accounts"** (`https://grafana.<mission>.swsoc.smce.nasa.gov/org/serviceaccounts/`).

3. **Create a New Service Account**:

   - Click **"New Service Account"**.

   - Provide a name and optional description (e.g., `SWxSOC Annotation Management`).

   - Assign an appropriate role. For annotation management, the **Editor** role is sufficient.

   - Click **"Create"** to generate the service account.

4. **Generate an API Key**:

   - In the service account details, click **"Add service account token"**.

   - Provide a name for the API key (e.g., `SWxSOC API Key`).

   - Set an expiration period to ensure we are following security best practice.

   - Click **"Generate token"** and copy the generated key. **Important**: You will not be able to view this key again, so ensure you save it securely.

5. **Utilize the API Key**:

   - Use the generated API key to authenticate requests to the Grafana API. But remember, if you would like to work outside an instrument package, you need to set the `SWXSOC_MISSION` environment variable to the appropriate mission context. If you are working within an instrument package, this variable is automatically set.


Storing the API Key Securely
----------------------------
To ensure security and avoid exposing the API key, store it as an environment variable.

1. Open a terminal and set the environment variable:

   .. code-block:: shell

      export GRAFANA_API_KEY="your-service-account-api-key"

2. Make the setting persistent by adding it to your shell configuration file:

   - For `bash`, add the line to `~/.bashrc` or `~/.bash_profile`.
   - For `zsh`, add the line to `~/.zshrc`.

3. Verify that the environment variable is set:

   .. code-block:: shell

      echo $GRAFANA_API_KEY

   This should display the API key.

Best Practices
--------------

- **Never Commit Your API Key**:  
  Do not hardcode the key in your scripts or commit it to a repository. Always use environment variables for secure handling.

- **Rotate Keys Regularly**:  
  Periodically revoke old keys and generate new ones to enhance security.

Requesting Access
-----------------
If you do not have the necessary permissions to create a service account or API key, and you still require access, contact a member of the SWxSOC team. They will assist you in obtaining a service account or API key with the appropriate permissions.

Usage Examples
--------------
The examples below demonstrate how to utilize these functions effectively.

Query Annotations
+++++++++++++++++

Retrieve annotations for a specific time range, dashboard, and panel, filtered by tags.

.. code-block:: python

   from datetime import datetime
   from swxsoc.util import util

   # Parameters
   start_time = datetime(2024, 9, 16, 13, 30, 0)
   end_time = datetime(2024, 9, 16, 13, 35, 0)
   dashboard_name = "WIP MEDDEA Housekeeping"
   panel_name = "Panel Title"
   tags = ["meddea", "test"]

   # Query annotations
   annotations = util.query_annotations(
       start_time=start_time,
       end_time=end_time,
       tags=tags,
       dashboard_name=dashboard_name,
       panel_name=panel_name
   )
   print("Queried Annotations:", annotations)


Create an Annotation
++++++++++++++++++++

Add a new annotation with custom details.

.. code-block:: python

   annotation_text = "Observed solar flare"

   # Create annotation
   new_annotation = util.create_annotation(
       start_time=start_time,
       end_time=end_time,
       text=annotation_text,
       tags=tags,
       dashboard_name=dashboard_name,
       panel_name=panel_name
   )
   print("Created Annotation:", new_annotation)


Remove an Annotation by ID
+++++++++++++++++++++++++++

Delete an annotation by its unique ID.

.. code-block:: python

   if "id" in new_annotation:
       removal_successful = util.remove_annotation_by_id(new_annotation["id"])
       print("Annotation Removed:", removal_successful)

Complete Example
----------------

Below is a complete example that integrates all the functions to manage annotations.

.. code-block:: python

   import logging
   from datetime import datetime
   from swxsoc.util import util

   # Configure logging
   logging.basicConfig(level=logging.INFO)

   # Parameters
   start_time = datetime(2024, 9, 16, 13, 30, 0)
   end_time = datetime(2024, 9, 16, 13, 35, 0)
   dashboard_name = "WIP MEDDEA Housekeeping"
   panel_name = "Panel Title"
   tags = ["meddea", "test"]
   annotation_text = "Observed solar flare"

   # Main workflow
   try:
       # Query annotations
       annotations = util.query_annotations(
           start_time=start_time,
           end_time=end_time,
           dashboard_name=dashboard_name,
           panel_name=panel_name,
           tags=tags
       )
       logging.info("Queried Annotations: %s", annotations)

       # Remove existing annotations
       for annotation in annotations:
           annotation_id = annotation.get("id")
           if annotation_id:
               util.remove_annotation_by_id(annotation_id)
               logging.info("Removed Annotation ID %s", annotation_id)

       # Create a new annotation
       new_annotation = util.create_annotation(
           start_time=start_time,
           end_time=end_time,
           text=annotation_text,
           tags=tags,
           dashboard_name=dashboard_name,
           panel_name=panel_name
       )
       logging.info("Created Annotation: %s", new_annotation)

   except Exception as e:
       logging.error("Error managing annotations: %s", e)




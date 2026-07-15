"""
Reprocessing Lambda invocation for the SWxSOC data pipeline.
"""

import json

import boto3

import swxsoc
from swxsoc.util.util import is_production_environment


def invoke_reprocessing_lambda(bucket: str, key: str):
    """
    Asynchronously invoke the SDC processing Lambda to reprocess an S3 object.

    Builds an SNS-wrapped S3 event payload and invokes the processing Lambda
    function (``dev-aws_sdc_processing_lambda_function`` in non-production
    environments, ``aws_sdc_processing_lambda_function`` in production; see
    :func:`swxsoc.util.util.is_production_environment`).

    Parameters
    ----------
    bucket : str
        The name of the S3 bucket containing the object to reprocess.
    key : str
        The key of the S3 object to reprocess.

    Returns
    -------
    dict
        The response from the Lambda ``invoke`` call.
    """
    data = {
        "Records": [
            {
                "Sns": {
                    "Message": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": bucket},
                                        "object": {"key": key},
                                    }
                                }
                            ]
                        }
                    )
                }
            }
        ]
    }

    lambda_client = boto3.client("lambda")

    function_name = (
        "aws_sdc_processing_lambda_function"
        if is_production_environment()
        else "dev-aws_sdc_processing_lambda_function"
    )

    swxsoc.log.info(f"Invoking Lambda function {function_name} with payload {data}")

    response = lambda_client.invoke(
        FunctionName=function_name, InvocationType="Event", Payload=json.dumps(data)
    )
    return response

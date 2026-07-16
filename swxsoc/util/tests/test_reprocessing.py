"""Tests for swxsoc.util.reprocessing (reprocessing Lambda invocation)"""

import json
from unittest.mock import MagicMock, patch

from swxsoc.util.reprocessing import invoke_reprocessing_lambda


@patch("swxsoc.util.reprocessing.boto3.client")
def test_invoke_reprocessing_lambda_development(mock_boto_client, monkeypatch):
    monkeypatch.delenv("LAMBDA_ENVIRONMENT", raising=False)
    mock_lambda_client = MagicMock()
    mock_boto_client.return_value = mock_lambda_client

    invoke_reprocessing_lambda("test-bucket", "test-key.cdf")

    mock_lambda_client.invoke.assert_called_once()
    _, kwargs = mock_lambda_client.invoke.call_args
    assert kwargs["FunctionName"] == "dev-aws_sdc_processing_lambda_function"
    assert kwargs["InvocationType"] == "Event"

    payload = json.loads(kwargs["Payload"])
    message = json.loads(payload["Records"][0]["Sns"]["Message"])
    s3_event = message["Records"][0]["s3"]
    assert s3_event["bucket"]["name"] == "test-bucket"
    assert s3_event["object"]["key"] == "test-key.cdf"


@patch("swxsoc.util.reprocessing.boto3.client")
def test_invoke_reprocessing_lambda_production(mock_boto_client, monkeypatch):
    monkeypatch.setenv("LAMBDA_ENVIRONMENT", "PRODUCTION")
    mock_lambda_client = MagicMock()
    mock_boto_client.return_value = mock_lambda_client

    invoke_reprocessing_lambda("test-bucket", "test-key.cdf")

    _, kwargs = mock_lambda_client.invoke.call_args
    assert kwargs["FunctionName"] == "aws_sdc_processing_lambda_function"

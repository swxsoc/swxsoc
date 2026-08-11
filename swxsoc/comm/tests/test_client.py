"""
Tests for the shared communications logic in :mod:`swxsoc.comm.client`.
"""

from pathlib import Path
from unittest.mock import patch

from swxsoc import _data_directory
from swxsoc.comm.client import ALERT_TYPES, CommsClient


def test_alert_types_cover_pipeline_events():
    for alert_type in [
        "upload",
        "sorted",
        "sorted_error",
        "processed",
        "processed_error",
        "download",
        "download_error",
        "error",
        "delete",
    ]:
        assert alert_type in ALERT_TYPES


def test_is_file_manifest_true():
    assert CommsClient.is_file_manifest("file_manifest_test.txt") is True


def test_is_file_manifest_false():
    test_path = Path(_data_directory) / "sample" / "test_file_manifest.txt"
    assert CommsClient.is_file_manifest(str(test_path)) is False


def test_generate_file_pipeline_message_default():
    assert (
        CommsClient.generate_file_pipeline_message("path/to/file.txt")
        == "Science File - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_alert_type_sorted():
    assert (
        CommsClient.generate_file_pipeline_message(
            "path/to/file.txt", alert_type="sorted"
        )
        == "File Sorted - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_unknown_alert_type():
    assert (
        CommsClient.generate_file_pipeline_message(
            "path/to/file.txt", alert_type="non-existing key"
        )
        == "Science File - ( _file.txt_ )"
    )


def test_generate_file_pipeline_message_alert_type_delete():
    assert (
        CommsClient.generate_file_pipeline_message(
            "path/to/file.txt", alert_type="delete"
        )
        is None
    )


def test_generate_file_pipeline_message_bucket_name():
    assert (
        CommsClient.generate_file_pipeline_message(
            "path/to/file.txt", bucket_name="my-bucket"
        )
        == "Science File - ( _file.txt_ ) (Bucket: _my-bucket_ )"
    )


@patch("swxsoc.comm.client.CommsClient.is_file_manifest", return_value=True)
def test_generate_file_pipeline_message_manifest(
    mocked_manifest, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    with open("test_file_manifest.txt", "w") as f:
        f.write("Manifest content")
    result = CommsClient.generate_file_pipeline_message("test_file_manifest.txt")
    assert result[0] == "Manifest File - ( _test_file_manifest.txt_ )"
    assert result[1] == "Manifest content"

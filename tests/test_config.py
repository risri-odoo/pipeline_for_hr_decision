import re

import pytest

import job


def test_missing_bucket_fails_fast():
    with pytest.raises(job.ConfigError, match="LAKE_BUCKET"):
        job.Settings.from_env({})


@pytest.mark.parametrize(
    "env,match",
    [
        ({"STEPS": "extract,load"}, "STEPS"),
        ({"STEPS": " , "}, "STEPS"),
        ({"ENGINE": "spark"}, "ENGINE"),
        ({"ENGINE": "polars"}, "ENGINE"),
        ({"RUN_DATE": "2026-02-30"}, "RUN_DATE"),
        ({"POSTGRES_PORT": "abc"}, "POSTGRES_PORT"),
        ({"LOG_LEVEL": "LOUD"}, "LOG_LEVEL"),
        ({"POSTGRES_HOST": " "}, "POSTGRES_HOST"),
    ],
)
def test_invalid_config(env, match):
    with pytest.raises(job.ConfigError, match=match):
        job.Settings.from_env({"LAKE_BUCKET": "b", **env})


def test_defaults_steps_start_with_extract():
    s = job.Settings.from_env({"LAKE_BUCKET": "b"})
    assert s.steps == ("extract", "bronze", "silver", "gold")
    assert s.engine == "pandas" and s.execution_env == "local"
    assert s.s3_endpoint == "http://localhost:9000" and s.aws_region == "us-east-1"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", s.run_date)
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", s.run_id)


def test_no_extract_needs_only_bucket():
    s = job.Settings.from_env({"LAKE_BUCKET": "b", "STEPS": "bronze,silver,gold", "POSTGRES_HOST": " "})
    assert s.steps == ("bronze", "silver", "gold")


def test_values_are_read_and_prefixes_normalised():
    s = job.Settings.from_env(
        {
            "LAKE_BUCKET": " lake ",
            "STEPS": "Silver, gold",
            "LANDING_PREFIX": "in/events",
            "LANDING_PG_PREFIX": "in/pg/",
            "POSTGRES_PORT": "6543",
            "POSTGRES_PASS": "pw",
            "RUN_ID": "my-run",
            "EXECUTION_ENV": "github-actions",
            "LOG_LEVEL": "debug",
        }
    )
    assert s.lake_bucket == "lake" and s.steps == ("silver", "gold")
    assert s.landing_prefix == "in/events/" and s.landing_pg_prefix == "in/pg/"
    assert s.postgres_port == 6543 and s.run_id == "my-run" and s.log_level == "DEBUG"


def test_password_not_in_repr():
    s = job.Settings.from_env({"LAKE_BUCKET": "b", "POSTGRES_PASS": "s3cret-value"})
    assert "s3cret-value" not in repr(s)


def test_new_run_id_format():
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", job.new_run_id())

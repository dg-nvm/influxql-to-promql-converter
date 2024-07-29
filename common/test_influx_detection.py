import pytest
from .influx_detection import AdvancedInfluxDetection


@pytest.fixture
def detector():
    return AdvancedInfluxDetection(
        {
            "grafana_datasources": {
                "InfluxUid": {"type": "influxdb", "name": "InfluxDatasource"},
                "FluxUid": {
                    "type": "influxdb",
                    "name": "FluxDatasource",
                    "jsonData": {"version": "Flux"},
                },
                "OtherUid": {"type": "prometheus", "name": "PrometheusDatasource"},
            }
        }
    )


@pytest.fixture()
def templating():
    return {
        "templating": {
            "list": [
                {"name": "InfluxDataSource", "query": "influxdb", "type": "datasource"},
            ]
        }
    }


@pytest.mark.parametrize(
    "target,expected",
    [
        (
            {
                "datasource": {"type": "influxdb", "uid": "$InfluxUid"},
                "measurement": "select 1",
            },
            True,
        ),
        (
            {
                "datasource": {"uid": "InfluxUid", "type": "influxdb"},
                "measurement": "select 1",
            },
            True,
        ),
        (
            {
                "datasource": "InfluxUid",
                "measurement": "select 1",
            },
            True,
        ),
        (
            {
                "datasource": {"uid": "NotInfluxDBUid", "type": "cloudwatch"},
                "measurement": "select 1",
            },
            False,
        ),
    ],
    ids=["templating", "uid", "uid-direct", "not-matching"],
)
def test_detection(target, expected, templating, detector):
    breakpoint()
    assert detector.is_target_influx(target, templating, None) is expected

from argparse import ArgumentParser
from datetime import timedelta
import importlib
import logging
import os
from string import Template
import time
import traceback
import yaml

import urllib3

urllib3.disable_warnings()


from pathlib import Path
import sys

from converter.influxql_to_promql.influxql_to_promql_dashboard_converter import (
    InfluxQLToM3DashboardConverter,
    ConvertError,
)

from shared_state.global_shared_state import GLOBAL_SHARED_STATE

file = Path(__file__).resolve()
sys.path.append(str(file.parent))

from common.error_manager import ErrorManager, ProcessingContext, GrafanaDashboard


logger = logging.getLogger(__name__)


def get_log_level_descriptor(log_level) -> int:
    if log_level:
        return getattr(logging, log_level.upper())
    return logging.INFO


def extend_module_name(module_name: str, module_type: str) -> str:
    return module_type + "." + module_name + "." + module_name + "_" + module_type


def create_class_name(module: str, module_type: str) -> str:
    import re

    capitalized_module_name = re.sub(
        r"(^|[_])\s*([a-zA-Z])", lambda p: p.group(0).upper(), module
    )
    return capitalized_module_name.replace("_", "") + module_type.capitalize()


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file to use")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="In set, will disable importer cache",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--exporting",
        action="store_true",
        help="Run exporters, default",
        dest="exporting",
        default=True,
    )
    group.add_argument(
        "--no-exporting",
        action="store_false",
        dest="exporting",
        help="Skip exporters, usefull for debugging processing",
    )
    return parser.parse_args()


def run():
    args = parse_args()
    start_time = time.time()

    run_configs = load_run_configs_from_template(args.config)
    config_count = 0
    cleanup_reports()
    for run_config in run_configs:
        config_count += 1
        modules = [
            list(),
            list(),
            list(),
        ]  # [0] - inputs , [1] - processors , [2] - exporters
        module_names = ["importer", "processor", "exporter"]
        dashboards = []
        report = {}
        invalid_dashboards = []
        folders = {}

        build_module_list_from_config(
            run_config, module_names, modules, args.no_cache, GLOBAL_SHARED_STATE
        )

        logger.setLevel(level=get_log_level_descriptor(run_config.get("log_level")))

        converter = None
        error_manager = None
        if modules[0]:
            error_manager = ErrorManager(logger=logger, processing_context=ProcessingContext(grafana_url=modules[0][0]._grafana_endpoint, grafana_organization_id=modules[0][0]._organization_id))  # type: ignore

            converter = get_converter_from_config(
                run_config, error_manager, GLOBAL_SHARED_STATE
            )

            logger.info(
                f"Starting processing run - {error_manager.context.grafana_url} / Org: {error_manager.context.grafana_organization_id}"
            )
            import_dashboards(dashboards, modules[0], folders, args.no_cache)

        if converter:
            influx_dashboards = []
            convert_dashboards(
                converter,
                dashboards,
                influx_dashboards,
                invalid_dashboards,
                error_manager,
            )
            if len(modules[1]) > 0:
                metric_to_objects = converter.metric_to_objects
                metric_to_objects = process_dashboards(
                    metric_to_objects, modules[1], report
                )
            else:
                metric_to_objects = []
            create_report(invalid_dashboards, metric_to_objects, modules[1], report)
        else:
            influx_dashboards = dashboards

        if modules[2] and (args.exporting):
            export_dashboards(influx_dashboards, modules[2], folders)

        logger.info(f"Finished processing - {len(influx_dashboards)} dashboards")

        if error_manager and (errors := error_manager.errors_csv()):
            Path("errors.csv").open("a").write(errors)

    logger.info(
        f"Finished processing {config_count} configurations in time:{timedelta(seconds=(time.time() - start_time))} ---"
    )


def cleanup_reports():
    Path("result_report.yml").write_text("")
    Path("errors.csv").write_text("")


def load_run_configs_from_template(config: str):
    with open(config, "r") as stream:
        config_template = Template(stream.read())
        config_string = config_template.safe_substitute(**os.environ)
        return yaml.safe_load_all(config_string)


def get_converter_from_config(config, error_manager, global_shared_state):
    if config.get("converter"):
        if config["converter"].get("influxql", {}).get("enabled", False):
            return InfluxQLToM3DashboardConverter(
                replacement_datasource=config.get("datasource"),
                error_manager=error_manager,
                global_shared_state=global_shared_state,
                log_level=get_log_level_descriptor(config.get("log_level")),
            )
        else:
            logger.info("No converter specified in the config")


def build_module_list_from_config(
    config, module_names, modules, no_cache, global_shared_state
) -> dict:
    for index in range(len(module_names)):
        if config.get(module_names[index]):
            for module in [
                _module
                for _module in config[module_names[index]]
                if config.get(module_names[index])
            ]:
                module_class = getattr(
                    importlib.import_module(
                        name=extend_module_name(module, module_names[index])
                    ),
                    create_class_name(module, module_names[index]),
                )
                # Instantiate the class (pass arguments to the constructor, if needed)
                module_instance = module_class(
                    config[module_names[index]][module],
                    global_shared_state,
                    get_log_level_descriptor(config.get("log_level")),
                )
                modules[index].append(module_instance)

    return config


def import_dashboards(dashboards, inputs, folders, no_cache=False):
    for input in inputs:
        logger.info(f"Starting to fetch dashboards from importer: {input}")
        db, f = input.fetch_dashboards_and_folders(no_cache)
        dashboards.extend(db)
        folders.update(f)


def export_dashboards(influx_dashboards, exporters, folders):
    for exporter in exporters:
        logger.info(f"Exporting dashboards with exporter: {exporter}")
        exporter.export_dashboards(influx_dashboards, folders)


def add_report_extended_info(report):
    enchanced_report = {}
    if len(report) > 0:
        for dashboard, values in report.items():
            if values.get("unreplaced_metrics"):
                values[
                    "Unable to find any match for the following metrics. Please check the dashboard manually"
                ] = values.pop("unreplaced_metrics")
            enchanced_report[f"Dashboard name: {dashboard}"] = report[dashboard]
    return enchanced_report


def create_report(invalid_dashboards, metric_to_objects, processors, report):
    logger.info("Building result report")
    if (
        len(processors) > 0 and len(metric_to_objects) > 0
    ):  # rebuild unreplaced metrics per dashboard
        add_unreplaced_metrics_to_report(metric_to_objects, report)
    report = add_report_extended_info(report)
    report["invalid dashboards"] = invalid_dashboards
    if len(report) > 0:
        with open("result_report.yml", "a") as outfile:
            yaml.dump(report, outfile, default_flow_style=False)
            outfile.write("---\n")
    logger.info("Finished building result report: result_report.yaml")


def process_dashboards(metric_to_objects, processors, report) -> dict:
    for processor in processors:
        logger.info(f"Processing dashboards with processor: {processor}")
        metric_to_objects = processor.process(metric_to_objects)
        for dashboard_key, processor_report in processor.get_json_report().items():
            if not report.get(dashboard_key):
                report[dashboard_key] = processor_report
            else:
                report[dashboard_key] = report[dashboard_key] | processor_report
    return metric_to_objects


def convert_dashboards(
    converter,
    dashboards,
    influx_dashboards,
    invalid_dashboards,
    error_manager: ErrorManager,
):
    logger.info(f"Starting dashboards conversion")
    for dashboard in dashboards:
        error_manager.context.dashboard = GrafanaDashboard(
            uid=dashboard["dashboard"]["uid"],
            title=dashboard["dashboard"]["title"],
            folder=dashboard["meta"]["folderTitle"],
            updater=dashboard["meta"]["updatedBy"],
        )
        try:
            converter.convert_dashboard(dashboard["dashboard"], dashboard["meta"])
            influx_dashboards.append(dashboard)
        except ConvertError as e:
            error_manager.add_error(
                f"Error converting dashboard, skipping - error:{e}", error_level="ERROR"
            )
            invalid_dashboards.append(dashboard["dashboard"]["title"])
        except Exception as e:
            logger.error(
                f"Unhandled error on dashboard processing: {e}\nError Context: {error_manager.context}\nTraceback:{traceback.format_exc()}"
            )
            exit(1)


def add_unreplaced_metrics_to_report(metric_to_objects, report):
    dashboard_metrics = dict()
    unreplaced_metrics_report = []
    for metric, dash_dict in metric_to_objects.items():
        for dashboard, panel in dash_dict.items():
            if not dashboard_metrics.get(dashboard):
                dashboard_metrics[dashboard] = []
            dashboard_metrics[dashboard].append(metric)

    for dashboard, metrics in dashboard_metrics.items():
        unreplaced_metrics_report.append({dashboard: {"unreplaced_metrics": metrics}})
    for dashboard_to_metrics in unreplaced_metrics_report:
        for dashboard, metrics in dashboard_to_metrics.items():
            if not report.get(dashboard):
                report[dashboard] = dashboard_to_metrics.get(dashboard)
            else:
                report[dashboard] = report[dashboard] | dashboard_to_metrics[dashboard]


if __name__ == "__main__":
    run()

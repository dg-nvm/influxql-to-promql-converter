import calendar
import copy
import itertools
import json
import logging
from datetime import datetime, timedelta

import requests

from ..processor import Processor
from collections import Counter
from thefuzz import fuzz
from thefuzz import process
import urllib.parse


class FindMetricsNamesProcessor(Processor):
    PROMETHEUS_METRICS_URL = "/label/__name__/values?match[]="
    PROMETHEUS_METRICS_QUERY_PREFIX = '{__name__=~"'
    PROMETHEUS_METRICS_QUERY_SUFFIX = '.*"}'
    MAX_MATCH_NEW_METRIC = 0
    MAX_MATCH_OLD_METRIC = 1
    MAX_MATCH_COMBINATIONS = 2
    MAX_MATCH_FILTER_PERCENT = 3
    MAX_MATCH_MATCH_PERCENT = 4

    def __init__(self, params: dict, global_shared_state, log_level=logging.INFO):
        super().__init__(__name__, global_shared_state, log_level)
        self._metrics_db_endpoint = params["metrics_auth"]["metrics_db_endpoint"]
        self._replace_strategies = self.order_repalce_strategies(
            params["replace_strategy"]["strategies"]
        )
        self._statistic_replace_min_match_percent = params["replace_strategy"].get(
            "min_match_percent"
        )
        self._statistic_replace_min_filter_percent = params["replace_strategy"].get(
            "min_filter_percent"
        )
        self._get_metrics_auth(params)

    def _get_metrics_auth(self, params: dict):
        self._metrics_oauth_header = None
        self._metrics_basic_auth_username = None
        self._metrics_basic_auth_password = None
        try:
            metrics_auth = params["metrics_auth"]
            oauth_header = metrics_auth.get("metrics_oauth_header")
            basic_auth = metrics_auth.get("metrics_basic_auth")
            if oauth_header:
                self._metrics_oauth_header = {
                    oauth_header["key"]: params["metrics_auth"]["metrics_oauth_header"][
                        "value"
                    ]
                }
            elif basic_auth:
                self._metrics_basic_auth_username = basic_auth["username"]
                self._metrics_basic_auth_password = basic_auth["password"]
        except KeyError:
            self._logger.error(
                "Invalid metrics auth config, trying to fetch metrics without authentication"
            )
            return

        if not oauth_header and not basic_auth:
            self._logger.warning(
                "Could not find metrics authentication credentials in the config file, trying to fetch metrics "
                "without authentication"
            )

    def group_tuples_by_service(self, metric_to_objects) -> dict:
        service_to_metrics = {}
        for metric, object_list in metric_to_objects.items():
            service = metric.split("_")[
                0
            ]  # get service name by splitting the metric (key) and extracting the first item
            if not service_to_metrics.get(service):
                service_to_metrics[service] = []
            service_to_metrics[service].append(metric)
        return service_to_metrics

    def drop_matching_metrics(
        self, sent_metrics_for_service, metric_to_objects
    ) -> (list, list):
        filtered_sent_metrics = copy.copy(sent_metrics_for_service)
        for metric in sent_metrics_for_service:
            if metric_to_objects.get(metric):
                for dashboard in metric_to_objects[metric]:
                    self._logger.debug(
                        f"Found exact match for metric: {metric} in dashboard {dashboard}"
                    )
                    self.add_to_report(
                        dashboard, __name__, self.create_report_object(metric)
                    )
                del metric_to_objects[metric]
                filtered_sent_metrics.remove(metric)
        return filtered_sent_metrics, metric_to_objects

    def process(self, metric_to_objects):
        service_to_metrics = self.group_tuples_by_service(metric_to_objects)
        all_sent_metrics = []
        for service, metrics in service_to_metrics.items():
            sent_metrics_for_service = self.get_sent_metrics_for_service(service)
            updated_sent_metrics_and_objects = self.drop_matching_metrics(
                sent_metrics_for_service, metric_to_objects
            )
            all_sent_metrics += updated_sent_metrics_and_objects[0]
            metric_to_objects = updated_sent_metrics_and_objects[1]
        # Remove duplicated metrics and sort - required for permutation method
        all_dashboards_metrics = list(metric_to_objects.keys())
        for strategy in self._replace_strategies:
            if len(all_sent_metrics) > 0:
                process_method = getattr(self, strategy + "_replace")
                self._logger.info(
                    f"Starting to process dashboards with strategy: {strategy}"
                )
                updated_sent_metrics_and_objects = process_method(
                    all_sent_metrics, all_dashboards_metrics, metric_to_objects
                )
                metric_to_objects = updated_sent_metrics_and_objects[1]
                all_sent_metrics = updated_sent_metrics_and_objects[0]
                self._logger.info(
                    f"Finished processing dashboards with strategy: {strategy}"
                )
        return metric_to_objects

    # Metric sent contains the same characters as the metric in the dashboard, but in a different order
    def permutation_replace(
        self, sent_metrics, current_dashboards_metrics, metric_to_objects
    ) -> (list, list):
        filtered_dashboard_metrics = copy.deepcopy(current_dashboards_metrics)
        filtered_sent_metrics = sent_metrics.copy()

        for dashboard_metric in current_dashboards_metrics:
            for sent_metric in sent_metrics:
                if len(sent_metric) == len(dashboard_metric):
                    if Counter(sent_metric) == Counter(dashboard_metric):
                        dashboards_to_remove = self.replace_metric(
                            dashboard_metric,
                            sent_metric,
                            metric_to_objects[dashboard_metric],
                        )
                        for dashboard in metric_to_objects[dashboard_metric].keys():
                            self.add_to_report(
                                dashboard,
                                __name__,
                                self.create_report_object(
                                    dashboard_metric, sent_metric
                                ),
                            )
                        self.remove_updated_dashboards_from_metric_to_object(
                            dashboards_to_remove, metric_to_objects[dashboard_metric]
                        )
                        filtered_sent_metrics.remove(sent_metric)
                        filtered_dashboard_metrics.remove(dashboard_metric)
        current_dashboards_metrics = filtered_dashboard_metrics
        return filtered_sent_metrics, metric_to_objects

    def statistic_combination_replace(
        self, all_sent_metrics, all_dashboards_metrics, metric_to_objects
    ) -> (list, list):
        filtered_sent_metrics = all_sent_metrics.copy()
        for sent_metric in all_sent_metrics:
            combinations = []
            max_match = (
                "",
                "",
                [],
                0,
                0,
            )  # new metric, old_metric, match name, filter percent, match percent
            for dashboard_metric in all_dashboards_metrics:
                filter_ratio = fuzz.WRatio(
                    sent_metric.replace("_", " "), dashboard_metric.replace("_", " ")
                )
                if filter_ratio >= self._statistic_replace_min_filter_percent:
                    if len(combinations) == 0:
                        sent_metric_words = sent_metric.split("_")
                        # get all possible combinations, starting with list length of 1
                        for i in range(1, len(sent_metric_words) + 1):
                            combinations += list(
                                itertools.combinations(sent_metric_words, i)
                            )
                    metric_with_spaces = dashboard_metric.replace("_", " ")
                    top_ranked_match = process.extractOne(
                        metric_with_spaces, combinations
                    )
                    if (
                        top_ranked_match
                        and top_ranked_match[1]
                        >= self._statistic_replace_min_match_percent
                    ):
                        if (
                            filter_ratio >= max_match[self.MAX_MATCH_FILTER_PERCENT]
                            and top_ranked_match[1]
                            >= max_match[self.MAX_MATCH_MATCH_PERCENT]
                        ):
                            max_match = (
                                sent_metric,
                                dashboard_metric,
                                top_ranked_match[0],
                                filter_ratio,
                                top_ranked_match[1],
                            )
            if max_match[self.MAX_MATCH_MATCH_PERCENT] > 0:  # new metric was found
                self.replace_and_report_new_match(
                    max_match, metric_to_objects, all_dashboards_metrics
                )
                filtered_sent_metrics.remove(sent_metric)
        return filtered_sent_metrics, metric_to_objects

    def replace_and_report_new_match(
        self, max_match, metric_to_objects, all_dashboard_metrics
    ):
        dashboards_to_remove = []
        for dashboard in metric_to_objects[max_match[self.MAX_MATCH_OLD_METRIC]]:
            dashboards_to_remove = self.replace_metric(
                max_match[self.MAX_MATCH_OLD_METRIC],
                max_match[self.MAX_MATCH_NEW_METRIC],
                metric_to_objects[max_match[self.MAX_MATCH_OLD_METRIC]],
            )
            self.add_to_report(
                dashboard,
                __name__,
                self.create_report_object(
                    max_match[self.MAX_MATCH_OLD_METRIC],
                    max_match[self.MAX_MATCH_NEW_METRIC],
                    max_match[self.MAX_MATCH_FILTER_PERCENT],
                    max_match[self.MAX_MATCH_MATCH_PERCENT],
                    max_match[self.MAX_MATCH_COMBINATIONS],
                ),
            )
        self.remove_updated_dashboards_from_metric_to_object(
            dashboards_to_remove,
            metric_to_objects[max_match[self.MAX_MATCH_OLD_METRIC]],
        )
        all_dashboard_metrics.remove(max_match[self.MAX_MATCH_OLD_METRIC])

    def get_sent_metrics_for_service(self, service_prefix) -> list:
        epoch_time = datetime.utcnow()
        five_min_time = datetime.utcnow() - timedelta(minutes=5)
        query = (
            self._metrics_db_endpoint
            + self.PROMETHEUS_METRICS_URL
            + urllib.parse.quote(
                self.PROMETHEUS_METRICS_QUERY_PREFIX
                + service_prefix
                + self.PROMETHEUS_METRICS_QUERY_SUFFIX,
                safe="*",
            )
            + "&start="
            + str(calendar.timegm(five_min_time.timetuple()))
            + "&end="
            + str(calendar.timegm(epoch_time.timetuple()))
        )

        if self._metrics_oauth_header:
            response = requests.get(query, headers=self._metrics_oauth_header)
        elif self._metrics_basic_auth_username:
            response = requests.get(
                query,
                auth=(
                    self._metrics_basic_auth_username,
                    self._metrics_basic_auth_password,
                ),
            )
        else:
            response = requests.get(query)

        metrics = []
        if response.status_code == 200:
            metrics = json.loads(response.content).get("data")
        else:
            self._logger.error(
                f"Encountered an error {response.status_code} while fetching metrics from db: {response.content}"
            )
        return metrics

    def order_repalce_strategies(self, strategies):
        if len(strategies) > 1:
            if strategies[0] == "statistic_combination":
                strategies[0] = "permutation"
                strategies[1] = "statistic_combination"
        return strategies

    def create_report_object(self, old_metric, new_metric=None, *args) -> dict:
        if len(args) > 0:  # statistic match
            return {
                "statistic_match": {
                    "converted": f"{old_metric} --> {new_metric}",
                    "filter_percent": args[0],
                    "match_percent": args[1],
                    "matched_combination": list(args[2]),
                }
            }
        if new_metric is None:
            return {"exact_match": f"{old_metric}"}

        return {
            "permutation_match": {
                "converted": f"{old_metric} --> {new_metric}",
            }
        }

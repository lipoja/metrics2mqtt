import threading
import time

import jsons
import psutil
from numpy import array, diff, average


class BaseMetric(object):
    def __init__(self, *args, **kwargs):
        self.icon = "mdi:desktop-tower-monitor"
        self.unit_of_measurement = "%"
        self.topics = None
        self.polled_result = None
        self.name = "unset"

    def get_config_topic(self, topic_prefix, system_name):
        sn = self.sanitize(system_name)
        n = self.sanitize(self.name)
        self.topics = {
            "state": "{}/sensor/{}/{}/state".format(topic_prefix, sn, n),
            "config": "{}/sensor/{}/{}/config".format(topic_prefix, sn, n),
            "avail": "{}/sensor/{}/{}/availability".format(topic_prefix, sn, n),
            "attrs": "{}/sensor/{}/{}/attributes".format(topic_prefix, sn, n),
        }

        return {
            "name": system_name + " " + self.name,
            "unique_id": sn + "_" + n,
            "qos": 1,
            "icon": self.icon,
            "unit_of_measurement": self.unit_of_measurement,
            "availability_topic": self.topics["avail"],
            "json_attributes_topic": self.topics["attrs"],
            "state_topic": self.topics["state"],
        }

    @staticmethod
    def sanitize(val):
        return val.lower().replace(" ", "_").replace("/", "_")

    def poll(self, result_queue=None):
        raise NotImplementedError


class CPUMetricThread(threading.Thread):
    def __init__(self, result_queue, metric):
        threading.Thread.__init__(self)
        self.result_queue = result_queue
        self.metric = metric

    def run(self):
        cpu_times = psutil.cpu_times_percent(
            interval=self.metric.interval, percpu=False
        )
        self.metric.polled_result = {
            "state": "{:.1f}".format(100.0 - cpu_times.idle),
            "attrs": jsons.dump(cpu_times),
        }
        self.result_queue.put(self.metric)


class CPUMetrics(BaseMetric):
    def __init__(self, interval):
        super(CPUMetrics, self).__init__()
        self.name = "CPU"
        self.icon = "mdi:chip"
        self.interval = interval
        self.result_queue = None

    def poll(self, result_queue=None):
        self.result_queue = result_queue
        th = CPUMetricThread(result_queue=result_queue, metric=self)
        th.daemon = True
        th.start()
        return True  # Expect a deferred result


class VirtualMemoryMetrics(BaseMetric):
    def __init__(self, *args, **kwargs):
        super(VirtualMemoryMetrics, self).__init__(*args, **kwargs)
        self.name = "Virtual Memory"
        self.icon = "mdi:memory"

    def poll(self, result_queue=None):
        vm = psutil.virtual_memory()
        self.polled_result = {
            "state": "{:.1f}".format(vm.percent),
            "attrs": jsons.dump(vm),
        }
        return False


class DiskUsageMetrics(BaseMetric):
    def __init__(self, mountpoint):
        super(DiskUsageMetrics, self).__init__()
        self.name = "Disk Usage"
        self.icon = "mdi:harddisk"
        self.mountpoint = mountpoint

    def poll(self, result_queue=None):
        disk = psutil.disk_usage(self.mountpoint)
        self.polled_result = {
            "state": "{:.1f}".format(disk.percent),
            "attrs": jsons.dump(disk),
        }
        return False

    def get_config_topic(self, topic_prefix, system_name):
        sn = self.sanitize(system_name)
        n = self.sanitize(self.mountpoint)
        self.topics = {
            "state": "{}/sensor/{}/disk_usage_{}/state".format(topic_prefix, sn, n),
            "config": "{}/sensor/{}/disk_usage_{}/config".format(topic_prefix, sn, n),
            "avail": "{}/sensor/{}/disk_usage_{}/availability".format(
                topic_prefix, sn, n
            ),
            "attrs": "{}/sensor/{}/disk_usage_{}/attributes".format(
                topic_prefix, sn, n
            ),
        }

        return {
            "name": system_name + " Disk Usage (" + self.mountpoint + " Volume)",
            "unique_id": sn + "_disk_usage_" + n,
            "qos": 1,
            "icon": self.icon,
            "unit_of_measurement": self.unit_of_measurement,
            "availability_topic": self.topics["avail"],
            "json_attributes_topic": self.topics["attrs"],
            "state_topic": self.topics["state"],
        }


class NetworkMetricThread(threading.Thread):
    def __init__(self, result_queue, metric):
        threading.Thread.__init__(self)
        self.result_queue = result_queue
        self.metric = metric

    def run(self):
        x = 0
        interval = self.metric.interval
        tx_bytes = []
        rx_bytes = []
        prev_tx = prev_rx = base_tx = base_rx = 0
        nics = psutil.net_io_counters(pernic=True)
        while x < interval:
            nics = psutil.net_io_counters(pernic=True)
            if self.metric.nic in nics:
                tx = nics[self.metric.nic].bytes_sent
                rx = nics[self.metric.nic].bytes_recv
                if tx < prev_tx:
                    # TX counter rollover
                    base_tx += prev_tx
                if rx < prev_rx:
                    # RX counter rollover
                    base_rx += prev_rx
                tx_bytes.append(base_tx + tx)
                rx_bytes.append(base_rx + rx)
                prev_tx = tx
                prev_rx = rx
            time.sleep(1)
            x += 1
        tx_rate_bytes_sec = average(diff(array(tx_bytes)))
        tx_rate = tx_rate_bytes_sec / 125.0  # bytes/sec to kilobits/sec
        rx_rate_bytes_sec = average(diff(array(rx_bytes)))
        rx_rate = rx_rate_bytes_sec / 125.0  # bytes/sec to kilobits/sec

        r = {
            "state": "{:.1f}".format(tx_rate + rx_rate),
            "attrs": nics[self.metric.nic]._asdict(),
        }
        r["attrs"].update(
            {
                "tx_rate": float("{:.1f}".format(tx_rate)),
                "rx_rate": float("{:.1f}".format(rx_rate)),
            }
        )
        self.metric.polled_result = r
        self.result_queue.put(self.metric)


class NetworkMetrics(BaseMetric):
    def __init__(self, nic, interval):
        super(NetworkMetrics, self).__init__()
        self.name = "Network Throughput"
        self.icon = "mdi:server-network"
        self.interval = interval
        self.result_queue = None
        self.unit_of_measurement = "kb/s"
        self.nic = nic

    def poll(self, result_queue=None):
        self.result_queue = result_queue
        th = NetworkMetricThread(result_queue=result_queue, metric=self)
        th.daemon = True
        th.start()
        return True  # Expect a deferred result

    def get_config_topic(self, topic_prefix, system_name):
        sn = self.sanitize(system_name)
        n = self.sanitize(self.nic)
        self.topics = {
            "state": "{}/sensor/{}/net_{}/state".format(topic_prefix, sn, n),
            "config": "{}/sensor/{}/net_{}/config".format(topic_prefix, sn, n),
            "avail": "{}/sensor/{}/net_{}/availability".format(topic_prefix, sn, n),
            "attrs": "{}/sensor/{}/net_{}/attributes".format(topic_prefix, sn, n),
        }

        return {
            "name": system_name + " Network (" + self.nic + ")",
            "unique_id": sn + "_net_" + n,
            "qos": 1,
            "icon": self.icon,
            "unit_of_measurement": self.unit_of_measurement,
            "availability_topic": self.topics["avail"],
            "json_attributes_topic": self.topics["attrs"],
            "state_topic": self.topics["state"],
        }

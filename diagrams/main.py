import csv
import matplotlib.pyplot as plt
import argparse
import os
import statistics
from typing import Dict, List
from pathlib import Path
from dataclasses import dataclass

@dataclass(frozen=True)
class WorkerStats:
    worker_id: str
    success: int
    failed: int
    timeouts: int
    channels_opened: int
    channels_reused: int
    channels_closed: int
    channels_connected: int
    channel_connection_errors: int
    total_connection_duration_ms: int
    requests_registered: int
    requests_completed: int
    total_request_duration_ms: int


@dataclass(frozen=True)
class RuntimeData:
    workers: int
    stats: List[WorkerStats]

def sum_stats(workers: List[WorkerStats], field: str) -> int:
    if not workers:
        return 0

    if not hasattr(workers[0], field):
        raise ValueError(f"WorkerStats does not have field: {field}")

    return sum(getattr(worker, field) for worker in workers)

def plot_graphs(graphs, out_folder: str):
    for name, data in graphs.items():
        worker_counts, measurements = zip(*data)  # Unzip the tuples
        plt.figure()  # Create a new figure for each graph
        plt.plot(worker_counts, measurements, marker='o', linestyle='-') # Added markers and lines
        plt.xscale('log', base=2) # Set x axis to log scale base 2
        plt.xticks(worker_counts, worker_counts) # Set x axis ticks to worker counts
        plt.xlabel("Worker Count")
        plt.ylabel(name)
        plt.title(name)
        plt.grid(True) # Added grid for better readability
        plt.tight_layout() # Adjust layout to prevent labels from overlapping

        filename =os.path.join(out_folder, f"{name}.png")
        plt.savefig(filename)
        plt.close()

def plot_data(data: Dict[int, RuntimeData], out_folder: str):
    # Load data manually

    sorted_dict = dict(sorted(data.items()))

    graphs = {}
    graphs["average request duration ms"] = []
    graphs["average connection time ms"] = []
    graphs["RPMS per worker"] = []
    graphs["RPS per worker"] = []
    graphs["RPS"] = []
    graphs["RPMS"] = []
    graphs["variance"] = []
    graphs["standard deviation ms"] = []

    for worker_count, run_data in sorted_dict.items():
        stats: List[WorkerStats] = run_data.stats

        for s in stats:
            print(s.total_request_duration_ms, s.success)
        print()

        total_request_ms = sum_stats(stats, "total_request_duration_ms")
        total_request_count = sum_stats(stats, "requests_completed")

        sum_requests = sum_stats(stats, "success")
        connection_ms_sum = sum_stats(stats, "total_connection_duration_ms")

        durations = [r.total_request_duration_ms for r in stats]
        durations = durations if len(durations) > 1 else [durations[0], durations[0]]
        variance = statistics.variance(durations)
        std_e = statistics.stdev(durations)

        ms_per_connection = connection_ms_sum / sum_requests

        duration_per_request_ms = total_request_ms / total_request_count
        request_per_ms = total_request_count / total_request_ms
        request_per_s = request_per_ms *  1000

        graphs["average request duration ms"].append((worker_count, duration_per_request_ms))
        graphs["average connection time ms"].append((worker_count, ms_per_connection))
        graphs["RPMS per worker"].append((worker_count, request_per_ms))
        graphs["RPS per worker"].append((worker_count, request_per_s))
        graphs["variance"].append((worker_count, variance))
        graphs["standard deviation ms"].append((worker_count, std_e))
        graphs["RPMS"].append((worker_count, request_per_ms * worker_count))
        graphs["RPS"].append((worker_count, request_per_s * worker_count))


        print("Worker count ", worker_count)
        print(f"\tAvg request: {duration_per_request_ms}ms")
        print(f"\tAvg connection: {ms_per_connection}ms")
        print(f"\tRequest/ms/worker: {request_per_ms}")
        print(f"\tRequest/s/worker: {request_per_s}")
        print(f"\tRequest/ms: {request_per_ms * worker_count}")
        print(f"\tRequest/s: {request_per_s * worker_count}")
        print(f"\tWorker request sum variance: {variance}ms^2")
        print(f"\tWorker request sum stdev: {std_e}ms")
        print()
    plot_graphs(graphs, out_folder)




def parse_data(csv_file: Path) -> RuntimeData:
    with open(csv_file, newline='') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    stats = []
    for row in data:
        ws = WorkerStats(
            int(row['worker_id']),
            int(row['success']),
            int(row['failed']),
            int(row['timeouts']),
            int(row['channelsOpened']),
            int(row['channelsReused']),
            int(row['channelsClosed']),
            int(row['channelConnected']),
            int(row['channelConnectionErrors']),
            int(row['connectDurationMs']),
            int(row['requestsRegistered']),
            int(row['requestCompleted']),
            int(row['requestDurationMs'])
        )
        stats.append(ws)
    return RuntimeData(len(data), stats)


def main():
    parser = argparse.ArgumentParser(description="Generate graphs from CSV data.")
    parser.add_argument("--folder", type=str, required=True, help="Path to a folder containing multiple CSV files.")
    parser.add_argument("--out", type=str, required=True, help="Output folder for generated graphs.")
    args = parser.parse_args()

    data_by_count = {}
    for file in os.listdir(args.folder):
        if file.endswith(".csv"):
            data = parse_data(Path(os.path.join(args.folder, file)))
            data_by_count[data.workers] = data

    plot_data(data_by_count, args.out)

if __name__ == "__main__":
    main()
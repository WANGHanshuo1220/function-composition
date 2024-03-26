from split import split
from extract import extract
from classify import classify

import time
from multiprocessing import Process
import tracemalloc


if __name__ == "__main__":

    tracemalloc.start()

    video_type = "small"
    parallel = 1
    detection_prob = 2

    # First stage
    split_ = split(video_type, parallel, detection_prob)

    # Second stage
    extract_workers = []

    # Third stage
    classify_workers = []

    for worker_id in range(parallel):
        extract_workers.append(extract(video_type, worker_id))
        classify_workers.append(classify(video_type, worker_id))

    # Run all stages
    steps = [[split_], extract_workers, classify_workers]
    times = []
    ths = []

    for step in range(len(steps)):
        for step_worker in steps[step]:
            ths.append(Process(target = step_worker.run))

    for t in range(len(ths)):
        ths[t].start()
    for t in range(len(ths)):
        ths[t].join()

    # total_time = 0.0
    # for index, time_ in enumerate(times):
    #     print(f"step {index+1} execution time = {time_:.8f}")
    #     total_time += time_
    # print(f"Total exectution time = {total_time:.8f}")
import time


if __name__ == "__main__":
    steps = []
    times = []

    for index in range(len(steps)):

        start_time = time.time()
        steps[index].run()
        end_time = time.time()

        execution_time = end_time - start_time
        times.append(execution_time)

    total_time = 0.0
    for index, time_ in enumerate(times):
        print(f"step {index+1} execution time = {time_:.8f}")
        total_time += time_
    print(f"Total exectution time = {total_time:.8f}")
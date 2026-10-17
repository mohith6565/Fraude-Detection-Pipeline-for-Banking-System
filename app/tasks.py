from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(max_workers=4)


def run_async(fn, *args, **kwargs):
    return _executor.submit(fn, *args, **kwargs)

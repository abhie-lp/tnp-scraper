def job_id(data: str) -> int:
    # Extract id from callback data. Eg. JOB_123, APP_23, INT_345
    return int(data.split("_", 1)[1])

import logging as log
import inspect
import os

file_name = os.path.basename(__file__)

log.basicConfig(
    level = log.DEBUG,
    format = "%(levelname)s | @ %(asctime)s %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers = [
        log.FileHandler("app.log"),
        log.StreamHandler()
    ]
)

def advanced_log(log_type, text):
    caller_frame = inspect.currentframe()
    if caller_frame is None or caller_frame.f_back is None:
        func_name = "|UNKNOWN FUNCTION|"
        cls_name = "|UNKNOWN CLASS|"
        file_name = "|UNKNOWN FILE|"
    else:
        caller = caller_frame.f_back
        func_name = caller.f_code.co_name

        file_name = os.path.basename(caller.f_code.co_filename)

        if "self" in caller.f_locals:
            cls_name = type(caller.f_locals["self"]).__name__
        else:
            cls_name = "<MODULE>"

    tag = f"|{file_name}|{cls_name}|{func_name.upper()}|:"

    if log_type.lower() not in ["debug", "info", "warning", "error", "critical"]:
        raise ValueError(f"Invalid log_type: {log_type}")

    log_method = getattr(log, log_type.lower())
    log_method(f"{tag} {text}")

advanced_log("info", "TEST LOG FROM log_util.py")


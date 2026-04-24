import logging
import queue
from logging.handlers import QueueHandler, QueueListener


def setup_logging():
    # Create a log queue
    log_queue = queue.Queue()

    # Set up a QueueHandler and attach it to the root logger
    queue_handler = QueueHandler(log_queue)

    # Set up the log format
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)  # Log messages go to the queue

    # Set up a QueueListener to pull logs from the queue and handle them
    listener = QueueListener(log_queue, stream_handler)
    listener.start()

    # Ensure the listener stops when the program exits
    import atexit

    atexit.register(listener.stop)

    return root_logger

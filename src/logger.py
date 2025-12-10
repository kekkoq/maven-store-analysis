import logging
import sys

# Define the logger instance to be used everywhere
logger = logging.getLogger('ETL_Logger')

def init_logger():
    """Configures and initializes the project logger."""
    
    # Check if the logger has handlers (to prevent duplicate setup on re-import)
    if logger.hasHandlers():
        return

    logger.setLevel(logging.INFO) # Set default logging level
    
    # 1. Console Handler (for viewing logs in real-time)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 2. Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Apply the formatter to the handler
    console_handler.setFormatter(formatter)
    
    # Add the handler to the logger
    logger.addHandler(console_handler)

# Note: The logger object is defined globally, but the setup is wrapped in init_logger.
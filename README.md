# FuelSync

A robust, type-safe Python client for the WEX EFS (Electronic Funds Source) SOAP API. FuelSync provides a clean interface for retrieving and analyzing fuel transaction data with full validation and error handling.

## Features

- 🔒 **Type-Safe**: Built with Pydantic models for request and response validation
- 🎯 **Clean API**: Intuitive interface with context manager support
- 📊 **Data Analysis Ready**: Built-in DataFrame conversion for pandas integration
- 🛡️ **Robust Error Handling**: Comprehensive logging and graceful error recovery
- 🔧 **Flexible Configuration**: YAML-based configuration with automatic discovery
- 📝 **Well-Documented**: Extensive docstrings and type hints throughout
- 🧪 **Production Ready**: Handles edge cases, nullable fields, and malformed data

## Installation

### Prerequisites

- Python 3.11 or higher
- Access to WEX EFS SOAP API credentials

### Install from source
```bash
git clone https://github.com/andrewjordan3/FuelSync.git
cd fuelsync
pip install -e .
```

### Install dependencies
```bash
pip install -r requirements.txt
```

**Core dependencies:**
- `pydantic` - Data validation and settings management
- `pyyaml` - YAML configuration parsing
- `requests` - HTTP client
- `lxml` - XML parsing
- `jinja2` - Template rendering for SOAP envelopes
- `pandas` - DataFrame operations and data analysis
- `pyarrow` - Parquet file format support

## Configuration

Create a `config.yaml` file in `src/fuelsync/config/`:

```yaml
efs:
  endpoint_url: "https://ws.efsllc.com/axis2/services/CardManagementWS/"
  username: "your_username"
  password: "your_password"

client:
  request_timeout: [10, 30]  # [connect_timeout, read_timeout] in seconds
  verify_ssl: true
  max_retries: 3
  retry_backoff_factor: 2

pipeline:
  default_start_date: "2025-08-01"  # ISO format: YYYY-MM-DD
  batch_size_days: 1
  lookback_days: 7
  request_delay_seconds: 0.5

storage:
  parquet_file: "data/transactions.parquet"
  compression: "snappy"  # Options: snappy, gzip, brotli, lz4, zstd

logging:
  console_level: "INFO"   # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file_level: "DEBUG"
  file_path: "fuelsync.log"
```

**Configuration Sections:**

### EFS API Settings
- `endpoint_url`: SOAP API endpoint (production or QA)
- `username`: API username
- `password`: API password

### Client Settings
- `request_timeout`: `[connect, read]` timeout in seconds
- `verify_ssl`: Enable SSL certificate verification (always `true` in production)
- `max_retries`: Number of retry attempts for failed requests
- `retry_backoff_factor`: Exponential backoff multiplier (sleep = factor ^ attempt)

### Pipeline Settings
- `default_start_date`: Earliest date to sync if no history exists (ISO format)
- `batch_size_days`: Days per API request (keep at 1 to avoid timeouts)
- `lookback_days`: Days to overlap for capturing late-arriving transactions
- `request_delay_seconds`: Rate limiting delay between batches

### Storage Settings
- `parquet_file`: Local path for persistent transaction data
- `compression`: Compression algorithm (snappy recommended for speed/size balance)

### Logging Settings
- `console_level`: Log level for terminal output
- `file_level`: Log level for file output
- `file_path`: Path to log file

## Quick Start

### Basic Usage
```python
from fuelsync import EfsClient
from fuelsync.models import GetMCTransExtLocV2Request
from fuelsync.response_models import GetMCTransExtLocV2Response
from datetime import datetime, timezone

# Use context manager for automatic login/logout
with EfsClient() as client:
    # Create a request for transaction data
    request = GetMCTransExtLocV2Request(
        beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
    )

    # Execute the operation
    response = client.execute_operation(request)

    # Parse the response
    parsed = GetMCTransExtLocV2Response.from_soap_response(response.text)

    # Access transaction data
    print(f"Found {parsed.transaction_count} transactions")
    print(f"Total amount: ${parsed.total_amount:,.2f}")

    for txn in parsed.transactions:
        print(f"Transaction {txn.transaction_id}: ${txn.net_total:.2f}")
```

### Transaction Summary
```python
from fuelsync import EfsClient
from fuelsync.models import TransSummaryRequest
from fuelsync.response_models import TransSummaryResponse
from datetime import datetime, timezone

with EfsClient() as client:
    request = TransSummaryRequest(
        beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
    )

    response = client.execute_operation(request)
    parsed = TransSummaryResponse.from_soap_response(response.text)

    print(f"Transaction count: {parsed.summary.tran_count}")
    print(f"Total amount: ${parsed.summary.tran_total:,.2f}")
```

### Rejected Transactions
```python
from fuelsync import EfsClient
from fuelsync.models import WSTranRejectSearch
from fuelsync.response_models import GetTranRejectsResponse
from datetime import datetime, timezone

with EfsClient() as client:
    request = WSTranRejectSearch(
        start_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
        card_num='1234567890'  # Optional filter
    )

    response = client.execute_operation(request)
    parsed = GetTranRejectsResponse.from_soap_response(response.text)

    for reject in parsed.rejects:
        print(f"Card: {reject.card_num}")
        print(f"Error: {reject.error_desc}")
        print(f"Location: {reject.loc_name}")
```

### Data Analysis with Pandas
```python
import pandas as pd
from fuelsync import EfsClient
from fuelsync.models import GetMCTransExtLocV2Request
from fuelsync.response_models import GetMCTransExtLocV2Response
from datetime import datetime, timezone

with EfsClient() as client:
    request = GetMCTransExtLocV2Request(
        beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
    )

    response = client.execute_operation(request)
    parsed = GetMCTransExtLocV2Response.from_soap_response(response.text)

    # Convert to DataFrame
    df = parsed.to_dataframe()

    # Analyze the data
    print(df.groupby('location_state')['net_total'].sum())
    print(df['transaction_date'].dt.hour.value_counts())
```

## API Documentation

### Available Operations

#### GetMCTransExtLocV2 (Recommended)
Get detailed transaction data with location information.

**Request Model:** `GetMCTransExtLocV2Request`
- `beg_date` (datetime): Start date for search
- `end_date` (datetime): End date for search

**Response Model:** `GetMCTransExtLocV2Response`
- Contains list of `WSMCTransExtLocV2` transactions

#### TransSummary
Get aggregate transaction count and total amount.

**Request Model:** `TransSummaryRequest`
- `beg_date` (datetime): Start date for summary
- `end_date` (datetime): End date for summary

**Response Model:** `TransSummaryResponse`
- Contains `WSTransSummary` with count and total

#### GetTranRejects
Search for rejected transactions.

**Request Model:** `WSTranRejectSearch`
- `start_date` (datetime): Start date for search
- `end_date` (datetime): End date for search
- `card_num` (str, optional): Filter by card number
- `invoice` (str, optional): Filter by invoice
- `location_id` (int, optional): Filter by location

**Response Model:** `GetTranRejectsResponse`
- Contains list of `WSTranReject` rejected transactions

### Datetime Formatting

FuelSync provides a utility function for formatting dates according to the EFS API specification:
```python
from fuelsync.utils import format_for_soap
from datetime import datetime, timezone, timedelta

# DateTime with timezone
dt = datetime(2025, 11, 14, 15, 30, 45, 123000,
              tzinfo=timezone(timedelta(hours=-6)))
formatted = format_for_soap(dt)
# Returns: '2025-11-14T15:30:45.123-06:00'

# Date object (converted to midnight UTC)
from datetime import date
d = date(2025, 11, 14)
formatted = format_for_soap(d)
# Returns: '2025-11-14T00:00:00.000+00:00'
```

### Logging

FuelSync uses Python's standard logging module with a hierarchical logger structure. Configure the package-level logger once, and all modules will inherit the configuration:

```python
from fuelsync.utils import setup_logger, load_config
import logging

# Simple console logging at DEBUG level
setup_logger(logging_level=logging.DEBUG)

# Or use configuration from config.yaml (recommended)
config = load_config()
setup_logger(config=config)

# Each module creates its own logger that inherits the configuration
logger = logging.getLogger(__name__)
logger.info("This will use the configured format and handlers")
```

All log messages show the originating module in the format:
```
2025-11-21 10:30:45 - INFO     - [fuelsync.pipeline] - Starting synchronization
```

## Project Structure
```
FuelSync/
├── src/
│   └── fuelsync/
│       ├── config/
│       │   └── config.yaml                    # Configuration file
│       ├── response_models/                   # Response Pydantic models
│       │   ├── __init__.py
│       │   ├── TransExtLocV2_response.py      # Detailed transaction response
│       │   ├── transSummary_response.py       # Summary response
│       │   ├── getTranRejects_response.py     # Rejected transactions
│       │   └── card_summary_response.py       # Card summary response
│       ├── templates/                         # Jinja2 SOAP templates
│       │   ├── getMCTransExtLocV2.xml
│       │   ├── transSummaryRequest.xml
│       │   ├── getTranRejects.xml
│       │   ├── logout.xml
│       │   └── ...
│       ├── utils/                             # Utility functions
│       │   ├── __init__.py
│       │   ├── config_loader.py               # YAML config loading & validation
│       │   ├── datetime_utils.py              # Date/time formatting for SOAP
│       │   ├── logger.py                      # Centralized logging setup
│       │   ├── login.py                       # EFS authentication
│       │   ├── model_tools.py                 # XML parsing helpers
│       │   └── xml_parser.py                  # SOAP XML utilities
│       ├── __init__.py                        # Package exports
│       ├── efs_client.py                      # Main SOAP API client
│       ├── models.py                          # Request Pydantic models
│       └── pipeline.py                        # Incremental data sync pipeline
├── pyproject.toml                             # Project metadata & dependencies
├── README.md                                  # This file
└── LICENSE                                    # MIT License
```

## Error Handling

FuelSync provides comprehensive error handling:
```python
from fuelsync import EfsClient
from fuelsync.models import GetMCTransExtLocV2Request
from datetime import datetime, timezone
import logging

# Enable debug logging to see detailed error information
from fuelsync.utils import setup_logger
setup_logger(logging_level=logging.DEBUG)

try:
    with EfsClient() as client:
        request = GetMCTransExtLocV2Request(
            beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
        )
        response = client.execute_operation(request)
        # Handle response...

except FileNotFoundError as e:
    print(f"Configuration file not found: {e}")
except RuntimeError as e:
    print(f"SOAP Fault or authentication error: {e}")
except requests.exceptions.Timeout:
    print(f"Request timed out")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Advanced Usage

### Manual Session Management

If you need more control over the session lifecycle:
```python
from fuelsync import EfsClient

# Create client (automatically logs in)
client = EfsClient()

try:
    # Perform multiple operations
    response1 = client.execute_operation(request1)
    response2 = client.execute_operation(request2)

finally:
    # Always logout
    client.logout()
```

### Custom Configuration Path
```python
from fuelsync import EfsClient
from pathlib import Path

# Use a custom config file
client = EfsClient(config_path=Path('/etc/fuelsync/config.yaml'))
```

## Development

### Setup Development Environment
```bash
# Clone the repository
git clone https://github.com/andrewjordan3/FuelSync.git
cd fuelsync

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Code Quality
```bash
# Format and lint with ruff
ruff check src/fuelsync
ruff format src/fuelsync

# Type checking
mypy src/fuelsync
```

### Testing

Tests are currently in development. Contributions welcome!

## Architecture

FuelSync follows a clean architecture with clear separation of concerns:

1. **Request Models** (`models.py`): Pydantic models that validate input parameters
2. **SOAP Templates** (`templates/`): Jinja2 templates for generating SOAP envelopes
3. **Client** (`efs_client.py`): Handles authentication and request execution
4. **Response Models** (`response_models/`): Parse and validate SOAP responses
5. **Utilities** (`utils/`): Reusable helpers for parsing, logging, and configuration

### Design Principles

- **Type Safety**: Pydantic models ensure data integrity at every step
- **Separation of Concerns**: Each module has a single, well-defined responsibility
- **Error Recovery**: Graceful handling of malformed data and network errors
- **Extensibility**: Easy to add new operations by following existing patterns

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure:
- Code follows the existing style (use `ruff format` for formatting)
- Run `ruff check` to ensure code quality
- New features include documentation
- Tests are appreciated but not required at this stage

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Pydantic](https://pydantic.dev/) for data validation
- SOAP handling powered by [lxml](https://lxml.de/)
- Template rendering with [Jinja2](https://jinja.palletsprojects.com/)

## Support

For issues, questions, or contributions, please open an issue on GitHub.

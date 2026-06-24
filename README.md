# target-qls-v3

`target-qls-v3` is a Singer target for the [QLS v2 API](https://api.pakketdienstqls.nl), built with the [Hotglue SDK](https://github.com/hotgluexyz/target-hotglue).

## Installation

```bash
pip install target-qls-v3
```

Or install from source:

```bash
git clone <repository-url>
cd target-qls-v3
pip install -e .
```

## Configuration

The target requires the following configuration:

| Property | Required | Description |
|---|---|---|
| `username` | ✅ | QLS v2 API username |
| `password` | ✅ | QLS v2 API password |
| `company_id` | ✅ | QLS v2 company ID, used to build the base URL |

Example `config.json`:

```json
{
  "username": "your-username",
  "password": "your-password",
  "company_id": "12345"
}
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's `.env` if the `--config=ENV` flag is provided:

```bash
target-qls-v3 --config=ENV
```

## Usage

### Running directly

```bash
tap-your-source | target-qls-v3 --config config.json
```

### Running with Meltano

```yaml
loaders:
  - name: target-qls-v3
    pip_url: target-qls-v3
    config:
      username: your-username
      password: your-password
      company_id: "12345"
```

Then run:

```bash
meltano install
meltano elt tap-your-source target-qls-v3
```

## Supported Streams

### BuyOrders

Writes purchase orders to the QLS v2 `purchase-orders` endpoint. The sink expects records with the following fields:

| Field | Required | Description |
|---|---|---|
| `id` | ✅ | Order ID (used as `customer_title`) |
| `remoteId` | ✅ | Remote ID of the order in QLS |
| `supplier_remoteId` | ✅ | Supplier identifier |
| `created_at` | ✅ | Order creation date (used as estimated arrival; weekends are automatically pushed to Monday) |
| `line_items` | ✅ | Array of order lines (see below) |

Each item in `line_items` should contain:

| Field | Required | Description |
|---|---|---|
| `remoteId` | ✅ | Remote ID of the line in QLS (empty string if new) |
| `product_remoteId` | ✅ | Fulfillment product ID |
| `quantity` | ✅ | Quantity ordered |

The sink performs an upsert: if the purchase order already exists in QLS (matched by `remoteId`), only new lines (those without a `remoteId`) are appended. If the order does not exist, it is created in full.

### UpdateInventory

Acknowledged but currently a no-op. Records are consumed without writing to the API.

## Development

### Setup

```bash
pipx install poetry
poetry install
```

### Running tests

```bash
poetry run pytest
```

### Linting

```bash
poetry run flake8 target_qls_v3
poetry run black target_qls_v3
```

## License

Apache 2.0

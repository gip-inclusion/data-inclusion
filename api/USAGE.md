# Usage

## Prerequisites

* Make sure the api application is installed (cf instructions [here](./CONTRIBUTING.md/#Setup)).

## `data-inclusion-api` cli

### `generate-token`

Generate a JWT token associated with the given email address.

```bash
# Generate a basic token for a user
data-inclusion-api generate-token user@example.com

# Generate an admin token
data-inclusion-api generate-token admin@example.com --admin

# Generate a widget token with allowed hosts
data-inclusion-api generate-token widget@example.com --allowed-host example.com

# Generate a widget token that allows all hosts
data-inclusion-api generate-token widget@example.com --allowed-host "*"
```


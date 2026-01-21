# tap-channeldock

`tap-channeldock` is a Singer tap for Channeldock that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) 

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-channeldock --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

Sample config:
```json
{
  "api_key": "my_api_key"
}
```


### Source Authentication and Authorization

## Usage

You can easily run `tap-channeldock` by itself or in a pipeline.

### Executing the Tap Directly

```bash
tap-channeldock --version
tap-channeldock --help
tap-channeldock --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

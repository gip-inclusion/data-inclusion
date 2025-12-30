# AI Agents Guidelines

## Code Style

### Python
- NEVER use try: except Exception unless it's designed as a catch-all. 99% of the time we want
  specialized exceptions. Also, only try/catch for expected failures that can happen during
  normal lifecycle.

### Comments

- **NEVER** add comments that explain "what" the code does
- Only add comments that explain "why" in complex situations where the intent is not obvious
- Code should be self-documenting through clear naming and structure

### Line Length

- Respect the line length configured in `pyproject.toml`
- Default ruff line length is 88 characters
- Use implicit string concatenation for long strings in function calls
- Use one-liners and direct values whenever possible instead of intrmediate variables or functions

### Tests

- Test function names should be descriptive enough to not need docstrings
- Do not add comments between test sections; the code should speak for itself



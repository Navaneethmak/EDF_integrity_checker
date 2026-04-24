FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Copy application code
COPY . .

# Set environment to use the venv
ENV PATH="/app/.venv/bin:$PATH"

# Run your application
CMD ["uv run run_code_parallel.py /app/input.csv"]

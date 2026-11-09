FROM python:3.12-slim

# Ownership marker checked by the MCP Registry (must match server.json "name").
LABEL io.modelcontextprotocol.server.name="io.github.risingwavelabs/risingwave-mcp"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV MCP_TRANSPORT=streamable-http
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000

EXPOSE 8000

CMD ["python", "src/main.py"]

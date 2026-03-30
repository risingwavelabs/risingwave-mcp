import os
import re
from fastmcp import FastMCP

# Load docs once at module level
_DOCS_PATH = os.path.join(os.path.dirname(__file__), "risingwave_docs.txt")
_docs_content = ""
_docs_sections = {}


def _load_docs():
    """Load and index the documentation file into sections."""
    global _docs_content, _docs_sections
    if _docs_content:
        return

    with open(_DOCS_PATH, "r", encoding="utf-8") as f:
        _docs_content = f.read()

    # Split into sections by ## headings
    parts = re.split(r"(?=^## \d+\. )", _docs_content, flags=re.MULTILINE)
    for part in parts:
        match = re.match(r"^## \d+\. (.+)", part)
        if match:
            section_name = match.group(1).strip()
            _docs_sections[section_name.lower()] = part.strip()


def register_docs_tools(mcp: FastMCP):
    """Register documentation query tools"""

    @mcp.tool
    def search_docs(query: str, max_results: int = 3) -> str:
        """
        Search RisingWave documentation for relevant information.
        Use this before executing SQL when you're unsure about syntax, connector
        parameters, data types, or RisingWave-specific features.

        Args:
            query: Search keywords (e.g., "kafka source connector", "watermark",
                   "CREATE MATERIALIZED VIEW", "iceberg sink", "data types")
            max_results: Maximum number of matching sections to return (default: 3)

        Returns:
            Relevant documentation sections matching the query
        """
        _load_docs()
        max_results = max(1, min(int(max_results), 10))
        keywords = [kw.lower() for kw in query.split() if len(kw) > 1]

        if not keywords:
            return "Error: Please provide search keywords"

        # Score each section by keyword matches
        scored = []
        for name, content in _docs_sections.items():
            content_lower = content.lower()
            # Count keyword occurrences
            score = 0
            for kw in keywords:
                score += content_lower.count(kw)
            # Bonus for section title match
            for kw in keywords:
                if kw in name:
                    score += 10
            if score > 0:
                scored.append((score, name, content))

        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored:
            return f"No documentation found matching '{query}'. Try broader keywords."

        results = []
        for score, name, content in scored[:max_results]:
            # Truncate very long sections to most relevant paragraphs
            if len(content) > 3000:
                paragraphs = content.split("\n\n")
                relevant = []
                char_count = 0
                # Always include the heading
                if paragraphs:
                    relevant.append(paragraphs[0])
                    char_count += len(paragraphs[0])
                # Then add paragraphs that match keywords
                for para in paragraphs[1:]:
                    para_lower = para.lower()
                    if any(kw in para_lower for kw in keywords):
                        relevant.append(para)
                        char_count += len(para)
                        if char_count > 3000:
                            break
                content = "\n\n".join(relevant)
                if char_count > 3000:
                    content += "\n\n... (section truncated, use get_doc_section for full content)"

            results.append(content)

        return "\n\n---\n\n".join(results)

    @mcp.tool
    def get_doc_section(section_name: str) -> str:
        """
        Get a specific documentation section by name.

        Available sections:
        - Quick Start & Connection
        - Architecture
        - Core Concepts
        - SQL Commands
        - Data Types
        - Functions
        - Source Connectors
        - Sink Connectors
        - Streaming Patterns
        - Iceberg Integration
        - Subscriptions
        - Client Libraries
        - PostgreSQL Compatibility
        - Common Pitfalls

        Args:
            section_name: Name of the section (case-insensitive, partial match supported)

        Returns:
            Full content of the matching documentation section
        """
        _load_docs()

        search = section_name.lower().strip()

        # Exact match first
        if search in _docs_sections:
            return _docs_sections[search]

        # Partial match
        for name, content in _docs_sections.items():
            if search in name or name in search:
                return content

        # Keyword match in section names
        for name, content in _docs_sections.items():
            if all(word in name for word in search.split()):
                return content

        available = "\n".join(f"  - {name}" for name in sorted(_docs_sections.keys()))
        return f"Section '{section_name}' not found. Available sections:\n{available}"

    @mcp.tool
    def list_doc_sections() -> str:
        """
        List all available documentation sections.

        Returns:
            List of section names that can be used with get_doc_section
        """
        _load_docs()
        sections = []
        for name in _docs_sections:
            content = _docs_sections[name]
            # Count approximate words
            words = len(content.split())
            sections.append(f"  - {name} (~{words} words)")
        return "Available RisingWave documentation sections:\n" + "\n".join(sections)

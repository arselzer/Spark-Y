# Running Backend Tests

This document explains how to run the backend test suite for the Yannasparkis project.

## Test Overview

The test suite includes comprehensive tests for:

1. **Hypergraph Extraction** (`test_hypergraph_extractor.py`)
   - GYO algorithm correctness
   - Join tree generation
   - Edge cases

2. **IMDB Schema Queries** (`test_imdb_query.py`)
   - Superhero query (acyclic chain/path pattern)
   - Simple chain queries
   - Disconnected forest bug verification

3. **Movie Link Queries** (`test_movie_link_query.py`)
   - Complex multi-alias queries (cn1/cn2, it1/it2, etc.)
   - Simplified movie_link patterns
   - Budget query (star pattern with 8 tables)

4. **JOB Benchmark Queries** (`test_job_queries.py`)
   - Loads all .sql files from JOB benchmark directory
   - 15 hand-crafted acyclic pattern tests
   - Comprehensive coverage of join topologies

## Running Tests

### Option 1: Using Docker (Recommended)

The easiest way to run tests is using the provided script:

```bash
# From project root
./scripts/run_tests.sh
```

This script:
- Starts the backend Docker container if not running
- Runs pytest with verbose output
- Shows detailed test results

### Option 2: Run Specific Tests in Docker

```bash
# Run all tests
docker-compose exec backend pytest tests/ -v

# Run specific test file
docker-compose exec backend pytest tests/test_job_queries.py -v

# Run specific test function
docker-compose exec backend pytest tests/test_job_queries.py::test_job_style_patterns -v

# Run with coverage
docker-compose exec backend pytest tests/ -v --cov=app --cov-report=html
```

### Option 3: Local Development (Without Docker)

If you have the backend dependencies installed locally:

```bash
cd backend

# Activate virtual environment
source venv/bin/activate  # or: . venv/bin/activate

# Install test dependencies
pip install pytest pytest-cov pyspark

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=app --cov-report=html
```

## Test Organization

### Test Files

```
backend/tests/
├── conftest.py                      # Shared fixtures (SparkSession, sample data)
├── test_hypergraph_extractor.py     # Core GYO algorithm tests (400+ lines)
├── test_imdb_query.py               # IMDB schema tests
├── test_movie_link_query.py         # Complex alias pattern tests
└── test_job_queries.py              # JOB benchmark tests (600+ lines)
```

### Key Test Functions

**test_job_queries.py:**
- `test_job_queries_acyclicity()` - Loads all JOB .sql files, expects ≥70% acyclic
- `test_specific_job_queries()` - Tests queries 1a-8a if available
- `test_job_style_patterns()` - 5 hand-crafted patterns (chain, star, tree, etc.)
- `test_additional_job_patterns()` - 10 more patterns (binary tree, Y-shape, etc.)

**test_movie_link_query.py:**
- `test_movie_link_query_should_be_acyclic()` - Complex 14-table query
- `test_simplified_movie_link()` - 6-table simplified version
- `test_budget_query_acyclic()` - 8-table star pattern

## Expected Results

All tests should pass if the GYO algorithm is working correctly:

```
✓ test_hypergraph_extractor.py::TestGYOAlgorithm - All pass
✓ test_imdb_query.py::test_imdb_superhero_query - ACYCLIC
✓ test_movie_link_query.py::test_budget_query_acyclic - ACYCLIC
✓ test_job_queries.py::test_job_style_patterns - All 5 patterns ACYCLIC
✓ test_job_queries.py::test_additional_job_patterns - All 10 patterns ACYCLIC
```

## Test Patterns Covered

The test suite covers 15 distinct acyclic query patterns:

### Chain Patterns (3)
1. Simple chain (3 tables): A→B→C
7. Path (4 nodes): Linear path through joins
10. Deep chain (5 tables): Extended linear structure

### Star Patterns (5)
2. Simple star (4 branches): Center with multiple branches
8. Symmetric star: Multiple identical branch types
11. Triple fork: Three independent branches
13. Wide star (6 branches): Many branches from center
12. Nested star: Star with sub-branches

### Tree Patterns (4)
3. Tree (depth 3): Multi-level hierarchy
6. Binary tree: Two symmetric branches
9. Y-shaped (3 branches): Three distinct branches
14. Asymmetric tree: Branches of different depths

### Complex Patterns (3)
4. Complex star: Star with nested joins on branches
5. Multi-level hierarchy: Multiple branch types with sub-joins
15. Parallel chains: Multiple 2-hop paths from center

## Debugging Failed Tests

If a test fails:

1. **Check detailed output:**
   ```bash
   pytest tests/test_job_queries.py::test_budget_query_acyclic -v -s
   ```
   The `-s` flag shows print statements with debug information

2. **Examine GYO steps:**
   Each test prints:
   - Number of nodes and edges
   - GYO reduction steps
   - Remaining edges (if cyclic)

3. **Check for common issues:**
   - Non-join attributes incorrectly included in equivalence classes
   - Join predicates not properly extracted from WHERE clause
   - GYO algorithm not looping back after node removal

## Coverage Report

After running tests with coverage:

```bash
# View HTML report
open backend/htmlcov/index.html  # macOS
xdg-open backend/htmlcov/index.html  # Linux
```

Target coverage: ≥80% for `app/hypergraph/extractor.py`

## Performance Notes

- Full test suite: ~30-60 seconds
- Individual test files: ~5-15 seconds
- SparkSession initialization: ~5 seconds (first test only)

## Continuous Integration

The tests are designed to run in CI/CD pipelines:

```yaml
# Example .github/workflows/test.yml
- name: Run backend tests
  run: |
    docker-compose up -d backend
    docker-compose exec -T backend pytest tests/ -v --cov=app
```

## Troubleshooting

**Issue**: "No module named pytest"
- **Solution**: Run `pip install -r backend/requirements.txt` or use Docker

**Issue**: "SparkSession failed to start"
- **Solution**: Ensure Java 21 is installed or use Docker

**Issue**: "JOB queries not found"
- **Solution**: Initialize git submodule: `git submodule update --init --recursive`

**Issue**: Tests pass locally but fail in Docker
- **Solution**: Rebuild Docker image: `docker-compose build backend`

## Adding New Tests

To add a new test pattern:

1. Add function to appropriate test file
2. Follow naming convention: `test_<pattern_name>`
3. Include docstring explaining the pattern
4. Print debug output for troubleshooting
5. Assert expected acyclicity/cyclicity

Example:

```python
def test_my_pattern(spark_session):
    """Test my custom query pattern"""

    query = """
    SELECT ...
    FROM ...
    WHERE ...
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "my_pattern")

    print(f"\nMy Pattern: {'ACYCLIC' if hypergraph.is_acyclic else 'CYCLIC'}")
    print(f"Nodes: {len(hypergraph.nodes)}, Edges: {len(hypergraph.edges)}")

    assert hypergraph.is_acyclic, "My pattern should be acyclic"
```

## References

- Backend Testing Guide: `backend/tests/README.md`
- CLAUDE.md Section 8: Testing & Debugging
- pytest Documentation: https://docs.pytest.org/

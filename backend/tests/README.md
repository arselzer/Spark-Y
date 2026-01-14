# Backend Tests

Comprehensive tests for the hypergraph extraction, GYO algorithm, and join tree generation.

## Test Structure

- `conftest.py` - Pytest configuration and fixtures (SparkSession, sample data)
- `test_hypergraph_extractor.py` - Main test suite

## Test Coverage

### GYO Algorithm Tests (`TestGYOAlgorithm`)

Tests for the Graham-Yu-Özsoyoglu acyclicity detection algorithm:

- **Acyclic Queries**:
  - `test_acyclic_chain_query` - Linear chain: A-B-C
  - `test_acyclic_star_query` - Star pattern: center connected to multiple leaves

- **Cyclic Queries**:
  - `test_cyclic_triangle_query` - Triangle cycle: A-B-C-A
  - `test_cyclic_complete_graph` - Complete graph K4 (highly cyclic)

- **Edge Cases**:
  - `test_empty_hypergraph` - Empty hypergraph
  - `test_single_relation` - Single table with no joins

### Join Tree Generation Tests (`TestJoinTreeGeneration`)

Tests for join tree construction from GYO reduction:

- `test_join_tree_structure_chain` - Verify tree structure for chain queries
- `test_join_tree_levels` - Verify level assignment (root at highest level)
- `test_join_tree_parent_child_relationships` - Verify bidirectional parent-child links

### End-to-End SQL Query Tests (`TestEndToEndQueries`)

Tests with actual SQL queries on sample data:

- `test_simple_acyclic_join_query` - Chain join: title → movie_companies → company_name
- `test_acyclic_star_join_query` - Star join: title as center with multiple branches
- `test_complex_acyclic_query` - Multi-branch tree query
- `test_query_without_joins` - Single table query (no joins)

### Edge Case Tests (`TestEdgeCases`)

Tests for special cases and error handling:

- `test_self_join` - Self-join on same table
- `test_disconnected_tables` - Cross product without join conditions
- `test_complex_join_conditions` - Multiple predicates in join conditions

## Running Tests

### In Docker Container

The recommended way to run tests is inside the Docker container where all dependencies are properly installed:

```bash
# Start the backend container
docker-compose up -d backend

# Run all tests
docker-compose exec backend pytest tests/ -v

# Run specific test file
docker-compose exec backend pytest tests/test_hypergraph_extractor.py -v

# Run specific test class
docker-compose exec backend pytest tests/test_hypergraph_extractor.py::TestGYOAlgorithm -v

# Run specific test
docker-compose exec backend pytest tests/test_hypergraph_extractor.py::TestGYOAlgorithm::test_acyclic_chain_query -v

# Run with coverage report
docker-compose exec backend pytest tests/ --cov=app.hypergraph --cov-report=html
```

### Local Development

If you have all dependencies installed locally:

```bash
cd backend

# Install dependencies
pip install -r requirements.txt

# Run tests
PYTHONPATH=. pytest tests/ -v

# With coverage
PYTHONPATH=. pytest tests/ --cov=app.hypergraph --cov-report=term-missing
```

## Test Data

The `conftest.py` file creates sample tables mimicking the IMDB schema:

- `company_name` - Movie production companies
- `title` - Movie titles
- `movie_companies` - Many-to-many relation between movies and companies
- `keyword` - Movie keywords/tags
- `movie_keyword` - Many-to-many relation between movies and keywords

This schema allows testing various join patterns:
- **Chain joins**: title → movie_companies → company_name
- **Star joins**: title as center with branches to multiple tables
- **Tree joins**: Multi-level hierarchical joins

## Expected Results

### Acyclic Queries

For acyclic queries, the tests verify:

1. ✅ `is_acyclic` is `True`
2. ✅ `join_tree` is not `None`
3. ✅ `join_tree` contains all relations from the query
4. ✅ Each `JoinTreeNode` has:
   - Valid `relation` name
   - Non-negative `level`
   - Consistent parent-child relationships
5. ✅ Root nodes are at the highest level
6. ✅ Parent-child relationships are bidirectional

### Cyclic Queries

For cyclic queries, the tests verify:

1. ✅ `is_acyclic` is `False`
2. ✅ `join_tree` is `None` (no tree for cyclic queries)

### Join Tree Properties

The join tree represents a valid join execution order:

- **Leaves** (lowest level): Relations that can be processed first
- **Root** (highest level): Relations processed last
- **Execution order**: Bottom-up (leaves → root)
- **Parent relations**: Removed later in GYO reduction
- **Child relations**: Removed earlier in GYO reduction

## Debugging Tests

To debug a specific test:

```bash
# Run with detailed output
docker-compose exec backend pytest tests/test_hypergraph_extractor.py::TestGYOAlgorithm::test_acyclic_chain_query -vv -s

# Run with Python debugger
docker-compose exec backend pytest tests/test_hypergraph_extractor.py --pdb

# Show local variables on failure
docker-compose exec backend pytest tests/ -l
```

## Adding New Tests

When adding new tests:

1. Add test method to appropriate test class
2. Use descriptive test names: `test_<feature>_<scenario>`
3. Add docstring explaining what is being tested
4. Use assertions with descriptive messages
5. Consider both positive and negative cases

Example:

```python
def test_my_new_feature(self, spark_session):
    """Test that my new feature works correctly"""
    extractor = HypergraphExtractor(spark_session)

    # Setup
    nodes = [...]
    edges = [...]

    # Execute
    is_acyclic, join_tree = extractor._check_acyclic_advanced(nodes, edges)

    # Assert
    assert is_acyclic is True, "Should be acyclic because..."
    assert len(join_tree) == 3, "Should have 3 nodes"
```

## CI/CD Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run backend tests
  run: |
    docker-compose up -d backend
    docker-compose exec -T backend pytest tests/ -v --cov=app.hypergraph
```

## Performance

Test execution times (approximate):

- Full test suite: ~30-60 seconds
- GYO algorithm tests: ~10 seconds
- Join tree tests: ~10 seconds
- End-to-end SQL tests: ~20 seconds
- Edge case tests: ~10 seconds

Most time is spent initializing the SparkSession (done once per session thanks to the fixture scope).

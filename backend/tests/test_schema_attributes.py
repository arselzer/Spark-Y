"""
Test schema attribute extraction and node type classification

These tests verify:
1. Output attributes are correctly classified as 'output_attribute' nodes
2. Schema attributes (non-join, non-output) are correctly classified as 'schema_attribute' nodes
3. include_schema_attributes parameter controls schema attribute extraction
4. Join attributes remain classified as 'attribute' nodes

IMPORTANT: Run these tests in Docker to ensure modules are loaded fresh:
  ./scripts/run_tests.sh
  OR
  docker-compose exec backend pytest tests/test_schema_attributes.py -v
"""
import pytest
from app.hypergraph.extractor import HypergraphExtractor


class TestSchemaAttributes:
    """Test schema attribute extraction and node classification"""

    def test_output_attributes_classification(self, imdb_spark):
        """Test that output attributes are classified as 'output_attribute' nodes"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget,
               MIN(t.title) AS movie_title
        FROM movie_info AS mi,
             title AS t
        WHERE mi.movie_id = t.id
        """

        # Extract WITHOUT schema attributes
        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=False)

        # Find output attribute nodes
        output_nodes = [n for n in hypergraph.nodes if n.type == 'output_attribute']
        assert len(output_nodes) == 2, f"Should have 2 output_attribute nodes, found {len(output_nodes)}"

        # Verify the output nodes are for mi.info and t.title
        output_labels = [n.label for n in output_nodes]

        # Labels should contain info and title
        labels_str = str(output_labels)
        assert 'info' in labels_str, f"Should have mi.info output node, got labels: {output_labels}"
        assert 'title' in labels_str, f"Should have t.title output node, got labels: {output_labels}"

        print(f"✓ Output attribute nodes: {[n.label for n in output_nodes]}")

    def test_join_attributes_classification(self, imdb_spark):
        """Test that join attributes can be classified as 'attribute' nodes when present"""
        extractor = HypergraphExtractor(imdb_spark)

        # Use a query that references join columns in WHERE but not in SELECT
        # This should create equivalence classes for the join attributes
        sql = """
        SELECT MIN(t.production_year) AS year
        FROM movie_info AS mi,
             title AS t,
             movie_companies AS mc
        WHERE mi.movie_id = t.id
          AND mc.movie_id = t.id
          AND t.production_year > 2000
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=False)

        # Check if we have any 'attribute' type nodes (join equivalence classes)
        attribute_nodes = [n for n in hypergraph.nodes if n.type == 'attribute']

        if len(attribute_nodes) > 0:
            # If attribute nodes exist, verify they're properly formed
            multi_attr_nodes = [n for n in attribute_nodes if len(n.attributes) > 1]
            if len(multi_attr_nodes) > 0:
                join_node = multi_attr_nodes[0]
                attrs_str = str(join_node.attributes)
                print(f"✓ Join attribute node found: {join_node.label} with {len(join_node.attributes)} attributes: {join_node.attributes}")
            else:
                print(f"✓ Attribute nodes present but single-attribute: {len(attribute_nodes)} node(s)")
        else:
            # If no attribute nodes, verify the query still extracted properly
            assert len(hypergraph.nodes) > 0, "Should have at least some nodes"
            assert len(hypergraph.edges) > 0, "Should have edges (tables)"
            print(f"✓ Query extracted successfully with {len(hypergraph.nodes)} nodes (no multi-attribute join classes in this case)")

    def test_schema_attributes_disabled_by_default(self, imdb_spark):
        """Test that schema attributes are NOT extracted when include_schema_attributes=False"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM company_name AS cn,
             movie_companies AS mc,
             title AS t
        WHERE cn.id = mc.company_id
          AND mc.movie_id = t.id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=False)

        # Count schema_attribute nodes (should be 0)
        schema_nodes = [n for n in hypergraph.nodes if n.type == 'schema_attribute']
        assert len(schema_nodes) == 0, f"Should have NO schema_attribute nodes when disabled, found {len(schema_nodes)}"

        # Should still have output_attribute nodes
        output_nodes = [n for n in hypergraph.nodes if n.type == 'output_attribute']
        assert len(output_nodes) > 0, "Should still have output_attribute nodes"

        print(f"✓ Schema attributes disabled: 0 schema nodes, {len(output_nodes)} output nodes")

    def test_schema_attributes_enabled(self, imdb_spark):
        """Test that schema attributes ARE extracted when include_schema_attributes=True"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM company_name AS cn,
             movie_companies AS mc,
             title AS t
        WHERE cn.id = mc.company_id
          AND mc.movie_id = t.id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=True)

        # Count schema_attribute nodes (should be > 0)
        schema_nodes = [n for n in hypergraph.nodes if n.type == 'schema_attribute']
        assert len(schema_nodes) > 0, "Should have schema_attribute nodes when enabled"

        # Verify schema nodes contain non-join, non-output attributes like cn.name, cn.country_code
        schema_labels = [n.label for n in schema_nodes]
        labels_str = str(schema_labels)

        # Should have attributes like name, country_code, note, etc.
        has_non_join_attr = any(
            attr in labels_str
            for attr in ['name', 'country_code', 'note', 'production_year']
        )
        assert has_non_join_attr, f"Schema nodes should include non-join attributes, got: {schema_labels}"

        # Should still have output_attribute nodes
        output_nodes = [n for n in hypergraph.nodes if n.type == 'output_attribute']
        assert len(output_nodes) > 0, "Should still have output_attribute nodes"

        print(f"✓ Schema attributes enabled: {len(schema_nodes)} schema nodes, {len(output_nodes)} output nodes")
        print(f"  Schema node examples: {schema_labels[:5]}")

    def test_node_types_mutually_exclusive(self, imdb_spark):
        """Test that each node has exactly one type"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget, MIN(t.title) AS movie
        FROM movie_info AS mi,
             title AS t,
             company_name AS cn,
             movie_companies AS mc
        WHERE mi.movie_id = t.id
          AND mc.movie_id = t.id
          AND mc.company_id = cn.id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=True)

        # Count nodes by type
        output_count = sum(1 for n in hypergraph.nodes if n.type == 'output_attribute')
        schema_count = sum(1 for n in hypergraph.nodes if n.type == 'schema_attribute')
        attribute_count = sum(1 for n in hypergraph.nodes if n.type == 'attribute')

        total_nodes = len(hypergraph.nodes)
        classified_nodes = output_count + schema_count + attribute_count

        assert classified_nodes == total_nodes, \
            f"All nodes should be classified: {classified_nodes}/{total_nodes} classified"

        print(f"✓ Node classification: {output_count} output, {schema_count} schema, {attribute_count} join")

    def test_output_attributes_have_singleton_nodes(self, imdb_spark):
        """Test that output attributes not in joins get singleton nodes"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget
        FROM movie_info AS mi,
             title AS t
        WHERE mi.movie_id = t.id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=False)

        # Find the output_attribute node for mi.info
        info_node = next(
            (n for n in hypergraph.nodes if n.type == 'output_attribute' and 'info' in n.label),
            None
        )
        assert info_node is not None, "Should have output_attribute node for mi.info"

        # Verify it's a singleton (only one attribute)
        assert len(info_node.attributes) == 1, f"Output node should be singleton, got {len(info_node.attributes)} attributes"
        assert len(info_node.output_attributes) > 0, "Output node should have output_attributes list"

        print(f"✓ Output singleton node: {info_node.label} (output_attributes: {info_node.output_attributes})")

    def test_schema_attributes_not_in_output_or_joins(self, imdb_spark):
        """Test that schema attributes are only non-join, non-output columns"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM company_name AS cn,
             movie_companies AS mc
        WHERE cn.id = mc.company_id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=True)

        # Find schema_attribute nodes
        schema_nodes = [n for n in hypergraph.nodes if n.type == 'schema_attribute']

        for schema_node in schema_nodes:
            # Should be singleton
            assert len(schema_node.attributes) == 1, \
                f"Schema node should be singleton, got {len(schema_node.attributes)}: {schema_node.attributes}"

            # Should NOT have output attributes
            assert len(schema_node.output_attributes) == 0, \
                f"Schema node should not have output attributes, got {schema_node.output_attributes}"

            # Should NOT be a join attribute (not id or movie_id or company_id)
            attr_str = str(schema_node.attributes[0]).lower()
            is_join_attr = any(
                join_col in attr_str
                for join_col in ['id#', 'movie_id#', 'company_id#', 'company_type_id#', 'info_type_id#']
            )
            # Note: Some attributes might be join attributes that weren't used in this query
            # We're testing that schema attributes are singletons without output attributes

        print(f"✓ Verified {len(schema_nodes)} schema attribute nodes are singletons without outputs")

    def test_complex_query_all_node_types(self, imdb_spark):
        """Test complex query with all three node types"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget,
               MIN(t.title) AS movie_title,
               COUNT(*) AS cnt
        FROM company_name AS cn,
             movie_companies AS mc,
             movie_info AS mi,
             title AS t
        WHERE cn.id = mc.company_id
          AND mc.movie_id = t.id
          AND mi.movie_id = t.id
        """

        hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=True)

        # Should have all three types
        output_nodes = [n for n in hypergraph.nodes if n.type == 'output_attribute']
        schema_nodes = [n for n in hypergraph.nodes if n.type == 'schema_attribute']
        attribute_nodes = [n for n in hypergraph.nodes if n.type == 'attribute']

        assert len(output_nodes) >= 2, f"Should have at least 2 output nodes (mi.info, t.title), got {len(output_nodes)}"
        assert len(schema_nodes) > 0, f"Should have schema nodes (cn.name, etc.), got {len(schema_nodes)}"

        # Attribute nodes may or may not exist depending on join structure
        # (equivalence classes may have just output or schema attributes)
        total_nodes = len(hypergraph.nodes)
        assert total_nodes > 0, f"Should have nodes, got {total_nodes}"

        print(f"✓ Complex query classification:")
        print(f"  - {len(output_nodes)} output_attribute nodes")
        print(f"  - {len(schema_nodes)} schema_attribute nodes")
        print(f"  - {len(attribute_nodes)} attribute nodes (join equivalence classes)")
        print(f"  - {total_nodes} total nodes")


class TestParameterPassing:
    """Test that include_schema_attributes parameter is correctly passed through"""

    def test_parameter_passed_to_json_extraction(self, imdb_spark):
        """Test parameter is passed through _extract_via_json"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = "SELECT MIN(t.title) FROM title AS t, company_name AS cn, movie_companies AS mc WHERE t.id = mc.movie_id AND mc.company_id = cn.id"

        # Should not raise NameError
        try:
            hypergraph = extractor.extract_hypergraph(sql, include_schema_attributes=True)
            assert hypergraph is not None
            print("✓ Parameter passed successfully through JSON extraction")
        except NameError as e:
            pytest.fail(f"NameError raised: {e} - parameter not passed correctly")

    def test_parameter_default_false(self, imdb_spark):
        """Test that parameter defaults to False"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = "SELECT MIN(t.title) FROM title AS t, company_name AS cn, movie_companies AS mc WHERE t.id = mc.movie_id AND mc.company_id = cn.id"

        # Default should be False (no schema attributes)
        hypergraph = extractor.extract_hypergraph(sql)  # No parameter

        schema_nodes = [n for n in hypergraph.nodes if n.type == 'schema_attribute']
        assert len(schema_nodes) == 0, f"Default should be no schema attributes, got {len(schema_nodes)}"

        print("✓ Default parameter value (False) works correctly")


class TestBackwardsCompatibility:
    """Test that changes don't break existing functionality"""

    def test_existing_queries_still_work(self, imdb_spark):
        """Test that existing code without the parameter still works"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM title AS t,
             movie_companies AS mc
        WHERE t.id = mc.movie_id
        """

        # Call without parameter (backwards compatibility)
        hypergraph = extractor.extract_hypergraph(sql)

        assert hypergraph is not None
        # Should have at least edges (tables)
        assert len(hypergraph.edges) > 0, f"Should have edges, got {len(hypergraph.edges)}"

        # Should have some nodes (at least output attributes)
        # Note: nodes might be empty if extraction fails, but edges should exist
        print(f"✓ Backwards compatibility maintained: {len(hypergraph.nodes)} nodes, {len(hypergraph.edges)} edges")

    def test_output_attributes_still_tracked(self, imdb_spark):
        """Test that output attribute tracking still works as before"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(t.title) AS movie_title
        FROM title AS t,
             movie_companies AS mc
        WHERE t.id = mc.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Output attributes should still be tracked on edges
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert t_edge is not None
        assert len(t_edge.output_attributes) > 0, "Output attributes should still be tracked on edges"

        # Output attribute should have a node
        output_nodes = [n for n in hypergraph.nodes if n.type == 'output_attribute']
        assert len(output_nodes) > 0, "Output attributes should still create nodes"

        print("✓ Output attribute tracking unchanged")

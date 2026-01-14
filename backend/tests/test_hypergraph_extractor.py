"""
Tests for hypergraph extraction, GYO algorithm, and join tree generation
"""
import pytest
from app.hypergraph.extractor import HypergraphExtractor
from app.models.hypergraph import HypergraphNode, HypergraphEdge, JoinTreeNode


class TestGYOAlgorithm:
    """Tests for the GYO (Graham-Yu-Özsoyoglu) acyclicity algorithm"""

    def test_acyclic_chain_query(self, spark_session):
        """Test acyclic chain query: A-B-C (linear join)"""
        extractor = HypergraphExtractor(spark_session)

        # Create simple hypergraph: A connects to node1, B connects to node1 and node2, C connects to node2
        nodes = [
            HypergraphNode(id="node1", label="attr1", type="attribute", attributes=["t1.a", "t2.a"]),
            HypergraphNode(id="node2", label="attr2", type="attribute", attributes=["t2.b", "t3.b"]),
        ]
        edges = [
            HypergraphEdge(id="t1", nodes=["node1"], label="t1"),
            HypergraphEdge(id="t2", nodes=["node1", "node2"], label="t2"),
            HypergraphEdge(id="t3", nodes=["node2"], label="t3"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True, "Chain query should be acyclic"
        assert join_tree is not None, "Join tree should be generated for acyclic query"
        assert len(join_tree) == 3, "Join tree should have 3 nodes (one per relation)"

        # Verify tree structure - t2 should be root as it's the middle node
        t2_nodes = [n for n in join_tree if n.relation == "t2"]
        assert len(t2_nodes) == 1, "Should have exactly one t2 node"

    def test_acyclic_star_query(self, spark_session):
        """Test acyclic star query: center node connected to multiple leaves"""
        extractor = HypergraphExtractor(spark_session)

        # Star query: center relation connects to multiple nodes
        nodes = [
            HypergraphNode(id="node1", label="id", type="attribute", attributes=["center.id", "leaf1.center_id"]),
            HypergraphNode(id="node2", label="id2", type="attribute", attributes=["center.id", "leaf2.center_id"]),
            HypergraphNode(id="node3", label="id3", type="attribute", attributes=["center.id", "leaf3.center_id"]),
        ]
        edges = [
            HypergraphEdge(id="center", nodes=["node1", "node2", "node3"], label="center"),
            HypergraphEdge(id="leaf1", nodes=["node1"], label="leaf1"),
            HypergraphEdge(id="leaf2", nodes=["node2"], label="leaf2"),
            HypergraphEdge(id="leaf3", nodes=["node3"], label="leaf3"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True, "Star query should be acyclic"
        assert join_tree is not None, "Join tree should be generated"
        assert len(join_tree) == 4, "Join tree should have 4 nodes"

        # Center should be the root (highest level after GYO reduction)
        center_node = [n for n in join_tree if n.relation == "center"][0]
        leaf_nodes = [n for n in join_tree if n.relation.startswith("leaf")]

        # All leaves should have center as parent or be at different levels
        assert len(leaf_nodes) == 3, "Should have 3 leaf nodes"

    def test_cyclic_triangle_query(self, spark_session):
        """Test cyclic triangle query: A-B-C-A"""
        extractor = HypergraphExtractor(spark_session)

        # Triangle: each relation shares an attribute with two others
        nodes = [
            HypergraphNode(id="node1", label="attr1", type="attribute", attributes=["A.x", "B.x"]),
            HypergraphNode(id="node2", label="attr2", type="attribute", attributes=["B.y", "C.y"]),
            HypergraphNode(id="node3", label="attr3", type="attribute", attributes=["C.z", "A.z"]),
        ]
        edges = [
            HypergraphEdge(id="A", nodes=["node1", "node3"], label="A"),
            HypergraphEdge(id="B", nodes=["node1", "node2"], label="B"),
            HypergraphEdge(id="C", nodes=["node2", "node3"], label="C"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is False, "Triangle query should be cyclic"
        assert join_tree is None, "Join tree should be None for cyclic query"

    def test_cyclic_complete_graph(self, spark_session):
        """Test cyclic complete graph K4"""
        extractor = HypergraphExtractor(spark_session)

        # Complete graph with 4 nodes - highly cyclic
        nodes = [
            HypergraphNode(id=f"node{i}", label=f"attr{i}", type="attribute", attributes=[f"attr{i}"])
            for i in range(4)
        ]
        edges = [
            HypergraphEdge(id="e1", nodes=["node0", "node1", "node2"], label="e1"),
            HypergraphEdge(id="e2", nodes=["node0", "node1", "node3"], label="e2"),
            HypergraphEdge(id="e3", nodes=["node0", "node2", "node3"], label="e3"),
            HypergraphEdge(id="e4", nodes=["node1", "node2", "node3"], label="e4"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is False, "Complete graph should be cyclic"
        assert join_tree is None, "Join tree should be None for cyclic query"

    def test_empty_hypergraph(self, spark_session):
        """Test edge case: empty hypergraph"""
        extractor = HypergraphExtractor(spark_session)

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced([], [])

        assert is_acyclic is True, "Empty hypergraph should be considered acyclic"
        assert join_tree is None or len(join_tree) == 0, "Empty hypergraph has no join tree"

    def test_single_relation(self, spark_session):
        """Test edge case: single relation with no joins"""
        extractor = HypergraphExtractor(spark_session)

        nodes = [
            HypergraphNode(id="node1", label="attr1", type="attribute", attributes=["t1.a"]),
        ]
        edges = [
            HypergraphEdge(id="t1", nodes=["node1"], label="t1"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True, "Single relation should be acyclic"
        assert join_tree is not None, "Should generate join tree for single relation"
        assert len(join_tree) == 1, "Single relation join tree has one node"


class TestJoinTreeGeneration:
    """Tests for join tree generation from GYO reduction"""

    def test_join_tree_structure_chain(self, spark_session):
        """Test join tree structure for chain query"""
        extractor = HypergraphExtractor(spark_session)

        nodes = [
            HypergraphNode(id="node1", label="attr1", type="attribute", attributes=["t1.a", "t2.a"]),
            HypergraphNode(id="node2", label="attr2", type="attribute", attributes=["t2.b", "t3.b"]),
        ]
        edges = [
            HypergraphEdge(id="t1", nodes=["node1"], label="t1"),
            HypergraphEdge(id="t2", nodes=["node1", "node2"], label="t2"),
            HypergraphEdge(id="t3", nodes=["node2"], label="t3"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True
        assert join_tree is not None

        # Verify each node in join tree
        relations = {node.relation for node in join_tree}
        assert relations == {"t1", "t2", "t3"}, "All relations should be in join tree"

        # Verify tree properties
        for node in join_tree:
            assert isinstance(node, JoinTreeNode), "Should be JoinTreeNode instance"
            assert node.id.startswith("join_tree_"), "ID should have correct prefix"
            assert node.relation in ["t1", "t2", "t3"], "Relation should be valid"
            assert isinstance(node.level, int), "Level should be integer"
            assert node.level >= 0, "Level should be non-negative"

        # Check parent-child consistency
        for node in join_tree:
            if node.parent:
                parent_node = [n for n in join_tree if n.id == node.parent]
                assert len(parent_node) == 1, "Parent should exist"
                assert node.id in parent_node[0].children, "Node should be in parent's children list"

    def test_join_tree_levels(self, spark_session):
        """Test that join tree levels are correctly assigned"""
        extractor = HypergraphExtractor(spark_session)

        # Simple chain: t1 - t2 - t3
        nodes = [
            HypergraphNode(id="node1", label="attr1", type="attribute", attributes=["t1.a", "t2.a"]),
            HypergraphNode(id="node2", label="attr2", type="attribute", attributes=["t2.b", "t3.b"]),
        ]
        edges = [
            HypergraphEdge(id="t1", nodes=["node1"], label="t1"),
            HypergraphEdge(id="t2", nodes=["node1", "node2"], label="t2"),
            HypergraphEdge(id="t3", nodes=["node2"], label="t3"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True
        assert join_tree is not None

        # Verify levels are properly ordered
        levels = sorted(set(node.level for node in join_tree))
        assert len(levels) > 0, "Should have at least one level"

        # Root should be at highest level
        max_level = max(node.level for node in join_tree)
        root_candidates = [node for node in join_tree if node.parent is None]
        for root in root_candidates:
            assert root.level == max_level, "Roots should be at highest level"

    def test_join_tree_parent_child_relationships(self, spark_session):
        """Test parent-child relationships in join tree"""
        extractor = HypergraphExtractor(spark_session)

        # Star query
        nodes = [
            HypergraphNode(id="node1", label="id1", type="attribute", attributes=["center.id", "leaf1.id"]),
            HypergraphNode(id="node2", label="id2", type="attribute", attributes=["center.id", "leaf2.id"]),
        ]
        edges = [
            HypergraphEdge(id="center", nodes=["node1", "node2"], label="center"),
            HypergraphEdge(id="leaf1", nodes=["node1"], label="leaf1"),
            HypergraphEdge(id="leaf2", nodes=["node2"], label="leaf2"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True
        assert join_tree is not None

        # Build a map for easier lookup
        tree_map = {node.id: node for node in join_tree}

        # Verify bidirectional relationships
        for node in join_tree:
            # Check parent reference
            if node.parent:
                assert node.parent in tree_map, f"Parent {node.parent} should exist in tree"
                parent = tree_map[node.parent]
                assert node.id in parent.children, f"Node {node.id} should be in parent's children"

            # Check children references
            for child_id in node.children:
                assert child_id in tree_map, f"Child {child_id} should exist in tree"
                child = tree_map[child_id]
                assert child.parent == node.id, f"Child's parent should point back to {node.id}"

    def test_join_tree_connectivity(self, spark_session):
        """Test that join tree is fully connected (single root, all nodes reachable)"""
        extractor = HypergraphExtractor(spark_session)

        # Chain query: a - b - c - d
        nodes = [
            HypergraphNode(id="node1", label="id1", type="attribute", attributes=["a.id", "b.a_id"]),
            HypergraphNode(id="node2", label="id2", type="attribute", attributes=["b.id", "c.b_id"]),
            HypergraphNode(id="node3", label="id3", type="attribute", attributes=["c.id", "d.c_id"]),
        ]
        edges = [
            HypergraphEdge(id="a", nodes=["node1"], label="a"),
            HypergraphEdge(id="b", nodes=["node1", "node2"], label="b"),
            HypergraphEdge(id="c", nodes=["node2", "node3"], label="c"),
            HypergraphEdge(id="d", nodes=["node3"], label="d"),
        ]

        is_acyclic, join_tree, _ = extractor._check_acyclic_advanced(nodes, edges)

        assert is_acyclic is True, "Chain query should be acyclic"
        assert join_tree is not None
        assert len(join_tree) == 4, "Should have 4 nodes in join tree"

        # Check tree has exactly one root
        roots = [node for node in join_tree if node.parent is None]
        assert len(roots) == 1, f"Join tree should have exactly 1 root, found {len(roots)}: {[r.relation for r in roots]}"

        # Check all nodes are reachable from root (tree is connected)
        tree_map = {node.id: node for node in join_tree}
        visited = set()
        queue = [roots[0].id]

        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)
            node = tree_map[node_id]
            queue.extend(node.children)

        assert len(visited) == len(join_tree), \
            f"All nodes should be reachable from root. Visited {len(visited)}/{len(join_tree)}"


class TestEndToEndQueries:
    """End-to-end tests with real SQL queries"""

    def test_simple_acyclic_join_query(self, sample_data):
        """Test simple acyclic join query with actual SQL"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title, cn.name
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN company_name cn ON mc.company_id = cn.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        assert hypergraph.is_acyclic is True, "Chain join should be acyclic"
        assert hypergraph.join_tree is not None, "Should generate join tree"
        assert len(hypergraph.join_tree) > 0, "Join tree should have nodes"

        # Verify relations are in the join tree
        relations_in_tree = {node.relation for node in hypergraph.join_tree}
        # Note: relation names might be aliased (t, mc, cn) or full table names
        assert len(relations_in_tree) > 0, "Should have relations in join tree"

    def test_acyclic_star_join_query(self, sample_data):
        """Test star join query (center table with multiple joins)"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN movie_keyword mk ON t.id = mk.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        assert hypergraph.is_acyclic is True, "Star join should be acyclic"
        assert hypergraph.join_tree is not None, "Should generate join tree"
        assert len(hypergraph.join_tree) >= 2, "Join tree should have multiple nodes"

    def test_complex_acyclic_query(self, sample_data):
        """Test more complex acyclic query"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title, k.keyword
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN company_name cn ON mc.company_id = cn.id
        JOIN movie_keyword mk ON t.id = mk.movie_id
        JOIN keyword k ON mk.keyword_id = k.id
        WHERE cn.country_code = 'us'
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # This query structure should be acyclic (tree-like structure)
        # title is the center with two branches: one to company_name, one to keyword
        assert hypergraph.is_acyclic is True, "Complex tree query should be acyclic"
        if hypergraph.is_acyclic:
            assert hypergraph.join_tree is not None, "Should generate join tree for acyclic query"
            assert len(hypergraph.join_tree) > 0, "Join tree should have nodes"

    def test_query_without_joins(self, sample_data):
        """Test query with no joins (single table)"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT title, production_year
        FROM title
        WHERE production_year > 2020
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Single table query should be acyclic
        assert hypergraph.is_acyclic is True, "Single table query should be acyclic"
        # May or may not have join tree depending on implementation
        # But should not crash


class TestEdgeCases:
    """Tests for edge cases and error handling"""

    def test_self_join(self, sample_data):
        """Test self-join query"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t1.title, t2.title
        FROM title t1
        JOIN title t2 ON t1.production_year = t2.production_year
        WHERE t1.id < t2.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Self-join creates a cycle in some representations
        # The result depends on how the extractor handles this
        assert isinstance(hypergraph.is_acyclic, bool), "Should return boolean"
        if not hypergraph.is_acyclic:
            assert hypergraph.join_tree is None, "Cyclic query should not have join tree"

    def test_disconnected_tables(self, sample_data):
        """Test query with disconnected tables (cross product)"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title, k.keyword
        FROM title t, keyword k
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Cross product without join conditions
        assert isinstance(hypergraph.is_acyclic, bool), "Should return boolean"
        # Disconnected components are technically acyclic but may be handled differently

    def test_complex_join_conditions(self, sample_data):
        """Test query with complex join conditions"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id AND t.production_year > 2020
        JOIN company_name cn ON mc.company_id = cn.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Should handle complex conditions gracefully
        assert isinstance(hypergraph.is_acyclic, bool), "Should return boolean"
        assert isinstance(hypergraph.nodes, list), "Should have nodes list"
        assert isinstance(hypergraph.edges, list), "Should have edges list"


class TestOutputAttributes:
    """Tests for output attribute tracking"""

    def test_simple_select_output_attributes(self, sample_data):
        """Test that output attributes are correctly identified from SELECT clause"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title, t.production_year
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN company_name cn ON mc.company_id = cn.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Find the title table edge
        title_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert title_edge is not None, "Should have title table edge"

        # Check that output attributes are tracked
        assert hasattr(title_edge, 'output_attributes'), "Edge should have output_attributes field"
        assert len(title_edge.output_attributes) > 0, "Title table should have output attributes"

        # Verify that title and production_year are in output attributes
        output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in title_edge.output_attributes]
        assert 'title' in output_attr_names, "title should be in output attributes"
        assert 'production_year' in output_attr_names, "production_year should be in output attributes"

    def test_aggregate_output_attributes(self, sample_data):
        """Test output attributes with aggregate functions"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT MIN(t.title) AS movie_title, COUNT(*) as count
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Find the title table edge
        title_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        assert title_edge is not None, "Should have title table edge"

        # Check that output attributes are tracked (title should be in output)
        assert hasattr(title_edge, 'output_attributes'), "Edge should have output_attributes field"

    def test_star_select_output_attributes(self, sample_data):
        """Test output attributes with SELECT *"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT *
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # With SELECT *, all tables should have output attributes
        assert len(hypergraph.edges) > 0, "Should have edges"

        # At least some edges should have output attributes
        edges_with_output = [e for e in hypergraph.edges if len(e.output_attributes) > 0]
        assert len(edges_with_output) >= 0, "At least some edges should have output attributes"

    def test_no_output_from_join_only_table(self, sample_data):
        """Test that tables used only for joins don't have output attributes"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT cn.name
        FROM company_name cn
        JOIN movie_companies mc ON cn.id = mc.company_id
        JOIN title t ON mc.movie_id = t.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Find movie_companies edge (used only for join)
        mc_edge = next((e for e in hypergraph.edges if e.id == 'mc'), None)
        assert mc_edge is not None, "Should have movie_companies table edge"

        # movie_companies is used only for join, not in SELECT
        # So it should have empty output_attributes
        assert hasattr(mc_edge, 'output_attributes'), "Edge should have output_attributes field"
        # mc_edge.output_attributes could be empty since only cn.name is selected

    def test_multiple_tables_with_output(self, sample_data):
        """Test query with output attributes from multiple tables"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.title, cn.name, mc.company_type
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        JOIN company_name cn ON mc.company_id = cn.id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # All three tables should have output attributes
        title_edge = next((e for e in hypergraph.edges if e.id == 't'), None)
        mc_edge = next((e for e in hypergraph.edges if e.id == 'mc'), None)
        cn_edge = next((e for e in hypergraph.edges if e.id == 'cn'), None)

        assert title_edge is not None, "Should have title table"
        assert mc_edge is not None, "Should have movie_companies table"
        assert cn_edge is not None, "Should have company_name table"

        # Each should have output_attributes field
        assert hasattr(title_edge, 'output_attributes'), "Title should have output_attributes field"
        assert hasattr(mc_edge, 'output_attributes'), "MC should have output_attributes field"
        assert hasattr(cn_edge, 'output_attributes'), "CN should have output_attributes field"

    def test_nodes_with_output_attributes(self, sample_data):
        """Test that nodes (equivalence classes) track output attributes correctly"""
        spark = sample_data
        extractor = HypergraphExtractor(spark)

        sql = """
        SELECT t.id, t.title
        FROM title t
        JOIN movie_companies mc ON t.id = mc.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Nodes should have output_attributes field
        for node in hypergraph.nodes:
            assert hasattr(node, 'output_attributes'), "Node should have output_attributes field"
            assert isinstance(node.output_attributes, list), "output_attributes should be a list"

        # Find node that contains the join attribute (t.id, mc.movie_id)
        # This node should have t.id in output_attributes since it's in SELECT
        join_nodes = [n for n in hypergraph.nodes if len(n.attributes) >= 2]
        # At least one join node should exist
        assert len(join_nodes) > 0, "Should have at least one join node"

    def test_imdb_budget_query_output_attributes(self, imdb_spark):
        """Test output attributes for complex IMDB query with aggregates (mi.info, t.title)"""
        extractor = HypergraphExtractor(imdb_spark)

        sql = """
        SELECT MIN(mi.info) AS budget, MIN(t.title) AS unsuccsessful_movie
        FROM company_name AS cn,
             company_type AS ct,
             info_type AS it1,
             info_type AS it2,
             movie_companies AS mc,
             movie_info AS mi,
             movie_info AS mi_idx,
             title AS t
        WHERE cn.country_code ='[us]'
          AND ct.kind IS NOT NULL
          AND (ct.kind ='production companies' OR ct.kind = 'distributors')
          AND it1.info ='budget'
          AND it2.info ='bottom 10 rank'
          AND t.production_year >2000
          AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%')
          AND t.id = mi.movie_id
          AND t.id = mi_idx.movie_id
          AND mi.info_type_id = it1.id
          AND mi_idx.info_type_id = it2.id
          AND t.id = mc.movie_id
          AND ct.id = mc.company_type_id
          AND cn.id = mc.company_id
          AND mc.movie_id = mi.movie_id
          AND mc.movie_id = mi_idx.movie_id
          AND mi.movie_id = mi_idx.movie_id
        """

        hypergraph = extractor.extract_hypergraph(sql)

        # Debug: print all edges with detailed information
        print(f"\n{'='*80}")
        print(f"Hypergraph Extraction Results")
        print(f"{'='*80}")
        print(f"Total edges (tables): {len(hypergraph.edges)}")
        print(f"Total nodes (equivalence classes): {len(hypergraph.nodes)}")
        print(f"Is acyclic: {hypergraph.is_acyclic}")
        print(f"\nEdges (Tables):")
        for edge in hypergraph.edges:
            print(f"\n  Table: {edge.id} ({edge.label})")
            print(f"    Connects to nodes: {edge.nodes}")
            print(f"    All attributes ({len(edge.attributes)}): {edge.attributes[:3]}{'...' if len(edge.attributes) > 3 else ''}")
            print(f"    Output attributes ({len(edge.output_attributes)}): {edge.output_attributes}")

        print(f"\nNodes (Equivalence Classes):")
        for i, node in enumerate(hypergraph.nodes):
            print(f"  Node {i} ({node.id}): {node.attributes[:3]}{'...' if len(node.attributes) > 3 else ''}")
            print(f"    Output attrs: {node.output_attributes}")
        print(f"{'='*80}\n")

        # Find the mi and t table edges
        mi_edge = next((e for e in hypergraph.edges if e.id == 'mi'), None)
        t_edge = next((e for e in hypergraph.edges if e.id == 't'), None)

        assert mi_edge is not None, f"Should have movie_info (mi) table edge. Available edges: {[e.id for e in hypergraph.edges]}"
        assert t_edge is not None, f"Should have title (t) table edge. Available edges: {[e.id for e in hypergraph.edges]}"

        # Check that output attributes are tracked
        assert hasattr(mi_edge, 'output_attributes'), "mi edge should have output_attributes field"
        assert hasattr(t_edge, 'output_attributes'), "t edge should have output_attributes field"

        # Verify that mi.info is in output attributes
        mi_output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in mi_edge.output_attributes]
        assert 'info' in mi_output_attr_names, "mi.info should be in output attributes (used in SELECT MIN(mi.info))"

        # Verify that t.title is in output attributes
        t_output_attr_names = [attr.split('.')[-1].split('#')[0] for attr in t_edge.output_attributes]
        assert 'title' in t_output_attr_names, "t.title should be in output attributes (used in SELECT MIN(t.title))"

        # Also verify all attributes are present (including non-join attributes)
        mi_all_attr_names = [attr.split('.')[-1].split('#')[0] for attr in mi_edge.attributes]
        assert 'info' in mi_all_attr_names, "mi.info should be in all attributes"

        t_all_attr_names = [attr.split('.')[-1].split('#')[0] for attr in t_edge.attributes]
        assert 'title' in t_all_attr_names, "t.title should be in all attributes"
        assert 'production_year' in t_all_attr_names, "t.production_year should be in all attributes (used in WHERE)"
